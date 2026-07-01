"""Event store: append-only log + inline projection + transactional outbox.

`append` does three things in one transaction:
  1. append the immutable event to claim_events,
  2. apply the projection to the `claims` read model (validated transition),
  3. write an outbox row for async fan-out to the message bus (Kafka).

The claims projection is updated synchronously so status queries are strongly consistent;
the outbox carries the same event to Kafka for decoupled consumers.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Protocol, runtime_checkable
from uuid import uuid4

from claimpipe.domain.events import DomainEvent, EventType
from claimpipe.domain.models import Claim, ModelPrediction
from claimpipe.projection import project


@runtime_checkable
class EventStore(Protocol):
    async def append(
        self,
        claim_id: str,
        type: EventType,
        payload: dict,
        *,
        idempotency_key: str | None = None,
    ) -> DomainEvent: ...

    async def get(self, claim_id: str) -> Claim | None: ...
    async def find_by_idempotency_key(self, key: str) -> Claim | None: ...
    async def events(self, claim_id: str) -> list[DomainEvent]: ...
    async def predictions(self, claim_id: str) -> list[ModelPrediction]: ...

    # outbox (read by the relay)
    async def fetch_unpublished(self, limit: int = 100) -> list[DomainEvent]: ...
    async def mark_published(self, event_ids: list[str]) -> None: ...


def _now() -> datetime:
    return datetime.now(UTC)


class InMemoryEventStore:
    """Hermetic test/dev implementation of the append-only store + projection + outbox."""

    def __init__(self) -> None:
        self._claims: dict[str, Claim] = {}
        self._events: dict[str, list[DomainEvent]] = {}
        self._seq: dict[str, int] = {}
        self._idem: dict[str, str] = {}
        self._outbox: list[dict] = []
        self._predictions: dict[str, list[dict]] = {}

    async def append(
        self,
        claim_id: str,
        type: EventType,
        payload: dict,
        *,
        idempotency_key: str | None = None,
    ) -> DomainEvent:
        seq = self._seq.get(claim_id, 0) + 1
        event = DomainEvent(
            event_id=uuid4().hex,
            claim_id=claim_id,
            seq=seq,
            type=type,
            payload=payload,
            occurred_at=_now(),
        )
        # projection first — raises IllegalTransition before anything is persisted
        new_claim = project(self._claims.get(claim_id), event)
        self._seq[claim_id] = seq
        self._claims[claim_id] = new_claim
        self._events.setdefault(claim_id, []).append(event)
        if idempotency_key:
            self._idem[idempotency_key] = claim_id
        if type == EventType.PREDICTIONS_READY:
            self._predictions[claim_id] = payload["predictions"]
        self._outbox.append({"event": event, "published": False})
        return event

    async def get(self, claim_id: str) -> Claim | None:
        return self._claims.get(claim_id)

    async def predictions(self, claim_id: str) -> list[ModelPrediction]:
        return [ModelPrediction(**p) for p in self._predictions.get(claim_id, [])]

    async def find_by_idempotency_key(self, key: str) -> Claim | None:
        claim_id = self._idem.get(key)
        return self._claims.get(claim_id) if claim_id else None

    async def events(self, claim_id: str) -> list[DomainEvent]:
        return list(self._events.get(claim_id, []))

    async def fetch_unpublished(self, limit: int = 100) -> list[DomainEvent]:
        return [o["event"] for o in self._outbox if not o["published"]][:limit]

    async def mark_published(self, event_ids: list[str]) -> None:
        ids = set(event_ids)
        for o in self._outbox:
            if o["event"].event_id in ids:
                o["published"] = True


class PostgresEventStore:
    """asyncpg-backed store. Not exercised in CI (InMemory is)."""

    def __init__(self, pool: object) -> None:
        self._pool = pool  # asyncpg.Pool

    async def append(
        self,
        claim_id: str,
        type: EventType,
        payload: dict,
        *,
        idempotency_key: str | None = None,
    ) -> DomainEvent:
        async with self._pool.acquire() as conn:  # type: ignore[attr-defined]
            async with conn.transaction():
                seq = await conn.fetchval(
                    "SELECT COALESCE(MAX(seq), 0) + 1 FROM claim_events WHERE claim_id=$1",
                    claim_id,
                )
                event = DomainEvent(
                    event_id=uuid4().hex,
                    claim_id=claim_id,
                    seq=seq,
                    type=type,
                    payload=payload,
                    occurred_at=_now(),
                )
                current = await self._get(conn, claim_id)
                new_claim = project(current, event)  # validate before writing

                await conn.execute(
                    """INSERT INTO claim_events (event_id, claim_id, seq, type, payload,
                                                 occurred_at)
                       VALUES ($1,$2,$3,$4,$5,$6)""",
                    event.event_id,
                    claim_id,
                    seq,
                    str(type),
                    json.dumps(payload),
                    event.occurred_at,
                )
                await conn.execute(
                    """INSERT INTO claims (claim_id, status, customer_id, callback_url,
                                           metadata, ocr_ref, idempotency_key, created_at,
                                           updated_at)
                       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                       ON CONFLICT (claim_id) DO UPDATE
                       SET status=EXCLUDED.status, ocr_ref=EXCLUDED.ocr_ref,
                           updated_at=EXCLUDED.updated_at""",
                    claim_id,
                    str(new_claim.status),
                    new_claim.metadata.customer_id,
                    new_claim.metadata.callback_url,
                    json.dumps(new_claim.metadata.model_dump()),
                    new_claim.ocr_ref,
                    idempotency_key,
                    new_claim.created_at,
                    new_claim.updated_at,
                )
                await conn.execute(
                    """INSERT INTO outbox (event_id, topic, claim_id, payload, published)
                       VALUES ($1,$2,$3,$4,false)""",
                    event.event_id,
                    "claim-events",
                    claim_id,
                    event.model_dump_json(),
                )
                if type == EventType.PREDICTIONS_READY:
                    for p in payload["predictions"]:
                        await conn.execute(
                            """INSERT INTO model_outputs (claim_id, model_name, model_version,
                                                          output, confidence, latency_ms,
                                                          tokens_cost)
                               VALUES ($1,$2,$3,$4,$5,$6,$7)
                               ON CONFLICT (claim_id, model_name, model_version) DO UPDATE
                               SET output=EXCLUDED.output, confidence=EXCLUDED.confidence""",
                            claim_id,
                            p["model_name"],
                            p["model_version"],
                            json.dumps(p["output"]),
                            p["confidence"],
                            p.get("latency_ms", 0),
                            p.get("tokens_cost", 0),
                        )
                return event

    async def _get(self, conn, claim_id: str) -> Claim | None:
        row = await conn.fetchrow("SELECT * FROM claims WHERE claim_id=$1", claim_id)
        if row is None:
            return None
        meta = row["metadata"]
        if isinstance(meta, str):
            meta = json.loads(meta)
        return Claim(
            claim_id=row["claim_id"],
            status=row["status"],
            metadata=meta,
            ocr_ref=row["ocr_ref"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    async def get(self, claim_id: str) -> Claim | None:
        async with self._pool.acquire() as conn:  # type: ignore[attr-defined]
            return await self._get(conn, claim_id)

    async def find_by_idempotency_key(self, key: str) -> Claim | None:
        async with self._pool.acquire() as conn:  # type: ignore[attr-defined]
            row = await conn.fetchrow(
                "SELECT claim_id FROM claims WHERE idempotency_key=$1", key
            )
            return await self._get(conn, row["claim_id"]) if row else None

    async def events(self, claim_id: str) -> list[DomainEvent]:
        async with self._pool.acquire() as conn:  # type: ignore[attr-defined]
            rows = await conn.fetch(
                "SELECT * FROM claim_events WHERE claim_id=$1 ORDER BY seq", claim_id
            )
        return [
            DomainEvent(
                event_id=r["event_id"],
                claim_id=r["claim_id"],
                seq=r["seq"],
                type=r["type"],
                payload=json.loads(r["payload"]) if isinstance(r["payload"], str) else r["payload"],
                occurred_at=r["occurred_at"],
            )
            for r in rows
        ]

    async def predictions(self, claim_id: str) -> list[ModelPrediction]:
        async with self._pool.acquire() as conn:  # type: ignore[attr-defined]
            rows = await conn.fetch(
                "SELECT * FROM model_outputs WHERE claim_id=$1 ORDER BY model_name", claim_id
            )
        return [
            ModelPrediction(
                model_name=r["model_name"],
                model_version=r["model_version"],
                output=json.loads(r["output"]) if isinstance(r["output"], str) else r["output"],
                confidence=r["confidence"],
                latency_ms=r["latency_ms"],
                tokens_cost=r["tokens_cost"],
            )
            for r in rows
        ]

    async def fetch_unpublished(self, limit: int = 100) -> list[DomainEvent]:
        async with self._pool.acquire() as conn:  # type: ignore[attr-defined]
            rows = await conn.fetch(
                "SELECT payload FROM outbox WHERE NOT published ORDER BY id LIMIT $1", limit
            )
        return [DomainEvent.model_validate_json(r["payload"]) for r in rows]

    async def mark_published(self, event_ids: list[str]) -> None:
        async with self._pool.acquire() as conn:  # type: ignore[attr-defined]
            await conn.execute(
                "UPDATE outbox SET published=true WHERE event_id = ANY($1::text[])", event_ids
            )
