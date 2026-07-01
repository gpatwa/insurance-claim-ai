"""Claim persistence behind a Protocol.

InMemory backs hermetic tests; Postgres is the deployment store. Idempotency-Key maps to a
single claim so duplicate submissions never create duplicate work.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Protocol, runtime_checkable

from claimpipe.domain.models import Claim, ClaimStatus


@runtime_checkable
class ClaimRepository(Protocol):
    async def create(self, claim: Claim, idempotency_key: str | None = None) -> None: ...
    async def get(self, claim_id: str) -> Claim | None: ...
    async def set_status(self, claim_id: str, status: ClaimStatus) -> None: ...
    async def set_ocr_ref(self, claim_id: str, ocr_ref: str) -> None: ...
    async def touch(self, claim_id: str) -> None: ...
    async def find_by_idempotency_key(self, key: str) -> Claim | None: ...


def _now() -> datetime:
    return datetime.now(UTC)


class InMemoryClaimRepository:
    def __init__(self) -> None:
        self._claims: dict[str, Claim] = {}
        self._idem: dict[str, str] = {}

    async def create(self, claim: Claim, idempotency_key: str | None = None) -> None:
        claim = claim.model_copy(update={"created_at": _now(), "updated_at": _now()})
        self._claims[claim.claim_id] = claim
        if idempotency_key:
            self._idem[idempotency_key] = claim.claim_id

    async def get(self, claim_id: str) -> Claim | None:
        return self._claims.get(claim_id)

    async def set_status(self, claim_id: str, status: ClaimStatus) -> None:
        claim = self._claims[claim_id]
        self._claims[claim_id] = claim.model_copy(update={"status": status, "updated_at": _now()})

    async def set_ocr_ref(self, claim_id: str, ocr_ref: str) -> None:
        claim = self._claims[claim_id]
        self._claims[claim_id] = claim.model_copy(
            update={"ocr_ref": ocr_ref, "updated_at": _now()}
        )

    async def touch(self, claim_id: str) -> None:
        claim = self._claims[claim_id]
        self._claims[claim_id] = claim.model_copy(update={"updated_at": _now()})

    async def find_by_idempotency_key(self, key: str) -> Claim | None:
        claim_id = self._idem.get(key)
        return self._claims.get(claim_id) if claim_id else None


class PostgresClaimRepository:
    """asyncpg-backed store for deployment. Not exercised in CI (InMemory is)."""

    def __init__(self, pool: object) -> None:
        self._pool = pool  # asyncpg.Pool

    async def create(self, claim: Claim, idempotency_key: str | None = None) -> None:
        async with self._pool.acquire() as conn:  # type: ignore[attr-defined]
            await conn.execute(
                """
                INSERT INTO claims (claim_id, status, customer_id, callback_url, metadata,
                                    idempotency_key)
                VALUES ($1, $2, $3, $4, $5, $6)
                """,
                claim.claim_id,
                str(claim.status),
                claim.metadata.customer_id,
                claim.metadata.callback_url,
                json.dumps(claim.metadata.model_dump()),
                idempotency_key,
            )

    async def get(self, claim_id: str) -> Claim | None:
        async with self._pool.acquire() as conn:  # type: ignore[attr-defined]
            row = await conn.fetchrow("SELECT * FROM claims WHERE claim_id = $1", claim_id)
        return _row_to_claim(row) if row else None

    async def set_status(self, claim_id: str, status: ClaimStatus) -> None:
        async with self._pool.acquire() as conn:  # type: ignore[attr-defined]
            await conn.execute(
                "UPDATE claims SET status=$2, updated_at=now() WHERE claim_id=$1",
                claim_id,
                str(status),
            )

    async def set_ocr_ref(self, claim_id: str, ocr_ref: str) -> None:
        async with self._pool.acquire() as conn:  # type: ignore[attr-defined]
            await conn.execute(
                "UPDATE claims SET ocr_ref=$2, updated_at=now() WHERE claim_id=$1",
                claim_id,
                ocr_ref,
            )

    async def touch(self, claim_id: str) -> None:
        async with self._pool.acquire() as conn:  # type: ignore[attr-defined]
            await conn.execute("UPDATE claims SET updated_at=now() WHERE claim_id=$1", claim_id)

    async def find_by_idempotency_key(self, key: str) -> Claim | None:
        async with self._pool.acquire() as conn:  # type: ignore[attr-defined]
            row = await conn.fetchrow("SELECT * FROM claims WHERE idempotency_key = $1", key)
        return _row_to_claim(row) if row else None


def _row_to_claim(row: dict) -> Claim:
    meta = row["metadata"]
    if isinstance(meta, str):
        meta = json.loads(meta)
    return Claim(
        claim_id=row["claim_id"],
        status=ClaimStatus(row["status"]),
        metadata=meta,
        pdf_ref=row.get("pdf_ref"),
        ocr_ref=row.get("ocr_ref"),
        created_at=row.get("created_at"),
        updated_at=row.get("updated_at"),
    )
