"""Notification service — a Kafka consumer of claim-events.

On CLAIM_PERSISTED it builds a signed payload and delivers it to the customer's callback URL
with retries, then emits CLAIM_NOTIFIED (or NOTIFY_FAILED on exhaustion). Decoupled from the
workflow: the bus is the seam, so notification scales independently and other consumers
(analytics, FWA) read the same stream.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
from typing import Protocol, runtime_checkable

import structlog

from claimpipe.domain.events import DomainEvent, EventType
from claimpipe.domain.models import NotificationPayload
from claimpipe.eventstore import EventStore

log = structlog.get_logger()

SIGNATURE_HEADER = "X-Claimpipe-Signature"


def sign(secret: str, body: bytes) -> str:
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def verify(secret: str, body: bytes, signature: str) -> bool:
    return hmac.compare_digest(sign(secret, body), signature)


@runtime_checkable
class WebhookClient(Protocol):
    async def post(self, url: str, body: bytes, signature: str) -> None:
        """Deliver the payload; raise on non-2xx / transport error."""
        ...


class HttpxWebhookClient:
    def __init__(self, timeout: float = 10.0) -> None:
        self._timeout = timeout

    async def post(self, url: str, body: bytes, signature: str) -> None:
        import httpx

        async with httpx.AsyncClient(timeout=self._timeout) as client:
            resp = await client.post(
                url,
                content=body,
                headers={"Content-Type": "application/json", SIGNATURE_HEADER: signature},
            )
            resp.raise_for_status()


class NotificationService:
    def __init__(
        self,
        store: EventStore,
        webhook: WebhookClient,
        *,
        secret: str,
        max_attempts: int = 5,
        backoff_seconds: float = 0.5,
    ) -> None:
        self._store = store
        self._webhook = webhook
        self._secret = secret
        self._max_attempts = max_attempts
        self._backoff = backoff_seconds

    async def handle_event(self, event: DomainEvent) -> None:
        if event.type != EventType.CLAIM_PERSISTED:
            return
        await self._notify(event.claim_id, idempotency_id=event.event_id)

    async def retry(self, claim_id: str) -> bool:
        """DLQ replay: re-attempt delivery for a previously failed claim."""
        return await self._notify(claim_id, idempotency_id=f"retry-{claim_id}")

    async def _notify(self, claim_id: str, *, idempotency_id: str) -> bool:
        claim = await self._store.get(claim_id)
        if claim is None:
            return False

        preds = await self._store.predictions(claim_id)
        payload = NotificationPayload(
            claim_id=claim.claim_id,
            status=claim.status,
            predictions=preds,
            idempotency_id=idempotency_id,
        )
        body = payload.model_dump_json().encode()
        signature = sign(self._secret, body)

        delivered = False
        for attempt in range(1, self._max_attempts + 1):
            try:
                await self._webhook.post(claim.metadata.callback_url, body, signature)
                delivered = True
                break
            except Exception as exc:  # noqa: BLE001 - deliberately broad: any failure retries
                log.warning(
                    "notify.attempt_failed", claim_id=claim_id, attempt=attempt, error=str(exc)
                )
                if attempt < self._max_attempts and self._backoff > 0:
                    await asyncio.sleep(self._backoff * attempt)

        event_type = EventType.CLAIM_NOTIFIED if delivered else EventType.NOTIFY_FAILED
        reason = {} if delivered else {"reason": "delivery_exhausted"}
        await self._store.append(claim_id, event_type, {"idempotency_id": idempotency_id, **reason})
        return delivered


async def replay_failed_notifications(store: EventStore, service: NotificationService) -> int:
    """Find NOTIFY_FAILED claims and re-attempt delivery. Returns the number recovered."""
    failed = await store.list_by_status("NOTIFY_FAILED")
    recovered = 0
    for claim_id in failed:
        if await service.retry(claim_id):
            recovered += 1
    return recovered


async def main() -> None:
    import asyncpg
    from aiokafka import AIOKafkaConsumer

    from claimpipe.config import get_settings
    from claimpipe.eventstore import PostgresEventStore

    settings = get_settings()
    pool = await asyncpg.create_pool(settings.postgres_dsn)
    service = NotificationService(
        PostgresEventStore(pool),
        HttpxWebhookClient(),
        secret=settings.webhook_hmac_secret,
        max_attempts=settings.webhook_max_attempts,
        backoff_seconds=settings.webhook_backoff_seconds,
    )
    consumer = AIOKafkaConsumer(
        settings.event_topic,
        bootstrap_servers=settings.kafka_bootstrap,
        group_id="notifier",
        enable_auto_commit=True,
    )
    await consumer.start()
    log.info("notifier.starting", topic=settings.event_topic)
    try:
        async for msg in consumer:
            event = DomainEvent.model_validate_json(msg.value)
            await service.handle_event(event)
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
