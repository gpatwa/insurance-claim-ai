"""M4: webhook notification (Kafka consumer of CLAIM_PERSISTED).

Hermetic: drive a claim to PERSISTED directly in the event store, then hand the
CLAIM_PERSISTED event to the notification service with a fake webhook client.
"""

from __future__ import annotations

from claimpipe.domain.events import EventType
from claimpipe.domain.models import ClaimStatus, ModelPrediction
from claimpipe.eventstore import InMemoryEventStore
from claimpipe.notifier import NotificationService, sign, verify
from tests.helpers import META

SECRET = "test-secret"


class _FakeWebhook:
    def __init__(self, fail_times: int = 0) -> None:
        self.fail_times = fail_times
        self.calls: list[tuple[str, bytes, str]] = []

    async def post(self, url: str, body: bytes, signature: str) -> None:
        self.calls.append((url, body, signature))
        if len(self.calls) <= self.fail_times:
            raise RuntimeError("503 from customer endpoint")


async def _drive_to_persisted(store: InMemoryEventStore, cid: str, callback: str):
    meta = {**META, "callback_url": callback}
    await store.append(cid, EventType.CLAIM_RECEIVED, {"metadata": meta})
    await store.append(cid, EventType.METADATA_LOGGED, {})
    await store.append(cid, EventType.OCR_STARTED, {})
    await store.append(cid, EventType.OCR_COMPLETED, {"ocr_ref": "r"})
    await store.append(cid, EventType.LLM_STARTED, {})
    pred = ModelPrediction(
        model_name="m", model_version="1", output={"category": "auto"}, confidence=0.9
    ).model_dump()
    await store.append(
        cid, EventType.PREDICTIONS_READY, {"predictions": [pred], "escalated": False}
    )
    return await store.append(cid, EventType.CLAIM_PERSISTED, {})


async def test_notify_success_signed_and_marks_notified() -> None:
    store = InMemoryEventStore()
    webhook = _FakeWebhook()
    svc = NotificationService(store, webhook, secret=SECRET, max_attempts=3, backoff_seconds=0)

    event = await _drive_to_persisted(store, "c1", "https://cust.test/hook")
    await svc.handle_event(event)

    assert len(webhook.calls) == 1
    url, body, signature = webhook.calls[0]
    assert url == "https://cust.test/hook"
    assert verify(SECRET, body, signature)  # HMAC valid
    assert b'"claim_id":"c1"' in body

    claim = await store.get("c1")
    assert claim.status == ClaimStatus.NOTIFIED


async def test_notify_dead_endpoint_marks_notify_failed() -> None:
    store = InMemoryEventStore()
    webhook = _FakeWebhook(fail_times=99)  # always fails
    svc = NotificationService(store, webhook, secret=SECRET, max_attempts=3, backoff_seconds=0)

    event = await _drive_to_persisted(store, "c2", "https://dead.test/hook")
    await svc.handle_event(event)

    assert len(webhook.calls) == 3  # exhausted max_attempts
    claim = await store.get("c2")
    assert claim.status == ClaimStatus.NOTIFY_FAILED


async def test_notify_ignores_non_persisted_events() -> None:
    store = InMemoryEventStore()
    webhook = _FakeWebhook()
    svc = NotificationService(store, webhook, secret=SECRET, max_attempts=3, backoff_seconds=0)

    await store.append("c3", EventType.CLAIM_RECEIVED, {"metadata": META})
    events = await store.events("c3")
    await svc.handle_event(events[0])  # CLAIM_RECEIVED — not actionable

    assert webhook.calls == []


def test_signature_roundtrip() -> None:
    body = b'{"claim_id":"x"}'
    assert verify(SECRET, body, sign(SECRET, body))
    assert not verify(SECRET, body, "deadbeef")
