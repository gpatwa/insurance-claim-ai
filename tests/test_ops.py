"""M6: observability + DLQ replay for failed notifications."""

from __future__ import annotations

import structlog

from claimpipe.domain.models import ClaimStatus
from claimpipe.eventstore import InMemoryEventStore
from claimpipe.notifier import NotificationService, replay_failed_notifications
from claimpipe.observability import claim_context, configure_logging
from tests.helpers import FakeWebhook, drive_to_persisted

SECRET = "test-secret"


def test_claim_context_binds_and_unbinds() -> None:
    configure_logging()
    with claim_context("claim-42"):
        assert structlog.contextvars.get_contextvars().get("claim_id") == "claim-42"
    assert "claim_id" not in structlog.contextvars.get_contextvars()


async def test_dlq_replay_recovers_failed_notification() -> None:
    store = InMemoryEventStore()
    event = await drive_to_persisted(store, "c1", "https://cust.test/hook")

    # first delivery fails permanently -> NOTIFY_FAILED
    dead = FakeWebhook(fail_times=99)
    svc_dead = NotificationService(store, dead, secret=SECRET, max_attempts=2, backoff_seconds=0)
    await svc_dead.handle_event(event)
    assert (await store.get("c1")).status == ClaimStatus.NOTIFY_FAILED

    # replay with a working endpoint recovers it
    good = FakeWebhook()
    svc_ok = NotificationService(store, good, secret=SECRET, max_attempts=2, backoff_seconds=0)
    recovered = await replay_failed_notifications(store, svc_ok)
    assert recovered == 1
    assert (await store.get("c1")).status == ClaimStatus.NOTIFIED
    assert store  # projection updated via NOTIFY_FAILED -> NOTIFIED transition
