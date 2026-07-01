"""M2.5: event-sourced foundation — append/projection, transition validation, outbox relay."""

from __future__ import annotations

import pytest

from claimpipe.adapters.bus import InMemoryBus
from claimpipe.domain.events import EventType
from claimpipe.domain.models import ClaimStatus
from claimpipe.eventstore import InMemoryEventStore
from claimpipe.projection import IllegalTransition
from claimpipe.relay import OutboxRelay
from tests.helpers import META


async def test_projection_folds_events() -> None:
    store = InMemoryEventStore()
    cid = "c1"
    await store.append(cid, EventType.CLAIM_RECEIVED, {"metadata": META})
    assert (await store.get(cid)).status == ClaimStatus.RECEIVED

    await store.append(cid, EventType.METADATA_LOGGED, {})
    await store.append(cid, EventType.OCR_STARTED, {})
    assert (await store.get(cid)).status == ClaimStatus.OCR_RUNNING

    await store.append(cid, EventType.OCR_COMPLETED, {"ocr_ref": "mem://claims/c1/ocr.txt"})
    claim = await store.get(cid)
    assert claim.status == ClaimStatus.OCR_DONE
    assert claim.ocr_ref == "mem://claims/c1/ocr.txt"

    seqs = [e.seq for e in await store.events(cid)]
    assert seqs == [1, 2, 3, 4]


async def test_illegal_transition_raises() -> None:
    store = InMemoryEventStore()
    cid = "c2"
    await store.append(cid, EventType.CLAIM_RECEIVED, {"metadata": META})
    # RECEIVED -> OCR_DONE is not a legal transition
    with pytest.raises(IllegalTransition):
        await store.append(cid, EventType.OCR_COMPLETED, {"ocr_ref": "x"})


async def test_duplicate_claim_received_raises() -> None:
    store = InMemoryEventStore()
    cid = "c3"
    await store.append(cid, EventType.CLAIM_RECEIVED, {"metadata": META})
    with pytest.raises(IllegalTransition):
        await store.append(cid, EventType.CLAIM_RECEIVED, {"metadata": META})


async def test_outbox_relay_publishes_to_bus() -> None:
    store = InMemoryEventStore()
    bus = InMemoryBus()
    relay = OutboxRelay(store, bus, topic="claim-events")

    await store.append("c4", EventType.CLAIM_RECEIVED, {"metadata": META})
    await store.append("c4", EventType.METADATA_LOGGED, {})

    n = await relay.run_once()
    assert n == 2
    assert len(bus.published) == 2
    topic, key, value = bus.published[0]
    assert topic == "claim-events"
    assert key == "c4"
    assert b"CLAIM_RECEIVED" in value

    # idempotent drain: nothing left to publish
    assert await relay.run_once() == 0
    assert await store.fetch_unpublished() == []
