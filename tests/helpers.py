"""Shared test helpers: build a Temporal worker wired with fakes."""

from __future__ import annotations

from temporalio.worker import Worker

from claimpipe.adapters.model_client import MockModelClient
from claimpipe.adapters.object_store import InMemoryObjectStore
from claimpipe.adapters.ocr import MockOCRClient
from claimpipe.domain.events import EventType
from claimpipe.domain.models import ModelPrediction
from claimpipe.eventstore import EventStore
from claimpipe.temporal.activities import ClaimActivities, ping
from claimpipe.temporal.workflows import ClaimWorkflow, PingWorkflow

TASK_QUEUE = "claimpipe-test"

META = {
    "customer_id": "cust-1",
    "callback_url": "https://example.test/webhook",
    "attributes": {"line": "auto"},
}


async def wait_for_pend(store: EventStore, claim_id: str) -> None:
    """Poll (real time — no timer skipping) until adjudication PENDs the claim.

    Escalated-claim tests must not await the workflow result to observe PEND: the pipeline
    parks at the REVIEW dormancy gate, and awaiting the result would skip the 30-day review
    window instead of leaving the claim pending.
    """
    import asyncio

    from claimpipe.domain.models import ClaimStatus

    for _ in range(200):
        claim = await store.get(claim_id)
        if claim and claim.decision == "PEND" and claim.status == ClaimStatus.ADJUDICATED:
            return
        await asyncio.sleep(0.05)
    raise AssertionError("claim never reached PEND/ADJUDICATED")


class FakeWebhook:
    """Records deliveries; fails the first `fail_times` calls."""

    def __init__(self, fail_times: int = 0) -> None:
        self.fail_times = fail_times
        self.calls: list[tuple[str, bytes, str]] = []

    async def post(self, url: str, body: bytes, signature: str) -> None:
        self.calls.append((url, body, signature))
        if len(self.calls) <= self.fail_times:
            raise RuntimeError("customer endpoint error")


async def drive_to_persisted(store: EventStore, cid: str, callback: str):
    """Append the event sequence that brings a claim to PERSISTED (bypasses the workflow)."""
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


def make_worker(
    env,
    store: EventStore,
    obj_store: InMemoryObjectStore | None = None,
    ocr=None,
    cost_model=None,
    accuracy_model=None,
    agent=None,
) -> Worker:
    acts = ClaimActivities(
        store,
        object_store=obj_store or InMemoryObjectStore(),
        ocr=ocr or MockOCRClient(),
        cost_model=cost_model or MockModelClient(name="mock-cost", confidence=0.95),
        accuracy_model=accuracy_model
        or MockModelClient(name="mock-accuracy", version="acc", confidence=0.99),
        agent=agent,
    )
    return Worker(
        env.client,
        task_queue=TASK_QUEUE,
        workflows=[ClaimWorkflow, PingWorkflow],
        activities=[ping, acts.record_event, acts.run_ocr, acts.run_llm, acts.run_adjudication],
    )
