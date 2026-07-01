"""Shared test helpers: build a Temporal worker wired with fakes."""

from __future__ import annotations

from temporalio.worker import Worker

from claimpipe.adapters.model_client import MockModelClient
from claimpipe.adapters.object_store import InMemoryObjectStore
from claimpipe.adapters.ocr import MockOCRClient
from claimpipe.eventstore import EventStore
from claimpipe.temporal.activities import ClaimActivities, ping
from claimpipe.temporal.workflows import ClaimWorkflow, PingWorkflow

TASK_QUEUE = "claimpipe-test"

META = {
    "customer_id": "cust-1",
    "callback_url": "https://example.test/webhook",
    "attributes": {"line": "auto"},
}


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
        activities=[ping, acts.record_event, acts.run_ocr, acts.run_llm],
    )
