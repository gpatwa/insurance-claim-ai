"""Shared test helpers: build a Temporal worker wired with fakes."""

from __future__ import annotations

from temporalio.worker import Worker

from claimpipe.adapters.object_store import InMemoryObjectStore
from claimpipe.adapters.ocr import MockOCRClient
from claimpipe.temporal.activities import ClaimActivities, ping
from claimpipe.temporal.workflows import ClaimWorkflow, PingWorkflow

TASK_QUEUE = "claimpipe-test"

META = {
    "customer_id": "cust-1",
    "callback_url": "https://example.test/webhook",
    "attributes": {"line": "auto"},
}


def make_worker(env, repo, store: InMemoryObjectStore | None = None, ocr=None) -> Worker:
    acts = ClaimActivities(
        repo,
        object_store=store or InMemoryObjectStore(),
        ocr=ocr or MockOCRClient(),
    )
    return Worker(
        env.client,
        task_queue=TASK_QUEUE,
        workflows=[ClaimWorkflow, PingWorkflow],
        activities=[ping, acts.log_metadata, acts.set_status, acts.run_ocr],
    )
