"""M2 end-to-end: submit → upload signal (dormancy gate) → OCR → text persisted + events.

Time is only skipped while awaiting the workflow result, so we send the signal *before*
awaiting completion; the 7-day gate timer never fires prematurely.
"""

from __future__ import annotations

import httpx
from temporalio.testing import WorkflowEnvironment

from claimpipe.adapters.object_store import InMemoryObjectStore
from claimpipe.api.app import create_app
from claimpipe.config import Settings
from claimpipe.domain.models import ClaimStatus
from claimpipe.eventstore import InMemoryEventStore
from tests.helpers import META, TASK_QUEUE, make_worker


def _client(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


class _FlakyOCR:
    """Fails `fail_times` then succeeds — exercises the OCR retry policy."""

    def __init__(self, fail_times: int) -> None:
        self.fail_times = fail_times
        self.calls = 0

    async def extract_text(self, pdf: bytes) -> str:
        self.calls += 1
        if self.calls <= self.fail_times:
            raise RuntimeError("ocr temporarily unavailable")
        return "OCR_TEXT recovered claim body"


async def test_ocr_happy_path() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        store = InMemoryEventStore()
        obj = InMemoryObjectStore()
        settings = Settings(temporal_task_queue=TASK_QUEUE)
        async with make_worker(env, store, obj_store=obj):
            app = create_app(store=store, temporal_client=env.client, settings=settings)
            async with _client(app) as ac:
                claim_id = (await ac.post("/claims", json={"metadata": META})).json()["claim_id"]
                await obj.put(f"{claim_id}/source.pdf", b"%PDF-1.7 sample")
                r = await ac.post(f"/claims/{claim_id}/uploaded")
                assert r.status_code == 202

                handle = env.client.get_workflow_handle(claim_id)
                assert await handle.result() == claim_id

                claim = await store.get(claim_id)
                assert claim is not None
                assert claim.status == ClaimStatus.OCR_DONE
                assert claim.ocr_ref is not None
                assert await obj.exists(f"{claim_id}/ocr.txt")

                # event log records the full lifecycle so far
                types = [e.type for e in await store.events(claim_id)]
                assert types == [
                    "CLAIM_RECEIVED",
                    "METADATA_LOGGED",
                    "OCR_STARTED",
                    "OCR_COMPLETED",
                ]


async def test_ocr_retries_then_succeeds() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        store = InMemoryEventStore()
        obj = InMemoryObjectStore()
        flaky = _FlakyOCR(fail_times=2)
        settings = Settings(temporal_task_queue=TASK_QUEUE)
        async with make_worker(env, store, obj_store=obj, ocr=flaky):
            app = create_app(store=store, temporal_client=env.client, settings=settings)
            async with _client(app) as ac:
                claim_id = (await ac.post("/claims", json={"metadata": META})).json()["claim_id"]
                await obj.put(f"{claim_id}/source.pdf", b"%PDF-1.7 sample")
                await ac.post(f"/claims/{claim_id}/uploaded")

                handle = env.client.get_workflow_handle(claim_id)
                assert await handle.result() == claim_id

                claim = await store.get(claim_id)
                assert claim is not None
                assert claim.status == ClaimStatus.OCR_DONE
                assert flaky.calls == 3  # 2 failures + 1 success
