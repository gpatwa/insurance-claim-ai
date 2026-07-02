"""M6: chaos/resilience — a flaky LLM (429/5xx) is retried by the activity and recovers."""

from __future__ import annotations

import httpx
from temporalio.testing import WorkflowEnvironment

from claimpipe.adapters.object_store import InMemoryObjectStore
from claimpipe.api.app import create_app
from claimpipe.config import Settings
from claimpipe.domain.models import ClaimStatus, ModelPrediction
from claimpipe.eventstore import InMemoryEventStore
from tests.helpers import META, TASK_QUEUE, make_worker


def _client(app) -> httpx.AsyncClient:
    # dev-integration key (submit + review) — see claimpipe.customers.DEV_KEYS
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        headers={"X-API-Key": "ck_dev_all_01"},
    )


class _FlakyModel:
    name = "cost"
    version = "0.0.1"

    def __init__(self, fail_times: int) -> None:
        self.fail_times = fail_times
        self.calls = 0

    async def predict(self, ocr_text: str) -> ModelPrediction:
        self.calls += 1
        if self.calls <= self.fail_times:
            raise RuntimeError("529 overloaded")
        return ModelPrediction(
            model_name=self.name,
            model_version=self.version,
            output={"category": "auto"},
            confidence=0.95,
        )


async def test_flaky_llm_recovers_via_activity_retry() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        store = InMemoryEventStore()
        obj = InMemoryObjectStore()
        flaky = _FlakyModel(fail_times=2)
        settings = Settings(temporal_task_queue=TASK_QUEUE)
        async with make_worker(env, store, obj_store=obj, cost_model=flaky):
            app = create_app(store=store, temporal_client=env.client, settings=settings)
            async with _client(app) as ac:
                cid = (await ac.post("/claims", json={"metadata": META})).json()["claim_id"]
                await obj.put(f"{cid}/source.pdf", b"%PDF sample")
                await ac.post(f"/claims/{cid}/uploaded")
                assert await env.client.get_workflow_handle(cid).result() == cid

                assert (await store.get(cid)).status == ClaimStatus.PERSISTED
                assert flaky.calls == 3  # 2 failures + 1 success (activity retried)
