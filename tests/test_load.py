"""M6: burst/load smoke — many claims through the pipeline concurrently.

Not the full 1K/min load test (that needs real infra), but proves the workflow handles a
concurrent burst with no errors and every claim reaches PERSISTED.
"""

from __future__ import annotations

import asyncio

import httpx
from temporalio.testing import WorkflowEnvironment

from claimpipe.adapters.object_store import InMemoryObjectStore
from claimpipe.api.app import create_app
from claimpipe.config import Settings
from claimpipe.domain.models import ClaimStatus
from claimpipe.eventstore import InMemoryEventStore
from tests.helpers import META, TASK_QUEUE, make_worker

BURST = 25


def _client(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


async def test_burst_all_reach_persisted() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        store = InMemoryEventStore()
        obj = InMemoryObjectStore()
        settings = Settings(temporal_task_queue=TASK_QUEUE)
        async with make_worker(env, store, obj_store=obj):
            app = create_app(store=store, temporal_client=env.client, settings=settings)
            async with _client(app) as ac:
                claim_ids: list[str] = []
                for _ in range(BURST):
                    cid = (await ac.post("/claims", json={"metadata": META})).json()["claim_id"]
                    await obj.put(f"{cid}/source.pdf", b"%PDF sample")
                    await ac.post(f"/claims/{cid}/uploaded")
                    claim_ids.append(cid)

                results = await asyncio.gather(
                    *[env.client.get_workflow_handle(c).result() for c in claim_ids]
                )
                assert len(results) == BURST
                for cid in claim_ids:
                    assert (await store.get(cid)).status == ClaimStatus.PERSISTED
