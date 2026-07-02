"""M1 API-level behavior: submit (202), idempotency, 404.

Workflow completion is covered in test_ocr.py; here we assert the synchronous API contract.
"""

from __future__ import annotations

import httpx
from temporalio.testing import WorkflowEnvironment

from claimpipe.api.app import create_app
from claimpipe.config import Settings
from claimpipe.eventstore import InMemoryEventStore
from tests.helpers import META, TASK_QUEUE, make_worker


def _client(app) -> httpx.AsyncClient:
    # dev-integration key (submit + review) — see claimpipe.customers.DEV_KEYS
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        headers={"X-API-Key": "ck_dev_all_01"},
    )


async def test_submit_returns_202() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        store = InMemoryEventStore()
        settings = Settings(temporal_task_queue=TASK_QUEUE)
        async with make_worker(env, store):
            app = create_app(store=store, temporal_client=env.client, settings=settings)
            async with _client(app) as ac:
                r = await ac.post("/claims", json={"metadata": META})
                assert r.status_code == 202
                body = r.json()
                assert body["status"] == "RECEIVED"
                assert body["claim_id"] in body["upload_url"]
                assert body["idempotent"] is False
                # CLAIM_RECEIVED event recorded
                assert (await store.events(body["claim_id"]))[0].type == "CLAIM_RECEIVED"


async def test_idempotent_submit_returns_same_claim() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        store = InMemoryEventStore()
        settings = Settings(temporal_task_queue=TASK_QUEUE)
        async with make_worker(env, store):
            app = create_app(store=store, temporal_client=env.client, settings=settings)
            async with _client(app) as ac:
                headers = {"Idempotency-Key": "abc-123"}
                r1 = await ac.post("/claims", json={"metadata": META}, headers=headers)
                r2 = await ac.post("/claims", json={"metadata": META}, headers=headers)
                assert r1.json()["claim_id"] == r2.json()["claim_id"]
                assert r2.json()["idempotent"] is True


async def test_unknown_claim_404() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        store = InMemoryEventStore()
        settings = Settings(temporal_task_queue=TASK_QUEUE)
        app = create_app(store=store, temporal_client=env.client, settings=settings)
        async with _client(app) as ac:
            r = await ac.get("/claims/does-not-exist")
            assert r.status_code == 404
