"""M1 end-to-end: ingestion API → claim record → durable workflow.

Hermetic: Temporal in-process time-skipping env + InMemory repo + httpx ASGI transport.
"""

from __future__ import annotations

import httpx
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from claimpipe.api.app import create_app
from claimpipe.config import Settings
from claimpipe.repository import InMemoryClaimRepository
from claimpipe.temporal.activities import ClaimActivities
from claimpipe.temporal.workflows import ClaimWorkflow

_META = {
    "customer_id": "cust-1",
    "callback_url": "https://example.test/webhook",
    "attributes": {"line": "auto"},
}


def _client(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


async def test_submit_creates_claim_and_starts_workflow() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        repo = InMemoryClaimRepository()
        acts = ClaimActivities(repo)
        settings = Settings(temporal_task_queue="claimpipe-test")
        async with Worker(
            env.client,
            task_queue="claimpipe-test",
            workflows=[ClaimWorkflow],
            activities=[acts.log_metadata],
        ):
            app = create_app(repo=repo, temporal_client=env.client, settings=settings)
            async with _client(app) as ac:
                r = await ac.post("/claims", json={"metadata": _META})
                assert r.status_code == 202
                body = r.json()
                claim_id = body["claim_id"]
                assert body["status"] == "RECEIVED"
                assert claim_id in body["upload_url"]

                # workflow ran the log_metadata stage and returned the claim_id
                handle = env.client.get_workflow_handle(claim_id)
                assert await handle.result() == claim_id

                # status queryable
                r2 = await ac.get(f"/claims/{claim_id}")
                assert r2.status_code == 200
                assert r2.json()["status"] == "RECEIVED"

                # record persisted + touched by the activity
                claim = await repo.get(claim_id)
                assert claim is not None
                assert claim.updated_at is not None


async def test_idempotent_submit_returns_same_claim() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        repo = InMemoryClaimRepository()
        acts = ClaimActivities(repo)
        settings = Settings(temporal_task_queue="claimpipe-test")
        async with Worker(
            env.client,
            task_queue="claimpipe-test",
            workflows=[ClaimWorkflow],
            activities=[acts.log_metadata],
        ):
            app = create_app(repo=repo, temporal_client=env.client, settings=settings)
            async with _client(app) as ac:
                headers = {"Idempotency-Key": "abc-123"}
                r1 = await ac.post("/claims", json={"metadata": _META}, headers=headers)
                r2 = await ac.post("/claims", json={"metadata": _META}, headers=headers)
                assert r1.json()["claim_id"] == r2.json()["claim_id"]
                assert r2.json()["idempotent"] is True


async def test_unknown_claim_404() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        repo = InMemoryClaimRepository()
        settings = Settings(temporal_task_queue="claimpipe-test")
        app = create_app(repo=repo, temporal_client=env.client, settings=settings)
        async with _client(app) as ac:
            r = await ac.get("/claims/does-not-exist")
            assert r.status_code == 404
