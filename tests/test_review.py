"""M11: human-in-the-loop review — work queue + reviewer verdict + REVIEW dormancy gate.

PEND decisions park at a durable gate until a reviewer decides (or the window lapses, in
which case the claim persists still-PEND rather than deciding on the human's behalf).
"""

from __future__ import annotations

from datetime import timedelta

import httpx
import pytest
from temporalio.testing import WorkflowEnvironment

from claimpipe.adapters.model_client import MockModelClient
from claimpipe.adapters.object_store import InMemoryObjectStore
from claimpipe.api.app import create_app
from claimpipe.claimtypes import ClaimTypeDef, InvalidPipeline, Stage
from claimpipe.config import Settings
from claimpipe.domain.models import ClaimStatus
from claimpipe.eventstore import InMemoryEventStore
from tests.helpers import META, TASK_QUEUE, make_worker, wait_for_pend


def _client(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


def test_review_requires_adjudicate() -> None:
    with pytest.raises(InvalidPipeline):
        ClaimTypeDef(name="bad", stages=[Stage.REVIEW, Stage.PERSIST]).validate_pipeline()


async def test_e2e_pend_claim_reviewed_and_approved() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        store = InMemoryEventStore()
        obj = InMemoryObjectStore()
        settings = Settings(temporal_task_queue=TASK_QUEUE)
        # low confidence → escalates → rules PEND for review
        async with make_worker(
            env, store, obj_store=obj, cost_model=MockModelClient(confidence=0.40)
        ):
            app = create_app(store=store, temporal_client=env.client, settings=settings)
            async with _client(app) as ac:
                claim_id = (await ac.post("/claims", json={"metadata": META})).json()[
                    "claim_id"
                ]
                await obj.put(f"{claim_id}/source.pdf", b"%PDF sample")
                await ac.post(f"/claims/{claim_id}/uploaded")

                # claim lands in the review work queue
                await wait_for_pend(store, claim_id)
                queue = (await ac.get("/review-queue")).json()["pending"]
                assert [c["claim_id"] for c in queue] == [claim_id]
                assert queue[0]["reason_codes"] == ["REVIEW_REQUIRED"]

                # reviewer decides — BEFORE awaiting the result (so the gate is
                # released by the signal, not the timeout)
                r = await ac.post(
                    f"/claims/{claim_id}/review",
                    json={
                        "decision": "APPROVE",
                        "reason_code": "VERIFIED_OK",
                        "reviewer": "alice@example.test",
                    },
                )
                assert r.status_code == 202

                handle = env.client.get_workflow_handle(claim_id)
                assert await handle.result() == claim_id

                claim = await store.get(claim_id)
                assert claim.status == ClaimStatus.PERSISTED
                assert claim.decision == "APPROVE"
                assert claim.reason_codes == ["VERIFIED_OK"]

                # audit: reviewer recorded on the REVIEW_COMPLETED event
                reviews = [
                    e for e in await store.events(claim_id) if e.type == "REVIEW_COMPLETED"
                ]
                assert len(reviews) == 1
                assert reviews[0].payload["reviewer"] == "alice@example.test"

                # queue is empty again
                assert (await ac.get("/review-queue")).json()["pending"] == []


async def test_review_non_pending_claim_409() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        store = InMemoryEventStore()
        obj = InMemoryObjectStore()
        settings = Settings(temporal_task_queue=TASK_QUEUE)
        # confident → auto-approves; review makes no sense
        async with make_worker(
            env, store, obj_store=obj, cost_model=MockModelClient(confidence=0.95)
        ):
            app = create_app(store=store, temporal_client=env.client, settings=settings)
            async with _client(app) as ac:
                claim_id = (await ac.post("/claims", json={"metadata": META})).json()[
                    "claim_id"
                ]
                await obj.put(f"{claim_id}/source.pdf", b"%PDF sample")
                await ac.post(f"/claims/{claim_id}/uploaded")
                handle = env.client.get_workflow_handle(claim_id)
                await handle.result()

                r = await ac.post(
                    f"/claims/{claim_id}/review",
                    json={"decision": "DENY", "reviewer": "bob"},
                )
                assert r.status_code == 409


async def test_unreviewed_claim_persists_still_pend() -> None:
    """If the review window lapses, the system never decides on the human's behalf."""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        store = InMemoryEventStore()
        obj = InMemoryObjectStore()
        settings = Settings(temporal_task_queue=TASK_QUEUE)
        async with make_worker(
            env, store, obj_store=obj, cost_model=MockModelClient(confidence=0.40)
        ):
            app = create_app(store=store, temporal_client=env.client, settings=settings)
            async with _client(app) as ac:
                claim_id = (await ac.post("/claims", json={"metadata": META})).json()[
                    "claim_id"
                ]
                await obj.put(f"{claim_id}/source.pdf", b"%PDF sample")
                await ac.post(f"/claims/{claim_id}/uploaded")

                # claim parks at the review gate…
                await wait_for_pend(store, claim_id)
                # …no reviewer acts: manually skip past the 30-day review window
                await env.sleep(timedelta(days=31))
                handle = env.client.get_workflow_handle(claim_id)
                assert await handle.result() == claim_id

                claim = await store.get(claim_id)
                assert claim.status == ClaimStatus.PERSISTED
                assert claim.decision == "PEND"  # still the human's call
                types = [e.type for e in await store.events(claim_id)]
                assert "REVIEW_COMPLETED" not in types
