"""M5: LangGraph escalation agent (extract → validate → recommend → critic).

Unit tests drive the graph directly; the E2E test proves the agent runs on the escalated
tier inside the Temporal activity and its recommendation is persisted.
"""

from __future__ import annotations

import httpx
from temporalio.testing import WorkflowEnvironment

from claimpipe.adapters.model_client import MockModelClient
from claimpipe.adapters.object_store import InMemoryObjectStore
from claimpipe.agent import ClaimReviewAgent
from claimpipe.api.app import create_app
from claimpipe.config import Settings
from claimpipe.eventstore import InMemoryEventStore
from tests.helpers import META, TASK_QUEUE, make_worker, wait_for_pend


def _client(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


async def test_agent_approves_when_confident() -> None:
    agent = ClaimReviewAgent(
        extractor=MockModelClient(confidence=0.9), critic=MockModelClient(confidence=0.9)
    )
    out = await agent.review("some claim text")
    assert out["validation"]["passed"] is True  # mock output has a category
    assert out["recommendation"] == "approve"
    assert out["critique"]["flag_for_human"] is False


async def test_agent_flags_low_confidence_for_human() -> None:
    agent = ClaimReviewAgent(
        extractor=MockModelClient(confidence=0.5), critic=MockModelClient(confidence=0.5)
    )
    out = await agent.review("ambiguous claim text")
    assert out["recommendation"] == "manual_review"
    assert out["critique"]["flag_for_human"] is True


async def test_e2e_escalation_runs_agent() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        store = InMemoryEventStore()
        obj = InMemoryObjectStore()
        agent = ClaimReviewAgent(
            extractor=MockModelClient(confidence=0.9), critic=MockModelClient(confidence=0.9)
        )
        settings = Settings(temporal_task_queue=TASK_QUEUE)
        async with make_worker(
            env,
            store,
            obj_store=obj,
            cost_model=MockModelClient(name="cost", confidence=0.40),  # forces escalation
            agent=agent,
        ):
            app = create_app(store=store, temporal_client=env.client, settings=settings)
            async with _client(app) as ac:
                cid = (await ac.post("/claims", json={"metadata": META})).json()["claim_id"]
                await obj.put(f"{cid}/source.pdf", b"%PDF sample")
                await ac.post(f"/claims/{cid}/uploaded")
                # escalated claims park at the REVIEW gate; observe PEND without completing
                await wait_for_pend(store, cid)

                preds = await store.predictions(cid)
                assert len(preds) == 2
                review = next(p for p in preds if p.model_name == "langgraph-review")
                assert review.output["recommendation"] in {"approve", "manual_review"}
                assert "validation" in review.output
