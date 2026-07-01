"""M3: LLM tiered routing + persistence.

Unit tests cover the pure routing decision; E2E tests drive the full workflow to PERSISTED and
assert the escalation behavior and model_outputs projection.
"""

from __future__ import annotations

import httpx
from temporalio.testing import WorkflowEnvironment

from claimpipe.adapters.model_client import MockModelClient
from claimpipe.adapters.object_store import InMemoryObjectStore
from claimpipe.api.app import create_app
from claimpipe.config import Settings
from claimpipe.domain.models import ClaimStatus
from claimpipe.eventstore import InMemoryEventStore
from claimpipe.llm import route_and_predict, should_escalate
from tests.helpers import META, TASK_QUEUE, make_worker


def _client(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


# ---- pure routing logic ----------------------------------------------------------------


def _pred(conf: float):
    return MockModelClient(confidence=conf)


async def test_no_escalation_when_confident_and_low_value() -> None:
    res = await route_and_predict(
        _pred(0.95), MockModelClient(name="acc"), "text", threshold=0.85, claim_value=100
    )
    assert res.escalated is False
    assert len(res.predictions) == 1


async def test_escalates_on_low_confidence() -> None:
    res = await route_and_predict(
        _pred(0.5), MockModelClient(name="acc"), "text", threshold=0.85, claim_value=100
    )
    assert res.escalated is True
    assert len(res.predictions) == 2


async def test_high_value_always_escalates() -> None:
    # confident, but the claim is above the high-value threshold
    cost = await MockModelClient(confidence=0.99).predict("t")
    assert should_escalate(
        cost, threshold=0.85, claim_value=50000, high_value_amount=25000
    ) is True


# ---- end-to-end through the workflow ---------------------------------------------------


async def _run(env, store, obj, cost, accuracy, meta) -> str:
    settings = Settings(temporal_task_queue=TASK_QUEUE)
    async with make_worker(env, store, obj_store=obj, cost_model=cost, accuracy_model=accuracy):
        app = create_app(store=store, temporal_client=env.client, settings=settings)
        async with _client(app) as ac:
            claim_id = (await ac.post("/claims", json={"metadata": meta})).json()["claim_id"]
            await obj.put(f"{claim_id}/source.pdf", b"%PDF sample")
            await ac.post(f"/claims/{claim_id}/uploaded")
            assert await env.client.get_workflow_handle(claim_id).result() == claim_id
            return claim_id


async def test_e2e_auto_accept_single_prediction() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        store = InMemoryEventStore()
        obj = InMemoryObjectStore()
        cid = await _run(
            env,
            store,
            obj,
            MockModelClient(name="cost", confidence=0.95),
            MockModelClient(name="acc", confidence=0.99),
            META,
        )
        claim = await store.get(cid)
        assert claim.status == ClaimStatus.PERSISTED
        preds = await store.predictions(cid)
        assert len(preds) == 1  # cost tier only
        assert preds[0].model_name == "cost"
        types = [e.type for e in await store.events(cid)]
        assert types[-4:] == [
            "LLM_STARTED",
            "PREDICTIONS_READY",
            "CLAIM_ADJUDICATED",
            "CLAIM_PERSISTED",
        ]


async def test_e2e_low_confidence_escalates() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        store = InMemoryEventStore()
        obj = InMemoryObjectStore()
        cid = await _run(
            env,
            store,
            obj,
            MockModelClient(name="cost", confidence=0.40),
            MockModelClient(name="acc", confidence=0.99),
            META,
        )
        preds = await store.predictions(cid)
        assert len(preds) == 2  # escalated to accuracy tier
        assert {p.model_name for p in preds} == {"cost", "acc"}
