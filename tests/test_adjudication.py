"""M10: adjudication core — decision tables decide APPROVE/DENY/PEND with reason codes.

Unit tests cover the pure rule engine (first-match-wins, condition ops, safe PEND
fallthrough); E2E tests prove the ADJUDICATE stage decides deterministically over facts the
LLM prepared, with the full audit tuple on the CLAIM_ADJUDICATED event.
"""

from __future__ import annotations

import httpx
from temporalio.testing import WorkflowEnvironment

from claimpipe.adapters.model_client import MockModelClient
from claimpipe.adapters.object_store import InMemoryObjectStore
from claimpipe.adjudication import (
    NO_RULE_MATCHED,
    Condition,
    Decision,
    Rule,
    RuleSet,
    adjudicate,
    default_rulesets,
)
from claimpipe.api.app import create_app
from claimpipe.config import Settings
from claimpipe.domain.models import ClaimStatus
from claimpipe.eventstore import InMemoryEventStore
from tests.helpers import META, TASK_QUEUE, make_worker, wait_for_pend


def _client(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


# ---- pure rule engine -------------------------------------------------------------------


def test_first_match_wins() -> None:
    rs = RuleSet(
        name="t",
        rules=[
            Rule(
                rule_id="deny-big",
                conditions=[Condition(field="amount", op="gt", value=1000)],
                outcome=Decision.DENY,
                reason_code="TOO_BIG",
            ),
            Rule(rule_id="approve-all", outcome=Decision.APPROVE, reason_code="OK"),
        ],
    )
    big = adjudicate(rs, {"amount": 5000})
    assert big.decision == Decision.DENY and big.rule_id == "deny-big"
    small = adjudicate(rs, {"amount": 10})
    assert small.decision == Decision.APPROVE and small.rule_id == "approve-all"


def test_missing_field_fails_condition_but_exists_ops_work() -> None:
    rs = RuleSet(
        name="t",
        rules=[
            Rule(
                rule_id="needs-conf",
                conditions=[Condition(field="confidence", op="lt", value=0.5)],
                outcome=Decision.PEND,
                reason_code="LOW",
            ),
            Rule(
                rule_id="no-conf",
                conditions=[Condition(field="confidence", op="not_exists")],
                outcome=Decision.PEND,
                reason_code="NO_MODEL_RAN",
            ),
        ],
    )
    # missing confidence: lt fails, not_exists matches
    res = adjudicate(rs, {})
    assert res.reason_codes == ["NO_MODEL_RAN"]


def test_unmatched_facts_pend_never_approve() -> None:
    rs = RuleSet(
        name="t",
        rules=[
            Rule(
                rule_id="only-rule",
                conditions=[Condition(field="never", op="eq", value=1)],
                outcome=Decision.APPROVE,
                reason_code="X",
            )
        ],
    )
    res = adjudicate(rs, {"amount": 1})
    assert res.decision == Decision.PEND
    assert res.reason_codes == [NO_RULE_MATCHED]
    assert res.rule_id is None


def test_default_rulesets_shape() -> None:
    sets = default_rulesets()
    assert set(sets) == {"generic-document", "auto-fnol"}
    # escalated claims pend for review under both
    for rs in sets.values():
        res = adjudicate(rs, {"escalated": True, "confidence": 0.99})
        assert res.decision == Decision.PEND
        assert res.reason_codes == ["REVIEW_REQUIRED"]


# ---- end-to-end through the workflow ----------------------------------------------------


async def _submit_and_finish(env, store, obj, cost_model, meta) -> str:
    settings = Settings(temporal_task_queue=TASK_QUEUE)
    async with make_worker(env, store, obj_store=obj, cost_model=cost_model):
        app = create_app(store=store, temporal_client=env.client, settings=settings)
        async with _client(app) as ac:
            claim_id = (await ac.post("/claims", json={"metadata": meta})).json()["claim_id"]
            await obj.put(f"{claim_id}/source.pdf", b"%PDF sample")
            await ac.post(f"/claims/{claim_id}/uploaded")
            assert await env.client.get_workflow_handle(claim_id).result() == claim_id

            # decision surfaces on the status endpoint too
            body = (await ac.get(f"/claims/{claim_id}")).json()
            assert body["decision"] is not None
            return claim_id


async def test_e2e_confident_claim_auto_approves() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        store = InMemoryEventStore()
        cid = await _submit_and_finish(
            env,
            store,
            InMemoryObjectStore(),
            MockModelClient(name="cost", confidence=0.95),
            META,
        )
        claim = await store.get(cid)
        assert claim.status == ClaimStatus.PERSISTED
        assert claim.decision == "APPROVE"
        assert claim.reason_codes == ["AUTO_APPROVED"]

        # audit trail: decision event carries rule set version + facts + matched rule
        adj = [e for e in await store.events(cid) if e.type == "CLAIM_ADJUDICATED"]
        assert len(adj) == 1
        payload = adj[0].payload
        assert payload["rule_id"] == "auto-approve"
        assert payload["rule_set_version"] == "v1"
        assert payload["facts"]["escalated"] is False


async def test_e2e_escalated_claim_pends_for_review() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        store = InMemoryEventStore()
        obj = InMemoryObjectStore()
        settings = Settings(temporal_task_queue=TASK_QUEUE)
        async with make_worker(
            env,
            store,
            obj_store=obj,
            # below threshold → escalates → rules PEND (claim parks at the REVIEW gate)
            cost_model=MockModelClient(name="cost", confidence=0.40),
        ):
            app = create_app(store=store, temporal_client=env.client, settings=settings)
            async with _client(app) as ac:
                cid = (await ac.post("/claims", json={"metadata": META})).json()["claim_id"]
                await obj.put(f"{cid}/source.pdf", b"%PDF sample")
                await ac.post(f"/claims/{cid}/uploaded")
                await wait_for_pend(store, cid)

        claim = await store.get(cid)
        assert claim.decision == "PEND"
        assert claim.reason_codes == ["REVIEW_REQUIRED"]


async def test_e2e_fnol_over_limit_denies() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        store = InMemoryEventStore()
        meta = {
            **META,
            "claim_type": "auto-fnol",
            "attributes": {
                "policy_number": "P-77",
                "incident_date": "2026-06-01",
                "amount": 150_000,
            },
        }
        cid = await _submit_and_finish(
            env, store, InMemoryObjectStore(), MockModelClient(confidence=0.95), meta
        )
        claim = await store.get(cid)
        # over-limit DENY outranks the escalation PEND (first match wins)
        assert claim.decision == "DENY"
        assert claim.reason_codes == ["EXCEEDS_LIMIT"]
        assert claim.status == ClaimStatus.PERSISTED
