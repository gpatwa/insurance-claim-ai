"""M13: reference data + multi-tenant configuration.

Reference data grounds adjudication facts in what the carrier KNOWS (policy status), not
just what the claimant SAYS. Tenancy makes types/schemas/pipelines/rules per-tenant
configuration on one deployment.
"""

from __future__ import annotations

import json

import httpx
from temporalio.testing import WorkflowEnvironment

from claimpipe.adapters.model_client import MockModelClient
from claimpipe.adapters.object_store import InMemoryObjectStore
from claimpipe.adjudication import Decision, Rule, RuleSet, adjudicate, default_rulesets
from claimpipe.api.app import create_app
from claimpipe.claimtypes import ClaimTypeDef, ClaimTypeRegistry, Stage
from claimpipe.config import Settings
from claimpipe.domain.models import ClaimStatus
from claimpipe.eventstore import InMemoryEventStore
from claimpipe.refdata import InMemoryRefData, PolicyRecord, enrich_facts
from claimpipe.tenancy import TenantConfig, TenantDirectory
from tests.helpers import META, TASK_QUEUE, make_worker

_FNOL = {
    "policyNo": "POL-DEAD",
    "lossDate": "2026-06-15",
    "estimatedAmount": 900.0,
    "reporter": {"id": "cust-7", "callbackUrl": "https://carrier.test/hook"},
}


def _client(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


# ---- reference data ---------------------------------------------------------------------


async def test_enrich_facts_adds_policy_fields() -> None:
    ref = InMemoryRefData()
    ref.add(PolicyRecord(policy_number="P-1", status="active", limit=50_000))

    facts = await enrich_facts(ref, {"policy_number": "P-1", "amount": 100})
    assert facts["policy_found"] is True
    assert facts["policy_status"] == "active"
    assert facts["policy_limit"] == 50_000

    unknown = await enrich_facts(ref, {"policy_number": "NOPE"})
    assert unknown["policy_found"] is False and "policy_status" not in unknown

    # no source wired / no policy in facts → untouched
    assert await enrich_facts(None, {"policy_number": "P-1"}) == {"policy_number": "P-1"}
    assert await enrich_facts(ref, {"amount": 1}) == {"amount": 1}


def test_policy_rules_deny_inactive_and_pend_unknown() -> None:
    rs = default_rulesets()["auto-fnol"]
    dead = adjudicate(rs, {"policy_status": "lapsed", "policy_found": True, "amount": 10})
    assert dead.decision == Decision.DENY
    assert dead.reason_codes == ["POLICY_INACTIVE"]

    unknown = adjudicate(rs, {"policy_found": False, "amount": 10})
    assert unknown.decision == Decision.PEND
    assert unknown.reason_codes == ["POLICY_NOT_FOUND"]

    # without refdata facts, policy rules are inert (backwards compatible)
    ok = adjudicate(rs, {"amount": 10, "confidence": 0.9, "escalated": False})
    assert ok.decision == Decision.APPROVE


async def test_e2e_inactive_policy_denied_via_refdata() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        store = InMemoryEventStore()
        obj = InMemoryObjectStore()
        ref = InMemoryRefData()
        ref.add(PolicyRecord(policy_number="POL-DEAD", status="lapsed"))
        settings = Settings(temporal_task_queue=TASK_QUEUE)
        async with make_worker(
            env, store, obj_store=obj, cost_model=MockModelClient(confidence=0.95), refdata=ref
        ):
            app = create_app(store=store, temporal_client=env.client, settings=settings)
            async with _client(app) as ac:
                r = await ac.post("/intake/fnol", content=json.dumps(_FNOL).encode())
                claim_id = r.json()["claim_id"]
                await obj.put(f"{claim_id}/source.pdf", b"%PDF fnol")
                await ac.post(f"/claims/{claim_id}/uploaded")
                assert await env.client.get_workflow_handle(claim_id).result() == claim_id

                claim = await store.get(claim_id)
                # the carrier's own records outrank the claimant's submission
                assert claim.decision == "DENY"
                assert claim.reason_codes == ["POLICY_INACTIVE"]
                # audit: enriched facts recorded on the decision event
                adj = [e for e in await store.events(claim_id) if e.type == "CLAIM_ADJUDICATED"]
                assert adj[0].payload["facts"]["policy_status"] == "lapsed"


# ---- multi-tenancy ----------------------------------------------------------------------


def _acme_directory() -> TenantDirectory:
    """Tenant 'acme' has its own claim type + rule set, unknown to other tenants."""
    reg = ClaimTypeRegistry()
    reg.register(
        ClaimTypeDef(name="acme-quick", stages=[Stage.ADJUDICATE, Stage.PERSIST])
    )
    rules = {
        "acme-quick": RuleSet(
            name="acme-quick",
            rules=[Rule(rule_id="acme-ok", outcome=Decision.APPROVE, reason_code="ACME_OK")],
        )
    }
    return TenantDirectory({"acme": TenantConfig("acme", registry=reg, rulesets=rules)})


async def test_tenant_scoped_claim_types_and_submission() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        store = InMemoryEventStore()
        tenants = _acme_directory()
        settings = Settings(temporal_task_queue=TASK_QUEUE)
        async with make_worker(env, store, tenants=tenants):
            app = create_app(
                store=store, temporal_client=env.client, settings=settings, tenants=tenants
            )
            async with _client(app) as ac:
                # tenant catalogs differ
                acme_types = (
                    await ac.get("/claim-types", headers={"X-Tenant-ID": "acme"})
                ).json()
                assert [t["name"] for t in acme_types["claim_types"]] == ["acme-quick"]
                default_types = (await ac.get("/claim-types")).json()
                assert "generic-document" in [
                    t["name"] for t in default_types["claim_types"]
                ]

                # acme's type is invalid for the default tenant…
                meta = {**META, "claim_type": "acme-quick"}
                assert (await ac.post("/claims", json={"metadata": meta})).status_code == 422

                # …and unknown tenants are rejected outright
                r = await ac.post(
                    "/claims", json={"metadata": meta}, headers={"X-Tenant-ID": "ghost"}
                )
                assert r.status_code == 404

                # for acme it submits, runs acme's rule set, and completes
                r = await ac.post(
                    "/claims", json={"metadata": meta}, headers={"X-Tenant-ID": "acme"}
                )
                assert r.status_code == 202
                claim_id = r.json()["claim_id"]
                assert r.json()["stages"] == ["ADJUDICATE", "PERSIST"]

                assert await env.client.get_workflow_handle(claim_id).result() == claim_id
                claim = await store.get(claim_id)
                assert claim.metadata.tenant_id == "acme"
                assert claim.status == ClaimStatus.PERSISTED
                assert claim.decision == "APPROVE"
                assert claim.reason_codes == ["ACME_OK"]


async def test_review_queue_is_tenant_scoped() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        store = InMemoryEventStore()
        obj = InMemoryObjectStore()
        settings = Settings(temporal_task_queue=TASK_QUEUE)
        # default tenant, low confidence → PENDs into the default tenant's queue
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

                from tests.helpers import wait_for_pend

                await wait_for_pend(store, claim_id)

                mine = (await ac.get("/review-queue")).json()
                assert [c["claim_id"] for c in mine["pending"]] == [claim_id]
                # another tenant sees an empty queue (unknown tenant → 404)
                assert (
                    await ac.get("/review-queue", headers={"X-Tenant-ID": "ghost"})
                ).status_code == 404