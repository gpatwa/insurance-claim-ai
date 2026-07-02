"""M12: intake & output adapters — external formats in, outbound documents out.

Intake adapters normalize wire formats (FNOL JSON, X12-837-style) into canonical claims;
output adapters render decided claims as documents (EOB JSON, denial letter). The pipeline
between them is format-agnostic.
"""

from __future__ import annotations

import json

import httpx
import pytest
from temporalio.testing import WorkflowEnvironment

from claimpipe.adapters.intake import (
    FnolIntakeAdapter,
    IntakeError,
    X12LikeIntakeAdapter,
)
from claimpipe.adapters.model_client import MockModelClient
from claimpipe.adapters.object_store import InMemoryObjectStore
from claimpipe.api.app import create_app
from claimpipe.config import Settings
from claimpipe.domain.models import ClaimStatus
from claimpipe.eventstore import InMemoryEventStore
from tests.helpers import TASK_QUEUE, make_worker


def _client(app) -> httpx.AsyncClient:
    # dev-integration key (submit + review) — see claimpipe.customers.DEV_KEYS
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        headers={"X-API-Key": "ck_dev_all_01"},
    )


_FNOL = {
    "policyNo": "POL-9",
    "lossDate": "2026-06-15",
    "estimatedAmount": 1200.0,
    "reporter": {"id": "cust-7", "callbackUrl": "https://carrier.test/hook"},
    "vehicle": "sedan",
}

_X12 = (
    "ISA*00*~GS*HC*~ST*837*0001~"
    "NM1*IL*1*DOE*JANE****MI*SUB-123~"
    "CLM*A37*500~"
    "DTP*472*D8*20260610~"
    "SE*5*0001~"
)


# ---- intake adapter units ---------------------------------------------------------------


def test_fnol_normalizes_to_auto_fnol() -> None:
    meta = FnolIntakeAdapter().normalize(json.dumps(_FNOL).encode())
    assert meta.claim_type == "auto-fnol"
    assert meta.customer_id == "cust-7"
    assert meta.attributes["policy_number"] == "POL-9"
    assert meta.attributes["amount"] == 1200.0
    assert meta.attributes["carrier_extras"] == {"vehicle": "sedan"}


def test_fnol_missing_fields_rejected() -> None:
    with pytest.raises(IntakeError, match="policyNo"):
        FnolIntakeAdapter().normalize(b'{"reporter": {}}')
    with pytest.raises(IntakeError, match="invalid JSON"):
        FnolIntakeAdapter().normalize(b"not-json")


def test_x12_like_parses_segments() -> None:
    meta = X12LikeIntakeAdapter().normalize(_X12.encode())
    assert meta.claim_type == "structured-claim"
    assert meta.customer_id == "SUB-123"
    assert meta.attributes == {"amount": 500.0, "service_date": "20260610"}


def test_x12_like_missing_segments_rejected() -> None:
    with pytest.raises(IntakeError, match="missing segments"):
        X12LikeIntakeAdapter().normalize(b"ST*837*0001~SE*2*0001~")


# ---- end-to-end: wire format in → document out ------------------------------------------


async def test_e2e_x12_claim_adjudicated_and_eob_rendered() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        store = InMemoryEventStore()
        settings = Settings(temporal_task_queue=TASK_QUEUE)
        async with make_worker(env, store):
            app = create_app(store=store, temporal_client=env.client, settings=settings)
            async with _client(app) as ac:
                r = await ac.post("/intake/x12-837", content=_X12.encode())
                assert r.status_code == 202
                body = r.json()
                claim_id = body["claim_id"]
                assert body["claim_type"] == "structured-claim"
                assert body["upload_url"] is None  # no document stage

                # no document, no LLM: rules adjudicate directly and the claim completes
                handle = env.client.get_workflow_handle(claim_id)
                assert await handle.result() == claim_id

                claim = await store.get(claim_id)
                assert claim.status == ClaimStatus.PERSISTED
                assert claim.decision == "APPROVE"  # 500 < review threshold

                eob = await ac.get(f"/claims/{claim_id}/documents/eob")
                assert eob.status_code == 200
                doc = eob.json()
                assert doc["document"] == "explanation_of_benefits"
                assert doc["decision"] == "APPROVE"
                assert doc["payable_amount"] == 500.0

                # denial letter is only for DENY decisions
                denial = await ac.get(f"/claims/{claim_id}/documents/denial-letter")
                assert denial.status_code == 409


async def test_e2e_fnol_over_limit_denied_with_letter() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        store = InMemoryEventStore()
        obj = InMemoryObjectStore()
        settings = Settings(temporal_task_queue=TASK_QUEUE)
        async with make_worker(
            env, store, obj_store=obj, cost_model=MockModelClient(confidence=0.95)
        ):
            app = create_app(store=store, temporal_client=env.client, settings=settings)
            async with _client(app) as ac:
                fnol = {**_FNOL, "estimatedAmount": 150_000.0}
                r = await ac.post("/intake/fnol", content=json.dumps(fnol).encode())
                assert r.status_code == 202
                claim_id = r.json()["claim_id"]
                assert r.json()["claim_type"] == "auto-fnol"

                await obj.put(f"{claim_id}/source.pdf", b"%PDF fnol doc")
                await ac.post(f"/claims/{claim_id}/uploaded")
                handle = env.client.get_workflow_handle(claim_id)
                assert await handle.result() == claim_id

                claim = await store.get(claim_id)
                # DENY (first match) outranks escalation review — and REVIEW no-ops
                assert claim.decision == "DENY"
                assert claim.reason_codes == ["EXCEEDS_LIMIT"]

                letter = await ac.get(f"/claims/{claim_id}/documents/denial-letter")
                assert letter.status_code == 200
                assert "DENIED" in letter.text
                assert "EXCEEDS_LIMIT" in letter.text
                assert claim_id in letter.text

                eob = (await ac.get(f"/claims/{claim_id}/documents/eob")).json()
                assert eob["decision"] == "DENY"
                assert eob["payable_amount"] == 0


async def test_unknown_adapters_404() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        store = InMemoryEventStore()
        settings = Settings(temporal_task_queue=TASK_QUEUE)
        app = create_app(store=store, temporal_client=env.client, settings=settings)
        async with _client(app) as ac:
            assert (await ac.post("/intake/no-such-format", content=b"{}")).status_code == 404
            assert (
                await ac.get("/claims/nope/documents/eob")
            ).status_code == 404  # unknown claim
            assert (
                await ac.get("/claims/nope/documents/no-such-doc")
            ).status_code == 404  # unknown format


async def test_malformed_intake_400() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        store = InMemoryEventStore()
        settings = Settings(temporal_task_queue=TASK_QUEUE)
        app = create_app(store=store, temporal_client=env.client, settings=settings)
        async with _client(app) as ac:
            r = await ac.post("/intake/fnol", content=b'{"policyNo": "P-1"}')
            assert r.status_code == 400
            assert "missing fields" in r.json()["detail"]