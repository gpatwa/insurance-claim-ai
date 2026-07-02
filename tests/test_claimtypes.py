"""M8+M9: claim-type registry, schema-driven intake, and the pipeline-as-config engine.

Proves that lines of business are configuration: the same ClaimWorkflow executes different
stage lists per claim type — full pipeline, OCR-only archival, and metadata-only — with the
projection state machine validating every path.
"""

from __future__ import annotations

import httpx
import pytest
from temporalio.testing import WorkflowEnvironment

from claimpipe.adapters.object_store import InMemoryObjectStore
from claimpipe.api.app import create_app
from claimpipe.claimtypes import (
    AttributeSpec,
    ClaimTypeDef,
    InvalidPipeline,
    Stage,
    default_registry,
    validate_attributes,
)
from claimpipe.config import Settings
from claimpipe.domain.models import ClaimStatus
from claimpipe.eventstore import InMemoryEventStore
from tests.helpers import META, TASK_QUEUE, make_worker


def _client(app) -> httpx.AsyncClient:
    # dev-integration key (submit + review) — see claimpipe.customers.DEV_KEYS
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        headers={"X-API-Key": "ck_dev_all_01"},
    )


# ---------------------------------------------------------------- registry / schema (M8)


def test_pipeline_constraints_enforced() -> None:
    # LLM without OCR is invalid
    with pytest.raises(InvalidPipeline):
        ClaimTypeDef(
            name="bad", stages=[Stage.UPLOAD, Stage.LLM, Stage.PERSIST]
        ).validate_pipeline()
    # must end with PERSIST
    with pytest.raises(InvalidPipeline):
        ClaimTypeDef(name="bad", stages=[Stage.UPLOAD, Stage.OCR]).validate_pipeline()
    # OCR requires UPLOAD
    with pytest.raises(InvalidPipeline):
        ClaimTypeDef(name="bad", stages=[Stage.OCR, Stage.PERSIST]).validate_pipeline()
    # duplicates rejected
    with pytest.raises(InvalidPipeline):
        ClaimTypeDef(
            name="bad", stages=[Stage.UPLOAD, Stage.UPLOAD, Stage.PERSIST]
        ).validate_pipeline()


def test_attribute_schema_validation() -> None:
    defn = ClaimTypeDef(
        name="t",
        stages=[Stage.PERSIST],
        attributes=[
            AttributeSpec(name="policy_number", type="string", required=True),
            AttributeSpec(name="amount", type="number", required=True),
        ],
    )
    assert validate_attributes(defn, {"policy_number": "P-1", "amount": 120.5}) == []
    errors = validate_attributes(defn, {"amount": "not-a-number"})
    assert any("policy_number" in e for e in errors)
    assert any("amount" in e for e in errors)
    # bool is not a number
    assert validate_attributes(defn, {"policy_number": "P-1", "amount": True}) != []


def test_default_registry_seeds() -> None:
    reg = default_registry()
    assert set(reg.names()) == {
        "generic-document",
        "metadata-only",
        "archive-document",
        "auto-fnol",
        "structured-claim",
    }
    assert reg.get("metadata-only").stages == [Stage.PERSIST]


# ------------------------------------------------------------- schema-driven intake (M8)


async def test_unknown_claim_type_rejected() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        store = InMemoryEventStore()
        settings = Settings(temporal_task_queue=TASK_QUEUE)
        app = create_app(store=store, temporal_client=env.client, settings=settings)
        async with _client(app) as ac:
            meta = {**META, "claim_type": "no-such-line"}
            r = await ac.post("/claims", json={"metadata": meta})
            assert r.status_code == 422
            assert "no-such-line" in r.json()["detail"]


async def test_missing_required_attributes_rejected() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        store = InMemoryEventStore()
        settings = Settings(temporal_task_queue=TASK_QUEUE)
        app = create_app(store=store, temporal_client=env.client, settings=settings)
        async with _client(app) as ac:
            meta = {**META, "claim_type": "auto-fnol", "attributes": {"amount": 900}}
            r = await ac.post("/claims", json={"metadata": meta})
            assert r.status_code == 422
            errs = r.json()["detail"]["attribute_errors"]
            assert any("policy_number" in e for e in errs)
            assert any("incident_date" in e for e in errs)


async def test_claim_types_endpoint() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        store = InMemoryEventStore()
        settings = Settings(temporal_task_queue=TASK_QUEUE)
        app = create_app(store=store, temporal_client=env.client, settings=settings)
        async with _client(app) as ac:
            r = await ac.get("/claim-types")
            assert r.status_code == 200
            names = [t["name"] for t in r.json()["claim_types"]]
            assert "metadata-only" in names and "generic-document" in names


# --------------------------------------------------------- pipeline-as-config engine (M9)


async def test_metadata_only_pipeline_skips_upload_ocr_llm() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        store = InMemoryEventStore()
        settings = Settings(temporal_task_queue=TASK_QUEUE)
        async with make_worker(env, store):
            app = create_app(store=store, temporal_client=env.client, settings=settings)
            async with _client(app) as ac:
                meta = {**META, "claim_type": "metadata-only"}
                r = await ac.post("/claims", json={"metadata": meta})
                assert r.status_code == 202
                body = r.json()
                claim_id = body["claim_id"]
                assert body["upload_url"] is None  # no document stage in this pipeline
                assert body["stages"] == ["PERSIST"]

                # completes without any upload signal — no dormancy gate in the pipeline
                handle = env.client.get_workflow_handle(claim_id)
                assert await handle.result() == claim_id

                claim = await store.get(claim_id)
                assert claim is not None and claim.status == ClaimStatus.PERSISTED
                types = [e.type for e in await store.events(claim_id)]
                assert types == ["CLAIM_RECEIVED", "METADATA_LOGGED", "CLAIM_PERSISTED"]
                assert await store.predictions(claim_id) == []


async def test_archive_document_pipeline_runs_ocr_without_llm() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        store = InMemoryEventStore()
        obj = InMemoryObjectStore()
        settings = Settings(temporal_task_queue=TASK_QUEUE)
        async with make_worker(env, store, obj_store=obj):
            app = create_app(store=store, temporal_client=env.client, settings=settings)
            async with _client(app) as ac:
                meta = {**META, "claim_type": "archive-document"}
                r = await ac.post("/claims", json={"metadata": meta})
                claim_id = r.json()["claim_id"]
                assert r.json()["upload_url"] is not None

                await obj.put(f"{claim_id}/source.pdf", b"%PDF-1.7 archive me")
                await ac.post(f"/claims/{claim_id}/uploaded")

                handle = env.client.get_workflow_handle(claim_id)
                assert await handle.result() == claim_id

                claim = await store.get(claim_id)
                assert claim is not None and claim.status == ClaimStatus.PERSISTED
                assert claim.ocr_ref is not None
                types = [e.type for e in await store.events(claim_id)]
                assert types == [
                    "CLAIM_RECEIVED",
                    "METADATA_LOGGED",
                    "OCR_STARTED",
                    "OCR_COMPLETED",
                    "CLAIM_PERSISTED",
                ]
                # no LLM stage ran for this claim type
                assert "LLM_STARTED" not in types
                assert await store.predictions(claim_id) == []
