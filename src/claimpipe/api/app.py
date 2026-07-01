"""FastAPI ingestion service.

Submission is async and event-sourced: the API validates JSON metadata, appends a
CLAIM_RECEIVED event (which creates the projection), starts the durable workflow
(workflow_id = claim_id → dedup/idempotency), and returns 202.
"""

from __future__ import annotations

from uuid import uuid4

from fastapi import FastAPI, Header, HTTPException, Request
from temporalio.client import Client

from claimpipe.api.schemas import (
    ClaimStatusResponse,
    ReviewRequest,
    SubmitClaimRequest,
    SubmitClaimResponse,
)
from claimpipe.claimtypes import (
    ClaimTypeRegistry,
    Stage,
    UnknownClaimType,
    default_registry,
    validate_attributes,
)
from claimpipe.config import Settings
from claimpipe.domain.events import EventType
from claimpipe.domain.models import ClaimStatus
from claimpipe.eventstore import EventStore
from claimpipe.temporal.workflows import ClaimWorkflow


def _upload_url(settings: Settings, claim_id: str) -> str:
    # M2: placeholder. Real presigned S3/MinIO PUT URL is a later refinement.
    return f"{settings.s3_endpoint_url}/{settings.s3_bucket}/{claim_id}/source.pdf"


def create_app(
    *,
    store: EventStore,
    temporal_client: Client,
    settings: Settings,
    registry: ClaimTypeRegistry | None = None,
) -> FastAPI:
    app = FastAPI(title="claimpipe ingestion")
    app.state.store = store
    app.state.temporal_client = temporal_client
    app.state.settings = settings
    app.state.registry = registry or default_registry()

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/claim-types")
    async def list_claim_types(request: Request) -> dict:
        reg: ClaimTypeRegistry = request.app.state.registry
        return {
            "claim_types": [reg.get(name).model_dump() for name in reg.names()],
        }

    @app.post("/claims", response_model=SubmitClaimResponse, status_code=202)
    async def submit_claim(
        req: SubmitClaimRequest,
        request: Request,
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    ) -> SubmitClaimResponse:
        store: EventStore = request.app.state.store
        client: Client = request.app.state.temporal_client
        settings: Settings = request.app.state.settings
        reg: ClaimTypeRegistry = request.app.state.registry

        # Schema-driven intake: resolve the claim type and validate its attribute schema.
        try:
            defn = reg.get(req.metadata.claim_type)
        except UnknownClaimType:
            raise HTTPException(
                status_code=422,
                detail=f"unknown claim_type '{req.metadata.claim_type}'; "
                f"known: {reg.names()}",
            ) from None
        errors = validate_attributes(defn, req.metadata.attributes)
        if errors:
            raise HTTPException(status_code=422, detail={"attribute_errors": errors})

        stages = [str(s) for s in defn.stages]
        has_upload = str(Stage.UPLOAD) in stages

        if idempotency_key:
            existing = await store.find_by_idempotency_key(idempotency_key)
            if existing is not None:
                return SubmitClaimResponse(
                    claim_id=existing.claim_id,
                    status=existing.status,
                    claim_type=existing.metadata.claim_type,
                    stages=stages,
                    upload_url=(
                        _upload_url(settings, existing.claim_id) if has_upload else None
                    ),
                    idempotent=True,
                )

        claim_id = str(uuid4())
        await store.append(
            claim_id,
            EventType.CLAIM_RECEIVED,
            {"metadata": req.metadata.model_dump()},
            idempotency_key=idempotency_key,
        )
        # Pipeline-as-config: pin the claim type's stage list into the workflow input, so
        # later registry changes never affect in-flight claims.
        await client.start_workflow(
            ClaimWorkflow.run,
            args=[claim_id, stages],
            id=claim_id,
            task_queue=settings.temporal_task_queue,
        )
        return SubmitClaimResponse(
            claim_id=claim_id,
            status=ClaimStatus.RECEIVED,
            claim_type=defn.name,
            stages=stages,
            upload_url=_upload_url(settings, claim_id) if has_upload else None,
        )

    @app.post("/claims/{claim_id}/uploaded", status_code=202)
    async def mark_uploaded(claim_id: str, request: Request) -> dict[str, str]:
        client: Client = request.app.state.temporal_client
        store: EventStore = request.app.state.store
        if await store.get(claim_id) is None:
            raise HTTPException(status_code=404, detail="claim not found")
        handle = client.get_workflow_handle(claim_id)
        await handle.signal(ClaimWorkflow.pdf_uploaded, f"{claim_id}/source.pdf")
        return {"claim_id": claim_id, "signal": "pdf_uploaded"}

    @app.get("/claims/{claim_id}", response_model=ClaimStatusResponse)
    async def get_claim(claim_id: str, request: Request) -> ClaimStatusResponse:
        store: EventStore = request.app.state.store
        claim = await store.get(claim_id)
        if claim is None:
            raise HTTPException(status_code=404, detail="claim not found")
        return ClaimStatusResponse(
            claim_id=claim.claim_id,
            status=claim.status,
            decision=claim.decision,
            reason_codes=claim.reason_codes,
        )

    @app.get("/review-queue")
    async def review_queue(request: Request) -> dict:
        """Work queue: adjudicated claims PENDing for a human reviewer."""
        store: EventStore = request.app.state.store
        pending = await store.list_pending_review()
        return {
            "pending": [
                {
                    "claim_id": c.claim_id,
                    "claim_type": c.metadata.claim_type,
                    "reason_codes": c.reason_codes,
                    "attributes": c.metadata.attributes,
                }
                for c in pending
            ]
        }

    @app.post("/claims/{claim_id}/review", status_code=202)
    async def submit_review(claim_id: str, req: ReviewRequest, request: Request) -> dict:
        """Reviewer's verdict for a PENDed claim — signals the workflow's REVIEW gate."""
        store: EventStore = request.app.state.store
        client: Client = request.app.state.temporal_client
        claim = await store.get(claim_id)
        if claim is None:
            raise HTTPException(status_code=404, detail="claim not found")
        if claim.decision != "PEND":
            raise HTTPException(
                status_code=409,
                detail=f"claim is not pending review (decision={claim.decision})",
            )
        handle = client.get_workflow_handle(claim_id)
        await handle.signal(ClaimWorkflow.review_completed, req.model_dump())
        return {"claim_id": claim_id, "signal": "review_completed"}

    @app.get("/claims/{claim_id}/predictions")
    async def get_predictions(claim_id: str, request: Request) -> dict:
        store: EventStore = request.app.state.store
        if await store.get(claim_id) is None:
            raise HTTPException(status_code=404, detail="claim not found")
        preds = await store.predictions(claim_id)
        return {"claim_id": claim_id, "predictions": [p.model_dump() for p in preds]}

    return app
