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
    SubmitClaimRequest,
    SubmitClaimResponse,
)
from claimpipe.config import Settings
from claimpipe.domain.events import EventType
from claimpipe.domain.models import ClaimStatus
from claimpipe.eventstore import EventStore
from claimpipe.temporal.workflows import ClaimWorkflow


def _upload_url(settings: Settings, claim_id: str) -> str:
    # M2: placeholder. Real presigned S3/MinIO PUT URL is a later refinement.
    return f"{settings.s3_endpoint_url}/{settings.s3_bucket}/{claim_id}/source.pdf"


def create_app(*, store: EventStore, temporal_client: Client, settings: Settings) -> FastAPI:
    app = FastAPI(title="claimpipe ingestion")
    app.state.store = store
    app.state.temporal_client = temporal_client
    app.state.settings = settings

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/claims", response_model=SubmitClaimResponse, status_code=202)
    async def submit_claim(
        req: SubmitClaimRequest,
        request: Request,
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    ) -> SubmitClaimResponse:
        store: EventStore = request.app.state.store
        client: Client = request.app.state.temporal_client
        settings: Settings = request.app.state.settings

        if idempotency_key:
            existing = await store.find_by_idempotency_key(idempotency_key)
            if existing is not None:
                return SubmitClaimResponse(
                    claim_id=existing.claim_id,
                    status=existing.status,
                    upload_url=_upload_url(settings, existing.claim_id),
                    idempotent=True,
                )

        claim_id = str(uuid4())
        await store.append(
            claim_id,
            EventType.CLAIM_RECEIVED,
            {"metadata": req.metadata.model_dump()},
            idempotency_key=idempotency_key,
        )
        await client.start_workflow(
            ClaimWorkflow.run,
            claim_id,
            id=claim_id,
            task_queue=settings.temporal_task_queue,
        )
        return SubmitClaimResponse(
            claim_id=claim_id,
            status=ClaimStatus.RECEIVED,
            upload_url=_upload_url(settings, claim_id),
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
        return ClaimStatusResponse(claim_id=claim.claim_id, status=claim.status)

    return app
