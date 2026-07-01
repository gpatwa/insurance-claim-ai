"""FastAPI ingestion service.

Submission is async: the API validates JSON metadata, issues a signed object-store upload URL
for the PDF, writes the claim record (RECEIVED), starts the durable workflow
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
from claimpipe.domain.models import Claim, ClaimStatus
from claimpipe.repository import ClaimRepository
from claimpipe.temporal.workflows import ClaimWorkflow


def _upload_url(settings: Settings, claim_id: str) -> str:
    # M1: placeholder. M2 replaces this with a real presigned S3/MinIO PUT URL.
    return f"{settings.s3_endpoint_url}/{settings.s3_bucket}/{claim_id}/source.pdf"


def create_app(*, repo: ClaimRepository, temporal_client: Client, settings: Settings) -> FastAPI:
    app = FastAPI(title="claimpipe ingestion")
    app.state.repo = repo
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
        repo: ClaimRepository = request.app.state.repo
        client: Client = request.app.state.temporal_client
        settings: Settings = request.app.state.settings

        if idempotency_key:
            existing = await repo.find_by_idempotency_key(idempotency_key)
            if existing is not None:
                return SubmitClaimResponse(
                    claim_id=existing.claim_id,
                    status=existing.status,
                    upload_url=_upload_url(settings, existing.claim_id),
                    idempotent=True,
                )

        claim_id = str(uuid4())
        claim = Claim(claim_id=claim_id, status=ClaimStatus.RECEIVED, metadata=req.metadata)
        await repo.create(claim, idempotency_key=idempotency_key)

        # workflow_id = claim_id makes the start idempotent at the Temporal layer too.
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

    @app.get("/claims/{claim_id}", response_model=ClaimStatusResponse)
    async def get_claim(claim_id: str, request: Request) -> ClaimStatusResponse:
        repo: ClaimRepository = request.app.state.repo
        claim = await repo.get(claim_id)
        if claim is None:
            raise HTTPException(status_code=404, detail="claim not found")
        return ClaimStatusResponse(claim_id=claim.claim_id, status=claim.status)

    return app
