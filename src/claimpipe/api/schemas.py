"""Request/response models for the ingestion API."""

from __future__ import annotations

from pydantic import BaseModel

from claimpipe.domain.models import ClaimMetadata, ClaimStatus


class SubmitClaimRequest(BaseModel):
    metadata: ClaimMetadata


class SubmitClaimResponse(BaseModel):
    claim_id: str
    status: ClaimStatus
    claim_type: str = "generic-document"
    stages: list[str] = []
    # None for claim types whose pipeline has no document-upload stage
    upload_url: str | None = None
    idempotent: bool = False


class ClaimStatusResponse(BaseModel):
    claim_id: str
    status: ClaimStatus
