"""Request/response models for the ingestion API."""

from __future__ import annotations

from pydantic import BaseModel

from claimpipe.domain.models import ClaimMetadata, ClaimStatus


class SubmitClaimRequest(BaseModel):
    metadata: ClaimMetadata


class SubmitClaimResponse(BaseModel):
    claim_id: str
    status: ClaimStatus
    upload_url: str
    idempotent: bool = False


class ClaimStatusResponse(BaseModel):
    claim_id: str
    status: ClaimStatus
