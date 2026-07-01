"""Request/response models for the ingestion API."""

from __future__ import annotations

from pydantic import BaseModel, Field

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
    decision: str | None = None
    reason_codes: list[str] = []


class ReviewRequest(BaseModel):
    """Human reviewer's verdict for a PENDed claim. Reviewers must decide — no re-PEND."""

    decision: str = Field(pattern="^(APPROVE|DENY)$")
    reason_code: str = "MANUAL_REVIEW"
    reviewer: str = Field(min_length=1)
