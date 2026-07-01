"""Domain models and the claim status state machine.

The status enum is the single source of truth for control flow. The LLM never drives
transitions — it only fills structured fields; the workflow advances the enum
deterministically (see design doc: "enum state machine is the single source of truth").
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, Field


class ClaimStatus(StrEnum):
    RECEIVED = "RECEIVED"
    OCR_RUNNING = "OCR_RUNNING"
    OCR_DONE = "OCR_DONE"
    LLM_RUNNING = "LLM_RUNNING"
    LLM_DONE = "LLM_DONE"
    PERSISTED = "PERSISTED"
    NOTIFIED = "NOTIFIED"
    # terminal / partial
    FAILED = "FAILED"
    PARTIAL_SUCCESS = "PARTIAL_SUCCESS"
    NOTIFY_FAILED = "NOTIFY_FAILED"


TERMINAL_STATUSES = {
    ClaimStatus.NOTIFIED,
    ClaimStatus.FAILED,
    ClaimStatus.NOTIFY_FAILED,
}


class ClaimMetadata(BaseModel):
    """Semi-structured JSON metadata submitted with the claim (~100 KB).

    `claim_type` selects a ClaimTypeDef from the registry, which supplies the attribute
    schema (validated at intake) and the pipeline stages the workflow engine executes.
    """

    customer_id: str = Field(min_length=1)
    callback_url: str = Field(min_length=1, description="Customer webhook endpoint")
    claim_type: str = "generic-document"
    schema_version: str = "v1"
    attributes: dict[str, object] = Field(default_factory=dict)


class Claim(BaseModel):
    claim_id: str
    status: ClaimStatus = ClaimStatus.RECEIVED
    metadata: ClaimMetadata
    pdf_ref: str | None = None
    ocr_ref: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class ModelPrediction(BaseModel):
    """Output of one LLM model over the OCR text — persisted per (claim, model, version)."""

    model_name: str
    model_version: str
    output: dict[str, object]
    confidence: float = Field(ge=0.0, le=1.0)
    latency_ms: int = 0
    tokens_cost: int = 0


class NotificationPayload(BaseModel):
    """What we send to the customer webhook."""

    claim_id: str
    status: ClaimStatus
    predictions: list[ModelPrediction] = Field(default_factory=list)
    idempotency_id: str
