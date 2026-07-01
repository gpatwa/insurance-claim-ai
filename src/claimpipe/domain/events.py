"""Domain events — the append-only source of truth.

Every state change is an event. The `claims` status projection and all downstream read
models are folds over this log; the status enum is never hand-authored as truth.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, Field


class EventType(StrEnum):
    CLAIM_RECEIVED = "CLAIM_RECEIVED"
    METADATA_LOGGED = "METADATA_LOGGED"
    OCR_STARTED = "OCR_STARTED"
    OCR_COMPLETED = "OCR_COMPLETED"
    CLAIM_FAILED = "CLAIM_FAILED"
    # reserved for later milestones
    LLM_STARTED = "LLM_STARTED"
    PREDICTIONS_READY = "PREDICTIONS_READY"
    CLAIM_ADJUDICATED = "CLAIM_ADJUDICATED"
    CLAIM_PERSISTED = "CLAIM_PERSISTED"
    CLAIM_NOTIFIED = "CLAIM_NOTIFIED"
    NOTIFY_FAILED = "NOTIFY_FAILED"


class DomainEvent(BaseModel):
    event_id: str
    claim_id: str
    seq: int = Field(ge=1, description="Per-claim monotonic sequence")
    type: EventType
    payload: dict = Field(default_factory=dict)
    occurred_at: datetime
