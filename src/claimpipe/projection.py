"""Projection: fold a DomainEvent into the `claims` read model, with validated transitions.

This is the ONE place the status state machine is enforced. Illegal transitions raise, so the
projection can never drift into an impossible state.
"""

from __future__ import annotations

from claimpipe.domain.events import DomainEvent, EventType
from claimpipe.domain.models import Claim, ClaimMetadata, ClaimStatus

# Legal transitions. Kept intentionally explicit so the machine is auditable and any illegal
# jump is a loud failure, not silent corruption.
ALLOWED: dict[ClaimStatus, set[ClaimStatus]] = {
    # RECEIVED may jump straight to ADJUDICATED/PERSISTED for claim types with no
    # document/LLM stages (pipeline-as-config: stages are per-claim-type data, not code).
    ClaimStatus.RECEIVED: {
        ClaimStatus.OCR_RUNNING,
        ClaimStatus.ADJUDICATED,
        ClaimStatus.PERSISTED,
        ClaimStatus.FAILED,
    },
    ClaimStatus.OCR_RUNNING: {ClaimStatus.OCR_DONE, ClaimStatus.FAILED},
    # OCR_DONE may skip LLM for types that only archive documents.
    ClaimStatus.OCR_DONE: {
        ClaimStatus.LLM_RUNNING,
        ClaimStatus.ADJUDICATED,
        ClaimStatus.PERSISTED,
        ClaimStatus.FAILED,
    },
    ClaimStatus.LLM_RUNNING: {
        ClaimStatus.LLM_DONE,
        ClaimStatus.PARTIAL_SUCCESS,
        ClaimStatus.FAILED,
    },
    ClaimStatus.LLM_DONE: {ClaimStatus.ADJUDICATED, ClaimStatus.PERSISTED, ClaimStatus.FAILED},
    ClaimStatus.PARTIAL_SUCCESS: {
        ClaimStatus.ADJUDICATED,
        ClaimStatus.PERSISTED,
        ClaimStatus.FAILED,
    },
    ClaimStatus.ADJUDICATED: {ClaimStatus.PERSISTED, ClaimStatus.FAILED},
    ClaimStatus.PERSISTED: {ClaimStatus.NOTIFIED, ClaimStatus.NOTIFY_FAILED},
    # DLQ replay: a failed notification can be retried to success
    ClaimStatus.NOTIFY_FAILED: {ClaimStatus.NOTIFIED},
}

# Events that move the lifecycle status. Events not listed here (e.g. METADATA_LOGGED) update
# fields/timestamps only.
_EVENT_STATUS: dict[EventType, ClaimStatus] = {
    EventType.OCR_STARTED: ClaimStatus.OCR_RUNNING,
    EventType.OCR_COMPLETED: ClaimStatus.OCR_DONE,
    EventType.LLM_STARTED: ClaimStatus.LLM_RUNNING,
    EventType.PREDICTIONS_READY: ClaimStatus.LLM_DONE,
    EventType.CLAIM_ADJUDICATED: ClaimStatus.ADJUDICATED,
    EventType.CLAIM_PERSISTED: ClaimStatus.PERSISTED,
    EventType.CLAIM_NOTIFIED: ClaimStatus.NOTIFIED,
    EventType.CLAIM_FAILED: ClaimStatus.FAILED,
    EventType.NOTIFY_FAILED: ClaimStatus.NOTIFY_FAILED,
}


class IllegalTransition(Exception):
    pass


def project(claim: Claim | None, event: DomainEvent) -> Claim:
    """Apply one event to the current projection, returning the new projection."""
    if event.type == EventType.CLAIM_RECEIVED:
        if claim is not None:
            raise IllegalTransition(f"CLAIM_RECEIVED for existing claim {event.claim_id}")
        meta = ClaimMetadata(**event.payload["metadata"])
        return Claim(
            claim_id=event.claim_id,
            status=ClaimStatus.RECEIVED,
            metadata=meta,
            created_at=event.occurred_at,
            updated_at=event.occurred_at,
        )

    if claim is None:
        raise IllegalTransition(f"{event.type} before CLAIM_RECEIVED for {event.claim_id}")

    updates: dict[str, object] = {"updated_at": event.occurred_at}

    if event.type == EventType.OCR_COMPLETED:
        updates["ocr_ref"] = event.payload["ocr_ref"]

    if event.type == EventType.CLAIM_ADJUDICATED:
        updates["decision"] = event.payload["decision"]
        updates["reason_codes"] = list(event.payload.get("reason_codes", []))

    new_status = _EVENT_STATUS.get(event.type)
    if new_status is not None and new_status != claim.status:
        if new_status not in ALLOWED.get(claim.status, set()):
            raise IllegalTransition(f"{claim.status} -> {new_status} ({event.type})")
        updates["status"] = new_status

    return claim.model_copy(update=updates)
