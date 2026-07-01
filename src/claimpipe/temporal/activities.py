"""Temporal activities. All I/O (event store, OCR, object store, …) lives here; workflows stay
deterministic.

Activities are methods on a class holding injected dependencies. State changes go through the
event store (append event → inline projection → outbox), never direct status writes.
"""

from __future__ import annotations

from temporalio import activity

from claimpipe.adapters.object_store import ObjectStore
from claimpipe.adapters.ocr import OCRClient
from claimpipe.domain.events import EventType
from claimpipe.eventstore import EventStore


@activity.defn
async def ping(name: str) -> str:
    """M0 smoke activity."""
    activity.logger.info("ping")
    return f"pong:{name}"


class ClaimActivities:
    def __init__(
        self,
        store: EventStore,
        object_store: ObjectStore | None = None,
        ocr: OCRClient | None = None,
    ) -> None:
        self._store = store
        self._obj = object_store
        self._ocr = ocr

    @activity.defn
    async def record_event(self, claim_id: str, event_type: str, payload: dict) -> None:
        """Append a domain event (projection + outbox happen inside the store, one txn)."""
        await self._store.append(claim_id, EventType(event_type), payload)

    @activity.defn
    async def run_ocr(self, claim_id: str, pdf_key: str) -> str:
        """Design stage B: read PDF, OCR it, store text, then emit OCR_COMPLETED."""
        assert self._obj is not None and self._ocr is not None
        pdf = await self._obj.get(pdf_key)
        text = await self._ocr.extract_text(pdf)
        ocr_key = f"{claim_id}/ocr.txt"
        ref = await self._obj.put(ocr_key, text.encode("utf-8"), content_type="text/plain")
        await self._store.append(claim_id, EventType.OCR_COMPLETED, {"ocr_ref": ref})
        return ref
