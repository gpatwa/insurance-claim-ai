"""Temporal activities. All I/O (event store, OCR, object store, …) lives here; workflows stay
deterministic.

Activities are methods on a class holding injected dependencies. State changes go through the
event store (append event → inline projection → outbox), never direct status writes.
"""

from __future__ import annotations

from temporalio import activity

from claimpipe.adapters.model_client import ModelClient
from claimpipe.adapters.object_store import ObjectStore
from claimpipe.adapters.ocr import OCRClient
from claimpipe.domain.events import EventType
from claimpipe.eventstore import EventStore
from claimpipe.llm import route_and_predict


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
        cost_model: ModelClient | None = None,
        accuracy_model: ModelClient | None = None,
        confidence_threshold: float = 0.85,
        high_value_amount: float = 25000.0,
    ) -> None:
        self._store = store
        self._obj = object_store
        self._ocr = ocr
        self._cost_model = cost_model
        self._accuracy_model = accuracy_model
        self._threshold = confidence_threshold
        self._high_value = high_value_amount

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

    @activity.defn
    async def run_llm(self, claim_id: str) -> bool:
        """Design stage C: tiered routing over the OCR text; emit PREDICTIONS_READY. Returns
        whether the claim was escalated to the accuracy tier."""
        assert self._obj is not None
        assert self._cost_model is not None and self._accuracy_model is not None
        claim = await self._store.get(claim_id)
        assert claim is not None
        text = (await self._obj.get(f"{claim_id}/ocr.txt")).decode("utf-8")
        claim_value = float(claim.metadata.attributes.get("amount", 0) or 0)

        result = await route_and_predict(
            self._cost_model,
            self._accuracy_model,
            text,
            threshold=self._threshold,
            claim_value=claim_value,
            high_value_amount=self._high_value,
        )
        await self._store.append(
            claim_id,
            EventType.PREDICTIONS_READY,
            {
                "predictions": [p.model_dump() for p in result.predictions],
                "escalated": result.escalated,
            },
        )
        return result.escalated
