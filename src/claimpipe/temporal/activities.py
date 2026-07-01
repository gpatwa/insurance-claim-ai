"""Temporal activities. All I/O (event store, OCR, object store, …) lives here; workflows stay
deterministic.

Activities are methods on a class holding injected dependencies. State changes go through the
event store (append event → inline projection → outbox), never direct status writes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from temporalio import activity

from claimpipe.adapters.model_client import ModelClient
from claimpipe.adapters.object_store import ObjectStore
from claimpipe.adapters.ocr import OCRClient
from claimpipe.adjudication import RuleSet, adjudicate, default_rulesets
from claimpipe.domain.events import EventType
from claimpipe.domain.models import ModelPrediction
from claimpipe.eventstore import EventStore
from claimpipe.llm import should_escalate
from claimpipe.refdata import RefDataSource, enrich_facts

if TYPE_CHECKING:  # avoid importing langgraph into the workflow sandbox
    from claimpipe.agent import ClaimReviewAgent
    from claimpipe.tenancy import TenantDirectory


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
        agent: ClaimReviewAgent | None = None,
        confidence_threshold: float = 0.85,
        high_value_amount: float = 25000.0,
        rulesets: dict[str, RuleSet] | None = None,
        refdata: RefDataSource | None = None,
        tenants: TenantDirectory | None = None,
    ) -> None:
        self._store = store
        self._obj = object_store
        self._ocr = ocr
        self._cost_model = cost_model
        self._accuracy_model = accuracy_model
        self._agent = agent
        self._threshold = confidence_threshold
        self._high_value = high_value_amount
        self._rulesets = rulesets if rulesets is not None else default_rulesets()
        self._refdata = refdata
        self._tenants = tenants

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
        """Design stage C: tiered routing over the OCR text; emit PREDICTIONS_READY.

        Cost tier runs first. On escalation, the LangGraph review agent runs (if wired);
        otherwise a plain accuracy-tier model call. Returns whether the claim escalated.
        """
        assert self._obj is not None
        assert self._cost_model is not None and self._accuracy_model is not None
        claim = await self._store.get(claim_id)
        assert claim is not None
        text = (await self._obj.get(f"{claim_id}/ocr.txt")).decode("utf-8")
        claim_value = float(claim.metadata.attributes.get("amount", 0) or 0)

        cost = await self._cost_model.predict(text)
        predictions = [cost]
        escalated = should_escalate(
            cost,
            threshold=self._threshold,
            claim_value=claim_value,
            high_value_amount=self._high_value,
        )
        if escalated:
            if self._agent is not None:
                review = await self._agent.review(text)
                predictions.append(
                    ModelPrediction(
                        model_name="langgraph-review",
                        model_version="1",
                        output={
                            "recommendation": review.get("recommendation"),
                            "validation": review.get("validation"),
                            "critique": review.get("critique"),
                            "extracted": review.get("extracted"),
                        },
                        confidence=float(review.get("confidence", 0.0)),
                    )
                )
            else:
                predictions.append(await self._accuracy_model.predict(text))

        await self._store.append(
            claim_id,
            EventType.PREDICTIONS_READY,
            {
                "predictions": [p.model_dump() for p in predictions],
                "escalated": escalated,
            },
        )
        return escalated

    @activity.defn
    async def run_adjudication(self, claim_id: str) -> str:
        """Deterministic decision: apply the claim type's versioned rule set to the facts.

        Rules DECIDE; the LLM only prepared the facts. The full (rule set version, facts,
        matched rule) tuple is recorded on the event — reproducible and audit-grade.
        Returns the decision string.
        """
        claim = await self._store.get(claim_id)
        assert claim is not None

        preds = await self._store.predictions(claim_id)
        escalated = False
        for ev in await self._store.events(claim_id):
            if ev.type == EventType.PREDICTIONS_READY:
                escalated = bool(ev.payload.get("escalated", False))

        facts: dict = {
            **claim.metadata.attributes,
            "claim_type": claim.metadata.claim_type,
            "escalated": escalated,
        }
        if preds:
            facts["confidence"] = max(p.confidence for p in preds)
        # ground facts in reference data (policy status/limits) when a source is wired
        facts = await enrich_facts(self._refdata, facts)

        # Tenant-specific rule sets when a directory is wired; else the default sets.
        rulesets = self._rulesets
        if self._tenants is not None:
            try:
                rulesets = self._tenants.get(claim.metadata.tenant_id).rulesets
            except Exception:  # noqa: BLE001 - unknown tenant → safe empty set → PEND
                rulesets = {}

        # No rule set registered for the type → empty set → safe PEND (never auto-approve).
        rule_set = rulesets.get(
            claim.metadata.claim_type,
            RuleSet(name=claim.metadata.claim_type, version="unregistered", rules=[]),
        )
        result = adjudicate(rule_set, facts)
        await self._store.append(
            claim_id, EventType.CLAIM_ADJUDICATED, result.model_dump()
        )
        return str(result.decision)
