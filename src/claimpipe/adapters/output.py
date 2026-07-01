"""Output adapters: render a decided claim as an outbound document.

The mirror of intake: each adapter turns the canonical (claim, predictions) into one wire
format — an EOB-style JSON remittance, a plain-text denial letter, later an X12 835 or PDF
EOB. Rendering is a pure projection over already-decided state; adapters never decide.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from claimpipe.domain.models import Claim, ModelPrediction


class OutputError(Exception):
    """Raised when a document can't be rendered for this claim's state."""


@runtime_checkable
class OutputAdapter(Protocol):
    name: str
    content_type: str

    def render(self, claim: Claim, predictions: list[ModelPrediction]) -> bytes:
        """Render the outbound document; raise OutputError if the state doesn't allow it."""
        ...


def _require_decision(claim: Claim) -> str:
    if claim.decision is None:
        raise OutputError(f"claim {claim.claim_id} has no decision yet")
    return claim.decision


class EobJsonAdapter:
    """Explanation-of-benefits-style JSON remittance (835-shaped, simplified)."""

    name = "eob"
    content_type = "application/json"

    def render(self, claim: Claim, predictions: list[ModelPrediction]) -> bytes:
        import json

        decision = _require_decision(claim)
        amount = claim.metadata.attributes.get("amount")
        doc = {
            "document": "explanation_of_benefits",
            "claim_id": claim.claim_id,
            "claim_type": claim.metadata.claim_type,
            "customer_id": claim.metadata.customer_id,
            "decision": decision,
            "reason_codes": claim.reason_codes,
            "claimed_amount": amount,
            # payment math is a placeholder until a pricing stage exists
            "payable_amount": amount if decision == "APPROVE" else 0,
            "model_summaries": [
                {"model": p.model_name, "confidence": p.confidence} for p in predictions
            ],
        }
        return json.dumps(doc, indent=2).encode()


class DenialLetterAdapter:
    """Plain-text denial letter. Only renders for DENY decisions."""

    name = "denial-letter"
    content_type = "text/plain"

    def render(self, claim: Claim, predictions: list[ModelPrediction]) -> bytes:
        decision = _require_decision(claim)
        if decision != "DENY":
            raise OutputError(f"denial letter requires a DENY decision, got {decision}")
        reasons = ", ".join(claim.reason_codes) or "UNSPECIFIED"
        letter = (
            f"RE: Claim {claim.claim_id}\n"
            f"\n"
            f"Dear {claim.metadata.customer_id},\n"
            f"\n"
            f"After review, your claim has been DENIED for the following reason(s): "
            f"{reasons}.\n"
            f"\n"
            f"You have the right to appeal this determination. Please refer to your policy\n"
            f"documents for the appeals process and applicable deadlines.\n"
        )
        return letter.encode()


def default_output_adapters() -> dict[str, OutputAdapter]:
    return {a.name: a for a in (EobJsonAdapter(), DenialLetterAdapter())}
