"""LLM tiered routing (pure decision logic — no I/O, unit-testable).

Cost-tier model runs first; the accuracy tier is invoked only when the cost-tier confidence is
below threshold or the claim is high-value. This keeps the expensive model on the minority of
claims that need it (see design doc: escalation policy).
"""

from __future__ import annotations

from dataclasses import dataclass

from claimpipe.adapters.model_client import ModelClient
from claimpipe.domain.models import ModelPrediction


@dataclass
class RoutingResult:
    predictions: list[ModelPrediction]
    escalated: bool


def should_escalate(
    cost_prediction: ModelPrediction,
    *,
    threshold: float,
    claim_value: float,
    high_value_amount: float,
) -> bool:
    return cost_prediction.confidence < threshold or claim_value >= high_value_amount


async def route_and_predict(
    cost_model: ModelClient,
    accuracy_model: ModelClient,
    ocr_text: str,
    *,
    threshold: float,
    claim_value: float = 0.0,
    high_value_amount: float = 25000.0,
) -> RoutingResult:
    cost = await cost_model.predict(ocr_text)
    predictions = [cost]
    escalate = should_escalate(
        cost, threshold=threshold, claim_value=claim_value, high_value_amount=high_value_amount
    )
    if escalate:
        predictions.append(await accuracy_model.predict(ocr_text))
    return RoutingResult(predictions=predictions, escalated=escalate)
