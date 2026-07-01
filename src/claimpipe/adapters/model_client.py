"""LLM adapter — the "mix of providers" behind one ModelClient Protocol.

Real impls (Anthropic via Bedrock/Vertex/API) land in M3; the mock keeps the pipeline
runnable offline and in CI.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from claimpipe.domain.models import ModelPrediction


@runtime_checkable
class ModelClient(Protocol):
    name: str
    version: str

    async def predict(self, ocr_text: str) -> ModelPrediction:
        """Produce a structured prediction over the OCR text."""
        ...


class MockModelClient:
    """Deterministic mock model. Confidence derived from text length so tests can
    exercise both the auto-accept and escalation paths."""

    def __init__(
        self,
        name: str = "mock-cost-tier",
        version: str = "0.0.1",
        confidence: float = 0.95,
    ):
        self.name = name
        self.version = version
        self._confidence = confidence

    async def predict(self, ocr_text: str) -> ModelPrediction:
        return ModelPrediction(
            model_name=self.name,
            model_version=self.version,
            output={"summary": ocr_text[:64], "category": "auto"},
            confidence=self._confidence,
            latency_ms=1,
            tokens_cost=len(ocr_text) // 4,
        )
