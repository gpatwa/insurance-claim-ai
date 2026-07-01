"""LLM adapter — the "mix of providers" behind one ModelClient Protocol.

MockModelClient keeps the pipeline runnable offline / in CI. AnthropicModelClient is the real
impl (Claude via API/Bedrock/Vertex) using structured output + a client-side rate limiter.
"""

from __future__ import annotations

import json
from typing import Protocol, runtime_checkable

from claimpipe.domain.models import ModelPrediction

# JSON schema pins the output shape deterministically (see design doc: determinism via
# constrained output, not sampling knobs).
_OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "summary": {"type": "string"},
        "category": {"type": "string"},
        "confidence": {"type": "number"},
    },
    "required": ["summary", "category", "confidence"],
    "additionalProperties": False,
}


@runtime_checkable
class ModelClient(Protocol):
    name: str
    version: str

    async def predict(self, ocr_text: str) -> ModelPrediction:
        """Produce a structured prediction over the OCR text."""
        ...


class MockModelClient:
    """Deterministic mock. Confidence is configurable so tests can drive both the auto-accept
    and escalation paths."""

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


class AnthropicModelClient:
    """Real Claude client. Deployment impl; not exercised in CI.

    Uses structured output (`output_config.format`) for a schema-guaranteed response, and a
    token-bucket limiter sized to the provider quota so we never exceed rate limits.
    """

    def __init__(self, *, model: str, name: str, version: str, max_per_minute: int = 300):
        self._model = model
        self.name = name
        self.version = version
        self._max_per_minute = max_per_minute
        self._client = None
        self._limiter = None

    def _ensure(self):
        if self._client is None:
            import anthropic
            from aiolimiter import AsyncLimiter

            self._client = anthropic.AsyncAnthropic()
            self._limiter = AsyncLimiter(self._max_per_minute, time_period=60)

    async def predict(self, ocr_text: str) -> ModelPrediction:
        self._ensure()
        assert self._client is not None and self._limiter is not None
        system = (
            "You extract structured fields from an insurance claim's OCR text. "
            "Return summary, category, and a calibrated confidence in [0,1]."
        )
        async with self._limiter:
            resp = await self._client.messages.create(
                model=self._model,
                max_tokens=1024,
                system=system,
                output_config={"format": {"type": "json_schema", "schema": _OUTPUT_SCHEMA}},
                messages=[{"role": "user", "content": ocr_text}],
            )
        text = next(b.text for b in resp.content if b.type == "text")
        data = json.loads(text)
        return ModelPrediction(
            model_name=self.name,
            model_version=self.version,
            output={"summary": data["summary"], "category": data["category"]},
            confidence=float(data["confidence"]),
            latency_ms=0,
            tokens_cost=resp.usage.input_tokens + resp.usage.output_tokens,
        )
