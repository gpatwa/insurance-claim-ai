"""LangGraph claim-review agent for the escalation tier.

A small graph — extract → validate → recommend → critic — that runs *inside a Temporal
activity*. Each node is independently testable; the deterministic `validate` node keeps the
rules out of the LLM, and `critic` guards against low-confidence recommendations. Models are
injected (mock in CI, Claude in deployment).
"""

from __future__ import annotations

from typing import TypedDict

from langgraph.graph import END, START, StateGraph

from claimpipe.adapters.model_client import ModelClient


class AgentState(TypedDict, total=False):
    ocr_text: str
    extracted: dict
    confidence: float
    validation: dict
    recommendation: str
    critique: dict


class ClaimReviewAgent:
    def __init__(self, extractor: ModelClient, critic: ModelClient) -> None:
        self._extractor = extractor
        self._critic = critic
        self._app = self._build()

    def _build(self):
        g = StateGraph(AgentState)
        g.add_node("extract", self._extract)
        g.add_node("validate", self._validate)
        g.add_node("recommend", self._recommend)
        g.add_node("critic", self._critique)
        g.add_edge(START, "extract")
        g.add_edge("extract", "validate")
        g.add_edge("validate", "recommend")
        g.add_edge("recommend", "critic")
        g.add_edge("critic", END)
        return g.compile()

    async def _extract(self, state: AgentState) -> dict:
        pred = await self._extractor.predict(state["ocr_text"])
        return {"extracted": pred.output, "confidence": pred.confidence}

    async def _validate(self, state: AgentState) -> dict:
        # deterministic rules — the LLM does not decide validity
        extracted = state.get("extracted", {})
        passed = bool(extracted.get("category"))
        return {"validation": {"passed": passed, "checked": ["category_present"]}}

    async def _recommend(self, state: AgentState) -> dict:
        passed = state.get("validation", {}).get("passed", False)
        confident = state.get("confidence", 0.0) >= 0.8
        return {"recommendation": "approve" if passed and confident else "manual_review"}

    async def _critique(self, state: AgentState) -> dict:
        # a verifier pass — flags low-confidence recommendations for a human
        pred = await self._critic.predict(str(state.get("extracted", {})))
        flagged = pred.confidence < 0.7 or state.get("recommendation") == "manual_review"
        return {"critique": {"confidence": pred.confidence, "flag_for_human": flagged}}

    async def review(self, ocr_text: str) -> AgentState:
        return await self._app.ainvoke({"ocr_text": ocr_text})
