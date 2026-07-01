"""Temporal workflows. Deterministic orchestration only — no direct I/O.

The workflow is the authoritative state machine and the decider; it emits domain events
(never writes status directly). Live progress is exposed via a query; the durable status lives
in the `claims` projection folded from those events.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from claimpipe.domain.events import EventType
    from claimpipe.domain.models import ClaimStatus
    from claimpipe.temporal.activities import ClaimActivities, ping

_STD_RETRY = RetryPolicy(maximum_attempts=5)
_OCR_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=5,
)


@workflow.defn
class PingWorkflow:
    """M0 smoke workflow."""

    @workflow.run
    async def run(self, name: str) -> str:
        return await workflow.execute_activity(
            ping, name, start_to_close_timeout=timedelta(seconds=10)
        )


@workflow.defn
class ClaimWorkflow:
    """Per-claim durable workflow.

    Stages so far: log metadata (A) → await PDF upload (dormancy gate) → OCR (B).
    Later milestones append LLM → persist → notify.
    """

    def __init__(self) -> None:
        self._uploaded: bool = False
        self._pdf_key: str | None = None
        self._status: str = str(ClaimStatus.RECEIVED)

    @workflow.signal
    def pdf_uploaded(self, pdf_key: str) -> None:
        self._pdf_key = pdf_key
        self._uploaded = True

    @workflow.query
    def status(self) -> str:
        """Live in-flight status (the durable projection also carries it)."""
        return self._status

    @workflow.run
    async def run(self, claim_id: str) -> str:
        await self._emit(claim_id, EventType.METADATA_LOGGED)

        # Dormancy gate: wait (durably, scale-to-zero) for the upload-complete signal.
        try:
            await workflow.wait_condition(lambda: self._uploaded, timeout=timedelta(days=7))
        except TimeoutError:
            await self._emit(claim_id, EventType.CLAIM_FAILED, {"reason": "upload_timeout"})
            self._status = str(ClaimStatus.FAILED)
            return claim_id
        assert self._pdf_key is not None

        await self._emit(claim_id, EventType.OCR_STARTED)
        self._status = str(ClaimStatus.OCR_RUNNING)
        await workflow.execute_activity_method(
            ClaimActivities.run_ocr,
            args=[claim_id, self._pdf_key],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=_OCR_RETRY,
        )
        self._status = str(ClaimStatus.OCR_DONE)
        return claim_id

    async def _emit(
        self, claim_id: str, event_type: EventType, payload: dict | None = None
    ) -> None:
        await workflow.execute_activity_method(
            ClaimActivities.record_event,
            args=[claim_id, str(event_type), payload or {}],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=_STD_RETRY,
        )
