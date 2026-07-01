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
    from claimpipe.claimtypes import Stage
    from claimpipe.domain.events import EventType
    from claimpipe.domain.models import ClaimStatus
    from claimpipe.temporal.activities import ClaimActivities, ping

# Fallback pipeline when no stage list is supplied (backwards compatible with the
# pre-registry behavior; the API normally pins the claim type's stages at submission).
DEFAULT_STAGES = [
    str(Stage.UPLOAD),
    str(Stage.OCR),
    str(Stage.LLM),
    str(Stage.ADJUDICATE),
    str(Stage.PERSIST),
]

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
    """Per-claim durable workflow — a generic pipeline ENGINE.

    The stage list comes from the claim type's registry definition (pinned into the workflow
    input at submission), so lines of business differ by configuration, not by workflow code.
    Stage vocabulary: UPLOAD (dormancy gate) → OCR → LLM → PERSIST.
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
    async def run(self, claim_id: str, stages: list[str] | None = None) -> str:
        pipeline = stages or DEFAULT_STAGES
        await self._emit(claim_id, EventType.METADATA_LOGGED)

        for stage in pipeline:
            done = await self._run_stage(claim_id, stage)
            if not done:  # stage failed terminally (e.g. upload timeout)
                return claim_id
        return claim_id

    async def _run_stage(self, claim_id: str, stage: str) -> bool:
        """Execute one pipeline stage; False means the claim terminated (FAILED)."""
        if stage == Stage.UPLOAD:
            # Dormancy gate: wait (durably, scale-to-zero) for the upload-complete signal.
            try:
                await workflow.wait_condition(
                    lambda: self._uploaded, timeout=timedelta(days=7)
                )
            except TimeoutError:
                await self._emit(
                    claim_id, EventType.CLAIM_FAILED, {"reason": "upload_timeout"}
                )
                self._status = str(ClaimStatus.FAILED)
                return False
            return True

        if stage == Stage.OCR:
            assert self._pdf_key is not None, "OCR stage requires UPLOAD before it"
            await self._emit(claim_id, EventType.OCR_STARTED)
            self._status = str(ClaimStatus.OCR_RUNNING)
            await workflow.execute_activity_method(
                ClaimActivities.run_ocr,
                args=[claim_id, self._pdf_key],
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=_OCR_RETRY,
            )
            self._status = str(ClaimStatus.OCR_DONE)
            return True

        if stage == Stage.LLM:
            # Tiered routing (emits PREDICTIONS_READY inside the activity).
            await self._emit(claim_id, EventType.LLM_STARTED)
            self._status = str(ClaimStatus.LLM_RUNNING)
            await workflow.execute_activity_method(
                ClaimActivities.run_llm,
                claim_id,
                start_to_close_timeout=timedelta(minutes=10),
                retry_policy=_STD_RETRY,
            )
            self._status = str(ClaimStatus.LLM_DONE)
            return True

        if stage == Stage.ADJUDICATE:
            # Deterministic rules decide (the event is emitted inside the activity).
            await workflow.execute_activity_method(
                ClaimActivities.run_adjudication,
                claim_id,
                start_to_close_timeout=timedelta(minutes=1),
                retry_policy=_STD_RETRY,
            )
            self._status = str(ClaimStatus.ADJUDICATED)
            return True

        if stage == Stage.PERSIST:
            # Checkpoint: the notifier consumes CLAIM_PERSISTED from the bus.
            await self._emit(claim_id, EventType.CLAIM_PERSISTED)
            self._status = str(ClaimStatus.PERSISTED)
            return True

        raise ValueError(f"unknown pipeline stage: {stage}")

    async def _emit(
        self, claim_id: str, event_type: EventType, payload: dict | None = None
    ) -> None:
        await workflow.execute_activity_method(
            ClaimActivities.record_event,
            args=[claim_id, str(event_type), payload or {}],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=_STD_RETRY,
        )
