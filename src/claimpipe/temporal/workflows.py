"""Temporal workflows. Deterministic orchestration only — no direct I/O.

The workflow advances the ClaimStatus state machine; the LLM never drives transitions.
Long waits (PDF upload) are event-driven dormancy gates (a signal + wait_condition), not polling.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from claimpipe.domain.models import ClaimStatus
    from claimpipe.temporal.activities import ClaimActivities, ping

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

    @workflow.signal
    def pdf_uploaded(self, pdf_key: str) -> None:
        """Sent once the client has PUT the PDF to the signed URL."""
        self._pdf_key = pdf_key
        self._uploaded = True

    @workflow.run
    async def run(self, claim_id: str) -> str:
        await workflow.execute_activity_method(
            ClaimActivities.log_metadata,
            claim_id,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=5),
        )

        # Dormancy gate: wait (durably, scale-to-zero) for the upload-complete signal.
        try:
            await workflow.wait_condition(lambda: self._uploaded, timeout=timedelta(days=7))
        except TimeoutError:
            await self._set_status(claim_id, ClaimStatus.FAILED)
            return claim_id
        assert self._pdf_key is not None

        await self._set_status(claim_id, ClaimStatus.OCR_RUNNING)
        await workflow.execute_activity_method(
            ClaimActivities.run_ocr,
            args=[claim_id, self._pdf_key],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=_OCR_RETRY,
        )
        await self._set_status(claim_id, ClaimStatus.OCR_DONE)
        return claim_id

    async def _set_status(self, claim_id: str, status: ClaimStatus) -> None:
        await workflow.execute_activity_method(
            ClaimActivities.set_status,
            args=[claim_id, str(status)],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=5),
        )
