"""Temporal workflows. Deterministic orchestration only — no direct I/O.

The workflow advances the ClaimStatus state machine; the LLM never drives transitions.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from claimpipe.temporal.activities import ClaimActivities, ping


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
    """Per-claim durable workflow. M1: log the metadata (stage A). Later milestones append
    OCR → LLM → persist → notify stages."""

    @workflow.run
    async def run(self, claim_id: str) -> str:
        await workflow.execute_activity_method(
            ClaimActivities.log_metadata,
            claim_id,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=5),
        )
        return claim_id
