"""Temporal workflows. Deterministic orchestration only — no direct I/O."""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from claimpipe.temporal.activities import ping


@workflow.defn
class PingWorkflow:
    """M0 smoke workflow: calls the ping activity and returns its result."""

    @workflow.run
    async def run(self, name: str) -> str:
        return await workflow.execute_activity(
            ping,
            name,
            start_to_close_timeout=timedelta(seconds=10),
        )
