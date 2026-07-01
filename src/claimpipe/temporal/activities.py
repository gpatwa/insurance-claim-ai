"""Temporal activities. All I/O (DB, OCR, LLM, object store, webhooks) lives here;
workflows stay deterministic.

Activities are methods on a class holding injected dependencies (repo, adapters). The worker
binds real deps; tests bind fakes — same code path either way.
"""

from __future__ import annotations

from temporalio import activity

from claimpipe.repository import ClaimRepository


@activity.defn
async def ping(name: str) -> str:
    """M0 smoke activity — proves the worker executes activities end to end."""
    activity.logger.info("ping")
    return f"pong:{name}"


class ClaimActivities:
    def __init__(self, repo: ClaimRepository) -> None:
        self._repo = repo

    @activity.defn
    async def log_metadata(self, claim_id: str) -> None:
        """Design stage A: log/record claim metadata. Touches updated_at to prove the
        workflow → activity → repo path ran."""
        activity.logger.info("log_metadata", extra={"claim_id": claim_id})
        await self._repo.touch(claim_id)
