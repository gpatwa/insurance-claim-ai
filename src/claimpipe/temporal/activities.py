"""Temporal activities. All I/O (OCR, LLM, DB, object store, webhooks) lives here;
workflows stay deterministic.
"""

from __future__ import annotations

from temporalio import activity


@activity.defn
async def ping(name: str) -> str:
    """M0 smoke activity — proves the worker executes activities end to end."""
    activity.logger.info("ping", extra={"name": name})
    return f"pong:{name}"
