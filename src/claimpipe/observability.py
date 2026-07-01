"""Observability: structured JSON logging with per-claim context.

Every log line inside `claim_context(claim_id)` carries the claim_id, so traces/logs are
filterable per claim across API, workflow, and consumers. OpenTelemetry tracing (deployment)
is wired in `tracing.py`; this module is the always-on, dependency-light baseline.
"""

from __future__ import annotations

from contextlib import contextmanager

import structlog


def configure_logging() -> None:
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ]
    )


@contextmanager
def claim_context(claim_id: str):
    """Bind claim_id to all log lines emitted within the block."""
    structlog.contextvars.bind_contextvars(claim_id=claim_id)
    try:
        yield
    finally:
        structlog.contextvars.unbind_contextvars("claim_id")
