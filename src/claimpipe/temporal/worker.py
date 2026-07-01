"""Temporal worker entrypoint. Connects to the self-hosted Temporal cluster, wires real
dependencies (Postgres repo), and polls the claimpipe task queue.
"""

from __future__ import annotations

import asyncio

import structlog
from temporalio.client import Client
from temporalio.worker import Worker

from claimpipe.config import get_settings
from claimpipe.repository import PostgresClaimRepository
from claimpipe.temporal.activities import ClaimActivities, ping
from claimpipe.temporal.workflows import ClaimWorkflow, PingWorkflow

log = structlog.get_logger()


async def _build_repo(dsn: str) -> PostgresClaimRepository:
    import asyncpg  # local import so CI/tests don't need a running Postgres

    pool = await asyncpg.create_pool(dsn)
    return PostgresClaimRepository(pool)


async def main() -> None:
    settings = get_settings()
    client = await Client.connect(
        settings.temporal_address, namespace=settings.temporal_namespace
    )
    repo = await _build_repo(settings.postgres_dsn)
    acts = ClaimActivities(repo)
    log.info("worker.starting", task_queue=settings.temporal_task_queue)
    worker = Worker(
        client,
        task_queue=settings.temporal_task_queue,
        workflows=[PingWorkflow, ClaimWorkflow],
        activities=[ping, acts.log_metadata],
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
