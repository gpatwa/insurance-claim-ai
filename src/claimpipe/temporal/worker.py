"""Temporal worker entrypoint. Connects to the (self-hosted) Temporal cluster and
polls the claimpipe task queue.
"""

from __future__ import annotations

import asyncio

import structlog
from temporalio.client import Client
from temporalio.worker import Worker

from claimpipe.config import get_settings
from claimpipe.temporal.activities import ping
from claimpipe.temporal.workflows import PingWorkflow

log = structlog.get_logger()


async def main() -> None:
    settings = get_settings()
    client = await Client.connect(
        settings.temporal_address, namespace=settings.temporal_namespace
    )
    log.info("worker.starting", task_queue=settings.temporal_task_queue)
    worker = Worker(
        client,
        task_queue=settings.temporal_task_queue,
        workflows=[PingWorkflow],
        activities=[ping],
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
