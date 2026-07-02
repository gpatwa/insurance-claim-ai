"""Temporal worker entrypoint. Connects to the self-hosted Temporal cluster, wires real
dependencies (Postgres event store, S3 object store, OCR), and polls the claimpipe task queue.
"""

from __future__ import annotations

import asyncio

import structlog
from temporalio.client import Client
from temporalio.worker import Worker

from claimpipe.adapters.model_client import AnthropicModelClient
from claimpipe.adapters.object_store import S3ObjectStore
from claimpipe.adapters.ocr import MockOCRClient
from claimpipe.config import get_settings
from claimpipe.eventstore import PostgresEventStore
from claimpipe.temporal.activities import ClaimActivities, ping
from claimpipe.temporal.workflows import ClaimWorkflow, PingWorkflow

log = structlog.get_logger()


async def main() -> None:
    import asyncpg  # local import so CI/tests don't need a running Postgres

    settings = get_settings()
    client = await Client.connect(
        settings.temporal_address, namespace=settings.temporal_namespace
    )
    pool = await asyncpg.create_pool(settings.postgres_dsn)
    store = PostgresEventStore(pool)
    obj = S3ObjectStore(
        bucket=settings.s3_bucket,
        endpoint_url=settings.s3_endpoint_url,
        region=settings.s3_region,
        access_key=settings.s3_access_key,
        secret_key=settings.s3_secret_key,
    )
    ocr = MockOCRClient()  # swap for the real blackbox OCR adapter in deployment

    agent = None
    if settings.use_mock_llm:
        # Dev/smoke mode: deterministic models, no API keys, no agent graph.
        from claimpipe.adapters.model_client import MockModelClient

        cost_model: object = MockModelClient(name="mock-cost", confidence=0.95)
        accuracy_model: object = MockModelClient(
            name="mock-accuracy", version="acc", confidence=0.99
        )
        log.info("worker.mock_llm_enabled")
    else:
        cost_model = AnthropicModelClient(
            model="claude-haiku-4-5", name="claude-haiku-4-5", version="cost"
        )
        accuracy_model = AnthropicModelClient(
            model="claude-opus-4-8", name="claude-opus-4-8", version="accuracy"
        )
        from claimpipe.agent import ClaimReviewAgent

        agent = ClaimReviewAgent(extractor=accuracy_model, critic=cost_model)

    from claimpipe.refdata import InMemoryRefData, PolicyRecord
    from claimpipe.tenancy import default_directory

    refdata = InMemoryRefData()  # swap for the policy-admin adapter in deployment
    if settings.refdata_file:
        import json

        with open(settings.refdata_file, encoding="utf-8") as fh:  # noqa: ASYNC230 - one-time startup read
            records = json.load(fh)
        for rec in records:
            refdata.add(PolicyRecord(**rec))
        log.info("worker.refdata_seeded", count=len(records), file=settings.refdata_file)
    tenants = default_directory()
    acts = ClaimActivities(
        store,
        object_store=obj,
        ocr=ocr,
        cost_model=cost_model,
        accuracy_model=accuracy_model,
        agent=agent,
        confidence_threshold=settings.confidence_threshold,
        high_value_amount=settings.high_value_amount,
        refdata=refdata,
        tenants=tenants,
    )
    log.info("worker.starting", task_queue=settings.temporal_task_queue)
    worker = Worker(
        client,
        task_queue=settings.temporal_task_queue,
        workflows=[PingWorkflow, ClaimWorkflow],
        activities=[ping, acts.record_event, acts.run_ocr, acts.run_llm, acts.run_adjudication],
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
