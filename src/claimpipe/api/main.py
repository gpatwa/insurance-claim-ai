"""Real API entrypoint: builds Postgres event store + Temporal client, serves with uvicorn.

Everything runs on ONE event loop: asyncpg connections are bound to the loop they were
created on, so the pool must be built inside the same loop uvicorn serves from (building it
in a separate startup loop breaks every request with "attached to a different loop").
"""

from __future__ import annotations

import asyncio

import uvicorn
from temporalio.client import Client

from claimpipe.adapters.object_store import S3ObjectStore
from claimpipe.api.app import create_app
from claimpipe.config import get_settings
from claimpipe.customers import default_customers, load_customers_file
from claimpipe.eventstore import PostgresEventStore


async def serve() -> None:
    import asyncpg

    settings = get_settings()
    client = await Client.connect(
        settings.temporal_address, namespace=settings.temporal_namespace
    )
    pool = await asyncpg.create_pool(settings.postgres_dsn)
    obj = S3ObjectStore(
        bucket=settings.s3_bucket,
        endpoint_url=settings.s3_endpoint_url,
        region=settings.s3_region,
        access_key=settings.s3_access_key,
        secret_key=settings.s3_secret_key,
    )
    customers = (
        load_customers_file(settings.customers_file)
        if settings.customers_file
        else default_customers()
    )
    app = create_app(
        store=PostgresEventStore(pool),
        temporal_client=client,
        settings=settings,
        object_store=obj,
        customers=customers,
    )
    config = uvicorn.Config(app, host="0.0.0.0", port=settings.api_port, log_level="info")
    await uvicorn.Server(config).serve()


def main() -> None:
    asyncio.run(serve())


if __name__ == "__main__":
    main()
