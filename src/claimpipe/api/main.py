"""Real API entrypoint: builds Postgres event store + Temporal client, serves with uvicorn."""

from __future__ import annotations

import asyncio

import uvicorn
from temporalio.client import Client

from claimpipe.api.app import create_app
from claimpipe.config import get_settings
from claimpipe.eventstore import PostgresEventStore


async def _build():
    import asyncpg

    settings = get_settings()
    client = await Client.connect(
        settings.temporal_address, namespace=settings.temporal_namespace
    )
    pool = await asyncpg.create_pool(settings.postgres_dsn)
    return create_app(
        store=PostgresEventStore(pool), temporal_client=client, settings=settings
    )


def main() -> None:
    app = asyncio.get_event_loop().run_until_complete(_build())
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
