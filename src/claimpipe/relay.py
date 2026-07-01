"""Outbox relay: publishes unpublished events from the store's outbox to the message bus.

At-least-once delivery — consumers dedupe on event_id. Runs as its own process so the write
path (Temporal activities) never blocks on Kafka.
"""

from __future__ import annotations

import asyncio

import structlog

from claimpipe.adapters.bus import MessageBus
from claimpipe.eventstore import EventStore

log = structlog.get_logger()


class OutboxRelay:
    def __init__(self, store: EventStore, bus: MessageBus, topic: str = "claim-events") -> None:
        self._store = store
        self._bus = bus
        self._topic = topic

    async def run_once(self, batch: int = 100) -> int:
        events = await self._store.fetch_unpublished(limit=batch)
        for ev in events:
            await self._bus.publish(self._topic, ev.claim_id, ev.model_dump_json().encode())
        if events:
            await self._store.mark_published([ev.event_id for ev in events])
        return len(events)

    async def run(self, interval: float = 1.0) -> None:
        while True:
            n = await self.run_once()
            if n == 0:
                await asyncio.sleep(interval)


async def main() -> None:
    import asyncpg

    from claimpipe.adapters.bus import KafkaBus
    from claimpipe.config import get_settings
    from claimpipe.eventstore import PostgresEventStore

    settings = get_settings()
    pool = await asyncpg.create_pool(settings.postgres_dsn)
    bus = KafkaBus(settings.kafka_bootstrap)
    await bus.start()
    relay = OutboxRelay(PostgresEventStore(pool), bus, topic=settings.event_topic)
    log.info("relay.starting", topic=settings.event_topic)
    try:
        await relay.run()
    finally:
        await bus.stop()


if __name__ == "__main__":
    asyncio.run(main())
