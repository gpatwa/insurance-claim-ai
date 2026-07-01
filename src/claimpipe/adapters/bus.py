"""Message bus adapter — the event *transport* (Kafka), distinct from the event *store*.

Kafka is the fan-out pipe, not the source of truth. InMemory backs CI; Kafka/Redpanda in
deployment. The rest of the code depends only on the Protocol.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class MessageBus(Protocol):
    async def publish(self, topic: str, key: str, value: bytes) -> None: ...


class InMemoryBus:
    """Records published messages for assertions in tests."""

    def __init__(self) -> None:
        self.published: list[tuple[str, str, bytes]] = []

    async def publish(self, topic: str, key: str, value: bytes) -> None:
        self.published.append((topic, key, value))


class KafkaBus:
    """aiokafka-backed producer (Kafka / Redpanda). Deployment impl; not in CI."""

    def __init__(self, bootstrap_servers: str) -> None:
        self._bootstrap = bootstrap_servers
        self._producer = None

    async def start(self) -> None:
        from aiokafka import AIOKafkaProducer

        self._producer = AIOKafkaProducer(bootstrap_servers=self._bootstrap)
        await self._producer.start()

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()

    async def publish(self, topic: str, key: str, value: bytes) -> None:
        assert self._producer is not None, "call start() first"
        await self._producer.send_and_wait(topic, key=key.encode(), value=value)
