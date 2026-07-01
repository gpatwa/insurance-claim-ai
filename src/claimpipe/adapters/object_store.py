"""Object-store adapter.

Portable S3 API behind a Protocol: InMemory for tests, MinIO/S3/GCS/Blob in deployment.
The rest of the codebase depends only on the Protocol.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class ObjectStore(Protocol):
    async def put(
        self, key: str, data: bytes, content_type: str = "application/octet-stream"
    ) -> str:
        """Store bytes at key; return a reference (e.g. s3://bucket/key)."""
        ...

    async def get(self, key: str) -> bytes: ...

    async def exists(self, key: str) -> bool: ...


class InMemoryObjectStore:
    """Test/dev implementation. Not for production."""

    def __init__(self, bucket: str = "claims") -> None:
        self._bucket = bucket
        self._store: dict[str, bytes] = {}

    async def put(
        self, key: str, data: bytes, content_type: str = "application/octet-stream"
    ) -> str:
        self._store[key] = data
        return f"mem://{self._bucket}/{key}"

    async def get(self, key: str) -> bytes:
        return self._store[key]

    async def exists(self, key: str) -> bool:
        return key in self._store
