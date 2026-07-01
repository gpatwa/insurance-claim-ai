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


class S3ObjectStore:
    """S3-compatible store (MinIO/S3/GCS/Blob) via aioboto3. Deployment impl; not in CI."""

    def __init__(
        self,
        *,
        bucket: str,
        endpoint_url: str | None,
        region: str,
        access_key: str,
        secret_key: str,
    ) -> None:
        self._bucket = bucket
        self._endpoint_url = endpoint_url
        self._region = region
        self._access_key = access_key
        self._secret_key = secret_key

    def _session(self):
        import aioboto3

        return aioboto3.Session().client(
            "s3",
            endpoint_url=self._endpoint_url,
            region_name=self._region,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
        )

    async def put(
        self, key: str, data: bytes, content_type: str = "application/octet-stream"
    ) -> str:
        async with self._session() as s3:
            await s3.put_object(Bucket=self._bucket, Key=key, Body=data, ContentType=content_type)
        return f"s3://{self._bucket}/{key}"

    async def get(self, key: str) -> bytes:
        async with self._session() as s3:
            resp = await s3.get_object(Bucket=self._bucket, Key=key)
            return await resp["Body"].read()

    async def exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError

        async with self._session() as s3:
            try:
                await s3.head_object(Bucket=self._bucket, Key=key)
                return True
            except ClientError:
                return False
