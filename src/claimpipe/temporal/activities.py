"""Temporal activities. All I/O (DB, OCR, LLM, object store, webhooks) lives here;
workflows stay deterministic.

Activities are methods on a class holding injected dependencies. The worker binds real deps;
tests bind fakes — same code path either way.
"""

from __future__ import annotations

from temporalio import activity

from claimpipe.adapters.object_store import ObjectStore
from claimpipe.adapters.ocr import OCRClient
from claimpipe.domain.models import ClaimStatus
from claimpipe.repository import ClaimRepository


@activity.defn
async def ping(name: str) -> str:
    """M0 smoke activity."""
    activity.logger.info("ping")
    return f"pong:{name}"


class ClaimActivities:
    def __init__(
        self,
        repo: ClaimRepository,
        object_store: ObjectStore | None = None,
        ocr: OCRClient | None = None,
    ) -> None:
        self._repo = repo
        self._store = object_store
        self._ocr = ocr

    @activity.defn
    async def log_metadata(self, claim_id: str) -> None:
        """Design stage A: log/record claim metadata."""
        activity.logger.info("log_metadata")
        await self._repo.touch(claim_id)

    @activity.defn
    async def set_status(self, claim_id: str, status: str) -> None:
        await self._repo.set_status(claim_id, ClaimStatus(status))

    @activity.defn
    async def run_ocr(self, claim_id: str, pdf_key: str) -> str:
        """Design stage B: read PDF from object store, OCR it, store the text, record the ref.
        Retries/backoff are configured on the workflow side."""
        assert self._store is not None and self._ocr is not None
        pdf = await self._store.get(pdf_key)
        text = await self._ocr.extract_text(pdf)
        ocr_key = f"{claim_id}/ocr.txt"
        ref = await self._store.put(ocr_key, text.encode("utf-8"), content_type="text/plain")
        await self._repo.set_ocr_ref(claim_id, ref)
        return ref
