"""OCR adapter — the blackbox OCR service behind a swappable Protocol."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class OCRClient(Protocol):
    async def extract_text(self, pdf: bytes) -> str:
        """Run OCR over PDF bytes and return extracted text."""
        ...


class MockOCRClient:
    """Deterministic mock for tests/local dev."""

    async def extract_text(self, pdf: bytes) -> str:
        return f"OCR_TEXT[{len(pdf)} bytes] lorem ipsum claim body"
