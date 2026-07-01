"""M0 end-to-end smoke tests.

The workflow test uses Temporal's in-process time-skipping test environment — no external
server or Docker required, so it runs hermetically in CI.
"""

from __future__ import annotations

import uuid

import pytest
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from claimpipe.adapters.model_client import MockModelClient
from claimpipe.adapters.object_store import InMemoryObjectStore
from claimpipe.adapters.ocr import MockOCRClient
from claimpipe.domain.models import ClaimStatus
from claimpipe.temporal.activities import ping
from claimpipe.temporal.workflows import PingWorkflow


async def test_ping_workflow_end_to_end() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="claimpipe-test",
            workflows=[PingWorkflow],
            activities=[ping],
        ):
            result = await env.client.execute_workflow(
                PingWorkflow.run,
                "m0",
                id=f"ping-{uuid.uuid4()}",
                task_queue="claimpipe-test",
            )
    assert result == "pong:m0"


async def test_object_store_roundtrip() -> None:
    store = InMemoryObjectStore()
    ref = await store.put("claims/c1/pdf", b"%PDF-1.7 ...")
    assert ref.endswith("claims/c1/pdf")
    assert await store.exists("claims/c1/pdf")
    assert await store.get("claims/c1/pdf") == b"%PDF-1.7 ..."


async def test_mock_ocr_and_model() -> None:
    ocr = MockOCRClient()
    text = await ocr.extract_text(b"%PDF-1.7 " + b"x" * 100)
    assert "OCR_TEXT" in text

    model = MockModelClient()
    pred = await model.predict(text)
    assert 0.0 <= pred.confidence <= 1.0
    assert pred.model_name == "mock-cost-tier"


def test_status_state_machine_has_terminal_states() -> None:
    # guardrails: the enum is the source of truth for control flow
    assert ClaimStatus.RECEIVED != ClaimStatus.NOTIFIED
    assert ClaimStatus("PARTIAL_SUCCESS") is ClaimStatus.PARTIAL_SUCCESS


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
