"""Intake adapters: normalize external submission formats into the canonical claim.

The platform's interoperability seam. Each adapter turns one wire format (FNOL JSON, an
EDI-837-style interchange, portal payloads, ...) into ClaimMetadata; everything downstream —
schema validation, pipeline, adjudication — is format-agnostic. Adding a format is adding an
adapter, not touching the pipeline.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from claimpipe.domain.models import ClaimMetadata


class IntakeError(Exception):
    """Raised when a submission can't be normalized (malformed / missing fields)."""


@runtime_checkable
class IntakeAdapter(Protocol):
    name: str

    def normalize(self, raw: bytes) -> ClaimMetadata:
        """Turn a raw submission body into canonical claim metadata."""
        ...


class FnolIntakeAdapter:
    """First Notice of Loss (auto line): maps carrier FNOL JSON onto the auto-fnol type.

    Expected shape (carrier-side field names, deliberately different from ours):
        {"policyNo": "...", "lossDate": "YYYY-MM-DD", "estimatedAmount": 1234.5,
         "reporter": {"id": "...", "callbackUrl": "..."}, ...extras}
    """

    name = "fnol"

    def normalize(self, raw: bytes) -> ClaimMetadata:
        import json

        try:
            doc = json.loads(raw)
        except ValueError as exc:
            raise IntakeError(f"invalid JSON: {exc}") from None

        reporter = doc.get("reporter") or {}
        missing = [
            k
            for k, v in {
                "policyNo": doc.get("policyNo"),
                "lossDate": doc.get("lossDate"),
                "estimatedAmount": doc.get("estimatedAmount"),
                "reporter.id": reporter.get("id"),
                "reporter.callbackUrl": reporter.get("callbackUrl"),
            }.items()
            if v in (None, "")
        ]
        if missing:
            raise IntakeError(f"missing fields: {missing}")

        return ClaimMetadata(
            customer_id=str(reporter["id"]),
            callback_url=str(reporter["callbackUrl"]),
            claim_type="auto-fnol",
            attributes={
                "policy_number": str(doc["policyNo"]),
                "incident_date": str(doc["lossDate"]),
                "amount": float(doc["estimatedAmount"]),
                # keep unmapped carrier fields for downstream context
                "carrier_extras": {
                    k: v
                    for k, v in doc.items()
                    if k not in {"policyNo", "lossDate", "estimatedAmount", "reporter"}
                },
            },
        )


class X12LikeIntakeAdapter:
    """DEMO-GRADE reader for an X12-837-shaped interchange (segments `~`, elements `*`).

    Deliberately a small subset — NOT a compliant X12 parser (no envelopes/acks/loops).
    It exists to prove the seam: a real 837 adapter (via an EDI library or clearinghouse
    SDK) drops in behind the same Protocol without touching the pipeline.

    Reads: NM1*IL (subscriber id), CLM (claim id + amount), DTP*472 (service date).
    """

    name = "x12-837"

    def __init__(self, callback_url: str = "https://edi-gateway.internal/ack") -> None:
        # EDI submitters don't send webhooks; acks go to the gateway.
        self._callback_url = callback_url

    def normalize(self, raw: bytes) -> ClaimMetadata:
        text = raw.decode("utf-8", errors="replace").strip()
        subscriber: str | None = None
        amount: float | None = None
        service_date: str | None = None

        for segment in (s.strip() for s in text.split("~")):
            parts = segment.split("*")
            match parts[0]:
                case "NM1" if len(parts) > 9 and parts[1] == "IL":
                    subscriber = parts[9]
                case "CLM" if len(parts) > 2:
                    try:
                        amount = float(parts[2])
                    except ValueError:
                        raise IntakeError(f"CLM amount not numeric: {parts[2]}") from None
                case "DTP" if len(parts) > 3 and parts[1] == "472":
                    service_date = parts[3]

        missing = [
            name
            for name, v in [
                ("NM1*IL subscriber", subscriber),
                ("CLM amount", amount),
                ("DTP*472 service date", service_date),
            ]
            if v is None
        ]
        if missing:
            raise IntakeError(f"missing segments: {missing}")

        return ClaimMetadata(
            customer_id=str(subscriber),
            callback_url=self._callback_url,
            # EDI claims arrive structured — no document/OCR, straight to adjudication
            claim_type="structured-claim",
            attributes={"amount": amount, "service_date": service_date},
        )


def default_intake_adapters() -> dict[str, IntakeAdapter]:
    return {a.name: a for a in (FnolIntakeAdapter(), X12LikeIntakeAdapter())}
