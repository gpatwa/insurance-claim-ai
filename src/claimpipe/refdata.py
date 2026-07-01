"""Reference (master) data: the source adjudication facts are grounded in.

Submission attributes are what the claimant says; reference data is what the carrier knows
(policy status, limits — later members, providers, fee schedules). The adjudication activity
enriches facts from here before rules run, so decisions rest on both.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from pydantic import BaseModel


class PolicyRecord(BaseModel):
    policy_number: str
    status: str  # "active" | "inactive" | "lapsed"
    line: str = ""
    limit: float | None = None


@runtime_checkable
class RefDataSource(Protocol):
    async def get_policy(self, policy_number: str) -> PolicyRecord | None: ...


class InMemoryRefData:
    """Test/dev implementation; a real one fronts the policy-admin system of record."""

    def __init__(self, policies: dict[str, PolicyRecord] | None = None) -> None:
        self._policies = policies or {}

    def add(self, record: PolicyRecord) -> None:
        self._policies[record.policy_number] = record

    async def get_policy(self, policy_number: str) -> PolicyRecord | None:
        return self._policies.get(policy_number)


async def enrich_facts(refdata: RefDataSource | None, facts: dict) -> dict:
    """Fold reference data into adjudication facts (no-op when no source is wired).

    Adds: policy_found (bool) and policy_status / policy_limit when the policy exists.
    """
    if refdata is None or "policy_number" not in facts:
        return facts
    record = await refdata.get_policy(str(facts["policy_number"]))
    enriched = dict(facts)
    enriched["policy_found"] = record is not None
    if record is not None:
        enriched["policy_status"] = record.status
        if record.limit is not None:
            enriched["policy_limit"] = record.limit
    return enriched
