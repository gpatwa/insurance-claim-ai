"""Adjudication core: deterministic, versioned decision tables.

Rules DECIDE (approve/deny/pend + reason codes); the LLM only PREPARES facts. This is the
compliance-safe split: every decision is reproducible from (rule set version, facts), both of
which are recorded on the CLAIM_ADJUDICATED event — an audit trail by construction.

Semantics: rules evaluate in order, first match wins (decision-table style). A rule matches
when ALL its conditions hold. If nothing matches, the claim PENDs (never auto-approve by
fallthrough). Conditions are a small closed predicate vocabulary — data, not code, so rule
sets can be tuned without a deploy.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field


class Decision(StrEnum):
    APPROVE = "APPROVE"
    DENY = "DENY"
    PEND = "PEND"  # needs human review (work queue lands in a later milestone)


NO_RULE_MATCHED = "NO_RULE_MATCHED"


class Condition(BaseModel):
    """One predicate over the facts dict. Missing fields fail every op except *_exists."""

    field: str
    op: str = Field(pattern="^(eq|ne|gt|gte|lt|lte|in|exists|not_exists)$")
    value: object | None = None

    def matches(self, facts: dict) -> bool:
        present = self.field in facts and facts[self.field] is not None
        if self.op == "exists":
            return present
        if self.op == "not_exists":
            return not present
        if not present:
            return False
        actual = facts[self.field]
        match self.op:
            case "eq":
                return actual == self.value
            case "ne":
                return actual != self.value
            case "in":
                return isinstance(self.value, (list, tuple)) and actual in self.value
            case "gt" | "gte" | "lt" | "lte":
                try:
                    a, b = float(actual), float(self.value)  # type: ignore[arg-type]
                except (TypeError, ValueError):
                    return False
                return {
                    "gt": a > b,
                    "gte": a >= b,
                    "lt": a < b,
                    "lte": a <= b,
                }[self.op]
        return False  # pragma: no cover


class Rule(BaseModel):
    rule_id: str
    description: str = ""
    conditions: list[Condition] = Field(default_factory=list)  # empty = catch-all
    outcome: Decision
    reason_code: str

    def matches(self, facts: dict) -> bool:
        return all(c.matches(facts) for c in self.conditions)


class RuleSet(BaseModel):
    name: str
    version: str = "v1"
    rules: list[Rule]


class AdjudicationResult(BaseModel):
    decision: Decision
    reason_codes: list[str]
    rule_id: str | None
    rule_set: str
    rule_set_version: str
    facts: dict


def adjudicate(rule_set: RuleSet, facts: dict) -> AdjudicationResult:
    for rule in rule_set.rules:
        if rule.matches(facts):
            return AdjudicationResult(
                decision=rule.outcome,
                reason_codes=[rule.reason_code],
                rule_id=rule.rule_id,
                rule_set=rule_set.name,
                rule_set_version=rule_set.version,
                facts=facts,
            )
    # Safe default: an unmatched claim is never silently approved.
    return AdjudicationResult(
        decision=Decision.PEND,
        reason_codes=[NO_RULE_MATCHED],
        rule_id=None,
        rule_set=rule_set.name,
        rule_set_version=rule_set.version,
        facts=facts,
    )


def default_rulesets() -> dict[str, RuleSet]:
    """Seed rule sets keyed by claim type. Tunable data — swap for a DB/config source later."""
    review_and_confidence = [
        Rule(
            rule_id="pend-escalated",
            description="Escalated claims need human review",
            conditions=[Condition(field="escalated", op="eq", value=True)],
            outcome=Decision.PEND,
            reason_code="REVIEW_REQUIRED",
        ),
        Rule(
            rule_id="pend-low-confidence",
            description="Model confidence below the auto-processing floor",
            conditions=[Condition(field="confidence", op="lt", value=0.6)],
            outcome=Decision.PEND,
            reason_code="LOW_CONFIDENCE",
        ),
        Rule(
            rule_id="auto-approve",
            description="Confident, unescalated claims auto-approve",
            outcome=Decision.APPROVE,
            reason_code="AUTO_APPROVED",
        ),
    ]
    deny_over_limit = Rule(
        rule_id="deny-over-limit",
        description="Claimed amount exceeds the policy line limit",
        conditions=[Condition(field="amount", op="gt", value=100_000)],
        outcome=Decision.DENY,
        reason_code="EXCEEDS_LIMIT",
    )
    # Reference-data-grounded rules (facts only present when a RefDataSource is wired).
    # Coverage checks come before amount checks: an inactive policy is denied regardless.
    policy_rules = [
        Rule(
            rule_id="deny-policy-inactive",
            description="Policy is not active per the policy-admin system of record",
            conditions=[Condition(field="policy_status", op="ne", value="active")],
            outcome=Decision.DENY,
            reason_code="POLICY_INACTIVE",
        ),
        Rule(
            rule_id="pend-policy-not-found",
            description="Claimed policy number is unknown to reference data",
            conditions=[Condition(field="policy_found", op="eq", value=False)],
            outcome=Decision.PEND,
            reason_code="POLICY_NOT_FOUND",
        ),
    ]
    return {
        "generic-document": RuleSet(name="generic-document", rules=review_and_confidence),
        "auto-fnol": RuleSet(
            name="auto-fnol",
            rules=[*policy_rules, deny_over_limit, *review_and_confidence],
        ),
        # Structured (e.g. EDI) claims: no model ran, so no confidence/escalation facts —
        # decide on the claim data alone; anything unusual still PENDs via fallthrough rules.
        "structured-claim": RuleSet(
            name="structured-claim",
            rules=[
                deny_over_limit,
                Rule(
                    rule_id="pend-high-value",
                    description="High-value structured claims get a human look",
                    conditions=[Condition(field="amount", op="gte", value=25_000)],
                    outcome=Decision.PEND,
                    reason_code="REVIEW_REQUIRED",
                ),
                Rule(
                    rule_id="auto-approve",
                    description="Routine structured claims auto-approve",
                    outcome=Decision.APPROVE,
                    reason_code="AUTO_APPROVED",
                ),
            ],
        ),
    }
