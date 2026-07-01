"""Claim-type registry: the generalization seam that turns the pipeline into a platform.

A ClaimTypeDef declares, as *data*:
  - the attribute schema its metadata must satisfy (validated at intake), and
  - the ordered pipeline stages the workflow engine executes for it.

Adding a line of business (health-837, auto FNOL, property, ...) is registering a new
definition — not writing a new workflow. Definitions are versioned; the resolved stage list is
pinned into the workflow input at submission, so in-flight claims are unaffected by later
registry changes.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field


class Stage(StrEnum):
    """Pipeline stage vocabulary the workflow engine knows how to execute."""

    UPLOAD = "UPLOAD"  # dormancy gate: wait for document upload
    OCR = "OCR"  # extract text from the uploaded document
    LLM = "LLM"  # tiered model routing (+ agent escalation)
    ADJUDICATE = "ADJUDICATE"  # deterministic rules decide APPROVE/DENY/PEND + reasons
    REVIEW = "REVIEW"  # dormancy gate: PEND decisions wait for a human reviewer
    PERSIST = "PERSIST"  # checkpoint: predictions/record final, triggers notify consumer


class AttributeSpec(BaseModel):
    """One field of a claim type's metadata-attributes schema."""

    name: str
    type: str = Field(pattern="^(string|number|boolean)$")
    required: bool = False


class InvalidPipeline(Exception):
    pass


class ClaimTypeDef(BaseModel):
    name: str = Field(min_length=1)
    version: str = "v1"
    description: str = ""
    attributes: list[AttributeSpec] = Field(default_factory=list)
    stages: list[Stage]

    def validate_pipeline(self) -> None:
        """Structural constraints on the stage list (checked at registration)."""
        if len(self.stages) != len(set(self.stages)):
            raise InvalidPipeline(f"{self.name}: duplicate stages")
        if not self.stages or self.stages[-1] is not Stage.PERSIST:
            raise InvalidPipeline(f"{self.name}: pipeline must end with PERSIST")
        if Stage.OCR in self.stages and Stage.UPLOAD not in self.stages:
            raise InvalidPipeline(f"{self.name}: OCR requires UPLOAD before it")
        if Stage.LLM in self.stages and Stage.OCR not in self.stages:
            raise InvalidPipeline(f"{self.name}: LLM requires OCR before it")
        if Stage.REVIEW in self.stages and Stage.ADJUDICATE not in self.stages:
            raise InvalidPipeline(f"{self.name}: REVIEW requires ADJUDICATE before it")
        order = {s: i for i, s in enumerate(self.stages)}
        for earlier, later in [
            (Stage.UPLOAD, Stage.OCR),
            (Stage.OCR, Stage.LLM),
            (Stage.LLM, Stage.ADJUDICATE),
            (Stage.ADJUDICATE, Stage.REVIEW),
        ]:
            if earlier in order and later in order and order[earlier] > order[later]:
                raise InvalidPipeline(f"{self.name}: {earlier} must precede {later}")


def validate_attributes(defn: ClaimTypeDef, attributes: dict) -> list[str]:
    """Validate metadata attributes against the claim type's schema; return error strings."""
    errors: list[str] = []
    py_types = {"string": str, "number": (int, float), "boolean": bool}
    for spec in defn.attributes:
        if spec.name not in attributes:
            if spec.required:
                errors.append(f"missing required attribute '{spec.name}'")
            continue
        value = attributes[spec.name]
        expected = py_types[spec.type]
        # bool is a subclass of int — reject bools where numbers are expected
        if spec.type == "number" and isinstance(value, bool):
            errors.append(f"attribute '{spec.name}' must be a number")
        elif not isinstance(value, expected):
            errors.append(f"attribute '{spec.name}' must be a {spec.type}")
    return errors


class UnknownClaimType(Exception):
    pass


class ClaimTypeRegistry:
    def __init__(self) -> None:
        self._types: dict[str, ClaimTypeDef] = {}

    def register(self, defn: ClaimTypeDef) -> None:
        defn.validate_pipeline()
        self._types[defn.name] = defn

    def get(self, name: str) -> ClaimTypeDef:
        try:
            return self._types[name]
        except KeyError:
            raise UnknownClaimType(name) from None

    def names(self) -> list[str]:
        return sorted(self._types)


DEFAULT_CLAIM_TYPE = "generic-document"


def default_registry() -> ClaimTypeRegistry:
    """Seed registry. Line-agnostic core: a full-pipeline default plus two synthetic types
    that prove stages are config, not code."""
    reg = ClaimTypeRegistry()
    reg.register(
        ClaimTypeDef(
            name=DEFAULT_CLAIM_TYPE,
            description="PDF + JSON claim: OCR, tiered LLM scoring, adjudicate, persist.",
            attributes=[AttributeSpec(name="amount", type="number", required=False)],
            stages=[
                Stage.UPLOAD,
                Stage.OCR,
                Stage.LLM,
                Stage.ADJUDICATE,
                Stage.REVIEW,
                Stage.PERSIST,
            ],
        )
    )
    reg.register(
        ClaimTypeDef(
            name="metadata-only",
            description="Structured-data claim with no document: validate and persist.",
            stages=[Stage.PERSIST],
        )
    )
    reg.register(
        ClaimTypeDef(
            name="archive-document",
            description="Document archival: OCR + store, no model scoring.",
            stages=[Stage.UPLOAD, Stage.OCR, Stage.PERSIST],
        )
    )
    reg.register(
        ClaimTypeDef(
            name="auto-fnol",
            description="Demo FNOL line with a required attribute schema.",
            attributes=[
                AttributeSpec(name="policy_number", type="string", required=True),
                AttributeSpec(name="incident_date", type="string", required=True),
                AttributeSpec(name="amount", type="number", required=True),
            ],
            stages=[
                Stage.UPLOAD,
                Stage.OCR,
                Stage.LLM,
                Stage.ADJUDICATE,
                Stage.REVIEW,
                Stage.PERSIST,
            ],
        )
    )
    return reg
