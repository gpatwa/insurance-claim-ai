"""Customer front door: API-key authentication mapped to customers, roles, and tenants.

Every API key resolves to a Customer that carries:
  - customer_id  — stamped onto submitted claims (never trusted from the request body)
  - tenant_id    — which tenant configuration applies (never trusted from a header)
  - roles        — "submit" (claims in/out for OWN claims) and/or "review" (work queue)

Keys are stored as SHA-256 hashes; the plaintext exists only in the caller's credential
store. Predefined dev customers below ship with documented plaintext keys for local use —
in production, load customers from CLAIMPIPE_CUSTOMERS_FILE (or a real IdP) instead.
"""

from __future__ import annotations

import hashlib

from pydantic import BaseModel, Field

API_KEY_HEADER = "X-API-Key"

ROLE_SUBMIT = "submit"
ROLE_REVIEW = "review"


def hash_key(api_key: str) -> str:
    return hashlib.sha256(api_key.encode()).hexdigest()


class Customer(BaseModel):
    customer_id: str
    name: str = ""
    tenant_id: str = "default"
    roles: set[str] = Field(default_factory=lambda: {ROLE_SUBMIT})

    def can(self, role: str) -> bool:
        return role in self.roles


class CustomerRegistry:
    """Keyed by SHA-256 of the API key."""

    def __init__(self) -> None:
        self._by_key_hash: dict[str, Customer] = {}

    def register(self, api_key: str, customer: Customer) -> None:
        self._by_key_hash[hash_key(api_key)] = customer

    def register_hashed(self, key_hash: str, customer: Customer) -> None:
        self._by_key_hash[key_hash] = customer

    def authenticate(self, api_key: str | None) -> Customer | None:
        if not api_key:
            return None
        return self._by_key_hash.get(hash_key(api_key))


# Predefined dev customers (plaintext keys documented for local/demo use ONLY).
DEV_KEYS = {
    "ck_acme_submitter_01": Customer(
        customer_id="acme-carrier",
        name="ACME Carrier (portal submitter)",
        tenant_id="default",
        roles={ROLE_SUBMIT},
    ),
    "ck_lakeside_clearing_01": Customer(
        customer_id="lakeside-clearinghouse",
        name="Lakeside PT via clearinghouse (EDI submitter)",
        tenant_id="default",
        roles={ROLE_SUBMIT},
    ),
    "ck_payer_reviewer_01": Customer(
        customer_id="payer-adjusters",
        name="Payer adjuster team (review only)",
        tenant_id="default",
        roles={ROLE_REVIEW},
    ),
    "ck_dev_all_01": Customer(
        customer_id="dev-integration",
        name="Dev/integration key (submit + review)",
        tenant_id="default",
        roles={ROLE_SUBMIT, ROLE_REVIEW},
    ),
}


def default_customers() -> CustomerRegistry:
    reg = CustomerRegistry()
    for key, customer in DEV_KEYS.items():
        reg.register(key, customer)
    return reg


def load_customers_file(path: str) -> CustomerRegistry:
    """Production loading: JSON list of {key_hash, customer_id, name, tenant_id, roles}."""
    import json

    reg = CustomerRegistry()
    with open(path, encoding="utf-8") as fh:
        for entry in json.load(fh):
            key_hash = entry.pop("key_hash")
            entry["roles"] = set(entry.get("roles", [ROLE_SUBMIT]))
            reg.register_hashed(key_hash, Customer(**entry))
    return reg
