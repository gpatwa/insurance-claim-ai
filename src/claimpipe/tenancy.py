"""Multi-tenant configuration: each tenant gets its own claim-type registry + rule sets.

One platform deployment serves many carriers/business units; what differs per tenant is
configuration (types, schemas, pipelines, rules) — never code. The "default" tenant keeps
single-tenant deployments zero-config.
"""

from __future__ import annotations

from claimpipe.adjudication import RuleSet, default_rulesets
from claimpipe.claimtypes import ClaimTypeRegistry, default_registry

DEFAULT_TENANT = "default"
TENANT_HEADER = "X-Tenant-ID"


class UnknownTenant(Exception):
    pass


class TenantConfig:
    def __init__(
        self,
        name: str,
        *,
        registry: ClaimTypeRegistry | None = None,
        rulesets: dict[str, RuleSet] | None = None,
    ) -> None:
        self.name = name
        self.registry = registry if registry is not None else default_registry()
        self.rulesets = rulesets if rulesets is not None else default_rulesets()


class TenantDirectory:
    def __init__(self, tenants: dict[str, TenantConfig] | None = None) -> None:
        self._tenants = tenants or {}
        if DEFAULT_TENANT not in self._tenants:
            self._tenants[DEFAULT_TENANT] = TenantConfig(DEFAULT_TENANT)

    def get(self, name: str) -> TenantConfig:
        try:
            return self._tenants[name]
        except KeyError:
            raise UnknownTenant(name) from None

    def names(self) -> list[str]:
        return sorted(self._tenants)


def default_directory() -> TenantDirectory:
    return TenantDirectory()
