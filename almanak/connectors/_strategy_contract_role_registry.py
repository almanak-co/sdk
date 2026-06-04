"""Strategy-side contract-role registration site (VIB-4928 PR-3a).

Sibling of :mod:`almanak.connectors._strategy_flash_loan_registry`, scoped to
the contract-role concern. The intent compiler's six per-chain address tables
(``PROTOCOL_ROUTERS``, ``LP_POSITION_MANAGERS``, ``SWAP_QUOTER_ADDRESSES``,
``LENDING_POOL_ADDRESSES``, ``LENDING_POOL_DATA_PROVIDERS``,
``BALANCER_VAULT_ADDRESSES``) fan out over this registry instead of
hand-importing each connector's ``addresses.py``. Adding an address-owning
connector means one ``contract_roles.py`` data module plus a
``CONNECTOR.contract_roles`` import reference in the connector's own manifest,
not five edits across ``compiler_constants.py``.

Lives one level up from ``_strategy_base/`` because it owns strategy-side
registry bootstrap; ``_strategy_base/`` stays protocol-clean (no concrete
connector imports). This file imports only connector manifests.

**Registration order is load-bearing.** The compiler iterates
``CONTRACT_ROLE_REGISTRY.protocols_with_role(role)`` (registration order,
filtered to the role) as the *outer* loop, and the per-protocol key order
within each chain's sub-dict is exactly that iteration order. The manifest
``contract_roles.order`` values match the pre-PR-3a hand-rolled ``_build_*``
source-list order (the longest list, ``_build_lp_position_managers``, is the
canonical superset; the ``agni_finance`` slug rides immediately after
``uniswap_v3`` because it only ever co-occurs with ``uniswap_v3`` on a chain,
so its exact global position is output-irrelevant). Keep it stable unless
intentionally changing that
surface. The full-dict equivalence pins in
``tests/unit/intents/test_contract_role_registry_equivalence.py`` enforce it.

The completeness invariant: every ``_BUILTIN_LOADERS`` address-owning connector
that appears in any PR-3a table MUST publish its roles from its manifest. This
is enforced statically by
``tests/unit/connectors/test_contract_role_registry_completeness.py``.

Gateway-boundary note: this module is strategy-side and performs no network
egress. The connector ``contract_roles`` modules it loads are pure data
(role → kind tuples).
"""

from __future__ import annotations

from almanak.connectors._connector import CONNECTOR_REGISTRY, ConnectorDiscoveryError, ImportRef
from almanak.connectors._strategy_base.contract_role_registry import (
    CONTRACT_ROLE_REGISTRY,
    ContractRole,
    ContractRoleRegistry,
    ContractRoleSpec,
    NpmView,
)

__all__ = [
    "CONTRACT_ROLE_REGISTRY",
    "ContractRole",
    "ContractRoleRegistry",
    "ContractRoleSpec",
    "NpmView",
]


def _ordered_refs(refs: list[ImportRef]) -> list[ImportRef]:
    """Return import refs in explicit order, with unordered refs last."""
    return sorted(refs, key=lambda ref: (ref.order is None, ref.order or 0))


def _load_specs(import_ref: ImportRef) -> tuple[ContractRoleSpec, ...]:
    """Load and validate one connector-owned contract-role spec tuple."""
    specs = import_ref.load()
    if not isinstance(specs, tuple):
        raise ConnectorDiscoveryError(
            f"{import_ref.module}.{import_ref.attribute} must be a tuple[ContractRoleSpec, ...], "
            f"got {type(specs).__qualname__}"
        )
    bad_specs = [spec for spec in specs if not isinstance(spec, ContractRoleSpec)]
    if bad_specs:
        raise ConnectorDiscoveryError(
            f"{import_ref.module}.{import_ref.attribute} must contain only ContractRoleSpec values, "
            f"got {[type(spec).__qualname__ for spec in bad_specs]!r}"
        )
    return specs


def _register_all() -> None:
    """Register every connector-owned contract role declaration.

    Descriptor-backed connectors are discovered here. Import targets are stored
    as strings on each connector descriptor so loading this module does not
    transitively import every connector's data module until registry bootstrap.
    """
    refs: list[ImportRef] = []
    for connector_manifest in CONNECTOR_REGISTRY.with_contract_roles():
        if connector_manifest.contract_roles is None:
            continue
        refs.append(connector_manifest.contract_roles)

    for import_ref in _ordered_refs(refs):
        specs = _load_specs(import_ref)
        for spec in specs:
            CONTRACT_ROLE_REGISTRY.register(
                protocol=spec.protocol,
                roles=spec.roles,
                address_protocol=spec.address_protocol,
                npm_view=spec.npm_view,
                surface_exclusions=spec.surface_exclusions,
                router_aliases=spec.router_aliases,
            )


_register_all()
