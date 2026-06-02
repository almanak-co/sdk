"""Strategy-side contract-role registration site (VIB-4928 PR-3a).

Sibling of :mod:`almanak.connectors._strategy_flash_loan_registry`, scoped to
the contract-role concern. The intent compiler's six per-chain address tables
(``PROTOCOL_ROUTERS``, ``LP_POSITION_MANAGERS``, ``SWAP_QUOTER_ADDRESSES``,
``LENDING_POOL_ADDRESSES``, ``LENDING_POOL_DATA_PROVIDERS``,
``BALANCER_VAULT_ADDRESSES``) fan out over this registry instead of
hand-importing each connector's ``addresses.py`` — so adding an
address-owning connector means one ``contract_roles.py`` data module + one
import block below, not five edits across ``compiler_constants.py``.

Lives one level up from ``_strategy_base/`` because it imports every
address-owning connector's ``contract_roles`` data module, and
``_strategy_base/`` must stay protocol-clean (no concrete connector imports).

**Registration order is load-bearing.** The compiler iterates
``CONTRACT_ROLE_REGISTRY.protocols_with_role(role)`` (registration order,
filtered to the role) as the *outer* loop, and the per-protocol key order
within each chain's sub-dict is exactly that iteration order. The order below
matches the pre-PR-3a hand-rolled ``_build_*`` source-list order (the longest
list — ``_build_lp_position_managers`` — is the canonical superset; the
``agni_finance`` slug rides immediately after ``uniswap_v3`` because it only
ever co-occurs with ``uniswap_v3`` on a chain, so its exact global position is
output-irrelevant). Keep it stable unless intentionally changing that
surface — the full-dict equivalence pins in
``tests/unit/intents/test_contract_role_registry_equivalence.py`` enforce it.

The completeness invariant — every ``_BUILTIN_LOADERS`` address-owning
connector that appears in any PR-3a table MUST register here — is enforced
statically by ``tests/unit/connectors/test_contract_role_registry_completeness.py``.

Gateway-boundary note: this module is strategy-side and performs no network
egress. The connector ``contract_roles`` modules it imports are pure data
(role → kind tuples).
"""

from __future__ import annotations

from almanak.connectors._strategy_base.contract_role_registry import (
    CONTRACT_ROLE_REGISTRY,
    ContractRole,
    ContractRoleRegistry,
    ContractRoleSpec,
)

__all__ = ["CONTRACT_ROLE_REGISTRY", "ContractRole", "ContractRoleRegistry", "ContractRoleSpec"]


def _register_all() -> None:
    """Register every strategy-side connector's contract roles.

    Imports are local to the function so that loading this module does not
    transitively import each connector's ``contract_roles`` data module until
    the registry is actually constructed. The import order below is the
    load-bearing registration order (see the module docstring).
    """
    from almanak.connectors.aave_v3.contract_roles import CONTRACT_ROLES as _aave_v3
    from almanak.connectors.aerodrome.contract_roles import CONTRACT_ROLES as _aerodrome
    from almanak.connectors.balancer_v2.contract_roles import CONTRACT_ROLES as _balancer_v2
    from almanak.connectors.camelot.contract_roles import CONTRACT_ROLES as _camelot
    from almanak.connectors.fluid.contract_roles import CONTRACT_ROLES as _fluid
    from almanak.connectors.pancakeswap_v3.contract_roles import CONTRACT_ROLES as _pancakeswap_v3
    from almanak.connectors.spark.contract_roles import CONTRACT_ROLES as _spark
    from almanak.connectors.sushiswap_v3.contract_roles import CONTRACT_ROLES as _sushiswap_v3
    from almanak.connectors.traderjoe_v2.contract_roles import CONTRACT_ROLES as _traderjoe_v2
    from almanak.connectors.uniswap_v3.contract_roles import CONTRACT_ROLES as _uniswap_v3
    from almanak.connectors.uniswap_v4.contract_roles import CONTRACT_ROLES as _uniswap_v4

    # Load-bearing order — see module docstring. The DEX/swap connectors come
    # first (matching the _build_lp_position_managers source order, the
    # canonical superset), then the lending connectors, then Balancer.
    ordered_specs: tuple[tuple[ContractRoleSpec, ...], ...] = (
        _uniswap_v3,  # also declares the agni_finance fork (adjacent)
        _uniswap_v4,
        _sushiswap_v3,
        _pancakeswap_v3,
        _aerodrome,  # also declares the aerodrome_slipstream pseudo-slug
        _traderjoe_v2,
        _camelot,
        _fluid,
        _aave_v3,
        _spark,
        _balancer_v2,
    )
    for specs in ordered_specs:
        for spec in specs:
            CONTRACT_ROLE_REGISTRY.register(
                protocol=spec.protocol,
                roles=spec.roles,
                address_protocol=spec.address_protocol,
            )


_register_all()
