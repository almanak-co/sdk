"""Contract-role declarations for the Uniswap V3 connector (VIB-4928 PR-3a).

Maps the connector's semantic contract roles to the ordered
connector-private kinds in :mod:`almanak.connectors.uniswap_v3.addresses`.
The Uniswap V3 connector owns two framework-facing slugs — ``uniswap_v3`` and
its ``agni_finance`` fork (both ship distinct per-chain ``addresses.py``
tables) — so both are declared here.

Consumed by the boot file
``almanak.connectors._strategy_contract_role_registry``; see
:mod:`almanak.connectors._strategy_base.contract_role_registry`.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.contract_role_registry import (
    ContractRole,
    ContractRoleSpec,
    NpmView,
)

CONTRACT_ROLES: tuple[ContractRoleSpec, ...] = (
    ContractRoleSpec(
        protocol="uniswap_v3",
        roles={
            ContractRole.ROUTER: ("swap_router",),
            ContractRole.LP_POSITION_MANAGER: ("position_manager",),
            ContractRole.QUOTER: ("quoter_v2",),
        },
        # Canonical UniV3-family NPM map. (The backfill hashes both uniswap_v3
        # and sushiswap_v3 LP positions against this map, but only uniswap_v3 +
        # agni_finance contribute the *address* — see VIB-4971.)
        npm_view=NpmView.UNIV3,
        # Blast is published in addresses.py (router / position_manager /
        # quoter) but the central PROTOCOL_ROUTERS / LP_POSITION_MANAGERS /
        # SWAP_QUOTER_ADDRESSES tables never surfaced it.
        surface_exclusions={
            ContractRole.ROUTER: frozenset({"blast"}),
            ContractRole.LP_POSITION_MANAGER: frozenset({"blast"}),
            ContractRole.QUOTER: frozenset({"blast"}),
        },
    ),
    ContractRoleSpec(
        protocol="agni_finance",
        roles={
            ContractRole.ROUTER: ("swap_router",),
            ContractRole.LP_POSITION_MANAGER: ("position_manager",),
            ContractRole.QUOTER: ("quoter_v2",),
        },
        # Agni overlays its own NPM onto the canonical UniV3 map (Mantle).
        npm_view=NpmView.UNIV3,
    ),
)
