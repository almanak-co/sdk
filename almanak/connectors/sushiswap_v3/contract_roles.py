"""Contract-role declarations for the SushiSwap V3 connector (VIB-4928 PR-3a).

See :mod:`almanak.connectors._strategy_base.contract_role_registry`.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.contract_role_registry import (
    ContractRole,
    ContractRoleSpec,
)

CONTRACT_ROLES: tuple[ContractRoleSpec, ...] = (
    ContractRoleSpec(
        protocol="sushiswap_v3",
        roles={
            ContractRole.ROUTER: ("swap_router",),
            ContractRole.LP_POSITION_MANAGER: ("position_manager",),
            ContractRole.QUOTER: ("quoter_v2",),
        },
        # SushiSwap V3 deliberately declares NO npm_view: its LP positions hash
        # against the canonical Uniswap NPM (the backfill routes it there), so it
        # must NOT join the UniV3 NPM map despite shipping its own
        # position_manager (see VIB-4971).
        #
        # Avalanche has a deployed router/position_manager/quoter but zero usable
        # liquidity (VIB-2069), so it's surfaced in none of the central tables;
        # the central quoter table also never surfaced its Optimism quoter.
        surface_exclusions={
            ContractRole.ROUTER: frozenset({"avalanche"}),
            ContractRole.LP_POSITION_MANAGER: frozenset({"avalanche"}),
            ContractRole.QUOTER: frozenset({"avalanche", "optimism"}),
        },
    ),
)
