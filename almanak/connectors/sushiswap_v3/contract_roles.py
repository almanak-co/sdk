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
    ),
)
