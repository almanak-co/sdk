"""Contract-role declarations for the Uniswap V4 connector (VIB-4928 PR-3a).

V4 contributes only an LP position-manager slot (its singleton PoolManager
architecture means it has no per-pool router/quoter the address tables key on
the way the V3 forks do). See
:mod:`almanak.connectors._strategy_base.contract_role_registry`.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.contract_role_registry import (
    ContractRole,
    ContractRoleSpec,
)

CONTRACT_ROLES: tuple[ContractRoleSpec, ...] = (
    ContractRoleSpec(
        protocol="uniswap_v4",
        roles={ContractRole.LP_POSITION_MANAGER: ("position_manager",)},
    ),
)
