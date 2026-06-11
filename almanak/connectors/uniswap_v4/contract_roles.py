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
    NpmView,
)

CONTRACT_ROLES: tuple[ContractRoleSpec, ...] = (
    ContractRoleSpec(
        protocol="uniswap_v4",
        roles={ContractRole.LP_POSITION_MANAGER: ("position_manager",)},
        # VIB-4583: V4 LP positions hash their physical_identity_hash on this
        # PositionManager + tokenId. Declaring the NPM view lets the migration
        # backfill resolve the per-chain PositionManager address through
        # ``compiler_constants`` without naming this connector.
        npm_view=NpmView.UNIV4,
    ),
)
