"""Contract-role declarations for the PancakeSwap V3 connector (VIB-4928 PR-3a).

PancakeSwap V3 records its NonfungiblePositionManager under the ``nft`` kind
and its quoter under ``quoter`` (vs the canonical V3 forks' ``position_manager``
/ ``quoter_v2``) — the role layer here is exactly what reconciles those
connector-private differences. See
:mod:`almanak.connectors._strategy_base.contract_role_registry`.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.contract_role_registry import (
    ContractRole,
    ContractRoleSpec,
)

CONTRACT_ROLES: tuple[ContractRoleSpec, ...] = (
    ContractRoleSpec(
        protocol="pancakeswap_v3",
        roles={
            ContractRole.ROUTER: ("swap_router",),
            ContractRole.LP_POSITION_MANAGER: ("nft",),
            ContractRole.QUOTER: ("quoter",),
        },
    ),
)
