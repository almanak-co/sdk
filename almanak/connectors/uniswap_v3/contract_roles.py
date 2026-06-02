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
)

CONTRACT_ROLES: tuple[ContractRoleSpec, ...] = (
    ContractRoleSpec(
        protocol="uniswap_v3",
        roles={
            ContractRole.ROUTER: ("swap_router",),
            ContractRole.LP_POSITION_MANAGER: ("position_manager",),
            ContractRole.QUOTER: ("quoter_v2",),
        },
    ),
    ContractRoleSpec(
        protocol="agni_finance",
        roles={
            ContractRole.ROUTER: ("swap_router",),
            ContractRole.LP_POSITION_MANAGER: ("position_manager",),
            ContractRole.QUOTER: ("quoter_v2",),
        },
    ),
)
