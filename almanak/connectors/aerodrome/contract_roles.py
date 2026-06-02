"""Contract-role declarations for the Aerodrome connector (VIB-4928 PR-3a).

Aerodrome serves its fungible-LP slot from the same ``router`` address it uses
for swaps, and ships a separate concentrated-liquidity (Slipstream) NFT
position manager under ``cl_nft``. The latter is surfaced under the
``aerodrome_slipstream`` pseudo-slug, which resolves its addresses from the
shared ``aerodrome`` table (``address_protocol="aerodrome"``). See
:mod:`almanak.connectors._strategy_base.contract_role_registry`.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.contract_role_registry import (
    ContractRole,
    ContractRoleSpec,
)

CONTRACT_ROLES: tuple[ContractRoleSpec, ...] = (
    ContractRoleSpec(
        protocol="aerodrome",
        roles={
            ContractRole.ROUTER: ("router",),
            ContractRole.LP_POSITION_MANAGER: ("router",),
        },
    ),
    ContractRoleSpec(
        protocol="aerodrome_slipstream",
        roles={ContractRole.CL_POSITION_MANAGER: ("cl_nft",)},
        address_protocol="aerodrome",
    ),
)
