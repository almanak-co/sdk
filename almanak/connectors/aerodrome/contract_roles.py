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
    NpmView,
)

CONTRACT_ROLES: tuple[ContractRoleSpec, ...] = (
    ContractRoleSpec(
        protocol="aerodrome",
        roles={
            ContractRole.ROUTER: ("router",),
            ContractRole.LP_POSITION_MANAGER: ("router",),
        },
        # Aerodrome's Optimism router is also the Velodrome V2 router — the
        # Zodiac manifest generator looks it up under both names (VIB-4389).
        router_aliases={"velodrome": frozenset({"optimism"})},
    ),
    ContractRoleSpec(
        protocol="aerodrome_slipstream",
        # Keep the historical manager first so legacy single-address tables
        # retain their byte-for-byte value. Consumers that need physical NFT
        # authority enumerate both reviewed generations instead of guessing.
        roles={ContractRole.CL_POSITION_MANAGER: ("cl_nft", "cl_nft_current")},
        address_protocol="aerodrome",
        # Slipstream's cl_nft is its own backfill NPM view-map (Base today).
        npm_view=NpmView.SLIPSTREAM,
    ),
)
