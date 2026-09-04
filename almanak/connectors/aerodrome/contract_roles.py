"""Contract-role declarations for the Aerodrome connector.

Aerodrome serves its fungible-LP slot from the same ``router`` address it uses
for swaps. Its concentrated-liquidity surface (Slipstream) is surfaced under
the ``aerodrome_slipstream`` pseudo-slug, which resolves its addresses from the
shared ``aerodrome`` table (``address_protocol="aerodrome"``). Slipstream has
several reviewed factory generations, each with its own NFT position manager;
the ``CL_POSITION_MANAGER`` role therefore lists one generation-named kind per
reviewed generation so framework consumers see the whole reviewed set rather
than a singleton. Role-derived single-address tables (``LP_POSITION_MANAGERS``)
take the first kind and are membership/display views for this slug only; every
token-id-scoped path reads the full set (``SLIPSTREAM_NFT_POSITION_MANAGER_SETS``)
or resolves the manager from NFT ownership or the receipt. See
:mod:`almanak.connectors._strategy_base.contract_role_registry`.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.contract_role_registry import (
    ContractRole,
    ContractRoleSpec,
    NpmView,
)

from .addresses import SLIPSTREAM_LP_DEPLOYMENTS, slipstream_position_manager_kind

_SLIPSTREAM_POSITION_MANAGER_KINDS: tuple[str, ...] = tuple(
    dict.fromkeys(
        slipstream_position_manager_kind(deployment)
        for deployments in SLIPSTREAM_LP_DEPLOYMENTS.values()
        for deployment in deployments
    )
)

CONTRACT_ROLES: tuple[ContractRoleSpec, ...] = (
    ContractRoleSpec(
        protocol="aerodrome",
        roles={
            ContractRole.ROUTER: ("router",),
            ContractRole.LP_POSITION_MANAGER: ("router",),
        },
        # Aerodrome's Optimism router is also the Velodrome V2 router — the
        # Zodiac manifest generator looks it up under both names.
        router_aliases={"velodrome": frozenset({"optimism"})},
    ),
    ContractRoleSpec(
        protocol="aerodrome_slipstream",
        roles={ContractRole.CL_POSITION_MANAGER: _SLIPSTREAM_POSITION_MANAGER_KINDS},
        address_protocol="aerodrome",
        npm_view=NpmView.SLIPSTREAM,
    ),
)
