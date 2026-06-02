"""Contract-role declarations for the TraderJoe V2 connector (VIB-4928 PR-3a).

Liquidity Book serves its LP position-manager slot from the ``router`` (the
LBRouter) address.

**Why no ``ROUTER`` role (deliberate, byte-equivalence-preserving):** the
legacy central ``PROTOCOL_ROUTERS`` dict — which feeds the DefaultSwapAdapter /
connector-compiler V3-style ``exactInputSingle`` swap path — never carried a
``traderjoe_v2`` entry (its ``_build_protocol_routers`` source list omitted
it). TraderJoe is a Liquidity Book DEX, not a V3-style swap router, so its
LBRouter fills only the LP slot in ``LP_POSITION_MANAGERS``. Declaring a
``ROUTER`` role here would surface ``traderjoe_v2`` in ``PROTOCOL_ROUTERS`` on
every chain it supports (avalanche / arbitrum / bsc / ethereum) — a real
address-table regression the equivalence pins
(``test_protocol_routers``) catch. The PR-3a design brief's role table listed
``ROUTER`` for TraderJoe; the live ``PROTOCOL_ROUTERS`` membership is the
source of truth (per the brief's "live code wins" rule), so it is omitted.

See :mod:`almanak.connectors._strategy_base.contract_role_registry`.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.contract_role_registry import (
    ContractRole,
    ContractRoleSpec,
)

CONTRACT_ROLES: tuple[ContractRoleSpec, ...] = (
    ContractRoleSpec(
        protocol="traderjoe_v2",
        roles={ContractRole.LP_POSITION_MANAGER: ("router",)},
    ),
)
