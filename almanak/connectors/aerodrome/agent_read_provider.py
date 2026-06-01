"""Strategy-side agent-read provider for Aerodrome Slipstream (VIB-4860 / W8).

Publishes the on-chain *read descriptors* the agent-tool LP read handler
(``_execute_get_lp_position``) needs for Aerodrome's concentrated-liquidity
("Slipstream") positions:

* ``factory_address(chain)`` — the Slipstream CL factory (``cl_factory``)
  used by ``factory.getPool(token0, token1, tickSpacing)``.
* ``position_manager_address(chain)`` — the Slipstream NonfungiblePositionManager
  (``cl_nft``) used by ``positions(uint256)``.
* ``get_pool_selector()`` — ``0x28af8d0b``, the **int24 tick-spacing**
  ``getPool`` selector. This is the one place Slipstream genuinely differs
  from the uint24-fee Uniswap V3 family (selector ``0x1698ee82``) — the
  fork-specific knowledge W8 moves onto the connector.

Byte-equivalence (VIB-4860)
===========================

These values MUST match what ``_execute_get_lp_position`` resolved inline
before W8 for ``lp_protocol == "aerodrome_slipstream"``:

* ``factory_address`` ← ``AERODROME[chain]["cl_factory"]`` (the pre-W8 handler
  used the ``cl_factory`` key for the ``aerodrome_slipstream`` entry of
  ``_LP_PROTOCOL_REGISTRIES``, via ``_LP_FACTORY_KEY["aerodrome_slipstream"]``).
* ``position_manager_address`` ← ``AERODROME[chain]["cl_nft"]`` (the pre-W8
  ``nft_manager = AERODROME.get(chain, {}).get("cl_nft")`` branch).
* ``get_pool_selector`` ← ``"0x28af8d0b"`` (the Slipstream branch of the
  inline ``get_pool_selector = "0x28af8d0b" if ... == "aerodrome_slipstream"``).

Canonical name
==============

Registered under the protocol name ``aerodrome_slipstream`` — the exact key
the pre-W8 ``_LP_PROTOCOL_REGISTRIES`` used, and the only value
``_execute_get_lp_position`` accepts for the Slipstream branch (the LP
handler does *not* alias ``"aerodrome"`` → ``"aerodrome_slipstream"`` via
``normalize_protocol``, so the agent passes the explicit canonical slug).
Slipstream only deploys on Base today; Velodrome V2 on Optimism is a v2
(non-CL) interface with no ``cl_factory`` / ``cl_nft``, so the lookups
return ``None`` there — matching the pre-W8 ``.get(...)`` semantics.
"""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.agent_read_registry import (
    AgentReadCapability,
    AgentReadConnector,
)

# Slipstream's getPool encodes an int24 tick-spacing (not a uint24 fee), so it
# uses a different 4-byte selector than the Uniswap V3 family (0x1698ee82).
_GET_POOL_SELECTOR_SLIPSTREAM = "0x28af8d0b"


class AerodromeSlipstreamAgentReadConnector(AgentReadConnector, AgentReadCapability):
    """Agent-read descriptors for ``aerodrome_slipstream`` CL pools/positions."""

    protocol: ClassVar[ProtocolName] = ProtocolName("aerodrome_slipstream")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def agent_read_keys(self) -> frozenset[str]:
        return frozenset({"pool_state", "lp_position"})

    def factory_address(self, chain: str) -> str | None:
        from almanak.connectors.aerodrome.addresses import AERODROME

        chain_contracts = AERODROME.get(chain)
        # Defensive: a chain explicitly mapped to ``None`` (documented as
        # disabled) must not raise on ``.get("cl_factory")``.
        return chain_contracts.get("cl_factory") if isinstance(chain_contracts, dict) else None

    def position_manager_address(self, chain: str) -> str | None:
        from almanak.connectors.aerodrome.addresses import AERODROME

        chain_contracts = AERODROME.get(chain)
        return chain_contracts.get("cl_nft") if isinstance(chain_contracts, dict) else None

    def get_pool_selector(self) -> str:
        return _GET_POOL_SELECTOR_SLIPSTREAM

    def lending_pool_address(self, chain: str) -> str | None:
        return None


__all__ = ["AerodromeSlipstreamAgentReadConnector"]
