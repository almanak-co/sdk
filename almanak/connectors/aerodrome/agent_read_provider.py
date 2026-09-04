"""Strategy-side agent-read provider for Aerodrome Slipstream (VIB-4860 / W8).

Publishes the on-chain *read descriptors* the agent-tool LP read handler
(``_execute_get_lp_position``) needs for Aerodrome's concentrated-liquidity
("Slipstream") positions:

* ``factory_addresses(chain)`` — every reviewed Slipstream factory
  generation; pool discovery asks each one with
  ``factory.getPool(token0, token1, tickSpacing)`` and refuses a key that
  more than one answers. ``factory_address`` names a single factory only
  when the chain has exactly one reviewed generation.
* manager descriptors — a singleton default when unambiguous, otherwise the
  reviewed manager set and its exact generation/factory pairing.
* ``get_pool_selector()`` — ``0x28af8d0b``, the **int24 tick-spacing**
  ``getPool`` selector. This is the one place Slipstream genuinely differs
  from the uint24-fee Uniswap V3 family (selector ``0x1698ee82``) — the
  fork-specific knowledge W8 moves onto the connector.

Byte-equivalence (VIB-4860)
===========================

These values MUST match what ``_execute_get_lp_position`` resolved inline
before W8 for ``lp_protocol == "aerodrome_slipstream"``:

* ``factory_addresses`` ← the factories of ``slipstream_lp_deployments(chain)``;
  the pre-W8 singleton ``cl_factory`` read is retired because Base has more
  than one reviewed generation.
* ``position_manager_address`` preserves singleton behavior but returns
  ``None`` on multi-generation Base; exact manager/factory lookup replaces the
  unsafe legacy default for token-ID-scoped reads.
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
(non-CL) interface with no reviewed Slipstream generation, so the lookups
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
        factories = self.factory_addresses(chain)
        return factories[0] if len(factories) == 1 else None

    def factory_addresses(self, chain: str) -> tuple[str, ...]:
        from almanak.connectors.aerodrome.addresses import slipstream_lp_deployments

        return tuple(deployment.factory for deployment in slipstream_lp_deployments(chain))

    def position_manager_address(self, chain: str) -> str | None:
        from almanak.connectors.aerodrome.addresses import slipstream_lp_deployments

        deployments = slipstream_lp_deployments(chain)
        return deployments[0].position_manager if len(deployments) == 1 else None

    def reviewed_position_manager_addresses(self, chain: str) -> tuple[str, ...]:
        from almanak.connectors.aerodrome.addresses import slipstream_lp_deployments

        return tuple(deployment.position_manager for deployment in slipstream_lp_deployments(chain))

    def factory_address_for_position_manager(self, chain: str, position_manager: str) -> str | None:
        from almanak.connectors.aerodrome.addresses import slipstream_deployment_for_position_manager

        deployment = slipstream_deployment_for_position_manager(chain, position_manager)
        return deployment.factory if deployment is not None else None

    def get_pool_selector(self) -> str:
        return _GET_POOL_SELECTOR_SLIPSTREAM

    def lending_pool_address(self, chain: str) -> str | None:
        return None


__all__ = ["AerodromeSlipstreamAgentReadConnector"]
