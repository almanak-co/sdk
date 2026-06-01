"""Strategy-side agent-read provider for Aave V3 (VIB-4860 / W8).

Publishes the lending-market Pool address the agent-tool read handlers
(``_execute_list_lending_positions`` / the lending section of
``_execute_get_portfolio``) need for ``Pool.getUserAccountData(user)``.

Byte-equivalence (VIB-4860)
===========================

``lending_pool_address(chain)`` MUST match the pre-W8 inline lookup
``AAVE_V3_POOL_ADDRESSES.get(chain)`` (derived from the ``AAVE_V3`` table
in ``aave_v3.adapter``). Every Aave-V2/V3 fork that compiles through the
framework's ``LendingProtocolAdapter`` inherits the same Pool semantics; if
a fork connector wants its own agent-read lending row it registers its own
provider with its own canonical protocol name.

The 8-decimal base-currency scaling and 1e18 health-factor decode of the
``getUserAccountData`` result stay generic in the executor (identical for
every Aave-V3 deployment); only the Pool *address* is protocol-specific and
moves here.
"""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.agent_read_registry import (
    AgentReadCapability,
    AgentReadConnector,
)


class AaveV3AgentReadConnector(AgentReadConnector, AgentReadCapability):
    """Agent-read descriptors for Aave V3 lending accounts."""

    protocol: ClassVar[ProtocolName] = ProtocolName("aave_v3")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def agent_read_keys(self) -> frozenset[str]:
        return frozenset({"lending_account"})

    def factory_address(self, chain: str) -> str | None:
        return None

    def position_manager_address(self, chain: str) -> str | None:
        return None

    def get_pool_selector(self) -> str:
        # Not a CL DEX — no getPool. Return the v3 default so the descriptor
        # contract is total; the lending handler never calls this.
        return "0x1698ee82"

    def lending_pool_address(self, chain: str) -> str | None:
        from almanak.connectors.aave_v3.adapter import AAVE_V3_POOL_ADDRESSES

        return AAVE_V3_POOL_ADDRESSES.get(chain)


__all__ = ["AaveV3AgentReadConnector"]
