"""Strategy-side agent-read provider for Spark (VIB-4951).

Spark is an Aave V3 fork with an identical PoolDataProvider ABI — the
reserve-discovery plan reuses the shared Aave-fork builder wholesale; only
the per-chain ``pool_data_provider`` address (from ``spark/addresses.py``,
brokered via :class:`AddressRegistry`) differs. This intentionally does NOT
go through ``aave_helpers._resolve_pool_data_provider`` (whose fallback
hardcodes ``aave_v3``) — the connector resolves its own table.
"""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.agent_read_registry import (
    AgentReadCapability,
    AgentReadConnector,
)
from almanak.connectors._strategy_base.lending_reserve_read import (
    LendingReserveDiscoveryPlan,
    aave_fork_reserve_discovery_plan,
)


class SparkAgentReadConnector(AgentReadConnector, AgentReadCapability):
    """Agent-read descriptors for Spark lending reserves + accounts."""

    protocol: ClassVar[ProtocolName] = ProtocolName("spark")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def agent_read_keys(self) -> frozenset[str]:
        return frozenset({"lending_account", "lending_reserves"})

    def factory_address(self, chain: str) -> str | None:
        return None

    def position_manager_address(self, chain: str) -> str | None:
        return None

    def get_pool_selector(self) -> str:
        # Not a CL DEX — no getPool. v3 default keeps the descriptor total;
        # the lending handlers never call this (mirrors AaveV3AgentReadConnector).
        return "0x1698ee82"

    def lending_pool_address(self, chain: str) -> str | None:
        from almanak.connectors._strategy_base.address_registry import AddressRegistry

        return AddressRegistry.resolve_contract_address("spark", chain, "pool")

    def lending_reserve_discovery_plan(self, chain: str) -> LendingReserveDiscoveryPlan | None:
        from almanak.connectors._strategy_base.address_registry import AddressRegistry

        provider = AddressRegistry.resolve_contract_address("spark", chain, "pool_data_provider")
        if not provider:
            return None
        return aave_fork_reserve_discovery_plan("spark", provider)


__all__ = ["SparkAgentReadConnector"]
