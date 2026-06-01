"""Strategy-side agent-read provider for PancakeSwap V3 (VIB-4860 / W8).

PancakeSwap V3 is a Uniswap V3 fork: same ``slot0()`` / ``positions()`` ABI,
same uint24-fee ``getPool`` selector (``0x1698ee82``). The genuinely
protocol-specific descriptors the agent-tool ``get_pool_state`` /
``get_lp_position`` read handlers need are the **factory** and
**NonfungiblePositionManager** addresses — both resolved from this connector's
own ``addresses.py:PANCAKESWAP_V3`` table.

Address source (VIB-4860 / VIB-4902)
====================================

``factory_address(chain)`` ← ``PANCAKESWAP_V3[chain]["factory"]``.

``position_manager_address(chain)`` ← ``PANCAKESWAP_V3[chain]["nft"]`` — the
PancakeSwap V3 NonfungiblePositionManager.

VIB-4902: this previously returned the **Uniswap V3** NPM table
(``POSITION_MANAGER_ADDRESSES``), preserved from the pre-W8 inline handler for
byte-equivalence. That was wrong on every chain where Pancake's NPM differs
from Uniswap's (all of them), so ``get_lp_position`` / ``list_lp_positions``
queried the Uniswap NPM for a Pancake position and returned wrong/empty data.
Reading ``PANCAKESWAP_V3[chain]["nft"]`` — the same table the connector's
receipt parser and the teardown discovery walker read — fixes the on-chain
target and makes the agent-read path agree with teardown by construction.
"""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.agent_read_registry import (
    AgentReadCapability,
    AgentReadConnector,
)

# Uniswap V3 family uint24-fee getPool selector (shared by all V3 forks).
_GET_POOL_SELECTOR_V3 = "0x1698ee82"


class PancakeswapV3AgentReadConnector(AgentReadConnector, AgentReadCapability):
    """Agent-read descriptors for canonical ``pancakeswap_v3`` pools/positions."""

    protocol: ClassVar[ProtocolName] = ProtocolName("pancakeswap_v3")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def agent_read_keys(self) -> frozenset[str]:
        return frozenset({"pool_state", "lp_position"})

    def factory_address(self, chain: str) -> str | None:
        from almanak.connectors.pancakeswap_v3.addresses import PANCAKESWAP_V3

        chain_contracts = PANCAKESWAP_V3.get(chain)
        return chain_contracts.get("factory") if isinstance(chain_contracts, dict) else None

    def position_manager_address(self, chain: str) -> str | None:
        # VIB-4902: resolve PancakeSwap's OWN NonfungiblePositionManager from
        # the connector's address table (the same ``nft`` key the receipt
        # parser and teardown discovery walker read) — not the Uniswap V3 NPM.
        from almanak.connectors.pancakeswap_v3.addresses import PANCAKESWAP_V3

        chain_contracts = PANCAKESWAP_V3.get(chain)
        return chain_contracts.get("nft") if isinstance(chain_contracts, dict) else None

    def get_pool_selector(self) -> str:
        return _GET_POOL_SELECTOR_V3

    def lending_pool_address(self, chain: str) -> str | None:
        return None


__all__ = ["PancakeswapV3AgentReadConnector"]
