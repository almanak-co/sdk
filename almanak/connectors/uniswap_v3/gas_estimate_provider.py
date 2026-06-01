"""Strategy-side gas-estimate connector for Uniswap V3 (VIB-4858 / W6).

Publishes the canonical concentrated-liquidity ``lp_*`` action gas
estimates to ``STRATEGY_GAS_ESTIMATE_REGISTRY``. Uniswap V3 is the
**owning connector** for these action names — the legacy
``DEFAULT_GAS_ESTIMATES`` table used them as a generic CL-DEX baseline
and every UniV3 fork (SushiSwap V3, PancakeSwap V3, Aerodrome Slipstream,
Camelot, Agni Finance, Jaine DEX, …) inherited the same numbers via the
shared compiler path. Routing the lookup through this connector
preserves that behaviour while moving the data out of the central
framework dict.

Byte-equivalence (VIB-4858)
===========================

Each integer here MUST match the pre-W6 ``DEFAULT_GAS_ESTIMATES`` entry
for the same action. The byte-equivalence verification script and unit
test pin the contract; modifying any of these numbers requires a
documented rationale (gas profile changed on-chain) and a follow-up
review of every UniV3 fork connector that inherits them.

Trailing comments on each row reproduce the legacy table's
explanatory note, so the connector file remains a self-contained
reference.
"""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.gas_estimate_registry import (
    GasEstimateCapability,
    GasEstimateConnector,
)


class UniswapV3GasEstimateConnector(GasEstimateConnector, GasEstimateCapability):
    """Gas-estimate connector for Uniswap V3's ``lp_*`` action family."""

    protocol: ClassVar[ProtocolName] = ProtocolName("uniswap_v3")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    _ESTIMATES: ClassVar[dict[str, int]] = {
        # Uniswap V3 mint new position (wide ranges need more gas)
        "lp_mint": 500000,
        # Add liquidity to existing position
        "lp_increase_liquidity": 200000,
        # Remove liquidity from position (extra buffer for Arbitrum)
        "lp_decrease_liquidity": 250000,
        # Collect fees/tokens (buffer for fee growth updates)
        "lp_collect": 200000,
        # Burn position NFT (if fully withdrawn)
        "lp_burn": 100000,
    }

    def gas_estimate_keys(self) -> frozenset[str]:
        return frozenset(self._ESTIMATES)

    def gas_estimate(self, action: str, chain: str) -> int:
        return self._ESTIMATES[action]


__all__ = ["UniswapV3GasEstimateConnector"]
