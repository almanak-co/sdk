"""Pool price readers, aggregation, analytics, and historical data for on-chain DEX pools.

Provides protocol-specific pool readers that fetch live prices from
on-chain AMM pool contracts (Uniswap V3, Aerodrome, PancakeSwap V3),
TWAP and LWAP aggregation across multiple pools, pool analytics (TVL,
volume, fee APR/APY), and historical pool state data from The Graph,
DeFi Llama, and GeckoTerminal.
"""

from __future__ import annotations

from .aggregation import AggregatedPrice, PoolContribution, PriceAggregator
from .analytics import PoolAnalytics, PoolAnalyticsReader, PoolAnalyticsResult
from .history import PoolHistoryReader, PoolSnapshot
from .liquidity import (
    LiquidityDepth,
    LiquidityDepthReader,
    SlippageEstimate,
    SlippageEstimator,
    TickData,
)
from .reader import (
    AerodromePoolReader,
    PancakeSwapV3PoolReader,
    PoolPrice,
    PoolReaderRegistry,
    UniswapV3PoolPriceReader,
)

__all__ = [
    "AerodromePoolReader",
    "AggregatedPrice",
    "LiquidityDepth",
    "LiquidityDepthReader",
    "PancakeSwapV3PoolReader",
    "PoolAnalytics",
    "PoolAnalyticsReader",
    "PoolAnalyticsResult",
    "PoolContribution",
    "PoolHistoryReader",
    "PoolPrice",
    "PoolReaderRegistry",
    "PoolSnapshot",
    "PriceAggregator",
    "SlippageEstimate",
    "SlippageEstimator",
    "TickData",
    "UniswapV3PoolPriceReader",
]
