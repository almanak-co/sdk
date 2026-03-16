"""Uniswap V4 protocol connector.

Provides swap compilation, receipt parsing, and pool utilities for
Uniswap V4's singleton PoolManager architecture.

Key differences from V3:
- Singleton PoolManager contract (all pools in one contract)
- Pool keys include hooks address (currency0, currency1, fee, tickSpacing, hooks)
- Native ETH support (no mandatory WETH wrapping)
- Flash accounting model
- New Swap event signature from PoolManager

Example:
    from almanak.framework.connectors.uniswap_v4 import UniswapV4Adapter

    adapter = UniswapV4Adapter(chain="arbitrum")
    bundle = adapter.compile_swap_intent(intent, price_oracle)
"""

from almanak.framework.connectors.uniswap_v4.adapter import (
    UniswapV4Adapter,
    UniswapV4Config,
)
from almanak.framework.connectors.uniswap_v4.receipt_parser import (
    UniswapV4ReceiptParser,
)
from almanak.framework.connectors.uniswap_v4.sdk import (
    UniswapV4SDK,
)

__all__ = [
    "UniswapV4Adapter",
    "UniswapV4Config",
    "UniswapV4ReceiptParser",
    "UniswapV4SDK",
]
