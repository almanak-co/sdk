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
    UniswapV4UnsupportedPoolError,
)
from almanak.framework.connectors.uniswap_v4.hooks import (
    HookDataEncoder,
    HookFlags,
    PoolDiscoveryResult,
    PoolState,
    discover_pool,
)
from almanak.framework.connectors.uniswap_v4.receipt_parser import (
    UniswapV4ReceiptParser,
)
from almanak.framework.connectors.uniswap_v4.sdk import (
    UniswapV4SDK,
)

__all__ = [
    "HookDataEncoder",
    "HookFlags",
    "PoolDiscoveryResult",
    "PoolState",
    "UniswapV4Adapter",
    "UniswapV4Config",
    "UniswapV4ReceiptParser",
    "UniswapV4SDK",
    "UniswapV4UnsupportedPoolError",
    "discover_pool",
]

# Connector registration (VIB-4298). The registry powers the (connector,
# intent, chain) coverage gate in scripts/ci/check_connector_registry.py
# and will be consumed by PR 2's intent-test coverage check.
from almanak.framework.connectors.registry import register_connector  # noqa: E402
from almanak.framework.intents.vocabulary import IntentType  # noqa: E402

register_connector(
    name="uniswap_v4",
    intents=(
        IntentType.SWAP,
        IntentType.LP_OPEN,
        IntentType.LP_CLOSE,
        IntentType.LP_COLLECT_FEES,
    ),
    chains=(
        "ethereum",
        "arbitrum",
        "base",
    ),
)
