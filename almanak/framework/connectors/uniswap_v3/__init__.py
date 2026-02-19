"""Uniswap V3 Connector.

This module provides the Uniswap V3 adapter for executing token swaps
on Uniswap V3 across multiple chains.

Supported chains:
- Ethereum
- Arbitrum
- Optimism
- Polygon
- Base

Example:
    from almanak.framework.connectors.uniswap_v3 import UniswapV3Adapter, UniswapV3Config

    config = UniswapV3Config(
        chain="arbitrum",
        wallet_address="0x...",
    )
    adapter = UniswapV3Adapter(config)

    # Execute a swap
    result = adapter.swap_exact_input(
        token_in="USDC",
        token_out="WETH",
        amount_in=Decimal("1000"),
    )

    # Use SDK for lower-level operations
    from almanak.framework.connectors.uniswap_v3 import UniswapV3SDK

    sdk = UniswapV3SDK(chain="arbitrum", rpc_url="https://arb1.arbitrum.io/rpc")
    pool = sdk.get_pool_address(weth_addr, usdc_addr, fee_tier=3000)
"""

from .adapter import (
    DEFAULT_FEE_TIER,
    FEE_TIERS,
    UNISWAP_V3_ADDRESSES,
    UNISWAP_V3_GAS_ESTIMATES,
    SwapQuote,
    SwapResult,
    SwapType,
    TransactionData,
    UniswapV3Adapter,
    UniswapV3Config,
)
from .receipt_parser import (
    EVENT_NAME_TO_TYPE,
    EVENT_TOPICS,
    SWAP_EVENT_TOPIC,
    TOPIC_TO_EVENT,
    ParsedSwapResult,
    ParseResult,
    SwapEventData,
    TransferEventData,
    UniswapV3Event,
    UniswapV3EventType,
    UniswapV3ReceiptParser,
)
from .sdk import (
    FACTORY_ADDRESSES,
    MAX_TICK,
    MIN_TICK,
    # Constants
    Q96,
    Q128,
    QUOTER_ADDRESSES,
    ROUTER_ADDRESSES,
    TICK_SPACING,
    InvalidFeeError,
    InvalidTickError,
    PoolInfo,
    PoolNotFoundError,
    PoolState,
    QuoteError,
    SwapTransaction,
    UniswapV3SDK,
    UniswapV3SDKError,
    # Pool functions
    compute_pool_address,
    get_max_tick,
    get_min_tick,
    get_nearest_tick,
    price_to_sqrt_price_x96,
    price_to_tick,
    sort_tokens,
    sqrt_price_x96_to_price,
    sqrt_price_x96_to_tick,
    tick_to_price,
    # Tick math functions
    tick_to_sqrt_price_x96,
)

# Import SDK SwapQuote with alias to avoid naming conflict with adapter SwapQuote.
# Both classes serve different purposes:
# - SwapQuote (from adapter): High-level quote for adapter operations with price impact, effective price
# - SDKSwapQuote (from sdk): Low-level quote with protocol-specific details like tick info
# This pattern is intentional and consistent across connectors (uniswap_v3, traderjoe_v2, aerodrome).
from .sdk import SwapQuote as SDKSwapQuote

__all__ = [
    # Adapter exports
    "UniswapV3Adapter",
    "UniswapV3Config",
    "SwapQuote",
    "SwapResult",
    "SwapType",
    "TransactionData",
    "UNISWAP_V3_ADDRESSES",
    "UNISWAP_V3_GAS_ESTIMATES",
    "FEE_TIERS",
    "DEFAULT_FEE_TIER",
    # SDK exports
    "UniswapV3SDK",
    "SDKSwapQuote",
    "PoolInfo",
    "PoolState",
    "SwapTransaction",
    "UniswapV3SDKError",
    "InvalidFeeError",
    "InvalidTickError",
    "PoolNotFoundError",
    "QuoteError",
    # Tick math functions
    "tick_to_sqrt_price_x96",
    "sqrt_price_x96_to_tick",
    "tick_to_price",
    "price_to_tick",
    "sqrt_price_x96_to_price",
    "price_to_sqrt_price_x96",
    "get_nearest_tick",
    "get_min_tick",
    "get_max_tick",
    # Pool functions
    "compute_pool_address",
    "sort_tokens",
    # Constants
    "Q96",
    "Q128",
    "MIN_TICK",
    "MAX_TICK",
    "TICK_SPACING",
    "FACTORY_ADDRESSES",
    "ROUTER_ADDRESSES",
    "QUOTER_ADDRESSES",
    # Receipt parser exports
    "UniswapV3ReceiptParser",
    "UniswapV3Event",
    "UniswapV3EventType",
    "SwapEventData",
    "TransferEventData",
    "ParsedSwapResult",
    "ParseResult",
    "EVENT_TOPICS",
    "TOPIC_TO_EVENT",
    "EVENT_NAME_TO_TYPE",
    "SWAP_EVENT_TOPIC",
]
