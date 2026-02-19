"""SushiSwap V3 Connector Package.

This package provides the SushiSwap V3 protocol integration for the Almanak
Strategy Framework, including SDK, adapter, and receipt parser.

SushiSwap V3 is a concentrated liquidity AMM forked from Uniswap V3,
deployed across multiple chains including Arbitrum, Ethereum, Base,
Polygon, Avalanche, BSC, and Optimism.

Key Components:
- SushiSwapV3SDK: Low-level SDK for direct protocol interaction
- SushiSwapV3Adapter: High-level adapter for framework integration
- SushiSwapV3ReceiptParser: Transaction receipt parsing

Supported Operations:
- Token swaps (exact input and exact output)
- LP position management (mint, increase, decrease, collect)
- Quote fetching
- Pool address computation

Example:
    from almanak.framework.connectors.sushiswap_v3 import (
        SushiSwapV3SDK,
        SushiSwapV3Adapter,
        SushiSwapV3Config,
        SushiSwapV3ReceiptParser,
    )

    # Using the SDK directly
    sdk = SushiSwapV3SDK(chain="arbitrum")
    pool = sdk.get_pool_address(weth_address, usdc_address, fee_tier=3000)

    # Using the adapter
    config = SushiSwapV3Config(
        chain="arbitrum",
        wallet_address="0x...",
        price_provider={"ETH": Decimal("3400"), "USDC": Decimal("1")},
    )
    adapter = SushiSwapV3Adapter(config)
    result = adapter.swap_exact_input("USDC", "WETH", Decimal("1000"))

    # Parsing receipts
    parser = SushiSwapV3ReceiptParser(chain="arbitrum")
    parse_result = parser.parse_receipt(receipt)
"""

# Re-export SwapQuote from adapter (has enriched quote data)
from almanak.framework.connectors.sushiswap_v3.adapter import (
    DEFAULT_FEE_TIER,
    SUSHISWAP_V3_ADDRESSES,
    SUSHISWAP_V3_GAS_ESTIMATES,
    LPResult,
    SushiSwapV3Adapter,
    SushiSwapV3Config,
    SwapQuote,
    SwapResult,
    SwapType,
    TransactionData,
)
from almanak.framework.connectors.sushiswap_v3.receipt_parser import (
    EVENT_NAME_TO_TYPE,
    EVENT_TOPICS,
    POSITION_MANAGER_ADDRESSES,
    SWAP_EVENT_TOPIC,
    TOPIC_TO_EVENT,
    ParsedSwapResult,
    ParseResult,
    SushiSwapV3Event,
    SushiSwapV3EventType,
    SushiSwapV3ReceiptParser,
    SwapEventData,
    TransferEventData,
)
from almanak.framework.connectors.sushiswap_v3.sdk import (
    FACTORY_ADDRESSES,
    FEE_TIERS,
    MAX_TICK,
    MIN_TICK,
    Q96,
    Q128,
    QUOTER_ADDRESSES,
    ROUTER_ADDRESSES,
    TICK_SPACING,
    InvalidFeeError,
    InvalidTickError,
    LPTransaction,
    MintParams,
    PoolInfo,
    PoolNotFoundError,
    PoolState,
    QuoteError,
    SushiSwapV3SDK,
    SushiSwapV3SDKError,
    SwapTransaction,
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
    tick_to_sqrt_price_x96,
)

__all__ = [
    # SDK
    "SushiSwapV3SDK",
    "SushiSwapV3SDKError",
    "InvalidFeeError",
    "InvalidTickError",
    "PoolNotFoundError",
    "QuoteError",
    "PoolInfo",
    "PoolState",
    "SwapTransaction",
    "MintParams",
    "LPTransaction",
    # Adapter
    "SushiSwapV3Adapter",
    "SushiSwapV3Config",
    "SwapQuote",
    "SwapResult",
    "SwapType",
    "TransactionData",
    "LPResult",
    # Receipt Parser
    "SushiSwapV3ReceiptParser",
    "SushiSwapV3Event",
    "SushiSwapV3EventType",
    "SwapEventData",
    "TransferEventData",
    "ParsedSwapResult",
    "ParseResult",
    # Tick Math Functions
    "tick_to_sqrt_price_x96",
    "sqrt_price_x96_to_tick",
    "tick_to_price",
    "price_to_tick",
    "sqrt_price_x96_to_price",
    "price_to_sqrt_price_x96",
    "get_nearest_tick",
    "get_min_tick",
    "get_max_tick",
    # Pool Functions
    "compute_pool_address",
    "sort_tokens",
    # Constants
    "Q96",
    "Q128",
    "MIN_TICK",
    "MAX_TICK",
    "TICK_SPACING",
    "FEE_TIERS",
    "FACTORY_ADDRESSES",
    "ROUTER_ADDRESSES",
    "QUOTER_ADDRESSES",
    "POSITION_MANAGER_ADDRESSES",
    "EVENT_TOPICS",
    "TOPIC_TO_EVENT",
    "EVENT_NAME_TO_TYPE",
    "SWAP_EVENT_TOPIC",
    "DEFAULT_FEE_TIER",
    "SUSHISWAP_V3_ADDRESSES",
    "SUSHISWAP_V3_GAS_ESTIMATES",
]
