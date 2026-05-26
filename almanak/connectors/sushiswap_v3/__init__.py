"""SushiSwap V3 Connector Package (concentrated liquidity AMM).

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
    from almanak.connectors.sushiswap_v3 import (
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

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import (
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
    from .receipt_parser import (
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
    from .sdk import (
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
    "DEFAULT_FEE_TIER",
    "EVENT_NAME_TO_TYPE",
    "EVENT_TOPICS",
    "FACTORY_ADDRESSES",
    "FEE_TIERS",
    "InvalidFeeError",
    "InvalidTickError",
    "LPResult",
    "LPTransaction",
    "MAX_TICK",
    "MIN_TICK",
    "MintParams",
    "POSITION_MANAGER_ADDRESSES",
    "ParseResult",
    "ParsedSwapResult",
    "PoolInfo",
    "PoolNotFoundError",
    "PoolState",
    "Q128",
    "Q96",
    "QUOTER_ADDRESSES",
    "QuoteError",
    "ROUTER_ADDRESSES",
    "SUSHISWAP_V3_ADDRESSES",
    "SUSHISWAP_V3_GAS_ESTIMATES",
    "SWAP_EVENT_TOPIC",
    "SushiSwapV3Adapter",
    "SushiSwapV3Config",
    "SushiSwapV3Event",
    "SushiSwapV3EventType",
    "SushiSwapV3ReceiptParser",
    "SushiSwapV3SDK",
    "SushiSwapV3SDKError",
    "SwapEventData",
    "SwapQuote",
    "SwapResult",
    "SwapTransaction",
    "SwapType",
    "TICK_SPACING",
    "TOPIC_TO_EVENT",
    "TransactionData",
    "TransferEventData",
    "compute_pool_address",
    "get_max_tick",
    "get_min_tick",
    "get_nearest_tick",
    "price_to_sqrt_price_x96",
    "price_to_tick",
    "sort_tokens",
    "sqrt_price_x96_to_price",
    "sqrt_price_x96_to_tick",
    "tick_to_price",
    "tick_to_sqrt_price_x96",
]

_LAZY: dict[str, tuple[str, str]] = {
    "DEFAULT_FEE_TIER": (".adapter", "DEFAULT_FEE_TIER"),
    "EVENT_NAME_TO_TYPE": (".receipt_parser", "EVENT_NAME_TO_TYPE"),
    "EVENT_TOPICS": (".receipt_parser", "EVENT_TOPICS"),
    "FACTORY_ADDRESSES": (".sdk", "FACTORY_ADDRESSES"),
    "FEE_TIERS": (".sdk", "FEE_TIERS"),
    "InvalidFeeError": (".sdk", "InvalidFeeError"),
    "InvalidTickError": (".sdk", "InvalidTickError"),
    "LPResult": (".adapter", "LPResult"),
    "LPTransaction": (".sdk", "LPTransaction"),
    "MAX_TICK": (".sdk", "MAX_TICK"),
    "MIN_TICK": (".sdk", "MIN_TICK"),
    "MintParams": (".sdk", "MintParams"),
    "POSITION_MANAGER_ADDRESSES": (".receipt_parser", "POSITION_MANAGER_ADDRESSES"),
    "ParseResult": (".receipt_parser", "ParseResult"),
    "ParsedSwapResult": (".receipt_parser", "ParsedSwapResult"),
    "PoolInfo": (".sdk", "PoolInfo"),
    "PoolNotFoundError": (".sdk", "PoolNotFoundError"),
    "PoolState": (".sdk", "PoolState"),
    "Q128": (".sdk", "Q128"),
    "Q96": (".sdk", "Q96"),
    "QUOTER_ADDRESSES": (".sdk", "QUOTER_ADDRESSES"),
    "QuoteError": (".sdk", "QuoteError"),
    "ROUTER_ADDRESSES": (".sdk", "ROUTER_ADDRESSES"),
    "SUSHISWAP_V3_ADDRESSES": (".adapter", "SUSHISWAP_V3_ADDRESSES"),
    "SUSHISWAP_V3_GAS_ESTIMATES": (".adapter", "SUSHISWAP_V3_GAS_ESTIMATES"),
    "SWAP_EVENT_TOPIC": (".receipt_parser", "SWAP_EVENT_TOPIC"),
    "SushiSwapV3Adapter": (".adapter", "SushiSwapV3Adapter"),
    "SushiSwapV3Config": (".adapter", "SushiSwapV3Config"),
    "SushiSwapV3Event": (".receipt_parser", "SushiSwapV3Event"),
    "SushiSwapV3EventType": (".receipt_parser", "SushiSwapV3EventType"),
    "SushiSwapV3ReceiptParser": (".receipt_parser", "SushiSwapV3ReceiptParser"),
    "SushiSwapV3SDK": (".sdk", "SushiSwapV3SDK"),
    "SushiSwapV3SDKError": (".sdk", "SushiSwapV3SDKError"),
    "SwapEventData": (".receipt_parser", "SwapEventData"),
    "SwapQuote": (".adapter", "SwapQuote"),
    "SwapResult": (".adapter", "SwapResult"),
    "SwapTransaction": (".sdk", "SwapTransaction"),
    "SwapType": (".adapter", "SwapType"),
    "TICK_SPACING": (".sdk", "TICK_SPACING"),
    "TOPIC_TO_EVENT": (".receipt_parser", "TOPIC_TO_EVENT"),
    "TransactionData": (".adapter", "TransactionData"),
    "TransferEventData": (".receipt_parser", "TransferEventData"),
    "compute_pool_address": (".sdk", "compute_pool_address"),
    "get_max_tick": (".sdk", "get_max_tick"),
    "get_min_tick": (".sdk", "get_min_tick"),
    "get_nearest_tick": (".sdk", "get_nearest_tick"),
    "price_to_sqrt_price_x96": (".sdk", "price_to_sqrt_price_x96"),
    "price_to_tick": (".sdk", "price_to_tick"),
    "sort_tokens": (".sdk", "sort_tokens"),
    "sqrt_price_x96_to_price": (".sdk", "sqrt_price_x96_to_price"),
    "sqrt_price_x96_to_tick": (".sdk", "sqrt_price_x96_to_tick"),
    "tick_to_price": (".sdk", "tick_to_price"),
    "tick_to_sqrt_price_x96": (".sdk", "tick_to_sqrt_price_x96"),
}

_registered = False


def _register_once() -> None:
    """Fire ``register_connector`` once on first strategy-side access.

    Deferred so importing the connector's gateway-side surface during
    gateway boot does not pull ``framework.intents.vocabulary`` into the
    partially-initialised config-init chain (VIB-4835).
    """
    global _registered
    if _registered:
        return
    _registered = True
    try:
        from almanak.connectors._strategy_base.registry import register_connector
        from almanak.framework.intents.vocabulary import IntentType

        register_connector(
            name="sushiswap_v3",
            intents=(IntentType.SWAP, IntentType.LP_OPEN, IntentType.LP_CLOSE, IntentType.LP_COLLECT_FEES),
            chains=("ethereum", "arbitrum", "base", "optimism", "polygon", "bnb"),
        )
    except Exception:
        _registered = False
        raise


def __getattr__(name: str) -> Any:
    """PEP 562 lazy attribute access."""
    if name not in _LAZY:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    submodule, attr = _LAZY[name]
    import importlib

    module = importlib.import_module(submodule, package=__name__)
    value = getattr(module, attr)
    globals()[name] = value
    _register_once()
    return value
