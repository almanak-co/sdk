"""Uniswap V3 Connector (concentrated liquidity AMM).

This module provides the Uniswap V3 adapter for executing token swaps
on Uniswap V3 across multiple chains.

Supported chains:
- Ethereum
- Arbitrum
- Optimism
- Polygon
- Base
- Avalanche
- BNB Chain
- Monad

Example:
    from almanak.connectors.uniswap_v3 import UniswapV3Adapter, UniswapV3Config

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
    from almanak.connectors.uniswap_v3 import UniswapV3SDK

    sdk = UniswapV3SDK(chain="arbitrum", rpc_url="https://arb1.arbitrum.io/rpc")
    pool = sdk.get_pool_address(weth_addr, usdc_addr, fee_tier=3000)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
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
        UniswapV3LPAdapter,
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
    from .sdk import (
        SwapQuote as SDKSwapQuote,
    )

__all__ = [
    "DEFAULT_FEE_TIER",
    "EVENT_NAME_TO_TYPE",
    "EVENT_TOPICS",
    "FACTORY_ADDRESSES",
    "FEE_TIERS",
    "InvalidFeeError",
    "InvalidTickError",
    "MAX_TICK",
    "MIN_TICK",
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
    "SDKSwapQuote",
    "SWAP_EVENT_TOPIC",
    "SwapEventData",
    "SwapQuote",
    "SwapResult",
    "SwapTransaction",
    "SwapType",
    "TICK_SPACING",
    "TOPIC_TO_EVENT",
    "TransactionData",
    "TransferEventData",
    "UNISWAP_V3_ADDRESSES",
    "UNISWAP_V3_GAS_ESTIMATES",
    "UniswapV3Adapter",
    "UniswapV3Config",
    "UniswapV3Event",
    "UniswapV3EventType",
    "UniswapV3LPAdapter",
    "UniswapV3ReceiptParser",
    "UniswapV3SDK",
    "UniswapV3SDKError",
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
    "FEE_TIERS": (".adapter", "FEE_TIERS"),
    "InvalidFeeError": (".sdk", "InvalidFeeError"),
    "InvalidTickError": (".sdk", "InvalidTickError"),
    "MAX_TICK": (".sdk", "MAX_TICK"),
    "MIN_TICK": (".sdk", "MIN_TICK"),
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
    "SDKSwapQuote": (".sdk", "SwapQuote"),
    "SWAP_EVENT_TOPIC": (".receipt_parser", "SWAP_EVENT_TOPIC"),
    "SwapEventData": (".receipt_parser", "SwapEventData"),
    "SwapQuote": (".adapter", "SwapQuote"),
    "SwapResult": (".adapter", "SwapResult"),
    "SwapTransaction": (".sdk", "SwapTransaction"),
    "SwapType": (".adapter", "SwapType"),
    "TICK_SPACING": (".sdk", "TICK_SPACING"),
    "TOPIC_TO_EVENT": (".receipt_parser", "TOPIC_TO_EVENT"),
    "TransactionData": (".adapter", "TransactionData"),
    "TransferEventData": (".receipt_parser", "TransferEventData"),
    "UNISWAP_V3_ADDRESSES": (".adapter", "UNISWAP_V3_ADDRESSES"),
    "UNISWAP_V3_GAS_ESTIMATES": (".adapter", "UNISWAP_V3_GAS_ESTIMATES"),
    "UniswapV3Adapter": (".adapter", "UniswapV3Adapter"),
    "UniswapV3Config": (".adapter", "UniswapV3Config"),
    "UniswapV3Event": (".receipt_parser", "UniswapV3Event"),
    "UniswapV3EventType": (".receipt_parser", "UniswapV3EventType"),
    "UniswapV3LPAdapter": (".adapter", "UniswapV3LPAdapter"),
    "UniswapV3ReceiptParser": (".receipt_parser", "UniswapV3ReceiptParser"),
    "UniswapV3SDK": (".sdk", "UniswapV3SDK"),
    "UniswapV3SDKError": (".sdk", "UniswapV3SDKError"),
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
            name="uniswap_v3",
            intents=(IntentType.SWAP, IntentType.LP_OPEN, IntentType.LP_CLOSE, IntentType.LP_COLLECT_FEES),
            chains=("ethereum", "arbitrum", "optimism", "polygon", "base", "avalanche", "bnb", "monad"),
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
