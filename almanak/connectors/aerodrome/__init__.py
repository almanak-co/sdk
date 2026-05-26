"""Aerodrome Finance Connector (Base L2).

This package provides integration with Aerodrome Finance, a Solidly-based
AMM on Base chain. Aerodrome supports dual pool types:
- Volatile pools: x*y=k formula (0.3% fee)
- Stable pools: x^3*y + y^3*x formula (0.05% fee)

Key Features:
- Token swaps (exact input)
- Liquidity provision (add/remove)
- Fungible LP tokens (not NFT positions like Uniswap V3)

Example:
    from almanak.connectors.aerodrome import AerodromeAdapter, AerodromeConfig

    config = AerodromeConfig(
        chain="base",
        wallet_address="0x...",
    )
    adapter = AerodromeAdapter(config)

    # Execute a volatile pool swap
    result = adapter.swap_exact_input(
        token_in="USDC",
        token_out="WETH",
        amount_in=Decimal("1000"),
        stable=False,
    )

    # Execute a stable pool swap
    result = adapter.swap_exact_input(
        token_in="USDC",
        token_out="USDbC",
        amount_in=Decimal("1000"),
        stable=True,
    )
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import (
        MAX_UINT128,
        AerodromeAdapter,
        AerodromeConfig,
        CLLiquidityResult,
        LiquidityResult,
        PoolType,
        SwapQuote,
        SwapResult,
        SwapType,
        TransactionData,
    )
    from .compiler import AerodromeCompiler
    from .receipt_parser import (
        BURN_EVENT_TOPIC,
        EVENT_NAME_TO_TYPE,
        EVENT_TOPICS,
        MINT_EVENT_TOPIC,
        SWAP_EVENT_TOPIC,
        TOPIC_TO_EVENT,
        AerodromeEvent,
        AerodromeEventType,
        AerodromeReceiptParser,
        AerodromeSlipstreamReceiptParser,
        BurnEventData,
        MintEventData,
        ParsedLiquidityResult,
        ParsedSwapResult,
        ParseResult,
        SwapEventData,
        TransferEventData,
    )
    from .sdk import (
        AERODROME_ADDRESSES,
        AERODROME_GAS_ESTIMATES,
        MAX_UINT256,
        AerodromeSDK,
        AerodromeSDKError,
        CLPositionInfo,
        InsufficientLiquidityError,
        PoolInfo,
        PoolNotFoundError,
        SwapRoute,
    )
    from .sdk import (
        SwapQuote as SDKSwapQuote,
    )

__all__ = [
    "AERODROME_ADDRESSES",
    "AERODROME_GAS_ESTIMATES",
    "AerodromeAdapter",
    "AerodromeCompiler",
    "AerodromeConfig",
    "AerodromeEvent",
    "AerodromeEventType",
    "AerodromeReceiptParser",
    "AerodromeSDK",
    "AerodromeSDKError",
    "AerodromeSlipstreamReceiptParser",
    "BURN_EVENT_TOPIC",
    "BurnEventData",
    "CLLiquidityResult",
    "CLPositionInfo",
    "EVENT_NAME_TO_TYPE",
    "EVENT_TOPICS",
    "InsufficientLiquidityError",
    "LiquidityResult",
    "MAX_UINT128",
    "MAX_UINT256",
    "MINT_EVENT_TOPIC",
    "MintEventData",
    "ParseResult",
    "ParsedLiquidityResult",
    "ParsedSwapResult",
    "PoolInfo",
    "PoolNotFoundError",
    "PoolType",
    "SDKSwapQuote",
    "SWAP_EVENT_TOPIC",
    "SwapEventData",
    "SwapQuote",
    "SwapResult",
    "SwapRoute",
    "SwapType",
    "TOPIC_TO_EVENT",
    "TransactionData",
    "TransferEventData",
]

_LAZY: dict[str, tuple[str, str]] = {
    "AERODROME_ADDRESSES": (".sdk", "AERODROME_ADDRESSES"),
    "AERODROME_GAS_ESTIMATES": (".sdk", "AERODROME_GAS_ESTIMATES"),
    "AerodromeAdapter": (".adapter", "AerodromeAdapter"),
    "AerodromeCompiler": (".compiler", "AerodromeCompiler"),
    "AerodromeConfig": (".adapter", "AerodromeConfig"),
    "AerodromeEvent": (".receipt_parser", "AerodromeEvent"),
    "AerodromeEventType": (".receipt_parser", "AerodromeEventType"),
    "AerodromeReceiptParser": (".receipt_parser", "AerodromeReceiptParser"),
    "AerodromeSDK": (".sdk", "AerodromeSDK"),
    "AerodromeSDKError": (".sdk", "AerodromeSDKError"),
    "AerodromeSlipstreamReceiptParser": (".receipt_parser", "AerodromeSlipstreamReceiptParser"),
    "BURN_EVENT_TOPIC": (".receipt_parser", "BURN_EVENT_TOPIC"),
    "BurnEventData": (".receipt_parser", "BurnEventData"),
    "CLLiquidityResult": (".adapter", "CLLiquidityResult"),
    "CLPositionInfo": (".sdk", "CLPositionInfo"),
    "EVENT_NAME_TO_TYPE": (".receipt_parser", "EVENT_NAME_TO_TYPE"),
    "EVENT_TOPICS": (".receipt_parser", "EVENT_TOPICS"),
    "InsufficientLiquidityError": (".sdk", "InsufficientLiquidityError"),
    "LiquidityResult": (".adapter", "LiquidityResult"),
    "MAX_UINT128": (".adapter", "MAX_UINT128"),
    "MAX_UINT256": (".sdk", "MAX_UINT256"),
    "MINT_EVENT_TOPIC": (".receipt_parser", "MINT_EVENT_TOPIC"),
    "MintEventData": (".receipt_parser", "MintEventData"),
    "ParseResult": (".receipt_parser", "ParseResult"),
    "ParsedLiquidityResult": (".receipt_parser", "ParsedLiquidityResult"),
    "ParsedSwapResult": (".receipt_parser", "ParsedSwapResult"),
    "PoolInfo": (".sdk", "PoolInfo"),
    "PoolNotFoundError": (".sdk", "PoolNotFoundError"),
    "PoolType": (".adapter", "PoolType"),
    "SDKSwapQuote": (".sdk", "SwapQuote"),
    "SWAP_EVENT_TOPIC": (".receipt_parser", "SWAP_EVENT_TOPIC"),
    "SwapEventData": (".receipt_parser", "SwapEventData"),
    "SwapQuote": (".adapter", "SwapQuote"),
    "SwapResult": (".adapter", "SwapResult"),
    "SwapRoute": (".sdk", "SwapRoute"),
    "SwapType": (".adapter", "SwapType"),
    "TOPIC_TO_EVENT": (".receipt_parser", "TOPIC_TO_EVENT"),
    "TransactionData": (".adapter", "TransactionData"),
    "TransferEventData": (".receipt_parser", "TransferEventData"),
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
    from almanak.connectors._strategy_base.registry import register_connector
    from almanak.framework.intents.vocabulary import IntentType

    register_connector(
        name="aerodrome",
        intents=(IntentType.SWAP, IntentType.LP_OPEN, IntentType.LP_CLOSE),
        chains=("base", "optimism"),
    )
    _registered = True


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
