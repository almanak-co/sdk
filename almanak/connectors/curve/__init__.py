"""Curve Finance Connector.

This module provides the Curve Finance adapter for executing swaps and
managing liquidity positions on Curve pools across multiple chains.

Supported chains:
- Ethereum
- Arbitrum

Supported operations:
- SWAP: Token swaps via Curve pools (StableSwap, CryptoSwap, Tricrypto)
- LP_OPEN: Add liquidity to Curve pools
- LP_CLOSE: Remove liquidity from Curve pools

Example:
    from almanak.connectors.curve import CurveAdapter, CurveConfig

    config = CurveConfig(
        chain="ethereum",
        wallet_address="0x...",
    )
    adapter = CurveAdapter(config)

    # Execute a swap
    result = adapter.swap(
        pool_address="0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7",  # 3pool
        token_in="USDC",
        token_out="DAI",
        amount_in=Decimal("1000"),
    )

    # Add liquidity
    lp_result = adapter.add_liquidity(
        pool_address="0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7",
        amounts=[Decimal("1000"), Decimal("1000"), Decimal("1000")],  # DAI, USDC, USDT
    )
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import (
        CURVE_ADDRESSES,
        CURVE_GAS_ESTIMATES,
        CURVE_POOLS,
        CurveAdapter,
        CurveConfig,
        LiquidityResult,
        PoolInfo,
        PoolType,
        SwapResult,
        TransactionData,
    )
    from .receipt_parser import (
        EVENT_TOPICS,
        TOPIC_TO_EVENT,
        AddLiquidityEventData,
        CurveEvent,
        CurveEventType,
        CurveReceiptParser,
        ParseResult,
        RemoveLiquidityEventData,
        SwapEventData,
    )

__all__ = [
    "AddLiquidityEventData",
    "CURVE_ADDRESSES",
    "CURVE_GAS_ESTIMATES",
    "CURVE_POOLS",
    "CurveAdapter",
    "CurveConfig",
    "CurveEvent",
    "CurveEventType",
    "CurveReceiptParser",
    "EVENT_TOPICS",
    "LiquidityResult",
    "ParseResult",
    "PoolInfo",
    "PoolType",
    "RemoveLiquidityEventData",
    "SwapEventData",
    "SwapResult",
    "TOPIC_TO_EVENT",
    "TransactionData",
]

_LAZY: dict[str, tuple[str, str]] = {
    "AddLiquidityEventData": (".receipt_parser", "AddLiquidityEventData"),
    "CURVE_ADDRESSES": (".adapter", "CURVE_ADDRESSES"),
    "CURVE_GAS_ESTIMATES": (".adapter", "CURVE_GAS_ESTIMATES"),
    "CURVE_POOLS": (".adapter", "CURVE_POOLS"),
    "CurveAdapter": (".adapter", "CurveAdapter"),
    "CurveConfig": (".adapter", "CurveConfig"),
    "CurveEvent": (".receipt_parser", "CurveEvent"),
    "CurveEventType": (".receipt_parser", "CurveEventType"),
    "CurveReceiptParser": (".receipt_parser", "CurveReceiptParser"),
    "EVENT_TOPICS": (".receipt_parser", "EVENT_TOPICS"),
    "LiquidityResult": (".adapter", "LiquidityResult"),
    "ParseResult": (".receipt_parser", "ParseResult"),
    "PoolInfo": (".adapter", "PoolInfo"),
    "PoolType": (".adapter", "PoolType"),
    "RemoveLiquidityEventData": (".receipt_parser", "RemoveLiquidityEventData"),
    "SwapEventData": (".receipt_parser", "SwapEventData"),
    "SwapResult": (".adapter", "SwapResult"),
    "TOPIC_TO_EVENT": (".receipt_parser", "TOPIC_TO_EVENT"),
    "TransactionData": (".adapter", "TransactionData"),
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
            name="curve",
            intents=(IntentType.SWAP, IntentType.LP_OPEN, IntentType.LP_CLOSE),
            chains=("ethereum", "arbitrum", "optimism", "polygon", "base"),
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
