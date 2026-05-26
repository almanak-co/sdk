"""PancakeSwap V3 Connector (concentrated liquidity AMM).

This module provides an adapter for interacting with PancakeSwap V3,
which is a Uniswap V3 fork with different fee tiers and addresses.

PancakeSwap V3 is a decentralized exchange supporting:
- Exact input swaps (swap specific amount of input token)
- Exact output swaps (receive specific amount of output token)
- Multiple fee tiers (100, 500, 2500, 10000 bps)

Supported chains:
- BNB Smart Chain (BSC)
- Ethereum
- Arbitrum
- Base

Example:
    from almanak.connectors.pancakeswap_v3 import (
        PancakeSwapV3Adapter,
        PancakeSwapV3Config,
    )

    config = PancakeSwapV3Config(
        chain="bnb",
        wallet_address="0x...",
    )
    adapter = PancakeSwapV3Adapter(config)

    # Swap exact input
    result = adapter.swap_exact_input(
        token_in="USDT",
        token_out="WBNB",
        amount_in=Decimal("100"),
    )
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import (
        DEFAULT_GAS_ESTIMATES,
        EXACT_INPUT_SINGLE_SELECTOR,
        EXACT_OUTPUT_SINGLE_SELECTOR,
        FEE_TIERS,
        PANCAKESWAP_V3_ADDRESSES,
        PancakeSwapV3Adapter,
        PancakeSwapV3Config,
        TransactionResult,
    )
    from .receipt_parser import (
        EVENT_NAME_TO_TYPE,
        EVENT_TOPICS,
        TOPIC_TO_EVENT,
        PancakeSwapV3EventType,
        PancakeSwapV3ReceiptParser,
        ParseResult,
        SwapEventData,
    )

__all__ = [
    "DEFAULT_GAS_ESTIMATES",
    "EVENT_NAME_TO_TYPE",
    "EVENT_TOPICS",
    "EXACT_INPUT_SINGLE_SELECTOR",
    "EXACT_OUTPUT_SINGLE_SELECTOR",
    "FEE_TIERS",
    "PANCAKESWAP_V3_ADDRESSES",
    "PancakeSwapV3Adapter",
    "PancakeSwapV3Config",
    "PancakeSwapV3EventType",
    "PancakeSwapV3ReceiptParser",
    "ParseResult",
    "SwapEventData",
    "TOPIC_TO_EVENT",
    "TransactionResult",
]

_LAZY: dict[str, tuple[str, str]] = {
    "DEFAULT_GAS_ESTIMATES": (".adapter", "DEFAULT_GAS_ESTIMATES"),
    "EVENT_NAME_TO_TYPE": (".receipt_parser", "EVENT_NAME_TO_TYPE"),
    "EVENT_TOPICS": (".receipt_parser", "EVENT_TOPICS"),
    "EXACT_INPUT_SINGLE_SELECTOR": (".adapter", "EXACT_INPUT_SINGLE_SELECTOR"),
    "EXACT_OUTPUT_SINGLE_SELECTOR": (".adapter", "EXACT_OUTPUT_SINGLE_SELECTOR"),
    "FEE_TIERS": (".adapter", "FEE_TIERS"),
    "PANCAKESWAP_V3_ADDRESSES": (".adapter", "PANCAKESWAP_V3_ADDRESSES"),
    "PancakeSwapV3Adapter": (".adapter", "PancakeSwapV3Adapter"),
    "PancakeSwapV3Config": (".adapter", "PancakeSwapV3Config"),
    "PancakeSwapV3EventType": (".receipt_parser", "PancakeSwapV3EventType"),
    "PancakeSwapV3ReceiptParser": (".receipt_parser", "PancakeSwapV3ReceiptParser"),
    "ParseResult": (".receipt_parser", "ParseResult"),
    "SwapEventData": (".receipt_parser", "SwapEventData"),
    "TOPIC_TO_EVENT": (".receipt_parser", "TOPIC_TO_EVENT"),
    "TransactionResult": (".adapter", "TransactionResult"),
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
        name="pancakeswap_v3",
        intents=(IntentType.SWAP, IntentType.LP_OPEN, IntentType.LP_CLOSE, IntentType.LP_COLLECT_FEES),
        chains=("bnb", "ethereum", "arbitrum", "base"),
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
