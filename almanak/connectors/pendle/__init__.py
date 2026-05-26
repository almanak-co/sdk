"""Pendle Protocol Connector.

Pendle Finance is a permissionless yield-trading protocol that enables:
- Tokenizing yield-bearing assets into PT (Principal) and YT (Yield) tokens
- Trading PT and YT on Pendle's AMM
- Providing liquidity to PT/SY pools
- Redeeming PT at maturity

Components:
- ``PendleSDK``: Low-level protocol interactions
- ``PendleAdapter``: ActionType to SDK mapping
- ``PendleReceiptParser``: Transaction receipt parsing

Supported Chains:
- Arbitrum (primary)
- Ethereum

Example::

    from almanak.connectors.pendle import (
        PendleSDK,
        PendleAdapter,
        PendleReceiptParser,
    )

    sdk = PendleSDK(rpc_url="https://arb1.arbitrum.io/rpc", chain="arbitrum")

Lazy attribute access (VIB-4835)
--------------------------------
Strategy-facing symbols (``PendleSDK``, ``PendleAdapter``, …) are exposed
via PEP 562 ``__getattr__``. ``almanak.gateway.core.settings`` imports
``almanak.connectors.pendle.gateway.settings`` at module load (composes
``PendleGatewaySettings`` into ``GatewaySettings`` via multi-inheritance);
Python runs this ``__init__.py`` first, and an eager import of
``almanak.framework.intents.vocabulary`` would explode a circular
config-init chain (see the matching note in ``enso/__init__.py``). Lazy
attributes avoid the cycle entirely.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import (
        PendleAdapter,
        PendleLPParams,
        PendleRedeemParams,
        PendleSwapParams,
        get_pendle_adapter,
    )
    from .compiler import PendleCompiler
    from .receipt_parser import (
        EVENT_TOPICS,
        BurnEventData,
        MintEventData,
        ParsedSwapResult,
        ParseResult,
        PendleEvent,
        PendleEventType,
        PendleReceiptParser,
        RedeemPYEventData,
        SwapEventData,
        TransferEventData,
    )
    from .sdk import (
        PENDLE_ADDRESSES,
        PENDLE_GAS_ESTIMATES,
        LiquidityParams,
        MarketInfo,
        PendleActionType,
        PendleQuote,
        PendleSDK,
        PendleTransactionData,
        SwapParams,
        get_pendle_sdk,
    )

__all__ = [
    "EVENT_TOPICS",
    "PENDLE_ADDRESSES",
    "PENDLE_GAS_ESTIMATES",
    "BurnEventData",
    "LiquidityParams",
    "MarketInfo",
    "MintEventData",
    "ParseResult",
    "ParsedSwapResult",
    "PendleActionType",
    "PendleAdapter",
    "PendleCompiler",
    "PendleEvent",
    "PendleEventType",
    "PendleLPParams",
    "PendleQuote",
    "PendleReceiptParser",
    "PendleRedeemParams",
    "PendleSDK",
    "PendleSwapParams",
    "PendleTransactionData",
    "RedeemPYEventData",
    "SwapEventData",
    "SwapParams",
    "TransferEventData",
    "get_pendle_adapter",
    "get_pendle_sdk",
]

_LAZY: dict[str, tuple[str, str]] = {
    # adapter
    "PendleAdapter": (".adapter", "PendleAdapter"),
    "PendleLPParams": (".adapter", "PendleLPParams"),
    "PendleRedeemParams": (".adapter", "PendleRedeemParams"),
    "PendleSwapParams": (".adapter", "PendleSwapParams"),
    "get_pendle_adapter": (".adapter", "get_pendle_adapter"),
    # compiler
    "PendleCompiler": (".compiler", "PendleCompiler"),
    # receipt_parser
    "EVENT_TOPICS": (".receipt_parser", "EVENT_TOPICS"),
    "BurnEventData": (".receipt_parser", "BurnEventData"),
    "MintEventData": (".receipt_parser", "MintEventData"),
    "ParsedSwapResult": (".receipt_parser", "ParsedSwapResult"),
    "ParseResult": (".receipt_parser", "ParseResult"),
    "PendleEvent": (".receipt_parser", "PendleEvent"),
    "PendleEventType": (".receipt_parser", "PendleEventType"),
    "PendleReceiptParser": (".receipt_parser", "PendleReceiptParser"),
    "RedeemPYEventData": (".receipt_parser", "RedeemPYEventData"),
    "SwapEventData": (".receipt_parser", "SwapEventData"),
    "TransferEventData": (".receipt_parser", "TransferEventData"),
    # sdk
    "PENDLE_ADDRESSES": (".sdk", "PENDLE_ADDRESSES"),
    "PENDLE_GAS_ESTIMATES": (".sdk", "PENDLE_GAS_ESTIMATES"),
    "LiquidityParams": (".sdk", "LiquidityParams"),
    "MarketInfo": (".sdk", "MarketInfo"),
    "PendleActionType": (".sdk", "PendleActionType"),
    "PendleQuote": (".sdk", "PendleQuote"),
    "PendleSDK": (".sdk", "PendleSDK"),
    "PendleTransactionData": (".sdk", "PendleTransactionData"),
    "SwapParams": (".sdk", "SwapParams"),
    "get_pendle_sdk": (".sdk", "get_pendle_sdk"),
}

_registered = False


def _register_once() -> None:
    global _registered
    if _registered:
        return
    from almanak.connectors._strategy_base.registry import register_connector
    from almanak.framework.intents.vocabulary import IntentType

    register_connector(
        name="pendle",
        intents=(
            IntentType.SWAP,
            IntentType.LP_OPEN,
            IntentType.LP_CLOSE,
            IntentType.WITHDRAW,
        ),
        chains=(
            "arbitrum",
            "ethereum",
        ),
    )
    _registered = True


def __getattr__(name: str) -> Any:
    if name not in _LAZY:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    submodule, attr = _LAZY[name]
    import importlib

    module = importlib.import_module(submodule, package=__name__)
    value = getattr(module, attr)
    globals()[name] = value
    _register_once()
    return value
