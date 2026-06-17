"""Shared Aster perp implementation (foundation core).

Single source of truth for the Aster Diamond perp venue (Aster / ApolloX on
BSC), consumed by the thin ``aster_perps`` and ``pancakeswap_perps`` connector
manifests. Underscore-prefixed so it is never discovered as a connector and is
treated as foundation by the connector-isolation guards: deleting either leaf
connector must not strand this implementation.

PancakeSwap Perps runs on top of Aster as broker id = 2; raw Aster use is
broker id = 0. The package-level lazy exports below mirror the public API both
leaves expose; the actual implementation lives in the sibling modules
(``sdk``, ``adapter``, ``receipt_parser``, ``compiler``, ``perps_read``,
``addresses``, ``gateway``). Lazy access keeps importing a submodule (e.g.
``addresses`` during descriptor discovery) free of registration side effects.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import (
        GAS_CLOSE_TRADE,
        GAS_OPEN_MARKET_TRADE,
        GAS_OPEN_MARKET_TRADE_BNB,
        AsterPerpsAdapter,
        AsterPerpsConfig,
        AsterPerpsTx,
        PerpOpenOrderResult,
        build_close_transaction,
        build_open_transaction,
    )
    from .receipt_parser import (
        AsterPerpsReceiptParser,
        CloseTradeReceivedEvent,
        CloseTradeSuccessfulEvent,
        MarketPendingTradeEvent,
        OpenMarketTradeEvent,
        ParsedReceipt,
        PendingTradeRefundEvent,
    )
    from .sdk import (
        ASTER_BROKER_RAW,
        EVENT_CLOSE_TRADE_RECEIVED,
        EVENT_CLOSE_TRADE_SUCCESSFUL,
        EVENT_MARKET_PENDING_TRADE,
        EVENT_OPEN_MARKET_TRADE,
        EVENT_PENDING_TRADE_REFUND,
        NATIVE_BNB_ADDRESS,
        PCS_BROKER_ID,
        PRICE_DECIMALS,
        QTY_DECIMALS,
        SELECTOR_CLOSE_TRADE,
        SELECTOR_OPEN_MARKET_TRADE,
        SELECTOR_OPEN_MARKET_TRADE_BNB,
        OpenTradeStruct,
        encode_close_trade_calldata,
        encode_get_pending_trade_calldata,
        encode_get_position_by_hash_calldata,
        encode_open_market_trade_calldata,
        get_margin_token_address,
        get_pair_base,
        get_router_address,
        slippage_to_limit_price,
        usd_size_to_qty,
    )

__all__ = [
    "ASTER_BROKER_RAW",
    "AsterPerpsAdapter",
    "AsterPerpsConfig",
    "AsterPerpsReceiptParser",
    "AsterPerpsTx",
    "CloseTradeReceivedEvent",
    "CloseTradeSuccessfulEvent",
    "EVENT_CLOSE_TRADE_RECEIVED",
    "EVENT_CLOSE_TRADE_SUCCESSFUL",
    "EVENT_MARKET_PENDING_TRADE",
    "EVENT_OPEN_MARKET_TRADE",
    "EVENT_PENDING_TRADE_REFUND",
    "GAS_CLOSE_TRADE",
    "GAS_OPEN_MARKET_TRADE",
    "GAS_OPEN_MARKET_TRADE_BNB",
    "MarketPendingTradeEvent",
    "NATIVE_BNB_ADDRESS",
    "OpenMarketTradeEvent",
    "OpenTradeStruct",
    "PCS_BROKER_ID",
    "PRICE_DECIMALS",
    "ParsedReceipt",
    "PendingTradeRefundEvent",
    "PerpOpenOrderResult",
    "QTY_DECIMALS",
    "SELECTOR_CLOSE_TRADE",
    "SELECTOR_OPEN_MARKET_TRADE",
    "SELECTOR_OPEN_MARKET_TRADE_BNB",
    "build_close_transaction",
    "build_open_transaction",
    "encode_close_trade_calldata",
    "encode_get_pending_trade_calldata",
    "encode_get_position_by_hash_calldata",
    "encode_open_market_trade_calldata",
    "get_margin_token_address",
    "get_pair_base",
    "get_router_address",
    "slippage_to_limit_price",
    "usd_size_to_qty",
]

_LAZY: dict[str, tuple[str, str]] = {
    "ASTER_BROKER_RAW": (".sdk", "ASTER_BROKER_RAW"),
    "AsterPerpsAdapter": (".adapter", "AsterPerpsAdapter"),
    "AsterPerpsConfig": (".adapter", "AsterPerpsConfig"),
    "AsterPerpsReceiptParser": (".receipt_parser", "AsterPerpsReceiptParser"),
    "AsterPerpsTx": (".adapter", "AsterPerpsTx"),
    "CloseTradeReceivedEvent": (".receipt_parser", "CloseTradeReceivedEvent"),
    "CloseTradeSuccessfulEvent": (".receipt_parser", "CloseTradeSuccessfulEvent"),
    "EVENT_CLOSE_TRADE_RECEIVED": (".sdk", "EVENT_CLOSE_TRADE_RECEIVED"),
    "EVENT_CLOSE_TRADE_SUCCESSFUL": (".sdk", "EVENT_CLOSE_TRADE_SUCCESSFUL"),
    "EVENT_MARKET_PENDING_TRADE": (".sdk", "EVENT_MARKET_PENDING_TRADE"),
    "EVENT_OPEN_MARKET_TRADE": (".sdk", "EVENT_OPEN_MARKET_TRADE"),
    "EVENT_PENDING_TRADE_REFUND": (".sdk", "EVENT_PENDING_TRADE_REFUND"),
    "GAS_CLOSE_TRADE": (".adapter", "GAS_CLOSE_TRADE"),
    "GAS_OPEN_MARKET_TRADE": (".adapter", "GAS_OPEN_MARKET_TRADE"),
    "GAS_OPEN_MARKET_TRADE_BNB": (".adapter", "GAS_OPEN_MARKET_TRADE_BNB"),
    "MarketPendingTradeEvent": (".receipt_parser", "MarketPendingTradeEvent"),
    "NATIVE_BNB_ADDRESS": (".sdk", "NATIVE_BNB_ADDRESS"),
    "OpenMarketTradeEvent": (".receipt_parser", "OpenMarketTradeEvent"),
    "OpenTradeStruct": (".sdk", "OpenTradeStruct"),
    "PCS_BROKER_ID": (".sdk", "PCS_BROKER_ID"),
    "PRICE_DECIMALS": (".sdk", "PRICE_DECIMALS"),
    "ParsedReceipt": (".receipt_parser", "ParsedReceipt"),
    "PendingTradeRefundEvent": (".receipt_parser", "PendingTradeRefundEvent"),
    "PerpOpenOrderResult": (".adapter", "PerpOpenOrderResult"),
    "QTY_DECIMALS": (".sdk", "QTY_DECIMALS"),
    "SELECTOR_CLOSE_TRADE": (".sdk", "SELECTOR_CLOSE_TRADE"),
    "SELECTOR_OPEN_MARKET_TRADE": (".sdk", "SELECTOR_OPEN_MARKET_TRADE"),
    "SELECTOR_OPEN_MARKET_TRADE_BNB": (".sdk", "SELECTOR_OPEN_MARKET_TRADE_BNB"),
    "build_close_transaction": (".adapter", "build_close_transaction"),
    "build_open_transaction": (".adapter", "build_open_transaction"),
    "encode_close_trade_calldata": (".sdk", "encode_close_trade_calldata"),
    "encode_get_pending_trade_calldata": (".sdk", "encode_get_pending_trade_calldata"),
    "encode_get_position_by_hash_calldata": (".sdk", "encode_get_position_by_hash_calldata"),
    "encode_open_market_trade_calldata": (".sdk", "encode_open_market_trade_calldata"),
    "get_margin_token_address": (".sdk", "get_margin_token_address"),
    "get_pair_base": (".sdk", "get_pair_base"),
    "get_router_address": (".sdk", "get_router_address"),
    "slippage_to_limit_price": (".sdk", "slippage_to_limit_price"),
    "usd_size_to_qty": (".sdk", "usd_size_to_qty"),
}


def __getattr__(name: str) -> Any:
    """PEP 562 lazy attribute access."""
    if name not in _LAZY:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    submodule, attr = _LAZY[name]
    import importlib

    module = importlib.import_module(submodule, package=__name__)
    value = getattr(module, attr)
    globals()[name] = value
    return value
