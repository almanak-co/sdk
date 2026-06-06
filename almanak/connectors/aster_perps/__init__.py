"""Aster Perps connector (Aster/ApolloX Diamond on BSC).

Aster is the on-chain perpetual trading platform (formerly ApolloX, rebranded
March 2025). PancakeSwap Perps runs on top of Aster as broker id = 2; raw Aster
use is broker id = 0. The canonical connector lives here; ``pancakeswap_perps``
is a thin shim that binds ``broker_id=2`` for backward compatibility.

Phase 1 scope (PRD: `docs/internal/discussions/aster-dex-integration-20260418.md`):
    - BSC only
    - Market orders only
    - Crypto markets (BTC/USD, ETH/USD, BNB/USD)
    - No SL/TP, no limit orders
    - Native BNB margin (openMarketTradeBNB) or ERC20 margin (openMarketTrade)

Multi-chain EVM, spot, Solana, funding-rate data are deferred to later phases
gated on named deep-research items (VIB-3044 epic).

Example usage — strategy-author facing::

    # Inside an IntentStrategy.decide()
    return Intent.perp_open(
        market="BTC/USD",
        collateral_token="BNB",
        collateral_amount=Decimal("0.1"),
        size_usd=Decimal("300"),
        is_long=True,
        max_slippage=Decimal("0.01"),
        protocol="aster_perps",              # canonical key
        leverage=Decimal("3"),
    )

    # Legacy callers may still pass protocol="pancakeswap_perps"; the compiler
    # routes that through the pancakeswap_perps shim, which forces broker_id=2.
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

_registered = False


def _register_once() -> None:
    """Compatibility no-op; strategy registration lives in connector.py."""
    global _registered
    if _registered:
        return
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
