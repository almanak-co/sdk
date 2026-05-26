"""Joe Lend (Banker Joe) Lending Connector — DORMANT / DEPRECATED.

Joe Lend (the lending arm of Trader Joe / LFJ on Avalanche) was wound down
by its governance in 2026. The on-chain jToken contracts now revert every
supply/borrow/repay/withdraw call with ``Error: wind down``.

The connector has been removed from the SDK's officially supported
protocol surface (info matrix, vocabulary, execution config, CLI, demo
strategies). ``JoeLendAdapter`` is retained only so historical receipts
can still be parsed via ``JoeLendReceiptParser``; instantiating the
adapter raises ``JoeLendDeprecatedError`` on construction.

Full removal is tracked for July (VIB-3960).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import (
        DEFAULT_GAS_ESTIMATES,
        ERC20_APPROVE_SELECTOR,
        JOELEND_BORROW_SELECTOR,
        JOELEND_ENTER_MARKETS_SELECTOR,
        JOELEND_EXIT_MARKET_SELECTOR,
        JOELEND_J_TOKENS,
        JOELEND_JOETROLLER_ADDRESS,
        JOELEND_MINT_NATIVE_SELECTOR,
        JOELEND_MINT_SELECTOR,
        JOELEND_REDEEM_SELECTOR,
        JOELEND_REDEEM_UNDERLYING_SELECTOR,
        JOELEND_REPAY_BORROW_NATIVE_SELECTOR,
        JOELEND_REPAY_BORROW_SELECTOR,
        MAX_UINT256,
        JoeLendAdapter,
        JoeLendConfig,
        JoeLendDeprecatedError,
        JoeLendMarketInfo,
        JoeLendPosition,
        TransactionResult,
    )
    from .receipt_parser import (
        EVENT_NAME_TO_TYPE,
        EVENT_TOPICS,
        TOPIC_TO_EVENT,
        JoeLendEvent,
        JoeLendEventType,
        JoeLendReceiptParser,
        ParseResult,
    )

__all__ = [
    "DEFAULT_GAS_ESTIMATES",
    "ERC20_APPROVE_SELECTOR",
    "EVENT_NAME_TO_TYPE",
    "EVENT_TOPICS",
    "JOELEND_BORROW_SELECTOR",
    "JOELEND_ENTER_MARKETS_SELECTOR",
    "JOELEND_EXIT_MARKET_SELECTOR",
    "JOELEND_JOETROLLER_ADDRESS",
    "JOELEND_J_TOKENS",
    "JOELEND_MINT_NATIVE_SELECTOR",
    "JOELEND_MINT_SELECTOR",
    "JOELEND_REDEEM_SELECTOR",
    "JOELEND_REDEEM_UNDERLYING_SELECTOR",
    "JOELEND_REPAY_BORROW_NATIVE_SELECTOR",
    "JOELEND_REPAY_BORROW_SELECTOR",
    "JoeLendAdapter",
    "JoeLendConfig",
    "JoeLendDeprecatedError",
    "JoeLendEvent",
    "JoeLendEventType",
    "JoeLendMarketInfo",
    "JoeLendPosition",
    "JoeLendReceiptParser",
    "MAX_UINT256",
    "ParseResult",
    "TOPIC_TO_EVENT",
    "TransactionResult",
]

_LAZY: dict[str, tuple[str, str]] = {
    "DEFAULT_GAS_ESTIMATES": (".adapter", "DEFAULT_GAS_ESTIMATES"),
    "ERC20_APPROVE_SELECTOR": (".adapter", "ERC20_APPROVE_SELECTOR"),
    "EVENT_NAME_TO_TYPE": (".receipt_parser", "EVENT_NAME_TO_TYPE"),
    "EVENT_TOPICS": (".receipt_parser", "EVENT_TOPICS"),
    "JOELEND_BORROW_SELECTOR": (".adapter", "JOELEND_BORROW_SELECTOR"),
    "JOELEND_ENTER_MARKETS_SELECTOR": (".adapter", "JOELEND_ENTER_MARKETS_SELECTOR"),
    "JOELEND_EXIT_MARKET_SELECTOR": (".adapter", "JOELEND_EXIT_MARKET_SELECTOR"),
    "JOELEND_JOETROLLER_ADDRESS": (".adapter", "JOELEND_JOETROLLER_ADDRESS"),
    "JOELEND_J_TOKENS": (".adapter", "JOELEND_J_TOKENS"),
    "JOELEND_MINT_NATIVE_SELECTOR": (".adapter", "JOELEND_MINT_NATIVE_SELECTOR"),
    "JOELEND_MINT_SELECTOR": (".adapter", "JOELEND_MINT_SELECTOR"),
    "JOELEND_REDEEM_SELECTOR": (".adapter", "JOELEND_REDEEM_SELECTOR"),
    "JOELEND_REDEEM_UNDERLYING_SELECTOR": (".adapter", "JOELEND_REDEEM_UNDERLYING_SELECTOR"),
    "JOELEND_REPAY_BORROW_NATIVE_SELECTOR": (".adapter", "JOELEND_REPAY_BORROW_NATIVE_SELECTOR"),
    "JOELEND_REPAY_BORROW_SELECTOR": (".adapter", "JOELEND_REPAY_BORROW_SELECTOR"),
    "JoeLendAdapter": (".adapter", "JoeLendAdapter"),
    "JoeLendConfig": (".adapter", "JoeLendConfig"),
    "JoeLendDeprecatedError": (".adapter", "JoeLendDeprecatedError"),
    "JoeLendEvent": (".receipt_parser", "JoeLendEvent"),
    "JoeLendEventType": (".receipt_parser", "JoeLendEventType"),
    "JoeLendMarketInfo": (".adapter", "JoeLendMarketInfo"),
    "JoeLendPosition": (".adapter", "JoeLendPosition"),
    "JoeLendReceiptParser": (".receipt_parser", "JoeLendReceiptParser"),
    "MAX_UINT256": (".adapter", "MAX_UINT256"),
    "ParseResult": (".receipt_parser", "ParseResult"),
    "TOPIC_TO_EVENT": (".receipt_parser", "TOPIC_TO_EVENT"),
    "TransactionResult": (".adapter", "TransactionResult"),
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
