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
    # Adapter
    JoeLendAdapter,
    JoeLendConfig,
    JoeLendDeprecatedError,
    # Data classes
    JoeLendMarketInfo,
    JoeLendPosition,
    TransactionResult,
)
from .receipt_parser import (
    EVENT_NAME_TO_TYPE,
    # Constants
    EVENT_TOPICS,
    TOPIC_TO_EVENT,
    # Event class
    JoeLendEvent,
    JoeLendEventType,
    # Parser
    JoeLendReceiptParser,
    ParseResult,
)

__all__ = [
    # Adapter
    "JoeLendAdapter",
    "JoeLendConfig",
    "JoeLendDeprecatedError",
    # Data classes
    "JoeLendMarketInfo",
    "JoeLendPosition",
    "TransactionResult",
    # Constants
    "JOELEND_JOETROLLER_ADDRESS",
    "JOELEND_J_TOKENS",
    "DEFAULT_GAS_ESTIMATES",
    # Function selectors
    "JOELEND_MINT_SELECTOR",
    "JOELEND_MINT_NATIVE_SELECTOR",
    "JOELEND_REDEEM_SELECTOR",
    "JOELEND_REDEEM_UNDERLYING_SELECTOR",
    "JOELEND_BORROW_SELECTOR",
    "JOELEND_REPAY_BORROW_SELECTOR",
    "JOELEND_REPAY_BORROW_NATIVE_SELECTOR",
    "JOELEND_ENTER_MARKETS_SELECTOR",
    "JOELEND_EXIT_MARKET_SELECTOR",
    "ERC20_APPROVE_SELECTOR",
    "MAX_UINT256",
    # Receipt Parser
    "JoeLendReceiptParser",
    "JoeLendEvent",
    "JoeLendEventType",
    "ParseResult",
    # Event constants
    "EVENT_TOPICS",
    "TOPIC_TO_EVENT",
    "EVENT_NAME_TO_TYPE",
]
