"""Joe Lend (Banker Joe) Lending Connector (Compound V2 fork on Avalanche).

This module provides adapters and utilities for interacting with Joe Lend (Banker Joe),
a lending/borrowing protocol on Avalanche using the jToken architecture.

Joe Lend Features:
- Compound V2-style jToken model
- Supply to earn yield (mint jTokens)
- Borrow against collateral (enterMarkets + borrow)
- Multiple asset markets: AVAX, USDC.e, USDT.e, WETH.e, WBTC.e, DAI.e

Supported Chains:
- Avalanche

Example:
    from almanak.framework.connectors.joelend import (
        JoeLendAdapter,
        JoeLendConfig,
        JoeLendReceiptParser,
    )

    config = JoeLendConfig(
        chain="avalanche",
        wallet_address="0x...",
    )
    adapter = JoeLendAdapter(config)

    # Supply USDC.e
    result = adapter.supply(asset="USDC.e", amount=Decimal("1000"))

    # Parse receipts
    parser = JoeLendReceiptParser(underlying_decimals=6)
    events = parser.parse_receipt(receipt)
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
