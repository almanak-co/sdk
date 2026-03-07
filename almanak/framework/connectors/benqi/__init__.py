"""BENQI Lending Connector (Compound V2 fork on Avalanche).

This module provides adapters and utilities for interacting with BENQI,
a leading lending/borrowing protocol on Avalanche using the qiToken architecture.

BENQI Features:
- Compound V2-style qiToken model
- Supply to earn yield (mint qiTokens)
- Borrow against collateral (enterMarkets + borrow)
- Multiple asset markets: AVAX, USDC, USDT, WETH.e, BTC.b, sAVAX

Supported Chains:
- Avalanche

Example:
    from almanak.framework.connectors.benqi import (
        BenqiAdapter,
        BenqiConfig,
        BenqiReceiptParser,
    )

    config = BenqiConfig(
        chain="avalanche",
        wallet_address="0x...",
    )
    adapter = BenqiAdapter(config)

    # Supply USDC
    result = adapter.supply(asset="USDC", amount=Decimal("1000"))

    # Parse receipts
    parser = BenqiReceiptParser(underlying_decimals=6)
    events = parser.parse_receipt(receipt)
"""

from .adapter import (
    BENQI_BORROW_SELECTOR,
    BENQI_COMPTROLLER_ADDRESS,
    BENQI_ENTER_MARKETS_SELECTOR,
    BENQI_MINT_NATIVE_SELECTOR,
    BENQI_MINT_SELECTOR,
    BENQI_QI_TOKENS,
    BENQI_REDEEM_SELECTOR,
    BENQI_REDEEM_UNDERLYING_SELECTOR,
    BENQI_REPAY_BORROW_NATIVE_SELECTOR,
    BENQI_REPAY_BORROW_SELECTOR,
    DEFAULT_GAS_ESTIMATES,
    ERC20_APPROVE_SELECTOR,
    MAX_UINT256,
    # Adapter
    BenqiAdapter,
    BenqiConfig,
    # Data classes
    BenqiMarketInfo,
    BenqiPosition,
    TransactionResult,
)
from .receipt_parser import (
    EVENT_NAME_TO_TYPE,
    # Constants
    EVENT_TOPICS,
    TOPIC_TO_EVENT,
    # Event class
    BenqiEvent,
    BenqiEventType,
    # Parser
    BenqiReceiptParser,
    ParseResult,
)

__all__ = [
    # Adapter
    "BenqiAdapter",
    "BenqiConfig",
    # Data classes
    "BenqiMarketInfo",
    "BenqiPosition",
    "TransactionResult",
    # Constants
    "BENQI_COMPTROLLER_ADDRESS",
    "BENQI_QI_TOKENS",
    "DEFAULT_GAS_ESTIMATES",
    # Function selectors
    "BENQI_MINT_SELECTOR",
    "BENQI_MINT_NATIVE_SELECTOR",
    "BENQI_REDEEM_SELECTOR",
    "BENQI_REDEEM_UNDERLYING_SELECTOR",
    "BENQI_BORROW_SELECTOR",
    "BENQI_REPAY_BORROW_SELECTOR",
    "BENQI_REPAY_BORROW_NATIVE_SELECTOR",
    "BENQI_ENTER_MARKETS_SELECTOR",
    "ERC20_APPROVE_SELECTOR",
    "MAX_UINT256",
    # Receipt Parser
    "BenqiReceiptParser",
    "BenqiEvent",
    "BenqiEventType",
    "ParseResult",
    # Event constants
    "EVENT_TOPICS",
    "TOPIC_TO_EVENT",
    "EVENT_NAME_TO_TYPE",
]
