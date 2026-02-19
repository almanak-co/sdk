"""Compound V3 (Comet) Connector.

This module provides adapters and utilities for interacting with Compound V3,
a lending protocol with single borrowable assets and multiple collateral options.

Compound V3 Features:
- Single borrowable asset (base) per market (USDC, WETH, USDT)
- Multiple collateral assets per market
- No cTokens for collateral (only base asset is tokenized)
- Simplified interest rate model
- Efficient liquidation mechanism

Supported Chains:
- Ethereum
- Arbitrum

Example:
    from almanak.framework.connectors.compound_v3 import (
        CompoundV3Adapter,
        CompoundV3Config,
        CompoundV3ReceiptParser,
    )

    # Initialize adapter
    config = CompoundV3Config(
        chain="ethereum",
        wallet_address="0x...",
        market="usdc",
    )
    adapter = CompoundV3Adapter(config)

    # Get market info
    market_info = adapter.get_market_info()
    print(f"Market: {market_info.name}")

    # Build a supply transaction
    result = adapter.supply(
        amount=Decimal("1000"),
    )

    # Parse transaction receipts
    parser = CompoundV3ReceiptParser()
    events = parser.parse_receipt(receipt)
"""

from .adapter import (
    COMPOUND_V3_ABSORB_SELECTOR,
    COMPOUND_V3_BUY_COLLATERAL_SELECTOR,
    # Constants
    COMPOUND_V3_COMET_ADDRESSES,
    COMPOUND_V3_MARKETS,
    COMPOUND_V3_SUPPLY_FROM_SELECTOR,
    # Function selectors
    COMPOUND_V3_SUPPLY_SELECTOR,
    COMPOUND_V3_SUPPLY_TO_SELECTOR,
    COMPOUND_V3_WITHDRAW_FROM_SELECTOR,
    COMPOUND_V3_WITHDRAW_SELECTOR,
    COMPOUND_V3_WITHDRAW_TO_SELECTOR,
    DEFAULT_GAS_ESTIMATES,
    ERC20_APPROVE_SELECTOR,
    MAX_UINT256,
    # Adapter
    CompoundV3Adapter,
    CompoundV3Config,
    CompoundV3HealthFactor,
    # Data classes
    CompoundV3MarketInfo,
    CompoundV3Position,
    TransactionResult,
)
from .receipt_parser import (
    EVENT_NAME_TO_TYPE,
    # Constants
    EVENT_TOPICS,
    TOPIC_TO_EVENT,
    # Event class
    CompoundV3Event,
    CompoundV3EventType,
    # Parser
    CompoundV3ReceiptParser,
    ParseResult,
)

__all__ = [
    # Adapter
    "CompoundV3Adapter",
    "CompoundV3Config",
    # Data classes
    "CompoundV3MarketInfo",
    "CompoundV3Position",
    "CompoundV3HealthFactor",
    "TransactionResult",
    # Constants
    "COMPOUND_V3_COMET_ADDRESSES",
    "COMPOUND_V3_MARKETS",
    "DEFAULT_GAS_ESTIMATES",
    # Function selectors
    "COMPOUND_V3_SUPPLY_SELECTOR",
    "COMPOUND_V3_SUPPLY_TO_SELECTOR",
    "COMPOUND_V3_SUPPLY_FROM_SELECTOR",
    "COMPOUND_V3_WITHDRAW_SELECTOR",
    "COMPOUND_V3_WITHDRAW_TO_SELECTOR",
    "COMPOUND_V3_WITHDRAW_FROM_SELECTOR",
    "COMPOUND_V3_ABSORB_SELECTOR",
    "COMPOUND_V3_BUY_COLLATERAL_SELECTOR",
    "ERC20_APPROVE_SELECTOR",
    "MAX_UINT256",
    # Receipt Parser
    "CompoundV3ReceiptParser",
    "CompoundV3Event",
    "CompoundV3EventType",
    "ParseResult",
    # Event constants
    "EVENT_TOPICS",
    "TOPIC_TO_EVENT",
    "EVENT_NAME_TO_TYPE",
]
