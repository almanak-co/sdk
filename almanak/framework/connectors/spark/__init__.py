"""Spark Connector.

This module provides an adapter for interacting with Spark lending protocol,
which is an Aave V3 fork with Spark-specific addresses and configurations.

Spark is a decentralized lending protocol supporting:
- Supply assets to earn yield
- Borrow against collateral
- Variable interest rates

Supported chains:
- Ethereum

Example:
    from almanak.framework.connectors.spark import SparkAdapter, SparkConfig

    config = SparkConfig(
        chain="ethereum",
        wallet_address="0x...",
    )
    adapter = SparkAdapter(config)

    # Supply collateral
    result = adapter.supply(
        asset="USDC",
        amount=Decimal("1000"),
    )

    # Borrow against collateral
    result = adapter.borrow(
        asset="DAI",
        amount=Decimal("500"),
    )

    # Parse transaction receipt
    from almanak.framework.connectors.spark import SparkReceiptParser

    parser = SparkReceiptParser()
    result = parser.parse_receipt(receipt)
"""

from .adapter import (
    # Constants
    DEFAULT_GAS_ESTIMATES,
    SPARK_BORROW_SELECTOR,
    SPARK_ORACLE_ADDRESSES,
    SPARK_POOL_ADDRESSES,
    SPARK_POOL_DATA_PROVIDER_ADDRESSES,
    SPARK_REPAY_SELECTOR,
    SPARK_STABLE_RATE_MODE,
    SPARK_SUPPLY_SELECTOR,
    SPARK_VARIABLE_RATE_MODE,
    SPARK_WITHDRAW_SELECTOR,
    # Adapter
    SparkAdapter,
    SparkConfig,
    # Data classes
    TransactionResult,
)
from .receipt_parser import (
    # Event data classes
    BorrowEventData,
    ParseResult,
    RepayEventData,
    # Event type enum
    SparkEventType,
    # Parser
    SparkReceiptParser,
    SupplyEventData,
    WithdrawEventData,
)

__all__ = [
    # Adapter
    "SparkAdapter",
    "SparkConfig",
    # Receipt Parser
    "SparkReceiptParser",
    "SparkEventType",
    # Data classes
    "TransactionResult",
    "ParseResult",
    "SupplyEventData",
    "WithdrawEventData",
    "BorrowEventData",
    "RepayEventData",
    # Constants
    "SPARK_POOL_ADDRESSES",
    "SPARK_POOL_DATA_PROVIDER_ADDRESSES",
    "SPARK_ORACLE_ADDRESSES",
    "SPARK_SUPPLY_SELECTOR",
    "SPARK_BORROW_SELECTOR",
    "SPARK_REPAY_SELECTOR",
    "SPARK_WITHDRAW_SELECTOR",
    "SPARK_STABLE_RATE_MODE",
    "SPARK_VARIABLE_RATE_MODE",
    "DEFAULT_GAS_ESTIMATES",
]
