"""Aave V3 Connector.

This module provides an adapter for interacting with Aave V3 lending protocol,
supporting supply, borrow, repay, withdraw, flash loans, E-Mode, isolation mode,
and comprehensive event parsing.

Aave V3 is a decentralized lending protocol supporting:
- Supply assets to earn yield
- Borrow against collateral
- Flash loans for atomic arbitrage
- Efficiency Mode (E-Mode) for correlated assets
- Isolation Mode for new assets with limited debt ceiling
- Variable and stable interest rates

Supported chains:
- Ethereum
- Arbitrum
- Optimism
- Polygon
- Base
- Avalanche

Example:
    from almanak.framework.connectors.aave_v3 import AaveV3Adapter, AaveV3Config

    config = AaveV3Config(
        chain="arbitrum",
        wallet_address="0x...",
    )
    adapter = AaveV3Adapter(config)

    # Supply collateral
    result = adapter.supply(
        asset="USDC",
        amount=Decimal("1000"),
    )

    # Borrow against collateral
    result = adapter.borrow(
        asset="WETH",
        amount=Decimal("0.5"),
    )

    # Execute flash loan
    result = adapter.flash_loan_simple(
        receiver_address="0x...",
        asset="USDC",
        amount=Decimal("100000"),
    )

    # Calculate health factor
    hf_calc = adapter.calculate_health_factor(
        positions=positions,
        reserve_data=reserve_data,
    )
"""

from .adapter import (
    AAVE_STABLE_RATE_MODE,
    AAVE_V3_ORACLE_ADDRESSES,
    # Constants
    AAVE_V3_POOL_ADDRESSES,
    AAVE_V3_POOL_DATA_PROVIDER_ADDRESSES,
    AAVE_VARIABLE_RATE_MODE,
    DEFAULT_GAS_ESTIMATES,
    EMODE_CATEGORIES,
    # Adapter
    AaveV3Adapter,
    AaveV3Config,
    AaveV3EModeCategory,
    AaveV3FlashLoanParams,
    AaveV3HealthFactorCalculation,
    # Enums
    AaveV3InterestRateMode,
    AaveV3Position,
    # Data classes
    AaveV3ReserveData,
    AaveV3UserAccountData,
    TransactionResult,
)
from .receipt_parser import (
    EVENT_NAME_TO_TYPE,
    # Constants
    EVENT_TOPICS,
    TOPIC_TO_EVENT,
    # Event class
    AaveV3Event,
    AaveV3EventType,
    # Parser
    AaveV3ReceiptParser,
    BorrowEventData,
    FlashLoanEventData,
    IsolationModeDebtUpdatedEventData,
    LiquidationCallEventData,
    ParseResult,
    RepayEventData,
    ReserveDataUpdatedEventData,
    # Event data classes
    SupplyEventData,
    UserEModeSetEventData,
    WithdrawEventData,
)

__all__ = [
    # Adapter
    "AaveV3Adapter",
    "AaveV3Config",
    # Data classes
    "AaveV3ReserveData",
    "AaveV3UserAccountData",
    "AaveV3Position",
    "AaveV3FlashLoanParams",
    "AaveV3HealthFactorCalculation",
    "TransactionResult",
    # Enums
    "AaveV3InterestRateMode",
    "AaveV3EModeCategory",
    # Receipt Parser
    "AaveV3ReceiptParser",
    "AaveV3Event",
    "AaveV3EventType",
    # Event data classes
    "SupplyEventData",
    "WithdrawEventData",
    "BorrowEventData",
    "RepayEventData",
    "FlashLoanEventData",
    "LiquidationCallEventData",
    "ReserveDataUpdatedEventData",
    "UserEModeSetEventData",
    "IsolationModeDebtUpdatedEventData",
    "ParseResult",
    # Constants
    "AAVE_V3_POOL_ADDRESSES",
    "AAVE_V3_POOL_DATA_PROVIDER_ADDRESSES",
    "AAVE_V3_ORACLE_ADDRESSES",
    "EMODE_CATEGORIES",
    "DEFAULT_GAS_ESTIMATES",
    "AAVE_STABLE_RATE_MODE",
    "AAVE_VARIABLE_RATE_MODE",
    "EVENT_TOPICS",
    "TOPIC_TO_EVENT",
    "EVENT_NAME_TO_TYPE",
]
