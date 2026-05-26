"""Aave V3 Connector (pooled lending).

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
- BNB
- Mantle
- X Layer

Example:
    from almanak.connectors.aave_v3 import AaveV3Adapter, AaveV3Config

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

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import (
        AAVE_STABLE_RATE_MODE,
        AAVE_V3_ORACLE_ADDRESSES,
        AAVE_V3_POOL_ADDRESSES,
        AAVE_V3_POOL_DATA_PROVIDER_ADDRESSES,
        AAVE_VARIABLE_RATE_MODE,
        DEFAULT_GAS_ESTIMATES,
        EMODE_CATEGORIES,
        AaveV3Adapter,
        AaveV3Config,
        AaveV3EModeCategory,
        AaveV3FlashLoanParams,
        AaveV3HealthFactorCalculation,
        AaveV3InterestRateMode,
        AaveV3Position,
        AaveV3ReserveData,
        AaveV3UserAccountData,
        TransactionResult,
    )
    from .receipt_parser import (
        EVENT_NAME_TO_TYPE,
        EVENT_TOPICS,
        TOPIC_TO_EVENT,
        AaveV3Event,
        AaveV3EventType,
        AaveV3ReceiptParser,
        BorrowEventData,
        FlashLoanEventData,
        IsolationModeDebtUpdatedEventData,
        LiquidationCallEventData,
        ParseResult,
        RepayEventData,
        ReserveDataUpdatedEventData,
        SupplyEventData,
        UserEModeSetEventData,
        WithdrawEventData,
    )

__all__ = [
    "AAVE_STABLE_RATE_MODE",
    "AAVE_V3_ORACLE_ADDRESSES",
    "AAVE_V3_POOL_ADDRESSES",
    "AAVE_V3_POOL_DATA_PROVIDER_ADDRESSES",
    "AAVE_VARIABLE_RATE_MODE",
    "AaveV3Adapter",
    "AaveV3Config",
    "AaveV3EModeCategory",
    "AaveV3Event",
    "AaveV3EventType",
    "AaveV3FlashLoanParams",
    "AaveV3HealthFactorCalculation",
    "AaveV3InterestRateMode",
    "AaveV3Position",
    "AaveV3ReceiptParser",
    "AaveV3ReserveData",
    "AaveV3UserAccountData",
    "BorrowEventData",
    "DEFAULT_GAS_ESTIMATES",
    "EMODE_CATEGORIES",
    "EVENT_NAME_TO_TYPE",
    "EVENT_TOPICS",
    "FlashLoanEventData",
    "IsolationModeDebtUpdatedEventData",
    "LiquidationCallEventData",
    "ParseResult",
    "RepayEventData",
    "ReserveDataUpdatedEventData",
    "SupplyEventData",
    "TOPIC_TO_EVENT",
    "TransactionResult",
    "UserEModeSetEventData",
    "WithdrawEventData",
]

_LAZY: dict[str, tuple[str, str]] = {
    "AAVE_STABLE_RATE_MODE": (".adapter", "AAVE_STABLE_RATE_MODE"),
    "AAVE_V3_ORACLE_ADDRESSES": (".adapter", "AAVE_V3_ORACLE_ADDRESSES"),
    "AAVE_V3_POOL_ADDRESSES": (".adapter", "AAVE_V3_POOL_ADDRESSES"),
    "AAVE_V3_POOL_DATA_PROVIDER_ADDRESSES": (".adapter", "AAVE_V3_POOL_DATA_PROVIDER_ADDRESSES"),
    "AAVE_VARIABLE_RATE_MODE": (".adapter", "AAVE_VARIABLE_RATE_MODE"),
    "AaveV3Adapter": (".adapter", "AaveV3Adapter"),
    "AaveV3Config": (".adapter", "AaveV3Config"),
    "AaveV3EModeCategory": (".adapter", "AaveV3EModeCategory"),
    "AaveV3Event": (".receipt_parser", "AaveV3Event"),
    "AaveV3EventType": (".receipt_parser", "AaveV3EventType"),
    "AaveV3FlashLoanParams": (".adapter", "AaveV3FlashLoanParams"),
    "AaveV3HealthFactorCalculation": (".adapter", "AaveV3HealthFactorCalculation"),
    "AaveV3InterestRateMode": (".adapter", "AaveV3InterestRateMode"),
    "AaveV3Position": (".adapter", "AaveV3Position"),
    "AaveV3ReceiptParser": (".receipt_parser", "AaveV3ReceiptParser"),
    "AaveV3ReserveData": (".adapter", "AaveV3ReserveData"),
    "AaveV3UserAccountData": (".adapter", "AaveV3UserAccountData"),
    "BorrowEventData": (".receipt_parser", "BorrowEventData"),
    "DEFAULT_GAS_ESTIMATES": (".adapter", "DEFAULT_GAS_ESTIMATES"),
    "EMODE_CATEGORIES": (".adapter", "EMODE_CATEGORIES"),
    "EVENT_NAME_TO_TYPE": (".receipt_parser", "EVENT_NAME_TO_TYPE"),
    "EVENT_TOPICS": (".receipt_parser", "EVENT_TOPICS"),
    "FlashLoanEventData": (".receipt_parser", "FlashLoanEventData"),
    "IsolationModeDebtUpdatedEventData": (".receipt_parser", "IsolationModeDebtUpdatedEventData"),
    "LiquidationCallEventData": (".receipt_parser", "LiquidationCallEventData"),
    "ParseResult": (".receipt_parser", "ParseResult"),
    "RepayEventData": (".receipt_parser", "RepayEventData"),
    "ReserveDataUpdatedEventData": (".receipt_parser", "ReserveDataUpdatedEventData"),
    "SupplyEventData": (".receipt_parser", "SupplyEventData"),
    "TOPIC_TO_EVENT": (".receipt_parser", "TOPIC_TO_EVENT"),
    "TransactionResult": (".adapter", "TransactionResult"),
    "UserEModeSetEventData": (".receipt_parser", "UserEModeSetEventData"),
    "WithdrawEventData": (".receipt_parser", "WithdrawEventData"),
}

_registered = False


def _register_once() -> None:
    """Fire ``register_connector`` once on first strategy-side access.

    Deferred so importing the connector's gateway-side surface during
    gateway boot does not pull ``framework.intents.vocabulary`` into the
    partially-initialised config-init chain (VIB-4835).
    """
    global _registered
    if _registered:
        return
    from almanak.connectors._strategy_base.registry import register_connector
    from almanak.framework.intents.vocabulary import IntentType

    register_connector(
        name="aave_v3",
        intents=(IntentType.SUPPLY, IntentType.BORROW, IntentType.REPAY, IntentType.WITHDRAW, IntentType.FLASH_LOAN),
        chains=("ethereum", "arbitrum", "optimism", "polygon", "base", "avalanche", "bnb", "mantle", "xlayer"),
    )
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
