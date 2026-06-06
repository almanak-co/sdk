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
    from almanak.connectors.spark import SparkAdapter, SparkConfig

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
    from almanak.connectors.spark import SparkReceiptParser

    parser = SparkReceiptParser()
    result = parser.parse_receipt(receipt)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import (
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
        SparkAdapter,
        SparkConfig,
        TransactionResult,
    )
    from .receipt_parser import (
        BorrowEventData,
        ParseResult,
        RepayEventData,
        SparkEventType,
        SparkReceiptParser,
        SupplyEventData,
        WithdrawEventData,
    )

__all__ = [
    "BorrowEventData",
    "DEFAULT_GAS_ESTIMATES",
    "ParseResult",
    "RepayEventData",
    "SPARK_BORROW_SELECTOR",
    "SPARK_ORACLE_ADDRESSES",
    "SPARK_POOL_ADDRESSES",
    "SPARK_POOL_DATA_PROVIDER_ADDRESSES",
    "SPARK_REPAY_SELECTOR",
    "SPARK_STABLE_RATE_MODE",
    "SPARK_SUPPLY_SELECTOR",
    "SPARK_VARIABLE_RATE_MODE",
    "SPARK_WITHDRAW_SELECTOR",
    "SparkAdapter",
    "SparkConfig",
    "SparkEventType",
    "SparkReceiptParser",
    "SupplyEventData",
    "TransactionResult",
    "WithdrawEventData",
]

_LAZY: dict[str, tuple[str, str]] = {
    "BorrowEventData": (".receipt_parser", "BorrowEventData"),
    "DEFAULT_GAS_ESTIMATES": (".adapter", "DEFAULT_GAS_ESTIMATES"),
    "ParseResult": (".receipt_parser", "ParseResult"),
    "RepayEventData": (".receipt_parser", "RepayEventData"),
    "SPARK_BORROW_SELECTOR": (".adapter", "SPARK_BORROW_SELECTOR"),
    "SPARK_ORACLE_ADDRESSES": (".adapter", "SPARK_ORACLE_ADDRESSES"),
    "SPARK_POOL_ADDRESSES": (".adapter", "SPARK_POOL_ADDRESSES"),
    "SPARK_POOL_DATA_PROVIDER_ADDRESSES": (".adapter", "SPARK_POOL_DATA_PROVIDER_ADDRESSES"),
    "SPARK_REPAY_SELECTOR": (".adapter", "SPARK_REPAY_SELECTOR"),
    "SPARK_STABLE_RATE_MODE": (".adapter", "SPARK_STABLE_RATE_MODE"),
    "SPARK_SUPPLY_SELECTOR": (".adapter", "SPARK_SUPPLY_SELECTOR"),
    "SPARK_VARIABLE_RATE_MODE": (".adapter", "SPARK_VARIABLE_RATE_MODE"),
    "SPARK_WITHDRAW_SELECTOR": (".adapter", "SPARK_WITHDRAW_SELECTOR"),
    "SparkAdapter": (".adapter", "SparkAdapter"),
    "SparkConfig": (".adapter", "SparkConfig"),
    "SparkEventType": (".receipt_parser", "SparkEventType"),
    "SparkReceiptParser": (".receipt_parser", "SparkReceiptParser"),
    "SupplyEventData": (".receipt_parser", "SupplyEventData"),
    "TransactionResult": (".adapter", "TransactionResult"),
    "WithdrawEventData": (".receipt_parser", "WithdrawEventData"),
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
