"""Spark connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    BacktestStrategyTypeDecl,
    Connector,
    ImportRef,
    LendingReadDecl,
    MetadataAmountEncoding,
)
from almanak.connectors._strategy_base.address_table import AddressTableSpec
from almanak.connectors._strategy_base.protocol_ownership import CapabilitiesSpec, SupportedChainsSpec
from almanak.connectors.spark.backtest_risk import BACKTEST_RISK as _BACKTEST_RISK

CONNECTOR = Connector(
    name="spark",
    kind=ProtocolKind.LENDING,
    backtest_strategy_type=BacktestStrategyTypeDecl(strategy_type="lending"),
    address_tables=(
        AddressTableSpec(
            protocol="spark",
            module="almanak.connectors.spark.addresses",
            attribute="SPARK",
        ),
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.spark.receipt_parser_provider",
        attribute="SparkReceiptParserConnector",
    ),
    contract_roles=ImportRef(
        module="almanak.connectors.spark.contract_roles",
        attribute="CONTRACT_ROLES",
        order=10,
    ),
    compiler=ImportRef(
        module="almanak.connectors.spark.compiler",
        attribute="SparkCompiler",
    ),
    capabilities=CapabilitiesSpec(
        keys=("spark",),
        module="almanak.connectors.spark.capabilities",
    ),
    supported_chains=SupportedChainsSpec(
        keys=("spark",),
        module="almanak.connectors.spark.supported_chains",
    ),
    # Aave-fork reads: own opt-in attributes backed by the shared Aave-fork specs.
    # backtest_default_supply_apy / borrow_apy moved from interest.py (plan 022);
    # values verbatim from the pre-rewire hardcoded dict (0.05 / 0.055).
    lending_read=LendingReadDecl(
        backtest_default_supply_apy="0.05",
        backtest_default_borrow_apy="0.055",
        backtest_provider=ImportRef(
            module="almanak.connectors.spark.backtest_apy",
            attribute="SparkAPYProvider",
        ),
        spec=ImportRef(module="almanak.connectors.spark.lending_read", attribute="LENDING_READ_SPEC"),
        account_state=ImportRef(module="almanak.connectors.spark.lending_read", attribute="ACCOUNT_STATE_READ_SPEC"),
    ),
    # Aave-fork compiler: lending metadata amounts are wei-encoded (VIB-3747).
    metadata_amount_encoding=MetadataAmountEncoding(lending="wei"),
    backtest_risk=_BACKTEST_RISK,
    strategy_intents=("SUPPLY", "BORROW", "REPAY", "WITHDRAW"),
    strategy_chains=("ethereum",),
)

__all__ = ["CONNECTOR"]
