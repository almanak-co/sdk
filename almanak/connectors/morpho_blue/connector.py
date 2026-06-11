"""Morpho Blue connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    BacktestStrategyTypeDecl,
    Connector,
    FeeModelDecl,
    ImportRef,
    LendingReadDecl,
    StrategyMatrixEntry,
)
from almanak.connectors._strategy_base.address_table import AddressTableSpec
from almanak.connectors._strategy_base.protocol_ownership import CapabilitiesSpec

CONNECTOR = Connector(
    name="morpho_blue",
    kind=ProtocolKind.LENDING,
    fee_model=FeeModelDecl(
        model=ImportRef(module="almanak.connectors.morpho_blue.fee_model", attribute="MorphoFeeModel"),
        name="morpho",
        description="Morpho lending protocol fee model (fee-free operations)",
        aliases=("morpho_blue", "morpho_optimizer"),
    ),
    backtest_strategy_type=BacktestStrategyTypeDecl(strategy_type="lending", aliases=("morpho",)),
    aliases=("morpho",),
    address_tables=(
        AddressTableSpec(
            protocol="morpho_blue",
            module="almanak.connectors.morpho_blue.addresses",
            attribute="MORPHO_BLUE",
        ),
    ),
    gateway_connector=ImportRef(
        module="almanak.connectors.morpho_blue.gateway.provider",
        attribute="MorphoBlueGatewayConnector",
        order=27,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.morpho_blue.receipt_parser_provider",
        attribute="MorphoBlueReceiptParserConnector",
    ),
    contract_monitoring=ImportRef(
        module="almanak.connectors.morpho_blue.contract_monitoring",
        attribute="MORPHO_BLUE_CONTRACT_MONITORING_SPECS",
    ),
    flash_loan_provider_name="morpho",
    flash_loan_provider=ImportRef(
        module="almanak.connectors.morpho_blue.flash_loan_provider",
        attribute="MorphoFlashLoanProvider",
        order=3,
    ),
    flash_loan_builder=ImportRef(
        module="almanak.connectors.morpho_blue.flash_loan",
        attribute="build_morpho_flash_loan",
    ),
    compiler=ImportRef(
        module="almanak.connectors.morpho_blue.compiler",
        attribute="MorphoBlueCompiler",
    ),
    capabilities=CapabilitiesSpec(
        keys=("morpho", "morpho_blue"),
        module="almanak.connectors.morpho_blue.capabilities",
    ),
    primitive=ImportRef(
        module="almanak.connectors.morpho_blue.primitive",
        attribute="PRIMITIVE",
    ),
    # Market-scoped account state (VIB-4929 PR-3a): non-USD-native; market params inject lltv.
    lending_read=LendingReadDecl(
        rate_history_chains=("ethereum", "base"),
        account_state=ImportRef(
            module="almanak.connectors.morpho_blue.lending_read", attribute="ACCOUNT_STATE_READ_SPEC"
        ),
        market_table=ImportRef(module="almanak.connectors.morpho_blue.addresses", attribute="MORPHO_MARKETS"),
        aliases=("morpho", "morphoblue"),
    ),
    strategy_intents=("SUPPLY", "BORROW", "REPAY", "WITHDRAW", "FLASH_LOAN"),
    strategy_chains=("ethereum", "base", "arbitrum", "polygon", "monad"),
    # Matrix output stays lending-only even though flash-loan intent is registered.
    strategy_matrix_entries=(
        StrategyMatrixEntry(
            matrix_name="morpho_blue",
            category="lending",
            chains=frozenset(("ethereum", "base", "arbitrum", "polygon", "monad")),
        ),
    ),
)

__all__ = ["CONNECTOR"]
