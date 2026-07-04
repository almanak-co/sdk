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
    YieldPokeDecl,
)
from almanak.connectors._strategy_base.address_table import AddressTableSpec
from almanak.connectors._strategy_base.protocol_ownership import CapabilitiesSpec
from almanak.connectors.morpho_blue.backtest_risk import BACKTEST_RISK as _BACKTEST_RISK

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
    agent_read_connector=ImportRef(
        module="almanak.connectors.morpho_blue.agent_read_provider",
        attribute="MorphoBlueAgentReadConnector",
        order=6,
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
    # backtest_default_supply_apy / borrow_apy override the VIB-5040 deliberate omission;
    # values come from interest.py's existing hardcoded behavior (0.035 / 0.04) so net
    # simulation behavior is unchanged — this merely moves the literals to the manifest.
    lending_read=LendingReadDecl(
        rate_history_chains=("ethereum", "base"),
        backtest_default_supply_apy="0.035",
        backtest_default_borrow_apy="0.04",
        backtest_provider=ImportRef(
            module="almanak.connectors.morpho_blue.backtest_apy",
            attribute="MorphoBlueAPYProvider",
        ),
        account_state=ImportRef(
            module="almanak.connectors.morpho_blue.lending_read", attribute="ACCOUNT_STATE_READ_SPEC"
        ),
        market_table=ImportRef(module="almanak.connectors.morpho_blue.addresses", attribute="MORPHO_MARKETS"),
        aliases=("morpho", "morphoblue"),
        # Plan 027 Step 5: Morpho Blue supports the is_collateral flag on
        # withdraw intents (selects loan-token vs collateral-token side).
        # Gate declared here so executor/ax can branch without naming the
        # protocol as a literal string.
        accepts_is_collateral=True,
        # VIB-5418: Morpho markets are ISOLATED (one collateral + one loan token),
        # so a per-market on-chain read's debt IS the whole-position debt. Lets the
        # teardown lending guard KEEP a zero-debt collateral withdraw_all on a
        # measured per-reserve read even when the account-level USD aggregate is
        # unmeasured (empty snapshot prices for the cross-asset market).
        market_isolated=True,
    ),
    yield_poke=YieldPokeDecl(
        chains=("ethereum",),
        poke=ImportRef(module="almanak.connectors.morpho_blue.backtest_poke", attribute="poke_morpho_blue"),
    ),
    backtest_risk=_BACKTEST_RISK,
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
