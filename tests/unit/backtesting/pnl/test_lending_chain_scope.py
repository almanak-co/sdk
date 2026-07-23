"""Lending chain-scope gate: DECLARED-but-mismatched venue/chain fails closed.

Observed defect: a SUPPLY on fluid/ethereum FILLED in the backtest and
accrued the global 3% surrogate APY, while the fluid connector's lending
``StrategyMatrixEntry`` declares {arbitrum, base} only and the live compiler
fails closed on the same combination. The generic lending lane now consults
the connector-declared lending chain scope at fill time:

- protocol DECLARES a lending matrix, run chain NOT in it -> typed rejection
  (``UNDECLARED_LENDING_CHAIN``), zero state mutation, zero costs;
- protocol DECLARES a lending matrix, run chain IN it -> fills as before;
- protocol declares NO lending matrix at all (duck-typed/generic test
  protocols) -> generic behavior preserved.

Companion doctrine: ``InterestCalculator.get_*_apy_for_protocol`` no longer
hands out the global surrogate APY silently — it warns once per protocol.
"""

from tests.backtesting_funding import pnl_token_funding as _pnl_token_funding

import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktestConfig,
    PnLBacktester,
    _declared_lending_chains,
    _lending_chain_scope_rejection,
)
from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio
from almanak.framework.backtesting.pnl.sizing import RejectionCode
from almanak.framework.intents.lending_intents import BorrowIntent, SupplyIntent
from tests.unit.backtesting.pnl._mocks import MockDataProvider

TS = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
INITIAL_CASH = Decimal("10000")
SUPPLY_AMOUNT = Decimal("5000")


def _market(chain: str) -> MarketState:
    return MarketState(
        timestamp=TS,
        prices={"USDC": Decimal("1"), "WETH": Decimal("2000")},
        chain=chain,
    )


def _backtester() -> PnLBacktester:
    return PnLBacktester(
        data_provider=MockDataProvider(),
        fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
        slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
    )


def _config() -> PnLBacktestConfig:
    return PnLBacktestConfig(
        start_time=TS,
        end_time=TS + timedelta(hours=1),
        token_funding=_pnl_token_funding(INITIAL_CASH),
        include_gas_costs=False,
    )


# =============================================================================
# Scope resolution helpers
# =============================================================================


class TestDeclaredLendingChains:
    def test_fluid_declares_arbitrum_and_base_only(self) -> None:
        declared = _declared_lending_chains("fluid")
        assert declared == frozenset({"arbitrum", "base"})

    def test_lending_alias_folds_to_connector(self) -> None:
        # The platform spec emits protocol="fluid_lending"; the gate must see
        # the same declaration through the lending-scoped alias.
        assert _declared_lending_chains("fluid_lending") == _declared_lending_chains("fluid")

    def test_no_matrix_protocol_returns_none(self) -> None:
        assert _declared_lending_chains("test_protocol") is None

    def test_rejection_names_protocol_and_chain(self) -> None:
        reason = _lending_chain_scope_rejection("fluid", "ethereum")
        assert reason is not None
        assert "protocol 'fluid' declares no lending support on chain 'ethereum'" in reason

    def test_declared_chain_passes(self) -> None:
        assert _lending_chain_scope_rejection("fluid", "arbitrum") is None

    def test_intents_times_chains_declaration_gates_the_majors(self) -> None:
        """Connectors without lending matrix rows (aave_v3, spark,
        compound_v3) declare via strategy_intents x strategy_chains — the
        gate must derive scope from that product, or exactly the most-used
        lending protocols stay ungated."""
        spark = _declared_lending_chains("spark")
        assert spark is not None and "ethereum" in spark
        reason = _lending_chain_scope_rejection("spark", "arbitrum")
        assert reason is not None
        assert "protocol 'spark' declares no lending support on chain 'arbitrum'" in reason
        assert _lending_chain_scope_rejection("spark", "ethereum") is None
        aave = _declared_lending_chains("aave_v3")
        assert aave is not None and {"ethereum", "arbitrum", "base"} <= aave

    def test_typoed_declared_chain_fails_loudly(self) -> None:
        """Connector-declared chain names are our metadata: a typo must
        raise (strict resolve), never silently reject valid intents."""
        from unittest import mock

        from almanak.framework.backtesting.pnl import engine as engine_mod

        engine_mod._lending_chain_scope_rejection.cache_clear()
        try:
            with mock.patch.object(
                engine_mod, "_declared_lending_chains", return_value=frozenset({"not_a_chain"})
            ):
                with pytest.raises(Exception):
                    engine_mod._lending_chain_scope_rejection("typo_proto", "ethereum")
        finally:
            engine_mod._lending_chain_scope_rejection.cache_clear()

    def test_registry_clear_invalidates_scope_caches(self) -> None:
        """The on_clear hook: a registry reset must clear the memoized scope
        decisions so they cannot go stale."""
        from almanak.connectors._connector import CONNECTOR_REGISTRY
        from almanak.framework.backtesting.pnl import engine as engine_mod

        assert _declared_lending_chains("fluid") is not None  # populate cache
        info_before = engine_mod._declared_lending_chains.cache_info()
        assert info_before.currsize > 0
        CONNECTOR_REGISTRY.clear()
        info_after = engine_mod._declared_lending_chains.cache_info()
        assert info_after.currsize == 0

    def test_chain_alias_resolves_before_comparison(self) -> None:
        # aave_v3 declares "ethereum"; the registry alias "mainnet" must not
        # spuriously reject.
        assert _lending_chain_scope_rejection("aave_v3", "mainnet") is None


# =============================================================================
# Engine lane: the observed defect shape and its control cases
# =============================================================================


class TestLendingChainScopeGate:
    @pytest.mark.asyncio
    async def test_supply_on_undeclared_chain_is_rejected(self) -> None:
        """Defect shape: fluid/ethereum SUPPLY -> typed rejection, no position, no accrual."""
        backtester = _backtester()
        config = _config()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        state = _market("ethereum")

        await backtester._execute_intent(
            SupplyIntent(protocol="fluid", token="USDC", amount=SUPPLY_AMOUNT),
            portfolio,
            state,
            TS,
            config,
        )

        assert portfolio.positions == []
        assert portfolio.cash_usd == INITIAL_CASH
        assert portfolio.get_total_value_usd(state) == INITIAL_CASH
        trade = portfolio.trades[-1]
        assert trade.success is False
        assert "protocol 'fluid' declares no lending support on chain 'ethereum'" in trade.metadata["failure_reason"]
        assert trade.metadata["rejection_code"] == RejectionCode.UNDECLARED_LENDING_CHAIN.value
        # Rejected fills charge nothing.
        assert trade.fee_usd == Decimal("0")
        assert trade.gas_cost_usd == Decimal("0")

    @pytest.mark.asyncio
    async def test_supply_alias_spelling_is_rejected_too(self) -> None:
        """protocol="fluid_lending" (platform-spec spelling) gates identically."""
        backtester = _backtester()
        config = _config()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        state = _market("ethereum")

        await backtester._execute_intent(
            SupplyIntent(protocol="fluid_lending", token="USDC", amount=SUPPLY_AMOUNT),
            portfolio,
            state,
            TS,
            config,
        )

        assert portfolio.positions == []
        trade = portfolio.trades[-1]
        assert trade.success is False
        assert trade.metadata["rejection_code"] == RejectionCode.UNDECLARED_LENDING_CHAIN.value

    @pytest.mark.asyncio
    async def test_borrow_on_undeclared_chain_is_rejected(self) -> None:
        backtester = _backtester()
        config = _config()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        state = _market("ethereum")

        await backtester._execute_intent(
            BorrowIntent(
                protocol="fluid",
                collateral_token="USDC",
                collateral_amount=Decimal("0"),
                borrow_token="USDC",
                borrow_amount=Decimal("1000"),
            ),
            portfolio,
            state,
            TS,
            config,
        )

        assert portfolio.positions == []
        assert portfolio.cash_usd == INITIAL_CASH
        trade = portfolio.trades[-1]
        assert trade.success is False
        assert trade.metadata["rejection_code"] == RejectionCode.UNDECLARED_LENDING_CHAIN.value

    @pytest.mark.asyncio
    async def test_supply_on_declared_chain_fills(self) -> None:
        """fluid/arbitrum (DECLARED chain) keeps working exactly as today."""
        backtester = _backtester()
        config = _config()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        state = _market("arbitrum")

        await backtester._execute_intent(
            SupplyIntent(protocol="fluid", token="USDC", amount=SUPPLY_AMOUNT),
            portfolio,
            state,
            TS,
            config,
        )

        assert len(portfolio.positions) == 1
        assert portfolio.positions[0].protocol == "fluid"
        assert portfolio.cash_usd == INITIAL_CASH - SUPPLY_AMOUNT
        assert portfolio.trades[-1].success is True

    @pytest.mark.asyncio
    async def test_no_matrix_protocol_keeps_generic_behavior(self) -> None:
        """A duck-typed protocol with no lending matrix declaration still fills."""
        backtester = _backtester()
        config = _config()
        portfolio = SimulatedPortfolio(initial_capital_usd=INITIAL_CASH)
        state = _market("ethereum")

        await backtester._execute_intent(
            SupplyIntent(protocol="test_protocol", token="USDC", amount=SUPPLY_AMOUNT),
            portfolio,
            state,
            TS,
            config,
        )

        assert len(portfolio.positions) == 1
        assert portfolio.trades[-1].success is True


# =============================================================================
# Loud global-surrogate APY fallback
# =============================================================================


class TestGlobalApyFallbackWarns:
    def test_undeclared_protocol_warns_once_and_returns_surrogate(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        from almanak.framework.backtesting.pnl.calculators.interest import (
            _GLOBAL_APY_FALLBACK_WARNED,
            InterestCalculator,
        )

        _GLOBAL_APY_FALLBACK_WARNED.discard(("supply", "totally_undeclared_venue"))
        calculator = InterestCalculator()
        with caplog.at_level(logging.WARNING):
            apy = calculator.get_supply_apy_for_protocol("totally_undeclared_venue")
            again = calculator.get_supply_apy_for_protocol("totally_undeclared_venue")

        assert apy == calculator.default_supply_apy
        assert again == apy
        fallback_warnings = [
            record for record in caplog.records if "totally_undeclared_venue" in record.getMessage()
        ]
        assert len(fallback_warnings) == 1
        assert "fabricated" in fallback_warnings[0].getMessage()

    def test_declared_protocol_does_not_warn(self, caplog: pytest.LogCaptureFixture) -> None:
        from almanak.framework.backtesting.pnl.calculators.interest import InterestCalculator

        calculator = InterestCalculator()
        with caplog.at_level(logging.WARNING):
            apy = calculator.get_supply_apy_for_protocol("aave_v3")

        assert apy == calculator.protocol_supply_apys["aave_v3"]
        assert not [record for record in caplog.records if "aave_v3" in record.getMessage()]
