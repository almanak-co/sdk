"""Characterization tests for PnLBacktester engine internals (Phase 6C.1).

These tests pin down the current behavior of two high-complexity methods in
``almanak/framework/backtesting/pnl/engine.py`` before the Phase 6C.2/6C.3
extraction:

- ``PnLBacktester._run_backtest`` (orchestrates preflight, initialization,
  simulation loop, error fallback, metrics, result assembly).
- ``PnLBacktester._calculate_token_flows`` (per-intent-type token inflow /
  outflow computation).

Intentionally test-only: no production code is modified. Mocks are placed at
the data-provider / strategy / preflight boundary so the tests exercise the
real control flow inside ``_run_backtest`` while staying fast and deterministic.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.backtesting.models import (
    BacktestEngine,
    IntentType,
    ParameterSourceTracker,
    PreflightCheckResult,
    PreflightReport,
)
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktester,
)
from almanak.framework.backtesting.pnl.error_handling import PreflightValidationError

# =============================================================================
# Mock data providers & strategies
# =============================================================================


class TickingDataProvider:
    """Minimal historical data provider that yields ``num_ticks`` market states.

    Intentionally avoids any network / oracle call. Returns stable WETH and USDC
    prices so ``SimulatedPortfolio.mark_to_market`` can run without errors.
    """

    provider_name = "mock_ticking"

    def __init__(self, num_ticks: int = 3, start_time: datetime | None = None) -> None:
        self.num_ticks = num_ticks
        self.start_time = start_time or datetime(2024, 1, 1, tzinfo=UTC)

    async def iterate(self, config: Any):
        for i in range(self.num_ticks):
            timestamp = self.start_time + timedelta(hours=i)
            eth_price = Decimal("3000") + Decimal(i)
            market_state = MarketState(
                timestamp=timestamp,
                prices={
                    "WETH": eth_price,
                    "ETH": eth_price,
                    "USDC": Decimal("1"),
                },
                chain="arbitrum",
                block_number=1000 + i,
            )
            yield timestamp, market_state


class EmptyDataProvider:
    """Data provider that yields nothing (empty backtest)."""

    provider_name = "mock_empty"

    async def iterate(self, config: Any):
        if False:  # pragma: no cover - never yields
            yield


class PartialCoverageDataProvider:
    """Data provider whose market states only cover a subset of config tokens.

    Yields ticks with prices for the ``supplied_tokens`` subset only. When
    combined with a config requesting additional tokens, this drives the
    data-quality tracker below 100% coverage, enabling tests of the
    institutional-mode data quality gate.
    """

    provider_name = "mock_partial_coverage"

    def __init__(
        self,
        num_ticks: int = 2,
        start_time: datetime | None = None,
        supplied_tokens: tuple[str, ...] = ("USDC",),
    ) -> None:
        self.num_ticks = num_ticks
        self.start_time = start_time or datetime(2024, 1, 1, tzinfo=UTC)
        self.supplied_tokens = supplied_tokens

    async def iterate(self, config: Any):
        for i in range(self.num_ticks):
            timestamp = self.start_time + timedelta(hours=i)
            prices: dict[str, Decimal] = {}
            if "USDC" in self.supplied_tokens:
                prices["USDC"] = Decimal("1")
            if "WETH" in self.supplied_tokens:
                prices["WETH"] = Decimal("3000")
            yield (
                timestamp,
                MarketState(
                    timestamp=timestamp,
                    prices=prices,
                    chain="arbitrum",
                    block_number=1000 + i,
                ),
            )


class RaisingDataProvider:
    """Data provider that yields one tick, then raises mid-iteration.

    Emitting one successful tick first ensures ``_run_backtest`` enters the
    simulation loop and records some partial progress before the exception
    propagates, which is the code path we want to pin.
    """

    provider_name = "mock_raising"

    def __init__(
        self,
        error: Exception | None = None,
        start_time: datetime | None = None,
    ) -> None:
        self.error = error or RuntimeError("boom: provider iterate failed")
        self.start_time = start_time or datetime(2024, 1, 1, tzinfo=UTC)

    async def iterate(self, config: Any):
        # Yield one realistic tick so the simulation loop executes at least once.
        first_market_state = MarketState(
            timestamp=self.start_time,
            prices={
                "WETH": Decimal("3000"),
                "ETH": Decimal("3000"),
                "USDC": Decimal("1"),
            },
            chain="arbitrum",
            block_number=1000,
        )
        yield self.start_time, first_market_state
        # Then raise to exercise the mid-loop failure path.
        raise self.error


@dataclass
class _FakeSwapIntent:
    """Small stand-in for a SwapIntent the engine can introspect."""

    intent_type: str = "SWAP"
    from_token: str = "USDC"
    to_token: str = "WETH"
    amount: Decimal = field(default_factory=lambda: Decimal("100"))
    protocol: str = "uniswap_v3"


class HoldStrategy:
    """Strategy that always holds (returns None)."""

    def __init__(self, deployment_id: str = "hold_strategy") -> None:
        self._deployment_id = deployment_id
        self.decide_calls = 0

    @property
    def deployment_id(self) -> str:
        return self._deployment_id

    def decide(self, market: Any) -> Any:
        self.decide_calls += 1
        return None


class RaisingStrategy:
    """Strategy whose decide() always raises a non-warmup exception.

    Uses RuntimeError (not ValueError) so the engine's indicator-warmup
    suppression path does NOT match - we want the error handler to classify
    this as a genuine decide() failure.
    """

    def __init__(self, deployment_id: str = "raising_strategy") -> None:
        self._deployment_id = deployment_id

    @property
    def deployment_id(self) -> str:
        return self._deployment_id

    def decide(self, market: Any) -> Any:
        raise RuntimeError("unexpected decide failure")


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def start_end_times() -> tuple[datetime, datetime]:
    start = datetime(2024, 1, 1, tzinfo=UTC)
    end = start + timedelta(hours=3)
    return start, end


@pytest.fixture
def make_config(start_end_times):
    start, end = start_end_times

    def _make(**overrides: Any) -> PnLBacktestConfig:
        kwargs: dict[str, Any] = {
            "start_time": start,
            "end_time": end,
            "initial_capital_usd": Decimal("10000"),
            "tokens": ["WETH", "USDC"],
            "preflight_validation": False,  # Most tests opt out; targeted tests opt in.
            "fail_on_preflight_error": True,
            "inclusion_delay_blocks": 0,
        }
        kwargs.update(overrides)
        return PnLBacktestConfig(**kwargs)

    return _make


@pytest.fixture
def make_backtester():
    def _make(data_provider: Any) -> PnLBacktester:
        return PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
        )

    return _make


# =============================================================================
# _run_backtest: happy path & lifecycle
# =============================================================================


class TestRunBacktestHappyPath:
    @pytest.mark.asyncio
    async def test_happy_path_returns_valid_backtest_result(self, make_backtester, make_config):
        """Simple hold strategy over deterministic data returns a valid BacktestResult."""
        backtester = make_backtester(TickingDataProvider(num_ticks=3))
        config = make_config()
        strategy = HoldStrategy()

        result = await backtester.backtest(strategy, config)

        assert result.error is None
        assert result.engine == BacktestEngine.PNL
        assert result.deployment_id == "hold_strategy"
        assert result.start_time == config.start_time
        assert result.end_time == config.end_time
        assert result.initial_capital_usd == Decimal("10000")
        assert result.chain == config.chain
        assert result.backtest_id is not None
        assert result.config_hash is not None
        # Strategy was consulted each tick.
        assert strategy.decide_calls == 3

    @pytest.mark.asyncio
    async def test_run_started_and_ended_timestamps_set(self, make_backtester, make_config):
        """``run_started_at`` / ``run_ended_at`` are populated and monotonic."""
        backtester = make_backtester(TickingDataProvider(num_ticks=2))
        config = make_config()

        before = datetime.now(UTC)
        result = await backtester.backtest(HoldStrategy(), config)
        after = datetime.now(UTC)

        assert result.run_started_at is not None
        assert result.run_ended_at is not None
        assert before <= result.run_started_at <= result.run_ended_at <= after
        assert result.run_duration_seconds >= 0.0
        expected = (result.run_ended_at - result.run_started_at).total_seconds()
        assert abs(result.run_duration_seconds - expected) < 1e-6

    @pytest.mark.asyncio
    async def test_phase_timings_recorded_for_each_phase(self, make_backtester, make_config):
        """Each ``bt_logger.phase(...)`` context manager produces a phase timing entry."""
        backtester = make_backtester(TickingDataProvider(num_ticks=2))
        # Enable preflight so we see all three phases.
        config = make_config(preflight_validation=True, fail_on_preflight_error=False)

        # Return a passing, empty preflight report so we don't assert on provider internals.
        passing_report = PreflightReport(passed=True, estimated_coverage=Decimal("1.0"))

        with patch.object(
            PnLBacktester,
            "run_preflight_validation",
            AsyncMock(return_value=passing_report),
        ):
            result = await backtester.backtest(HoldStrategy(), config)

        phase_names = {t["phase_name"] for t in result.phase_timings}
        assert "preflight_validation" in phase_names
        assert "initialization" in phase_names
        assert "simulation" in phase_names
        assert "metrics_calculation" in phase_names

    @pytest.mark.asyncio
    async def test_parameter_sources_tracker_populated(self, make_backtester, make_config):
        """``parameter_sources`` tracker is populated once initialization completes."""
        backtester = make_backtester(TickingDataProvider(num_ticks=1))
        config = make_config()

        result = await backtester.backtest(HoldStrategy(), config)

        assert isinstance(result.parameter_sources, ParameterSourceTracker)
        assert len(result.parameter_sources.records) > 0
        # Config-category records are always populated.
        assert len(result.parameter_sources.config_sources) > 0


# =============================================================================
# _run_backtest: preflight validation paths
# =============================================================================


class TestRunBacktestPreflight:
    @pytest.mark.asyncio
    async def test_preflight_disabled_skips_validation(self, make_backtester, make_config):
        """With ``preflight_validation=False`` the engine must not call validation."""
        backtester = make_backtester(TickingDataProvider(num_ticks=1))
        config = make_config(preflight_validation=False)

        with patch.object(
            PnLBacktester,
            "run_preflight_validation",
            AsyncMock(),
        ) as mock_preflight:
            result = await backtester.backtest(HoldStrategy(), config)

        mock_preflight.assert_not_called()
        assert result.preflight_report is None
        # Default preflight_passed is True when validation is disabled.
        assert result.preflight_passed is True

    @pytest.mark.asyncio
    async def test_preflight_enabled_and_passing_proceeds(self, make_backtester, make_config):
        """Passing preflight report propagates to the result and backtest proceeds."""
        backtester = make_backtester(TickingDataProvider(num_ticks=1))
        config = make_config(preflight_validation=True)

        passing_report = PreflightReport(
            passed=True,
            estimated_coverage=Decimal("0.95"),
            tokens_available=["WETH", "USDC"],
        )

        with patch.object(
            PnLBacktester,
            "run_preflight_validation",
            AsyncMock(return_value=passing_report),
        ) as mock_preflight:
            result = await backtester.backtest(HoldStrategy(), config)

        mock_preflight.assert_awaited_once()
        assert result.error is None
        assert result.preflight_passed is True
        assert result.preflight_report is passing_report

    @pytest.mark.asyncio
    async def test_preflight_enabled_failing_fail_fast_raises(self, make_backtester, make_config):
        """When preflight fails and ``fail_on_preflight_error=True`` the engine raises."""
        backtester = make_backtester(TickingDataProvider(num_ticks=1))
        config = make_config(preflight_validation=True, fail_on_preflight_error=True)

        failing_check = PreflightCheckResult(
            check_name="provider_capability",
            passed=False,
            message="simulated failure",
            severity="error",
        )
        failing_report = PreflightReport(
            passed=False,
            checks=[failing_check],
            recommendations=["fix provider"],
        )

        with patch.object(
            PnLBacktester,
            "run_preflight_validation",
            AsyncMock(return_value=failing_report),
        ):
            with pytest.raises(PreflightValidationError):
                await backtester.backtest(HoldStrategy(), config)

    @pytest.mark.asyncio
    async def test_preflight_enabled_failing_degraded_mode_continues(self, make_backtester, make_config):
        """When preflight fails but ``fail_on_preflight_error=False`` the backtest continues."""
        backtester = make_backtester(TickingDataProvider(num_ticks=1))
        config = make_config(preflight_validation=True, fail_on_preflight_error=False)

        failing_check = PreflightCheckResult(
            check_name="provider_capability",
            passed=False,
            message="simulated failure",
            severity="error",
        )
        failing_report = PreflightReport(passed=False, checks=[failing_check])

        with patch.object(
            PnLBacktester,
            "run_preflight_validation",
            AsyncMock(return_value=failing_report),
        ):
            result = await backtester.backtest(HoldStrategy(), config)

        # Backtest produces a (degraded) result and did NOT raise.
        assert result.error is None
        assert result.preflight_passed is False
        assert result.preflight_report is failing_report


# =============================================================================
# _run_backtest: error handling inside simulation
# =============================================================================


class TestRunBacktestErrorHandling:
    @pytest.mark.asyncio
    async def test_mid_loop_exception_returns_partial_result(self, make_backtester, make_config):
        """If ``data_provider.iterate`` raises after one tick, a partial result with ``error`` set is returned."""
        sentinel = RuntimeError("iterate failed")
        backtester = make_backtester(RaisingDataProvider(error=sentinel))
        config = make_config()
        strategy = HoldStrategy()

        result = await backtester.backtest(strategy, config)

        # The first tick was processed before the provider raised, so the
        # strategy must have been consulted exactly once. This pins the
        # "mid-loop after progress" distinction from a before-first-tick failure.
        assert strategy.decide_calls == 1
        assert result.error is not None
        assert "iterate failed" in result.error
        assert result.institutional_compliance is False
        assert any("Backtest failed" in v for v in result.compliance_violations)
        # Partial result still sets identity fields.
        assert result.deployment_id == "hold_strategy"
        assert result.start_time == config.start_time
        assert result.run_started_at is not None
        assert result.run_ended_at is not None

    @pytest.mark.asyncio
    async def test_strategy_decide_fatal_error_yields_partial_result(self, make_backtester, make_config):
        """A fatal (``should_stop``) error from strategy.decide() produces a partial BacktestResult with ``error`` set.

        The engine wraps the classified error in a RuntimeError, re-raises from the
        simulation phase, and the outer try/except builds a partial-result
        BacktestResult. Pins the current behaviour: no exception escapes backtest().
        """
        backtester = make_backtester(TickingDataProvider(num_ticks=2))
        config = make_config()

        result = await backtester.backtest(RaisingStrategy(), config)

        # Fatal decide() errors produce a partial result rather than bubbling up.
        assert result.error is not None
        assert "strategy.decide()" in result.error
        assert "unexpected decide failure" in result.error
        assert result.institutional_compliance is False
        assert any("Backtest failed" in v for v in result.compliance_violations)
        assert len(result.trades) == 0

    @pytest.mark.asyncio
    async def test_empty_data_range_produces_result_without_trades(self, make_backtester, make_config):
        """A data provider that yields nothing returns a well-formed zero-trade result."""
        backtester = make_backtester(EmptyDataProvider())
        config = make_config()

        result = await backtester.backtest(HoldStrategy(), config)

        assert result.error is None
        assert len(result.trades) == 0
        assert len(result.equity_curve) == 0
        # With no equity points, final value falls back to initial capital.
        assert result.final_capital_usd == config.initial_capital_usd


# =============================================================================
# _run_backtest: institutional mode / data quality gate
# =============================================================================


class TestRunBacktestDataQualityGate:
    @pytest.mark.asyncio
    async def test_institutional_mode_raises_on_low_coverage(self, make_backtester, make_config):
        """In institutional mode, coverage below ``min_data_coverage`` raises ValueError from the data-quality gate."""
        # Provider supplies only USDC. Config requires both USDC and WETH, so
        # each tick produces 1 successful + 1 failed lookup => coverage 0.5,
        # which is well below the institutional-mode default of 0.98.
        backtester = make_backtester(PartialCoverageDataProvider(num_ticks=2, supplied_tokens=("USDC",)))
        # Mock preflight so we exercise only the data-quality gate.
        config = make_config(
            institutional_mode=True,
            preflight_validation=True,
            fail_on_preflight_error=False,
        )

        passing_report = PreflightReport(passed=True, estimated_coverage=Decimal("1.0"))

        with (
            patch.object(
                PnLBacktester,
                "run_preflight_validation",
                AsyncMock(return_value=passing_report),
            ),
            pytest.raises(ValueError, match="Data quality gate failed"),
        ):
            await backtester.backtest(HoldStrategy(), config)

    @pytest.mark.asyncio
    async def test_non_institutional_mode_low_coverage_warns_only(self, make_backtester, make_config):
        """Outside institutional mode, low coverage is tracked as a violation but does not raise."""
        backtester = make_backtester(PartialCoverageDataProvider(num_ticks=2, supplied_tokens=("USDC",)))
        config = make_config(
            institutional_mode=False,
            preflight_validation=False,
        )

        result = await backtester.backtest(HoldStrategy(), config)

        # Backtest completes without raising.
        assert result.error is None
        # Data coverage violation is recorded but institutional_compliance is also
        # False because the violation ends up in compliance_violations.
        assert any("Data coverage below minimum threshold" in v for v in result.compliance_violations)
        # Pin the flag <-> violation coupling so a refactor cannot silently decouple them.
        assert result.institutional_compliance is False


# =============================================================================
# _calculate_token_flows: per-intent-type behaviour
# =============================================================================


def _backtester_for_flows() -> PnLBacktester:
    """Build a bare backtester instance sufficient for ``_calculate_token_flows`` tests."""
    return PnLBacktester(
        data_provider=EmptyDataProvider(),
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )


def _market_state() -> MarketState:
    return MarketState(
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        prices={
            "WETH": Decimal("3000"),
            "USDC": Decimal("1"),
            "USDT": Decimal("1"),
            "ARB": Decimal("2"),
        },
    )


class TestCalculateTokenFlows:
    def test_swap_flows_out_from_token_in_to_token(self):
        """SWAP: we send ``from_token`` and receive ``to_token`` minus fees+slippage."""
        engine = _backtester_for_flows()
        intent = _FakeSwapIntent(from_token="USDC", to_token="WETH")
        amount_usd = Decimal("3000")
        fee = Decimal("9")
        slip = Decimal("3")

        tokens_in, tokens_out = engine._calculate_token_flows(
            intent=intent,
            intent_type=IntentType.SWAP,
            amount_usd=amount_usd,
            executed_price=Decimal("3000"),
            fee_usd=fee,
            slippage_usd=slip,
            market_state=_market_state(),
        )

        # From token (USDC) leaves at price $1.
        assert tokens_out == {"USDC": amount_usd / Decimal("1")}
        # To token (WETH) arrives at USD-net / price.
        expected_weth = (amount_usd - fee - slip) / Decimal("3000")
        assert tokens_in["WETH"] == expected_weth
        assert set(tokens_in.keys()) == {"WETH"}

    def test_swap_flows_uppercase_token_names(self):
        """Token symbols are normalized to uppercase."""
        engine = _backtester_for_flows()
        intent = _FakeSwapIntent(from_token="usdc", to_token="weth")

        tokens_in, tokens_out = engine._calculate_token_flows(
            intent=intent,
            intent_type=IntentType.SWAP,
            amount_usd=Decimal("1000"),
            executed_price=Decimal("3000"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            market_state=_market_state(),
        )

        assert "USDC" in tokens_out and "usdc" not in tokens_out
        assert "WETH" in tokens_in and "weth" not in tokens_in

    def test_swap_missing_from_token_price_falls_back_to_usd_amount(self):
        """If the source token has no price, the outflow uses the raw USD amount."""
        engine = _backtester_for_flows()

        @dataclass
        class _Intent:
            intent_type: str = "SWAP"
            from_token: str = "XYZ"  # not in MarketState
            to_token: str = "WETH"

        tokens_in, tokens_out = engine._calculate_token_flows(
            intent=_Intent(),
            intent_type=IntentType.SWAP,
            amount_usd=Decimal("500"),
            executed_price=Decimal("1"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            market_state=_market_state(),
        )

        # Unknown source token uses USD amount as units.
        assert tokens_out == {"XYZ": Decimal("500")}
        # Known destination WETH still uses price.
        assert tokens_in == {"WETH": Decimal("500") / Decimal("3000")}

    def test_swap_missing_to_token_price_falls_back_to_usd_amount(self):
        """If the destination token has no price, the inflow uses the raw USD amount.

        Pins the symmetric ``KeyError`` branch on the inbound side of the swap
        (separate from the ``from_token`` branch exercised above) so a
        destination-side regression would fail this suite.
        """
        engine = _backtester_for_flows()

        @dataclass
        class _Intent:
            intent_type: str = "SWAP"
            from_token: str = "USDC"
            to_token: str = "XYZ"  # not in MarketState

        tokens_in, tokens_out = engine._calculate_token_flows(
            intent=_Intent(),
            intent_type=IntentType.SWAP,
            amount_usd=Decimal("500"),
            executed_price=Decimal("1"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            market_state=_market_state(),
        )

        # Known source USDC still uses price (USDC = $1 => 500 units).
        assert tokens_out == {"USDC": Decimal("500")}
        # Unknown destination token uses USD net amount as units.
        assert tokens_in == {"XYZ": Decimal("500")}

    def test_supply_flows_only_out(self):
        """SUPPLY: token leaves the wallet, nothing comes in."""
        engine = _backtester_for_flows()
        intent = MagicMock(spec=["token"])
        intent.token = "WETH"

        tokens_in, tokens_out = engine._calculate_token_flows(
            intent=intent,
            intent_type=IntentType.SUPPLY,
            amount_usd=Decimal("3000"),
            executed_price=Decimal("3000"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            market_state=_market_state(),
        )

        assert tokens_in == {}
        assert tokens_out == {"WETH": Decimal("1")}

    def test_withdraw_flows_only_in(self):
        """WITHDRAW: token arrives in the wallet, nothing leaves."""
        engine = _backtester_for_flows()
        intent = MagicMock(spec=["token"])
        intent.token = "WETH"

        tokens_in, tokens_out = engine._calculate_token_flows(
            intent=intent,
            intent_type=IntentType.WITHDRAW,
            amount_usd=Decimal("6000"),
            executed_price=Decimal("3000"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            market_state=_market_state(),
        )

        assert tokens_out == {}
        assert tokens_in == {"WETH": Decimal("2")}

    def test_borrow_flows_only_in(self):
        """BORROW: token arrives, nothing leaves."""
        engine = _backtester_for_flows()
        intent = MagicMock(spec=["token"])
        intent.token = "USDC"

        tokens_in, tokens_out = engine._calculate_token_flows(
            intent=intent,
            intent_type=IntentType.BORROW,
            amount_usd=Decimal("500"),
            executed_price=Decimal("1"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            market_state=_market_state(),
        )

        assert tokens_out == {}
        assert tokens_in == {"USDC": Decimal("500")}

    def test_repay_flows_only_out(self):
        """REPAY: token leaves, nothing arrives."""
        engine = _backtester_for_flows()
        intent = MagicMock(spec=["token"])
        intent.token = "USDC"

        tokens_in, tokens_out = engine._calculate_token_flows(
            intent=intent,
            intent_type=IntentType.REPAY,
            amount_usd=Decimal("500"),
            executed_price=Decimal("1"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            market_state=_market_state(),
        )

        assert tokens_in == {}
        assert tokens_out == {"USDC": Decimal("500")}

    def test_lp_open_splits_both_tokens_out_50_50_usd(self):
        """LP_OPEN: both tokens leave the wallet, USD split 50/50."""
        engine = _backtester_for_flows()
        intent = MagicMock(spec=["token0", "token1"])
        intent.token0 = "WETH"
        intent.token1 = "USDC"

        tokens_in, tokens_out = engine._calculate_token_flows(
            intent=intent,
            intent_type=IntentType.LP_OPEN,
            amount_usd=Decimal("6000"),
            executed_price=Decimal("3000"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            market_state=_market_state(),
        )

        half = Decimal("6000") / Decimal("2")
        assert tokens_in == {}
        assert tokens_out == {
            "WETH": half / Decimal("3000"),
            "USDC": half / Decimal("1"),
        }

    def test_lp_close_returns_both_tokens_in(self):
        """LP_CLOSE: both tokens return to the wallet, USD split 50/50."""
        engine = _backtester_for_flows()
        intent = MagicMock(spec=["token0", "token1"])
        intent.token0 = "WETH"
        intent.token1 = "USDC"

        tokens_in, tokens_out = engine._calculate_token_flows(
            intent=intent,
            intent_type=IntentType.LP_CLOSE,
            amount_usd=Decimal("6000"),
            executed_price=Decimal("3000"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            market_state=_market_state(),
        )

        half = Decimal("6000") / Decimal("2")
        assert tokens_out == {}
        assert tokens_in == {
            "WETH": half / Decimal("3000"),
            "USDC": half / Decimal("1"),
        }

    def test_vault_deposit_flows_out(self):
        """VAULT_DEPOSIT: deposit token leaves the wallet."""
        engine = _backtester_for_flows()

        @dataclass
        class _Intent:
            deposit_token: str = "USDC"

        tokens_in, tokens_out = engine._calculate_token_flows(
            intent=_Intent(),
            intent_type=IntentType.VAULT_DEPOSIT,
            amount_usd=Decimal("100"),
            executed_price=Decimal("1"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            market_state=_market_state(),
        )

        assert tokens_in == {}
        assert tokens_out == {"USDC": Decimal("100")}

    def test_vault_redeem_flows_in(self):
        """VAULT_REDEEM: deposit token returns to the wallet."""
        engine = _backtester_for_flows()

        @dataclass
        class _Intent:
            deposit_token: str = "USDC"

        tokens_in, tokens_out = engine._calculate_token_flows(
            intent=_Intent(),
            intent_type=IntentType.VAULT_REDEEM,
            amount_usd=Decimal("100"),
            executed_price=Decimal("1"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            market_state=_market_state(),
        )

        assert tokens_out == {}
        assert tokens_in == {"USDC": Decimal("100")}

    def test_fee_and_slippage_reduce_swap_inflow(self):
        """Swap inflow equals (amount_usd - fee - slippage) / to_price."""
        engine = _backtester_for_flows()
        intent = _FakeSwapIntent(from_token="USDC", to_token="WETH")

        tokens_in, _ = engine._calculate_token_flows(
            intent=intent,
            intent_type=IntentType.SWAP,
            amount_usd=Decimal("3000"),
            executed_price=Decimal("3000"),
            fee_usd=Decimal("30"),
            slippage_usd=Decimal("15"),
            market_state=_market_state(),
        )

        # (3000 - 30 - 15) / 3000 = 2955 / 3000
        assert tokens_in["WETH"] == (Decimal("3000") - Decimal("30") - Decimal("15")) / Decimal("3000")

    def test_hold_intent_produces_no_flows(self):
        """HOLD (or any unmatched intent type) leaves both flow dicts empty."""
        engine = _backtester_for_flows()
        intent = MagicMock(spec=[])

        tokens_in, tokens_out = engine._calculate_token_flows(
            intent=intent,
            intent_type=IntentType.HOLD,
            amount_usd=Decimal("0"),
            executed_price=Decimal("0"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            market_state=_market_state(),
        )

        assert tokens_in == {}
        assert tokens_out == {}
