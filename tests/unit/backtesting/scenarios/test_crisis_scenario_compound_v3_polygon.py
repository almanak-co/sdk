"""Unit tests for crisis scenario backtesting with Compound V3 lending on Polygon.

First crisis scenario backtest on Polygon (any protocol) and first crisis
scenario for a lending strategy (all prior were swap/LP). Uses deterministic
mock data -- no external API calls needed.

VIB-2089: Backtesting: Crisis scenario backtest Compound V3 lending on Polygon
Kitchen Loop iteration 137.
"""

from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.data import PriceUnavailableError

from almanak.framework.backtesting.pnl.data_provider import (
    OHLCV,
    HistoricalDataConfig,
    MarketState,
)
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktester,
)
from almanak.framework.backtesting.scenarios.crisis import (
    BLACK_THURSDAY,
    FTX_COLLAPSE,
    TERRA_COLLAPSE,
    CrisisScenario,
)
from almanak.framework.backtesting.scenarios.crisis_runner import (
    CrisisBacktestConfig,
    CrisisBacktestResult,
    build_crisis_metrics,
    compare_crisis_to_normal,
    run_crisis_backtest,
    run_multiple_crisis_backtests,
)

# =============================================================================
# Deterministic data provider for Polygon crisis periods
# =============================================================================


class PolygonCrisisDataProvider:
    """Data provider generating crash-style price patterns for Polygon chain.

    Simulates a crash pattern: decline -> trough -> partial recovery.
    Reports chain as 'polygon' and supports MATIC/USDC.e pair.
    Polygon gas prices are denominated in MATIC (typically 30-100 gwei).
    """

    def __init__(
        self,
        crash_severity: Decimal = Decimal("0.4"),
        recovery_pct: Decimal = Decimal("0.5"),
        start_time: datetime | None = None,
        duration_hours: int = 168,
    ):
        self._crash_severity = crash_severity
        self._recovery_pct = recovery_pct
        self._start_time = start_time or datetime(2020, 3, 12, tzinfo=UTC)
        self._duration_hours = duration_hours
        self._interval_seconds = 3600

        self._matic_prices = self._generate_crash_prices(
            initial=Decimal("1.20"), severity=crash_severity, recovery=recovery_pct
        )

    def _generate_crash_prices(
        self,
        initial: Decimal,
        severity: Decimal,
        recovery: Decimal,
    ) -> list[Decimal]:
        """Generate a crash -> trough -> recovery price series."""
        n = self._duration_hours + 1
        crash_end = int(n * 0.3)
        trough_end = int(n * 0.5)

        trough_price = initial * (Decimal("1") - severity)
        recovery_price = trough_price + (initial - trough_price) * recovery

        prices = []
        for i in range(n):
            if i <= crash_end:
                progress = Decimal(i) / Decimal(max(crash_end, 1))
                price = initial - (initial - trough_price) * progress
            elif i <= trough_end:
                offset = Decimal("0.01") if i % 2 == 0 else Decimal("-0.01")
                price = trough_price + offset
            else:
                progress = Decimal(i - trough_end) / Decimal(max(n - trough_end - 1, 1))
                price = trough_price + (recovery_price - trough_price) * progress
            prices.append(max(price, Decimal("0.01")))

        return prices

    async def get_price(self, token: str, timestamp: datetime) -> Decimal:
        token = token.upper()
        if token in ("USDC", "USDC.E", "USDT", "DAI"):
            return Decimal("1")
        if token in ("ETH", "WETH"):
            return Decimal("2500")  # Static ETH price for gas cost calculations
        delta = timestamp - self._start_time
        index = int(delta.total_seconds() / self._interval_seconds)
        if token in ("MATIC", "WMATIC", "POL"):
            series = self._matic_prices
        else:
            raise ValueError(f"No price for {token}")
        if 0 <= index < len(series):
            return series[index]
        return series[-1] if index >= len(series) else series[0]

    async def get_ohlcv(
        self, token: str, start: datetime, end: datetime, interval_seconds: int = 3600
    ) -> list[OHLCV]:
        result = []
        current = start
        while current <= end:
            price = await self.get_price(token, current)
            result.append(
                OHLCV(
                    timestamp=current,
                    open=price,
                    high=price * Decimal("1.01"),
                    low=price * Decimal("0.99"),
                    close=price,
                    volume=Decimal("5000000"),
                )
            )
            current += timedelta(seconds=interval_seconds)
        return result

    async def iterate(self, config: HistoricalDataConfig) -> AsyncIterator[tuple[datetime, MarketState]]:
        current = config.start_time
        index = 0
        while current <= config.end_time:
            prices = {}
            for token in config.tokens:
                try:
                    prices[token.upper()] = await self.get_price(token, current)
                except ValueError:
                    prices[token.upper()] = Decimal("1")
            # Always include ETH for gas cost calculations
            if "ETH" not in prices and "WETH" not in prices:
                prices["ETH"] = Decimal("2500")
            market_state = MarketState(
                timestamp=current,
                prices=prices,
                chain="polygon",
                block_number=50000000 + index * 100,
                gas_price_gwei=Decimal("50"),  # Polygon gas ~50 gwei MATIC
            )
            yield current, market_state
            index += 1
            current += timedelta(seconds=config.interval_seconds)

    @property
    def provider_name(self) -> str:
        return "polygon_crisis_deterministic"

    @property
    def supported_tokens(self) -> list[str]:
        return ["MATIC", "USDC.E"]

    @property
    def supported_chains(self) -> list[str]:
        return ["polygon"]

    @property
    def min_timestamp(self) -> datetime | None:
        return self._start_time

    @property
    def max_timestamp(self) -> datetime | None:
        return self._start_time + timedelta(hours=self._duration_hours)

    async def verify_archive_access(self) -> bool:
        return True

    async def close(self) -> None:
        return None


# =============================================================================
# Mock lending intents for Compound V3
# =============================================================================


@dataclass
class MockCompoundV3SupplyIntent:
    """Mock Compound V3 supply intent."""

    intent_type: str = "SUPPLY"
    token: str = "USDC.e"
    amount: Decimal = Decimal("10000")
    protocol: str = "compound_v3"
    market_id: str = "usdc_e"


@dataclass
class MockCompoundV3WithdrawIntent:
    """Mock Compound V3 withdraw intent."""

    intent_type: str = "WITHDRAW"
    token: str = "USDC.e"
    amount: Decimal = Decimal("10000")
    protocol: str = "compound_v3"
    market_id: str = "usdc_e"
    withdraw_all: bool = True


class CompoundV3CrisisLendingStrategy:
    """Crisis-aware Compound V3 lending strategy for Polygon.

    Supplies USDC to Compound V3 and monitors MATIC price drawdown.
    Withdraws on significant drawdown, re-supplies after recovery.
    """

    def __init__(
        self,
        initial_matic_price: Decimal = Decimal("1.20"),
        withdraw_threshold: Decimal = Decimal("0.15"),
        resupply_threshold: Decimal = Decimal("0.08"),
        supply_amount: Decimal = Decimal("10000"),
        strategy_id: str = "compound_v3_crisis_polygon",
    ):
        self._initial_matic_price = initial_matic_price
        self._withdraw_threshold = withdraw_threshold
        self._resupply_threshold = resupply_threshold
        self._supply_amount = supply_amount
        self._strategy_id = strategy_id
        self._state = "idle"  # idle, supplied
        self._peak_price = initial_matic_price
        self._cycle_count = 0

    @property
    def strategy_id(self) -> str:
        return self._strategy_id

    def decide(self, market: Any) -> Any | None:
        matic_price = self._get_matic_price(market)
        if matic_price is None:
            return None

        if self._state == "idle":
            # Supply on first cycle or after recovery
            if self._cycle_count == 0:
                self._state = "supplied"
                self._peak_price = matic_price
                self._cycle_count += 1
                return MockCompoundV3SupplyIntent(amount=self._supply_amount)

            # Re-supply after price recovery
            drawdown_from_peak = (
                (self._peak_price - matic_price) / self._peak_price
                if self._peak_price > 0
                else Decimal("0")
            )
            if drawdown_from_peak < self._resupply_threshold:
                self._state = "supplied"
                self._peak_price = matic_price
                self._cycle_count += 1
                return MockCompoundV3SupplyIntent(amount=self._supply_amount)
            return None

        if self._state == "supplied":
            # Track peak
            if matic_price > self._peak_price:
                self._peak_price = matic_price

            drawdown = (
                (self._peak_price - matic_price) / self._peak_price
                if self._peak_price > 0
                else Decimal("0")
            )

            if drawdown >= self._withdraw_threshold:
                self._state = "idle"
                return MockCompoundV3WithdrawIntent(amount=self._supply_amount)

            return None

        return None

    def _get_matic_price(self, market: Any) -> Decimal | None:
        for token in ("MATIC", "WMATIC", "POL"):
            try:
                return market.price(token)
            except (PriceUnavailableError, KeyError, ValueError, AttributeError):
                continue
        return None


class HoldOnlyStrategy:
    """Strategy that never trades -- baseline for comparison."""

    def __init__(self, strategy_id: str = "hold_baseline_polygon"):
        self._strategy_id = strategy_id

    @property
    def strategy_id(self) -> str:
        return self._strategy_id

    def decide(self, market: Any) -> None:
        return None


# =============================================================================
# Helper
# =============================================================================


def _make_backtester(
    scenario: CrisisScenario,
    crash_severity: Decimal = Decimal("0.4"),
    recovery_pct: Decimal = Decimal("0.5"),
    zero_costs: bool = True,
) -> PnLBacktester:
    """Create a backtester with deterministic Polygon data for a scenario."""
    data_provider = PolygonCrisisDataProvider(
        start_time=scenario.start_date,
        duration_hours=scenario.duration_days * 24,
        crash_severity=crash_severity,
        recovery_pct=recovery_pct,
    )
    fee_kwargs = {"fee_pct": Decimal("0")} if zero_costs else {}
    slip_kwargs = {"slippage_pct": Decimal("0")} if zero_costs else {}
    return PnLBacktester(
        data_provider=data_provider,
        fee_models={"default": DefaultFeeModel(**fee_kwargs)},
        slippage_models={"default": DefaultSlippageModel(**slip_kwargs)},
    )


# =============================================================================
# Tests: Hold baseline on Polygon
# =============================================================================


class TestCrisisLendingPolygonHoldBaseline:
    """Hold-only baseline -- verify backtester works with Polygon chain."""

    @pytest.mark.asyncio
    async def test_hold_during_black_thursday(self) -> None:
        """Hold-only strategy preserves capital during Black Thursday on Polygon."""
        scenario = BLACK_THURSDAY
        backtester = _make_backtester(scenario, crash_severity=Decimal("0.5"))
        strategy = HoldOnlyStrategy()

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="polygon",
            tokens=["MATIC", "USDC.E"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        assert isinstance(result, CrisisBacktestResult)
        assert result.result.success, f"Backtest failed: {result.result.error}"
        assert result.scenario_name == "black_thursday"
        assert result.result.final_capital_usd == Decimal("10000")
        assert result.result.metrics.total_trades == 0

    @pytest.mark.asyncio
    async def test_hold_during_ftx_collapse(self) -> None:
        """Hold-only through FTX collapse on Polygon."""
        scenario = FTX_COLLAPSE
        backtester = _make_backtester(scenario, crash_severity=Decimal("0.25"))
        strategy = HoldOnlyStrategy()

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="polygon",
            tokens=["MATIC", "USDC.E"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        assert result.result.success
        assert result.scenario_name == "ftx_collapse"
        assert result.result.final_capital_usd == Decimal("10000")


# =============================================================================
# Tests: Compound V3 lending during crises on Polygon
# =============================================================================


class TestCrisisCompoundV3Lending:
    """Tests with active Compound V3 supply/withdraw during crisis scenarios."""

    @pytest.mark.asyncio
    async def test_lending_during_black_thursday(self) -> None:
        """Lending strategy manages supply/withdraw during Black Thursday on Polygon."""
        scenario = BLACK_THURSDAY
        backtester = _make_backtester(
            scenario, crash_severity=Decimal("0.4"), recovery_pct=Decimal("0.3")
        )
        strategy = CompoundV3CrisisLendingStrategy(
            initial_matic_price=Decimal("1.20"),
            withdraw_threshold=Decimal("0.15"),
            resupply_threshold=Decimal("0.08"),
            supply_amount=Decimal("10000"),
        )

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="polygon",
            tokens=["MATIC", "USDC.E"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        assert result.result.success, f"Backtest failed: {result.result.error}"
        assert result.scenario_name == "black_thursday"
        assert result.result.metrics.total_trades >= 1

    @pytest.mark.asyncio
    async def test_lending_during_terra_collapse(self) -> None:
        """Lending strategy manages supply/withdraw during Terra collapse on Polygon."""
        scenario = TERRA_COLLAPSE
        backtester = _make_backtester(
            scenario, crash_severity=Decimal("0.35"), recovery_pct=Decimal("0.2")
        )
        strategy = CompoundV3CrisisLendingStrategy(
            initial_matic_price=Decimal("1.20"),
            withdraw_threshold=Decimal("0.20"),
            resupply_threshold=Decimal("0.10"),
        )

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="polygon",
            tokens=["MATIC", "USDC.E"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        assert result.result.success
        assert result.scenario_name == "terra_collapse"
        assert result.result.metrics.total_trades >= 1

    @pytest.mark.asyncio
    async def test_lending_during_ftx_collapse(self) -> None:
        """Lending strategy manages supply/withdraw during FTX collapse on Polygon."""
        scenario = FTX_COLLAPSE
        backtester = _make_backtester(
            scenario, crash_severity=Decimal("0.25"), recovery_pct=Decimal("0.4")
        )
        strategy = CompoundV3CrisisLendingStrategy(
            initial_matic_price=Decimal("1.20"),
            withdraw_threshold=Decimal("0.15"),
            resupply_threshold=Decimal("0.08"),
        )

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="polygon",
            tokens=["MATIC", "USDC.E"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        assert result.result.success
        assert result.scenario_name == "ftx_collapse"
        assert result.scenario_duration_days == 8
        assert result.result.metrics.total_trades >= 1

    @pytest.mark.asyncio
    async def test_different_decisions_per_scenario(self) -> None:
        """Strategy should produce different trade counts under different crisis severities."""
        scenarios_config = [
            (BLACK_THURSDAY, Decimal("0.5"), Decimal("0.3")),   # Severe crash
            (TERRA_COLLAPSE, Decimal("0.2"), Decimal("0.8")),   # Mild crash
            (FTX_COLLAPSE, Decimal("0.1"), Decimal("0.9")),     # Shallow crash
        ]
        trade_counts = []

        for scenario, severity, recovery in scenarios_config:
            backtester = _make_backtester(scenario, crash_severity=severity, recovery_pct=recovery)
            strategy = CompoundV3CrisisLendingStrategy(
                withdraw_threshold=Decimal("0.15"),
                resupply_threshold=Decimal("0.08"),
            )

            result = await run_crisis_backtest(
                strategy=strategy,
                scenario=scenario,
                backtester=backtester,
                initial_capital_usd=Decimal("10000"),
                chain="polygon",
                tokens=["MATIC", "USDC.E"],
                include_gas_costs=False,
                inclusion_delay_blocks=0,
            )
            assert result.result.success
            trade_counts.append(result.result.metrics.total_trades)

        # At least the severe crash should trigger trades (supply + withdraw)
        assert trade_counts[0] >= 1, "Severe crash should trigger at least 1 trade"
        # Severe crash should trigger at least as many trades as milder ones
        assert trade_counts[0] >= trade_counts[2], (
            f"Severe crash ({trade_counts[0]} trades) should trade >= shallow crash ({trade_counts[2]} trades)"
        )


# =============================================================================
# Tests: Crisis metrics on Polygon
# =============================================================================


class TestCrisisMetricsPolygon:
    """Tests for crisis-specific metrics with Compound V3 strategy on Polygon."""

    @pytest.mark.asyncio
    async def test_crisis_metrics_populated(self) -> None:
        """Crisis metrics dict should contain all expected fields."""
        scenario = BLACK_THURSDAY
        backtester = _make_backtester(scenario)
        strategy = CompoundV3CrisisLendingStrategy()

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="polygon",
            tokens=["MATIC", "USDC.E"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        assert result.result.success
        assert result.crisis_metrics is not None
        expected_keys = [
            "scenario_name",
            "max_drawdown_pct",
            "recovery_pct",
            "total_return_pct",
            "volatility",
            "sharpe_ratio",
            "total_trades",
            "winning_trades",
            "losing_trades",
            "win_rate",
            "total_costs_usd",
        ]
        for key in expected_keys:
            assert key in result.crisis_metrics, f"Missing crisis metric: {key}"

    @pytest.mark.asyncio
    async def test_build_crisis_metrics_directly(self) -> None:
        """build_crisis_metrics works with Polygon/Compound V3 backtest results."""
        scenario = FTX_COLLAPSE
        backtester = _make_backtester(scenario)
        strategy = CompoundV3CrisisLendingStrategy()

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="polygon",
            tokens=["MATIC", "USDC.E"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        metrics = build_crisis_metrics(result.result, scenario)
        assert metrics.scenario_name == "ftx_collapse"
        assert isinstance(metrics.max_drawdown_pct, Decimal)
        assert isinstance(metrics.total_return_pct, Decimal)
        assert isinstance(metrics.sharpe_ratio, Decimal)

    @pytest.mark.asyncio
    async def test_pnl_metrics_for_all_three_scenarios(self) -> None:
        """Each scenario should report drawdown, return, and Sharpe on Polygon."""
        for scenario in [BLACK_THURSDAY, TERRA_COLLAPSE, FTX_COLLAPSE]:
            backtester = _make_backtester(scenario)
            strategy = CompoundV3CrisisLendingStrategy()

            result = await run_crisis_backtest(
                strategy=strategy,
                scenario=scenario,
                backtester=backtester,
                initial_capital_usd=Decimal("10000"),
                chain="polygon",
                tokens=["MATIC", "USDC.E"],
                include_gas_costs=False,
                inclusion_delay_blocks=0,
            )

            assert result.result.success, f"{scenario.name} failed: {result.result.error}"
            m = result.result.metrics
            assert isinstance(m.max_drawdown_pct, Decimal), f"{scenario.name}: max_drawdown not Decimal"
            assert isinstance(m.total_return_pct, Decimal), f"{scenario.name}: total_return not Decimal"
            assert isinstance(m.sharpe_ratio, Decimal), f"{scenario.name}: sharpe not Decimal"


# =============================================================================
# Tests: Comparison and config on Polygon
# =============================================================================


class TestCrisisComparisonPolygon:
    """Tests for compare_crisis_to_normal with Polygon/Compound V3 data."""

    @pytest.mark.asyncio
    async def test_compare_crisis_to_normal_period(self) -> None:
        """Compare crisis performance to a normal period on Polygon."""
        scenario = BLACK_THURSDAY

        crisis_backtester = _make_backtester(
            scenario, crash_severity=Decimal("0.4"), recovery_pct=Decimal("0.3")
        )
        crisis_strategy = CompoundV3CrisisLendingStrategy()

        crisis_result = await run_crisis_backtest(
            strategy=crisis_strategy,
            scenario=scenario,
            backtester=crisis_backtester,
            initial_capital_usd=Decimal("10000"),
            chain="polygon",
            tokens=["MATIC", "USDC.E"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        normal_scenario = CrisisScenario(
            name="normal_period",
            start_date=scenario.start_date,
            end_date=scenario.end_date,
            description="Normal market conditions",
            warmup_days=0,
        )
        normal_backtester = _make_backtester(
            normal_scenario, crash_severity=Decimal("0.05"), recovery_pct=Decimal("0.9")
        )
        normal_strategy = CompoundV3CrisisLendingStrategy()

        normal_result = await run_crisis_backtest(
            strategy=normal_strategy,
            scenario=normal_scenario,
            backtester=normal_backtester,
            initial_capital_usd=Decimal("10000"),
            chain="polygon",
            tokens=["MATIC", "USDC.E"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        assert crisis_result.result.success
        assert normal_result.result.success

        comparison = compare_crisis_to_normal(crisis_result.result, normal_result.result)
        assert "return_diff_pct" in comparison
        assert "volatility_ratio" in comparison
        assert "drawdown_ratio" in comparison
        assert "sharpe_diff" in comparison
        assert "stress_resilience_score" in comparison


class TestCrisisConfigPolygon:
    """Tests for CrisisBacktestConfig with Polygon chain settings."""

    def test_config_with_polygon_chain(self) -> None:
        """CrisisBacktestConfig should accept polygon as chain."""
        config = CrisisBacktestConfig(
            scenario=BLACK_THURSDAY,
            initial_capital_usd=Decimal("10000"),
            chain="polygon",
            tokens=["MATIC", "USDC.E"],
            gas_price_gwei=Decimal("50"),  # Polygon gas ~50 gwei MATIC
        )

        assert config.chain == "polygon"
        assert config.gas_price_gwei == Decimal("50")

        pnl_config = config.to_pnl_config()
        assert pnl_config.chain == "polygon"
        assert pnl_config.gas_price_gwei == Decimal("50")

    def test_config_serialization_with_polygon(self) -> None:
        """Config round-trips through serialization with Polygon settings."""
        config = CrisisBacktestConfig(
            scenario=FTX_COLLAPSE,
            initial_capital_usd=Decimal("50000"),
            chain="polygon",
            tokens=["MATIC", "USDC.E"],
            gas_price_gwei=Decimal("50"),
            mev_simulation_enabled=True,
        )

        d = config.to_dict()
        restored = CrisisBacktestConfig.from_dict(d)

        assert restored.chain == "polygon"
        assert restored.scenario.name == "ftx_collapse"
        assert restored.gas_price_gwei == Decimal("50")


class TestCrisisMultipleScenariosPolygon:
    """Tests running multiple crisis scenarios on Polygon."""

    @pytest.mark.asyncio
    async def test_all_three_scenarios_succeed(self) -> None:
        """All predefined scenarios should complete with Compound V3 strategy on Polygon."""
        scenarios = [BLACK_THURSDAY, TERRA_COLLAPSE, FTX_COLLAPSE]
        results = []

        for scenario in scenarios:
            backtester = _make_backtester(scenario, crash_severity=Decimal("0.3"))
            strategy = CompoundV3CrisisLendingStrategy()

            result = await run_crisis_backtest(
                strategy=strategy,
                scenario=scenario,
                backtester=backtester,
                initial_capital_usd=Decimal("10000"),
                chain="polygon",
                tokens=["MATIC", "USDC.E"],
                include_gas_costs=False,
                inclusion_delay_blocks=0,
            )
            results.append(result)

        assert len(results) == 3
        assert all(r.result.success for r in results)
        assert results[0].scenario_name == "black_thursday"
        assert results[1].scenario_name == "terra_collapse"
        assert results[2].scenario_name == "ftx_collapse"

    @pytest.mark.asyncio
    async def test_run_multiple_helper_on_polygon(self) -> None:
        """run_multiple_crisis_backtests helper works with Polygon hold strategy."""
        scenarios = [BLACK_THURSDAY, TERRA_COLLAPSE]
        backtester = _make_backtester(BLACK_THURSDAY, crash_severity=Decimal("0.3"))
        strategy = HoldOnlyStrategy()

        results = await run_multiple_crisis_backtests(
            strategy=strategy,
            scenarios=scenarios,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="polygon",
            tokens=["MATIC", "USDC.E"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        assert len(results) == 2
        assert all(r.result.success for r in results)
        assert all(r.result.final_capital_usd == Decimal("10000") for r in results)


class TestCrisisResultPolygon:
    """Tests for CrisisBacktestResult with Polygon/Compound V3 data."""

    @pytest.mark.asyncio
    async def test_result_summary_readable(self) -> None:
        """summary() should produce readable output for Polygon/Compound V3."""
        scenario = BLACK_THURSDAY
        backtester = _make_backtester(scenario)
        strategy = CompoundV3CrisisLendingStrategy()

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="polygon",
            tokens=["MATIC", "USDC.E"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        summary = result.summary()
        assert "black_thursday" in summary
        assert "Total Return" in summary
        assert "Max Drawdown" in summary

    @pytest.mark.asyncio
    async def test_realistic_costs_on_polygon(self) -> None:
        """Compound V3 lending with realistic fees on Polygon."""
        scenario = FTX_COLLAPSE
        data_provider = PolygonCrisisDataProvider(
            start_time=scenario.start_date,
            duration_hours=scenario.duration_days * 24,
            crash_severity=Decimal("0.25"),
            recovery_pct=Decimal("0.4"),
        )

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0.001"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0.0005"))},
        )
        strategy = CompoundV3CrisisLendingStrategy()

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="polygon",
            tokens=["MATIC", "USDC.E"],
            include_gas_costs=True,
            gas_price_gwei=Decimal("50"),
            inclusion_delay_blocks=0,
        )

        assert result.result.success
        assert result.result.metrics.total_trades > 0, (
            "Expected at least one trade to validate realistic cost tracking"
        )
        total_costs = (
            result.result.metrics.total_fees_usd
            + result.result.metrics.total_slippage_usd
            + result.result.metrics.total_gas_usd
        )
        assert total_costs > Decimal("0"), "Realistic costs should be positive when trades occur"
        assert result.result.metrics.total_gas_usd >= Decimal("0"), (
            "Gas costs should be tracked when include_gas_costs=True"
        )
