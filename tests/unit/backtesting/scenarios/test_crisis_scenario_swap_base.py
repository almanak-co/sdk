"""Unit tests for crisis scenario backtesting with an Aerodrome swap strategy on Base.

Exercises crisis scenario backtesting on Base chain with Aerodrome protocol,
validating that the crisis backtest infrastructure works beyond Arbitrum/Uniswap V3.
Uses deterministic mock data (no external API calls needed).

VIB-1937: Backtesting: Crisis scenario backtest Aerodrome swap strategy on Base
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
# Deterministic data provider for Base crisis periods
# =============================================================================


class BaseCrisisDataProvider:
    """Data provider generating crash-style price patterns for Base chain.

    Simulates a crash pattern: decline -> trough -> partial recovery.
    Reports chain as 'base' and supports ETH/USDC pair (Aerodrome context).
    Base gas prices are similar to Arbitrum (L2 chain, low gas).
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

        self._eth_prices = self._generate_crash_prices(
            initial=Decimal("3000"), severity=crash_severity, recovery=recovery_pct
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
                offset = Decimal("20") if i % 2 == 0 else Decimal("-20")
                price = trough_price + offset
            else:
                progress = Decimal(i - trough_end) / Decimal(max(n - trough_end - 1, 1))
                price = trough_price + (recovery_price - trough_price) * progress
            prices.append(max(price, Decimal("1")))

        return prices

    async def get_price(self, token: str, timestamp: datetime) -> Decimal:
        token = token.upper()
        if token in ("USDC", "USDT", "DAI", "USDBC"):
            return Decimal("1")
        delta = timestamp - self._start_time
        index = int(delta.total_seconds() / self._interval_seconds)
        if token in ("WETH", "ETH"):
            series = self._eth_prices
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
                    volume=Decimal("10000000"),
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
            market_state = MarketState(
                timestamp=current,
                prices=prices,
                chain="base",
                block_number=20000000 + index * 100,
                gas_price_gwei=Decimal("0.05"),  # Base L2 gas prices (lower than Arbitrum)
            )
            yield current, market_state
            index += 1
            current += timedelta(seconds=config.interval_seconds)

    @property
    def provider_name(self) -> str:
        return "base_crisis_deterministic"

    @property
    def supported_tokens(self) -> list[str]:
        return ["WETH", "USDC"]

    @property
    def supported_chains(self) -> list[str]:
        return ["base"]

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
# Mock swap intents for Aerodrome
# =============================================================================


@dataclass
class MockAerodromeSwapIntent:
    """Mock Aerodrome swap intent for momentum strategy."""

    intent_type: str = "SWAP"
    from_token: str = "USDC"
    to_token: str = "WETH"
    amount_usd: Decimal = Decimal("3000")
    protocol: str = "aerodrome"
    max_slippage_bps: int = 100


class AerodromeMomentumStrategy:
    """Momentum swap strategy for Aerodrome on Base.

    BUY WETH on dips (crash), SELL on recovery.
    Uses Aerodrome as the swap protocol (Solidly-fork DEX native to Base).
    """

    def __init__(
        self,
        initial_price: Decimal = Decimal("3000"),
        buy_threshold: Decimal = Decimal("0.85"),
        sell_threshold: Decimal = Decimal("0.95"),
        trade_size_usd: Decimal = Decimal("3000"),
        strategy_id: str = "aerodrome_momentum_crisis",
    ):
        self._initial_price = initial_price
        self._buy_threshold = buy_threshold
        self._sell_threshold = sell_threshold
        self._trade_size_usd = trade_size_usd
        self._strategy_id = strategy_id
        self._last_action: str | None = None

    @property
    def strategy_id(self) -> str:
        return self._strategy_id

    def decide(self, market: Any) -> Any | None:
        try:
            eth_price = market.price("WETH")
        except (PriceUnavailableError, KeyError):
            try:
                eth_price = market.price("ETH")
            except (PriceUnavailableError, KeyError):
                return None

        ratio = eth_price / self._initial_price

        # BUY when price drops below threshold (oversold)
        if ratio < self._buy_threshold and self._last_action != "BUY":
            self._last_action = "BUY"
            return MockAerodromeSwapIntent(
                from_token="USDC",
                to_token="WETH",
                amount_usd=self._trade_size_usd,
                protocol="aerodrome",
            )

        # SELL when price recovers above threshold
        if ratio > self._sell_threshold and self._last_action == "BUY":
            self._last_action = "SELL"
            return MockAerodromeSwapIntent(
                from_token="WETH",
                to_token="USDC",
                amount_usd=self._trade_size_usd,
                protocol="aerodrome",
            )

        return None


class HoldOnlyStrategy:
    """Strategy that never trades — baseline for comparison."""

    def __init__(self, strategy_id: str = "hold_baseline_base"):
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
    """Create a backtester with deterministic Base data for a scenario."""
    data_provider = BaseCrisisDataProvider(
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
# Tests: Hold baseline on Base
# =============================================================================


class TestCrisisSwapBaseHoldBaseline:
    """Hold-only baseline — verify backtester works with Base chain."""

    @pytest.mark.asyncio
    async def test_hold_during_black_thursday(self) -> None:
        """Hold-only strategy preserves capital during Black Thursday on Base."""
        scenario = BLACK_THURSDAY
        backtester = _make_backtester(scenario, crash_severity=Decimal("0.5"))
        strategy = HoldOnlyStrategy()

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="base",
            tokens=["WETH", "USDC"],
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
        """Hold-only through FTX collapse on Base."""
        scenario = FTX_COLLAPSE
        backtester = _make_backtester(scenario, crash_severity=Decimal("0.25"))
        strategy = HoldOnlyStrategy()

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="base",
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        assert result.result.success
        assert result.scenario_name == "ftx_collapse"
        assert result.result.final_capital_usd == Decimal("10000")


# =============================================================================
# Tests: Aerodrome momentum strategy during crises on Base
# =============================================================================


class TestCrisisAerodromeTrading:
    """Tests with active Aerodrome swap trading during crisis scenarios on Base."""

    @pytest.mark.asyncio
    async def test_aerodrome_strategy_during_black_thursday(self) -> None:
        """Momentum strategy trades Aerodrome swaps during Black Thursday on Base."""
        scenario = BLACK_THURSDAY
        backtester = _make_backtester(
            scenario, crash_severity=Decimal("0.4"), recovery_pct=Decimal("0.3")
        )
        strategy = AerodromeMomentumStrategy(
            initial_price=Decimal("3000"),
            buy_threshold=Decimal("0.75"),
            sell_threshold=Decimal("0.95"),
            trade_size_usd=Decimal("3000"),
        )

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="base",
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        assert result.result.success, f"Backtest failed: {result.result.error}"
        assert result.scenario_name == "black_thursday"
        assert result.result.metrics.total_trades >= 1
        assert result.result.metrics.max_drawdown_pct > Decimal("0")

    @pytest.mark.asyncio
    async def test_aerodrome_strategy_during_terra_collapse(self) -> None:
        """Momentum strategy trades during Terra/Luna collapse on Base."""
        scenario = TERRA_COLLAPSE
        backtester = _make_backtester(
            scenario, crash_severity=Decimal("0.35"), recovery_pct=Decimal("0.2")
        )
        strategy = AerodromeMomentumStrategy(
            initial_price=Decimal("3000"),
            buy_threshold=Decimal("0.80"),
            sell_threshold=Decimal("0.95"),
        )

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="base",
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        assert result.result.success
        assert result.scenario_name == "terra_collapse"
        assert result.result.metrics.total_trades >= 1

    @pytest.mark.asyncio
    async def test_aerodrome_strategy_during_ftx_collapse(self) -> None:
        """Momentum strategy trades during FTX collapse on Base."""
        scenario = FTX_COLLAPSE
        backtester = _make_backtester(
            scenario, crash_severity=Decimal("0.25"), recovery_pct=Decimal("0.4")
        )
        strategy = AerodromeMomentumStrategy(
            initial_price=Decimal("3000"),
            buy_threshold=Decimal("0.80"),
            sell_threshold=Decimal("0.90"),
        )

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="base",
            tokens=["WETH", "USDC"],
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
            strategy = AerodromeMomentumStrategy(
                buy_threshold=Decimal("0.85"),
                sell_threshold=Decimal("0.95"),
            )

            result = await run_crisis_backtest(
                strategy=strategy,
                scenario=scenario,
                backtester=backtester,
                initial_capital_usd=Decimal("10000"),
                chain="base",
                tokens=["WETH", "USDC"],
                include_gas_costs=False,
                inclusion_delay_blocks=0,
            )
            assert result.result.success
            trade_counts.append(result.result.metrics.total_trades)

        # At least the severe crash should trigger trades
        assert trade_counts[0] >= 1, "Severe crash should trigger at least 1 trade"
        assert len(set(trade_counts)) > 1, (
            "Trade counts should differ across crisis severities/recovery profiles"
        )


# =============================================================================
# Tests: Crisis metrics on Base
# =============================================================================


class TestCrisisMetricsBase:
    """Tests for crisis-specific metrics with Aerodrome strategy on Base."""

    @pytest.mark.asyncio
    async def test_crisis_metrics_populated(self) -> None:
        """Crisis metrics dict should contain all expected fields."""
        scenario = BLACK_THURSDAY
        backtester = _make_backtester(scenario)
        strategy = AerodromeMomentumStrategy()

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="base",
            tokens=["WETH", "USDC"],
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
        """build_crisis_metrics should work with Base/Aerodrome backtest results."""
        scenario = FTX_COLLAPSE
        backtester = _make_backtester(scenario)
        strategy = AerodromeMomentumStrategy()

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="base",
            tokens=["WETH", "USDC"],
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
        """Each scenario should report drawdown, return, and Sharpe ratio on Base."""
        for scenario in [BLACK_THURSDAY, TERRA_COLLAPSE, FTX_COLLAPSE]:
            backtester = _make_backtester(scenario)
            strategy = AerodromeMomentumStrategy()

            result = await run_crisis_backtest(
                strategy=strategy,
                scenario=scenario,
                backtester=backtester,
                initial_capital_usd=Decimal("10000"),
                chain="base",
                tokens=["WETH", "USDC"],
                include_gas_costs=False,
                inclusion_delay_blocks=0,
            )

            assert result.result.success, f"{scenario.name} failed: {result.result.error}"
            m = result.result.metrics
            assert isinstance(m.max_drawdown_pct, Decimal), f"{scenario.name}: max_drawdown_pct not Decimal"
            assert isinstance(m.total_return_pct, Decimal), f"{scenario.name}: total_return_pct not Decimal"
            assert isinstance(m.sharpe_ratio, Decimal), f"{scenario.name}: sharpe_ratio not Decimal"


# =============================================================================
# Tests: Comparison and config on Base
# =============================================================================


class TestCrisisComparisonBase:
    """Tests for compare_crisis_to_normal with Base/Aerodrome data."""

    @pytest.mark.asyncio
    async def test_compare_crisis_to_normal_period(self) -> None:
        """Compare crisis performance to a normal period on Base."""
        scenario = BLACK_THURSDAY

        crisis_backtester = _make_backtester(
            scenario, crash_severity=Decimal("0.4"), recovery_pct=Decimal("0.3")
        )
        crisis_strategy = AerodromeMomentumStrategy()

        crisis_result = await run_crisis_backtest(
            strategy=crisis_strategy,
            scenario=scenario,
            backtester=crisis_backtester,
            initial_capital_usd=Decimal("10000"),
            chain="base",
            tokens=["WETH", "USDC"],
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
        normal_strategy = AerodromeMomentumStrategy()

        normal_result = await run_crisis_backtest(
            strategy=normal_strategy,
            scenario=normal_scenario,
            backtester=normal_backtester,
            initial_capital_usd=Decimal("10000"),
            chain="base",
            tokens=["WETH", "USDC"],
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


class TestCrisisConfigBase:
    """Tests for CrisisBacktestConfig with Base chain settings."""

    def test_config_with_base_chain(self) -> None:
        """CrisisBacktestConfig should accept base as chain."""
        config = CrisisBacktestConfig(
            scenario=BLACK_THURSDAY,
            initial_capital_usd=Decimal("10000"),
            chain="base",
            tokens=["WETH", "USDC"],
            gas_price_gwei=Decimal("0.05"),  # Base L2 gas price
        )

        assert config.chain == "base"
        assert config.gas_price_gwei == Decimal("0.05")

        pnl_config = config.to_pnl_config()
        assert pnl_config.chain == "base"
        assert pnl_config.gas_price_gwei == Decimal("0.05")

    def test_config_serialization_with_base(self) -> None:
        """Config round-trips through serialization with Base settings."""
        config = CrisisBacktestConfig(
            scenario=FTX_COLLAPSE,
            initial_capital_usd=Decimal("50000"),
            chain="base",
            tokens=["WETH", "USDC"],
            gas_price_gwei=Decimal("0.05"),
            mev_simulation_enabled=True,
        )

        d = config.to_dict()
        restored = CrisisBacktestConfig.from_dict(d)

        assert restored.chain == "base"
        assert restored.scenario.name == "ftx_collapse"
        assert restored.gas_price_gwei == Decimal("0.05")


class TestCrisisMultipleScenariosBase:
    """Tests running multiple crisis scenarios on Base."""

    @pytest.mark.asyncio
    async def test_all_three_scenarios_succeed(self) -> None:
        """All predefined scenarios should complete with Aerodrome strategy on Base."""
        scenarios = [BLACK_THURSDAY, TERRA_COLLAPSE, FTX_COLLAPSE]
        results = []

        for scenario in scenarios:
            backtester = _make_backtester(scenario, crash_severity=Decimal("0.3"))
            strategy = AerodromeMomentumStrategy()

            result = await run_crisis_backtest(
                strategy=strategy,
                scenario=scenario,
                backtester=backtester,
                initial_capital_usd=Decimal("10000"),
                chain="base",
                tokens=["WETH", "USDC"],
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
    async def test_run_multiple_helper_on_base(self) -> None:
        """run_multiple_crisis_backtests helper works with Base hold strategy."""
        scenarios = [BLACK_THURSDAY, TERRA_COLLAPSE]
        backtester = _make_backtester(BLACK_THURSDAY, crash_severity=Decimal("0.3"))
        strategy = HoldOnlyStrategy()

        results = await run_multiple_crisis_backtests(
            strategy=strategy,
            scenarios=scenarios,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="base",
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        assert len(results) == 2
        assert all(r.result.success for r in results)
        assert all(r.result.final_capital_usd == Decimal("10000") for r in results)


class TestCrisisResultBase:
    """Tests for CrisisBacktestResult with Base/Aerodrome data."""

    @pytest.mark.asyncio
    async def test_result_summary_readable(self) -> None:
        """summary() should produce readable output for Base/Aerodrome."""
        scenario = BLACK_THURSDAY
        backtester = _make_backtester(scenario)
        strategy = AerodromeMomentumStrategy()

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="base",
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        summary = result.summary()
        assert "black_thursday" in summary
        assert "Total Return" in summary
        assert "Max Drawdown" in summary

    @pytest.mark.asyncio
    async def test_realistic_costs_on_base(self) -> None:
        """Aerodrome strategy with realistic fees/slippage on Base."""
        scenario = FTX_COLLAPSE
        data_provider = BaseCrisisDataProvider(
            start_time=scenario.start_date,
            duration_hours=scenario.duration_days * 24,
            crash_severity=Decimal("0.25"),
            recovery_pct=Decimal("0.4"),
        )

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0.003"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0.001"))},
        )
        strategy = AerodromeMomentumStrategy()

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="base",
            tokens=["WETH", "USDC"],
            include_gas_costs=True,
            gas_price_gwei=Decimal("0.05"),
            inclusion_delay_blocks=0,
        )

        assert result.result.success
        assert result.result.metrics.total_trades > 0, (
            "Expected at least one trade to validate realistic cost tracking"
        )
        total_costs = (
            result.result.metrics.total_fees_usd
            + result.result.metrics.total_slippage_usd
        )
        assert total_costs > Decimal("0"), "Realistic costs should be positive when trades occur"
