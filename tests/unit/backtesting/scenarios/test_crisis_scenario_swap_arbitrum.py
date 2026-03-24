"""Unit tests for crisis scenario backtesting with a swap strategy on Arbitrum.

First crisis scenario backtest with a swap strategy (previous test used lending on
Ethereum). Exercises `run_crisis_backtest()` and `run_multiple_crisis_backtests()`
with a deterministic RSI-like swap strategy on Arbitrum, using predefined crisis
scenarios (Black Thursday, Terra Collapse, FTX Collapse).

Uses deterministic mock data (no external API calls needed).

VIB-1819: Backtesting: Crisis scenario backtest Uniswap V3 swap strategy on Arbitrum
"""

from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
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
# Deterministic data provider for Arbitrum crisis periods
# =============================================================================


class ArbitrumCrisisDataProvider:
    """Data provider generating crash-style price patterns for Arbitrum.

    Simulates a crash pattern: decline -> trough -> partial recovery.
    Reports chain as 'arbitrum' and supports ETH/USDC pair.
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
                progress = Decimal(i - trough_end) / Decimal(max(n - trough_end, 1))
                price = trough_price + (recovery_price - trough_price) * progress
            prices.append(max(price, Decimal("1")))

        return prices

    async def get_price(self, token: str, timestamp: datetime) -> Decimal:
        token = token.upper()
        if token in ("USDC", "USDT", "DAI"):
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
                chain="arbitrum",
                block_number=50000000 + index * 100,
                gas_price_gwei=Decimal("0.1"),  # Arbitrum L2 gas prices
            )
            yield current, market_state
            index += 1
            current += timedelta(seconds=config.interval_seconds)

    @property
    def provider_name(self) -> str:
        return "arbitrum_crisis_deterministic"

    @property
    def supported_tokens(self) -> list[str]:
        return ["WETH", "USDC"]

    @property
    def supported_chains(self) -> list[str]:
        return ["arbitrum"]

    @property
    def min_timestamp(self) -> datetime | None:
        return self._start_time

    @property
    def max_timestamp(self) -> datetime | None:
        return self._start_time + timedelta(hours=self._duration_hours)


# =============================================================================
# Mock swap intents for Uniswap V3
# =============================================================================


@dataclass
class MockSwapIntent:
    """Mock Uniswap V3 swap intent for RSI-like strategy."""

    intent_type: str = "SWAP"
    from_token: str = "USDC"
    to_token: str = "WETH"
    amount_usd: Decimal = Decimal("3000")
    protocol: str = "uniswap_v3"
    max_slippage_bps: int = 100


class DeterministicSwapStrategy:
    """RSI-like swap strategy that buys during crashes and sells during recovery.

    Simulates the uniswap_rsi demo strategy behavior:
    - Buy WETH when price drops significantly (oversold)
    - Sell WETH when price recovers (overbought)
    - Hold during trough period

    This tests the crisis backtest runner with swap intents, which is the
    primary intent type that uniswap_rsi uses.
    """

    def __init__(
        self,
        initial_price: Decimal = Decimal("3000"),
        buy_threshold: Decimal = Decimal("0.85"),
        sell_threshold: Decimal = Decimal("0.95"),
        trade_size_usd: Decimal = Decimal("3000"),
        strategy_id: str = "uniswap_rsi_crisis",
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
        except Exception:
            try:
                eth_price = market.price("ETH")
            except Exception:
                return None

        ratio = eth_price / self._initial_price

        # Buy when price drops below threshold (oversold)
        if ratio < self._buy_threshold and self._last_action != "BUY":
            self._last_action = "BUY"
            return MockSwapIntent(
                from_token="USDC",
                to_token="WETH",
                amount_usd=self._trade_size_usd,
            )

        # Sell when price recovers above threshold
        if ratio > self._sell_threshold and self._last_action == "BUY":
            self._last_action = "SELL"
            return MockSwapIntent(
                from_token="WETH",
                to_token="USDC",
                amount_usd=self._trade_size_usd,
            )

        return None


class HoldOnlyStrategy:
    """Strategy that never trades — baseline for comparison."""

    def __init__(self, strategy_id: str = "hold_baseline"):
        self._strategy_id = strategy_id

    @property
    def strategy_id(self) -> str:
        return self._strategy_id

    def decide(self, market: Any) -> None:
        return None


# =============================================================================
# Helper to create backtester for a given scenario
# =============================================================================


def _make_backtester(
    scenario: CrisisScenario,
    crash_severity: Decimal = Decimal("0.4"),
    recovery_pct: Decimal = Decimal("0.5"),
    zero_costs: bool = True,
) -> PnLBacktester:
    """Create a backtester with deterministic Arbitrum data for a scenario."""
    data_provider = ArbitrumCrisisDataProvider(
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
# Tests: Swap strategy on Arbitrum during crisis scenarios
# =============================================================================


class TestCrisisSwapArbitrumHoldBaseline:
    """Hold-only baseline tests — verify backtester works with Arbitrum chain."""

    @pytest.mark.asyncio
    async def test_hold_during_black_thursday_preserves_capital(self) -> None:
        """Hold-only strategy should preserve capital (all USDC, no ETH exposure)."""
        scenario = BLACK_THURSDAY
        backtester = _make_backtester(scenario, crash_severity=Decimal("0.5"))
        strategy = HoldOnlyStrategy()

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="arbitrum",
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
        """Hold-only through FTX collapse on Arbitrum."""
        scenario = FTX_COLLAPSE
        backtester = _make_backtester(scenario, crash_severity=Decimal("0.25"))
        strategy = HoldOnlyStrategy()

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="arbitrum",
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        assert result.result.success
        assert result.scenario_name == "ftx_collapse"
        assert result.result.final_capital_usd == Decimal("10000")


class TestCrisisSwapArbitrumTrading:
    """Tests with active swap trading during crisis scenarios on Arbitrum."""

    @pytest.mark.asyncio
    async def test_swap_strategy_during_black_thursday(self) -> None:
        """RSI-like swap strategy trades during Black Thursday crash on Arbitrum."""
        scenario = BLACK_THURSDAY
        backtester = _make_backtester(
            scenario, crash_severity=Decimal("0.4"), recovery_pct=Decimal("0.3")
        )
        strategy = DeterministicSwapStrategy(
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
            chain="arbitrum",
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        assert result.result.success, f"Backtest failed: {result.result.error}"
        assert result.scenario_name == "black_thursday"
        # Strategy should have made at least 1 trade (buy during crash)
        assert result.result.metrics.total_trades >= 1
        # With a 40% crash, max drawdown should be non-trivial
        assert result.result.metrics.max_drawdown_pct > Decimal("0")

    @pytest.mark.asyncio
    async def test_swap_strategy_during_terra_collapse(self) -> None:
        """Swap strategy trades during Terra/Luna collapse on Arbitrum."""
        scenario = TERRA_COLLAPSE
        backtester = _make_backtester(
            scenario, crash_severity=Decimal("0.35"), recovery_pct=Decimal("0.2")
        )
        strategy = DeterministicSwapStrategy(
            initial_price=Decimal("3000"),
            buy_threshold=Decimal("0.80"),
            sell_threshold=Decimal("0.95"),
        )

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="arbitrum",
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        assert result.result.success
        assert result.scenario_name == "terra_collapse"
        assert result.result.metrics.total_trades >= 1

    @pytest.mark.asyncio
    async def test_swap_strategy_during_ftx_collapse(self) -> None:
        """Swap strategy trades during FTX collapse on Arbitrum."""
        scenario = FTX_COLLAPSE
        backtester = _make_backtester(
            scenario, crash_severity=Decimal("0.25"), recovery_pct=Decimal("0.4")
        )
        strategy = DeterministicSwapStrategy(
            initial_price=Decimal("3000"),
            buy_threshold=Decimal("0.80"),
            sell_threshold=Decimal("0.90"),
        )

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="arbitrum",
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        assert result.result.success
        assert result.scenario_name == "ftx_collapse"
        assert result.scenario_duration_days == 8
        assert result.result.metrics.total_trades >= 1


class TestCrisisMetricsArbitrum:
    """Tests for crisis-specific metrics with swap strategy on Arbitrum."""

    @pytest.mark.asyncio
    async def test_crisis_metrics_populated(self) -> None:
        """Crisis metrics dict should contain all expected fields."""
        scenario = BLACK_THURSDAY
        backtester = _make_backtester(scenario)
        strategy = DeterministicSwapStrategy()

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="arbitrum",
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
    async def test_crisis_metrics_on_backtest_result(self) -> None:
        """BacktestResult.crisis_results should be a CrisisMetrics object."""
        scenario = TERRA_COLLAPSE
        backtester = _make_backtester(scenario)
        strategy = DeterministicSwapStrategy()

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="arbitrum",
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        assert result.result.success
        assert result.result.crisis_results is not None
        assert result.result.crisis_results.scenario_name == "terra_collapse"
        assert result.result.crisis_results.scenario_duration_days == 7

    @pytest.mark.asyncio
    async def test_build_crisis_metrics_directly(self) -> None:
        """build_crisis_metrics should work with swap backtest results."""
        scenario = FTX_COLLAPSE
        backtester = _make_backtester(scenario)
        strategy = DeterministicSwapStrategy()

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="arbitrum",
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        metrics = build_crisis_metrics(result.result, scenario)
        assert metrics.scenario_name == "ftx_collapse"
        assert isinstance(metrics.max_drawdown_pct, Decimal)
        assert isinstance(metrics.recovery_pct, Decimal)
        assert isinstance(metrics.total_return_pct, Decimal)
        assert isinstance(metrics.sharpe_ratio, Decimal)

    @pytest.mark.asyncio
    async def test_pnl_metrics_reported_for_each_scenario(self) -> None:
        """Each scenario should report max drawdown, total return, and Sharpe ratio."""
        for scenario in [BLACK_THURSDAY, TERRA_COLLAPSE, FTX_COLLAPSE]:
            backtester = _make_backtester(scenario)
            strategy = DeterministicSwapStrategy()

            result = await run_crisis_backtest(
                strategy=strategy,
                scenario=scenario,
                backtester=backtester,
                initial_capital_usd=Decimal("10000"),
                chain="arbitrum",
                tokens=["WETH", "USDC"],
                include_gas_costs=False,
                inclusion_delay_blocks=0,
            )

            assert result.result.success, f"{scenario.name} failed: {result.result.error}"
            m = result.result.metrics
            assert isinstance(m.max_drawdown_pct, Decimal), f"{scenario.name}: max_drawdown_pct not Decimal"
            assert isinstance(m.total_return_pct, Decimal), f"{scenario.name}: total_return_pct not Decimal"
            assert isinstance(m.sharpe_ratio, Decimal), f"{scenario.name}: sharpe_ratio not Decimal"


class TestCrisisComparisonArbitrum:
    """Tests for compare_crisis_to_normal with Arbitrum swap data."""

    @pytest.mark.asyncio
    async def test_compare_crisis_to_normal_period(self) -> None:
        """Compare crisis performance to a 'normal' period baseline."""
        scenario = BLACK_THURSDAY

        # Crisis period backtest
        crisis_backtester = _make_backtester(
            scenario, crash_severity=Decimal("0.4"), recovery_pct=Decimal("0.3")
        )
        crisis_strategy = DeterministicSwapStrategy()

        crisis_result = await run_crisis_backtest(
            strategy=crisis_strategy,
            scenario=scenario,
            backtester=crisis_backtester,
            initial_capital_usd=Decimal("10000"),
            chain="arbitrum",
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        # Normal period backtest (low volatility, small moves)
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
        normal_strategy = DeterministicSwapStrategy()

        normal_result = await run_crisis_backtest(
            strategy=normal_strategy,
            scenario=normal_scenario,
            backtester=normal_backtester,
            initial_capital_usd=Decimal("10000"),
            chain="arbitrum",
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


class TestCrisisMultipleScenarios:
    """Tests running multiple crisis scenarios sequentially on Arbitrum."""

    @pytest.mark.asyncio
    async def test_all_three_scenarios_succeed(self) -> None:
        """All predefined scenarios should complete successfully with swap strategy."""
        scenarios = [BLACK_THURSDAY, TERRA_COLLAPSE, FTX_COLLAPSE]
        results = []

        for scenario in scenarios:
            backtester = _make_backtester(scenario, crash_severity=Decimal("0.3"))
            strategy = DeterministicSwapStrategy()

            result = await run_crisis_backtest(
                strategy=strategy,
                scenario=scenario,
                backtester=backtester,
                initial_capital_usd=Decimal("10000"),
                chain="arbitrum",
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
    async def test_run_multiple_helper_on_arbitrum(self) -> None:
        """run_multiple_crisis_backtests helper works with Arbitrum hold strategy.

        Uses HoldOnlyStrategy (no trades) so a single backtester is safe —
        hold-only results are independent of price data alignment.
        """
        scenarios = [BLACK_THURSDAY, TERRA_COLLAPSE]
        backtester = _make_backtester(BLACK_THURSDAY, crash_severity=Decimal("0.3"))
        strategy = HoldOnlyStrategy()

        results = await run_multiple_crisis_backtests(
            strategy=strategy,
            scenarios=scenarios,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="arbitrum",
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        assert len(results) == 2
        assert all(r.result.success for r in results)
        assert results[0].scenario_name == "black_thursday"
        assert results[1].scenario_name == "terra_collapse"
        # Hold-only should preserve capital across all scenarios
        assert all(r.result.final_capital_usd == Decimal("10000") for r in results)


class TestCrisisConfigArbitrum:
    """Tests for CrisisBacktestConfig with Arbitrum chain settings."""

    def test_config_with_arbitrum_chain(self) -> None:
        """CrisisBacktestConfig should accept arbitrum as chain."""
        config = CrisisBacktestConfig(
            scenario=BLACK_THURSDAY,
            initial_capital_usd=Decimal("10000"),
            chain="arbitrum",
            tokens=["WETH", "USDC"],
            gas_price_gwei=Decimal("0.1"),  # Arbitrum L2 gas price
        )

        assert config.chain == "arbitrum"
        assert config.gas_price_gwei == Decimal("0.1")

        pnl_config = config.to_pnl_config()
        assert pnl_config.chain == "arbitrum"
        assert pnl_config.gas_price_gwei == Decimal("0.1")

    def test_config_serialization_with_arbitrum(self) -> None:
        """Config round-trips through serialization with Arbitrum settings."""
        config = CrisisBacktestConfig(
            scenario=FTX_COLLAPSE,
            initial_capital_usd=Decimal("50000"),
            chain="arbitrum",
            tokens=["WETH", "USDC"],
            gas_price_gwei=Decimal("0.1"),
            mev_simulation_enabled=True,
        )

        d = config.to_dict()
        restored = CrisisBacktestConfig.from_dict(d)

        assert restored.chain == "arbitrum"
        assert restored.scenario.name == "ftx_collapse"
        assert restored.gas_price_gwei == Decimal("0.1")
        assert restored.mev_simulation_enabled is True

    @pytest.mark.asyncio
    async def test_config_object_passed_to_runner(self) -> None:
        """run_crisis_backtest should accept config object directly."""
        scenario = TERRA_COLLAPSE
        config = CrisisBacktestConfig(
            scenario=scenario,
            initial_capital_usd=Decimal("10000"),
            chain="arbitrum",
            tokens=["WETH", "USDC"],
            gas_price_gwei=Decimal("0.1"),
        )

        backtester = _make_backtester(scenario)
        strategy = HoldOnlyStrategy()

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            config=config,
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        assert result.result.success
        assert result.scenario_name == "terra_collapse"


class TestCrisisResultArbitrum:
    """Tests for CrisisBacktestResult with Arbitrum swap data."""

    @pytest.mark.asyncio
    async def test_result_summary_readable(self) -> None:
        """summary() should produce human-readable output for Arbitrum swap."""
        scenario = BLACK_THURSDAY
        backtester = _make_backtester(scenario)
        strategy = DeterministicSwapStrategy()

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="arbitrum",
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        summary = result.summary()
        assert "black_thursday" in summary
        assert "Total Return" in summary
        assert "Max Drawdown" in summary
        assert "Sharpe Ratio" in summary
        assert "Total Trades" in summary

    @pytest.mark.asyncio
    async def test_result_serialization(self) -> None:
        """CrisisBacktestResult round-trips through dict serialization."""
        scenario = FTX_COLLAPSE
        backtester = _make_backtester(scenario)
        strategy = HoldOnlyStrategy()

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="arbitrum",
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        d = result.to_dict()
        assert d["success"] is True
        assert d["error"] is None
        assert d["scenario"]["name"] == "ftx_collapse"
        assert "crisis_metrics" in d

        restored = CrisisBacktestResult.from_dict(d)
        assert restored.scenario_name == "ftx_collapse"

    @pytest.mark.asyncio
    async def test_result_properties(self) -> None:
        """CrisisBacktestResult properties should work with swap results."""
        scenario = TERRA_COLLAPSE
        backtester = _make_backtester(scenario, crash_severity=Decimal("0.35"))
        strategy = DeterministicSwapStrategy()

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="arbitrum",
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        assert result.scenario_name == "terra_collapse"
        assert result.scenario_duration_days == 7
        assert isinstance(result.max_drawdown_during_crisis, Decimal)
        assert isinstance(result.total_return_during_crisis, Decimal)


class TestCrisisCustomScenarioArbitrum:
    """Tests for custom crisis scenarios on Arbitrum."""

    @pytest.mark.asyncio
    async def test_custom_scenario_with_swap_strategy(self) -> None:
        """Custom crisis scenario should work with swap strategy on Arbitrum."""
        svb_collapse = CrisisScenario(
            name="svb_collapse",
            start_date=datetime(2023, 3, 10, tzinfo=UTC),
            end_date=datetime(2023, 3, 15, tzinfo=UTC),
            description="SVB collapse caused USDC depeg and market stress",
            warmup_days=0,
        )

        backtester = _make_backtester(
            svb_collapse, crash_severity=Decimal("0.15"), recovery_pct=Decimal("0.8")
        )
        strategy = DeterministicSwapStrategy(
            buy_threshold=Decimal("0.90"),
            sell_threshold=Decimal("0.98"),
        )

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=svb_collapse,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="arbitrum",
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        assert result.result.success
        assert result.scenario_name == "svb_collapse"
        assert result.scenario_duration_days == 5

    @pytest.mark.asyncio
    async def test_custom_scenario_with_realistic_costs(self) -> None:
        """Custom scenario with realistic fee/slippage models on Arbitrum."""
        scenario = CrisisScenario(
            name="recent_volatility",
            start_date=datetime(2024, 8, 5, tzinfo=UTC),
            end_date=datetime(2024, 8, 10, tzinfo=UTC),
            description="August 2024 market volatility event",
            warmup_days=0,
        )

        data_provider = ArbitrumCrisisDataProvider(
            start_time=scenario.start_date,
            duration_hours=scenario.duration_days * 24,
            crash_severity=Decimal("0.20"),
            recovery_pct=Decimal("0.6"),
        )

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0.003"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0.001"))},
        )
        strategy = DeterministicSwapStrategy()

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="arbitrum",
            tokens=["WETH", "USDC"],
            include_gas_costs=True,
            gas_price_gwei=Decimal("0.1"),
            inclusion_delay_blocks=0,
        )

        assert result.result.success
        # With realistic costs, total costs should be > 0 if trades were made
        if result.result.metrics.total_trades > 0:
            total_costs = (
                result.result.metrics.total_fees_usd
                + result.result.metrics.total_slippage_usd
            )
            assert total_costs > Decimal("0"), "Realistic costs should be positive when trades occur"
