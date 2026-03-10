"""Integration tests for crisis scenario backtesting with Aave V3 lending.

First test of the scenario/crisis backtesting module in the Kitchen Loop.
Exercises `run_crisis_backtest()` and `run_multiple_crisis_backtests()` with
a deterministic lending strategy on Ethereum, using predefined crisis
scenarios (Black Thursday, Terra Collapse, FTX Collapse).

Uses deterministic mock data (no external API calls needed).

VIB-590: Backtest: Crisis Scenario Test Aave V3 Lending on Ethereum
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
    CrisisScenario,
    FTX_COLLAPSE,
    PREDEFINED_SCENARIOS,
    TERRA_COLLAPSE,
    get_scenario_by_name,
)
from almanak.framework.backtesting.scenarios.crisis_runner import (
    CrisisBacktestConfig,
    CrisisBacktestResult,
    run_crisis_backtest,
    run_multiple_crisis_backtests,
)

# =============================================================================
# Deterministic data provider for crisis periods
# =============================================================================


class CrisisDataProvider:
    """Data provider that generates crash-style price patterns.

    Simulates a crash pattern: decline -> trough -> partial recovery.
    The severity parameter controls how deep the crash goes.
    """

    def __init__(
        self,
        crash_severity: Decimal = Decimal("0.4"),
        recovery_pct: Decimal = Decimal("0.5"),
        start_time: datetime | None = None,
        duration_hours: int = 168,  # 7 days
    ):
        self._crash_severity = crash_severity
        self._recovery_pct = recovery_pct
        self._start_time = start_time or datetime(2020, 3, 12, tzinfo=UTC)
        self._duration_hours = duration_hours
        self._interval_seconds = 3600

        # Generate crash price pattern
        self._eth_prices = self._generate_crash_prices(
            initial=Decimal("3000"), severity=crash_severity, recovery=recovery_pct
        )
        self._usdc_prices = [Decimal("1")] * len(self._eth_prices)

    def _generate_crash_prices(
        self,
        initial: Decimal,
        severity: Decimal,
        recovery: Decimal,
    ) -> list[Decimal]:
        """Generate a crash -> trough -> recovery price series."""
        n = self._duration_hours + 1
        # Phase 1: Crash (first 30% of period)
        crash_end = int(n * 0.3)
        # Phase 2: Trough (30-50%)
        trough_end = int(n * 0.5)
        # Phase 3: Recovery (50-100%)

        trough_price = initial * (Decimal("1") - severity)
        recovery_price = trough_price + (initial - trough_price) * recovery

        prices = []
        for i in range(n):
            if i <= crash_end:
                # Linear crash
                progress = Decimal(i) / Decimal(max(crash_end, 1))
                price = initial - (initial - trough_price) * progress
            elif i <= trough_end:
                # Trough (slightly oscillating)
                offset = Decimal("20") if i % 2 == 0 else Decimal("-20")
                price = trough_price + offset
            else:
                # Recovery
                progress = Decimal(i - trough_end) / Decimal(max(n - trough_end, 1))
                price = trough_price + (recovery_price - trough_price) * progress

            prices.append(max(price, Decimal("1")))  # Floor at $1

        return prices

    async def get_price(self, token: str, timestamp: datetime) -> Decimal:
        token = token.upper()
        delta = timestamp - self._start_time
        index = int(delta.total_seconds() / self._interval_seconds)
        if token in ("WETH", "ETH"):
            series = self._eth_prices
        elif token in ("USDC", "USDT", "DAI"):
            return Decimal("1")
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
                chain=config.chains[0] if config.chains else "ethereum",
                block_number=15000000 + index * 100,
                gas_price_gwei=Decimal("100"),  # High gas during crisis
            )
            yield current, market_state
            index += 1
            current += timedelta(seconds=config.interval_seconds)

    @property
    def provider_name(self) -> str:
        return "crisis_deterministic"

    @property
    def supported_tokens(self) -> list[str]:
        return ["WETH", "USDC"]

    @property
    def supported_chains(self) -> list[str]:
        return ["ethereum", "arbitrum"]

    @property
    def min_timestamp(self) -> datetime | None:
        return self._start_time

    @property
    def max_timestamp(self) -> datetime | None:
        return self._start_time + timedelta(hours=self._duration_hours)


# =============================================================================
# Mock intents for Aave V3 lending
# =============================================================================


@dataclass
class MockSupplyIntent:
    """Mock Aave V3 supply intent."""

    intent_type: str = "SUPPLY"
    token: str = "WETH"
    amount_usd: Decimal = Decimal("5000")
    protocol: str = "aave_v3"
    apy: Decimal = Decimal("0.03")


@dataclass
class MockBorrowIntent:
    """Mock Aave V3 borrow intent."""

    intent_type: str = "BORROW"
    token: str = "USDC"
    amount_usd: Decimal = Decimal("2000")
    protocol: str = "aave_v3"
    apy: Decimal = Decimal("0.05")


@dataclass
class MockSwapIntent:
    """Mock swap intent."""

    intent_type: str = "SWAP"
    from_token: str = "USDC"
    to_token: str = "WETH"
    amount_usd: Decimal = Decimal("3000")
    protocol: str = "uniswap_v3"


class DeterministicLendingStrategy:
    """Strategy that supplies to Aave V3 then holds through the crisis."""

    def __init__(self, intents: list[Any | None], strategy_id: str = "aave_v3_crisis"):
        self._intents = intents
        self._strategy_id = strategy_id
        self._call_count = 0

    @property
    def strategy_id(self) -> str:
        return self._strategy_id

    def decide(self, market: Any) -> Any | None:
        if self._call_count < len(self._intents):
            intent = self._intents[self._call_count]
            self._call_count += 1
            return intent
        return None


# =============================================================================
# Tests: Crisis Scenario Definitions
# =============================================================================


class TestCrisisScenarioDefinitions:
    """Tests for crisis scenario data structures and registry."""

    def test_predefined_scenarios_exist(self):
        """All predefined scenarios are in the registry."""
        assert len(PREDEFINED_SCENARIOS) == 3
        assert "black_thursday" in PREDEFINED_SCENARIOS
        assert "terra_collapse" in PREDEFINED_SCENARIOS
        assert "ftx_collapse" in PREDEFINED_SCENARIOS

    def test_scenario_lookup_case_insensitive(self):
        """get_scenario_by_name handles case and hyphens."""
        assert get_scenario_by_name("BLACK_THURSDAY") is not None
        assert get_scenario_by_name("BLACK-THURSDAY") is not None
        assert get_scenario_by_name("black_thursday") is not None

    def test_scenario_duration(self):
        """Crisis scenarios have expected durations."""
        assert BLACK_THURSDAY.duration_days == 7
        assert TERRA_COLLAPSE.duration_days == 7
        assert FTX_COLLAPSE.duration_days == 8

    def test_scenario_serialization(self):
        """Scenarios round-trip through dict serialization."""
        for scenario in PREDEFINED_SCENARIOS.values():
            d = scenario.to_dict()
            restored = CrisisScenario.from_dict(d)
            assert restored.name == scenario.name
            assert restored.duration_days == scenario.duration_days


# =============================================================================
# Tests: Crisis Backtest Runner with Aave V3 Lending
# =============================================================================


class TestCrisisBacktestAaveV3:
    """Integration tests for crisis backtest runner with lending strategies."""

    @pytest.mark.asyncio
    async def test_hold_only_during_black_thursday(self) -> None:
        """Test hold-only strategy preserves capital during Black Thursday."""
        scenario = BLACK_THURSDAY
        data_provider = CrisisDataProvider(
            start_time=scenario.start_date,
            duration_hours=scenario.duration_days * 24,
            crash_severity=Decimal("0.5"),
        )

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
        )

        # Hold only -- no trades
        n_ticks = scenario.duration_days * 24 + 1
        strategy = DeterministicLendingStrategy(intents=[None] * n_ticks)

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="ethereum",
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        assert isinstance(result, CrisisBacktestResult)
        assert result.result.success, f"Backtest failed: {result.result.error}"
        assert result.scenario_name == "black_thursday"
        # Hold-only should preserve capital (all in USDC)
        assert result.result.final_capital_usd == Decimal("10000")
        assert result.result.metrics.total_trades == 0

    @pytest.mark.asyncio
    async def test_supply_and_hold_during_crash(self) -> None:
        """Test Aave V3 supply strategy during a crash scenario.

        Strategy: Supply WETH at start, hold through the crash.
        With ETH crashing, the strategy should lose value but not crash.
        """
        scenario = BLACK_THURSDAY
        data_provider = CrisisDataProvider(
            start_time=scenario.start_date,
            duration_hours=scenario.duration_days * 24,
            crash_severity=Decimal("0.4"),
            recovery_pct=Decimal("0.3"),
        )

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
        )

        # Buy ETH at start, supply to Aave, hold
        swap = MockSwapIntent(from_token="USDC", to_token="WETH", amount_usd=Decimal("8000"))
        supply = MockSupplyIntent(token="WETH", amount_usd=Decimal("8000"))
        n_ticks = scenario.duration_days * 24 + 1
        intents = [swap, supply] + [None] * (n_ticks - 2)

        strategy = DeterministicLendingStrategy(intents=intents)

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="ethereum",
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        assert result.result.success, f"Backtest failed: {result.result.error}"
        assert result.result.metrics.total_trades >= 1
        # With ETH crashing 40%, strategy should have lost value
        assert result.result.metrics.max_drawdown_pct > Decimal("0")

    @pytest.mark.asyncio
    async def test_crisis_metrics_calculated(self) -> None:
        """Test that crisis-specific metrics are populated."""
        scenario = TERRA_COLLAPSE
        data_provider = CrisisDataProvider(
            start_time=scenario.start_date,
            duration_hours=scenario.duration_days * 24,
            crash_severity=Decimal("0.3"),
        )

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
        )

        swap = MockSwapIntent(amount_usd=Decimal("5000"))
        n_ticks = scenario.duration_days * 24 + 1
        strategy = DeterministicLendingStrategy(intents=[swap] + [None] * (n_ticks - 1))

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="ethereum",
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        assert result.result.success
        assert result.scenario_name == "terra_collapse"
        assert result.scenario_duration_days == 7

        # Crisis metrics should be populated
        assert result.crisis_metrics is not None
        assert "scenario_name" in result.crisis_metrics
        assert "max_drawdown_pct" in result.crisis_metrics
        assert "recovery_pct" in result.crisis_metrics

    def test_crisis_backtest_config_serialization(self) -> None:
        """Test CrisisBacktestConfig round-trip serialization."""
        config = CrisisBacktestConfig(
            scenario=FTX_COLLAPSE,
            initial_capital_usd=Decimal("50000"),
            chain="ethereum",
            tokens=["WETH", "USDC"],
            gas_price_gwei=Decimal("100"),
        )

        d = config.to_dict()
        restored = CrisisBacktestConfig.from_dict(d)

        assert restored.scenario.name == "ftx_collapse"
        assert restored.initial_capital_usd == Decimal("50000")
        assert restored.chain == "ethereum"
        assert restored.gas_price_gwei == Decimal("100")

    @pytest.mark.asyncio
    async def test_crisis_result_summary(self) -> None:
        """Test CrisisBacktestResult.summary() produces readable output."""
        scenario = FTX_COLLAPSE
        data_provider = CrisisDataProvider(
            start_time=scenario.start_date,
            duration_hours=scenario.duration_days * 24,
            crash_severity=Decimal("0.25"),
        )

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
        )

        n_ticks = scenario.duration_days * 24 + 1
        strategy = DeterministicLendingStrategy(intents=[None] * n_ticks)

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=scenario,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="ethereum",
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        summary = result.summary()
        assert "ftx_collapse" in summary
        assert "Total Return" in summary
        assert "Max Drawdown" in summary
        assert "Sharpe Ratio" in summary

    @pytest.mark.asyncio
    async def test_multiple_crisis_backtests(self) -> None:
        """Test running backtest across all three predefined crisis scenarios."""
        scenarios = [BLACK_THURSDAY, TERRA_COLLAPSE, FTX_COLLAPSE]

        # Use a fresh strategy instance for each scenario (same intent pattern)
        results = []
        for scenario in scenarios:
            data_provider = CrisisDataProvider(
                start_time=scenario.start_date,
                duration_hours=scenario.duration_days * 24,
                crash_severity=Decimal("0.3"),
            )

            backtester = PnLBacktester(
                data_provider=data_provider,
                fee_models={"default": DefaultFeeModel()},
                slippage_models={"default": DefaultSlippageModel()},
            )

            n_ticks = scenario.duration_days * 24 + 1
            strategy = DeterministicLendingStrategy(intents=[None] * n_ticks)

            result = await run_crisis_backtest(
                strategy=strategy,
                scenario=scenario,
                backtester=backtester,
                initial_capital_usd=Decimal("10000"),
                chain="ethereum",
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
    async def test_custom_crisis_scenario(self) -> None:
        """Test with a custom crisis scenario (SVB collapse)."""
        svb_collapse = CrisisScenario(
            name="svb_collapse",
            start_date=datetime(2023, 3, 10, tzinfo=UTC),
            end_date=datetime(2023, 3, 15, tzinfo=UTC),
            description="Silicon Valley Bank collapse caused USDC depeg and crypto market stress",
        )

        data_provider = CrisisDataProvider(
            start_time=svb_collapse.start_date,
            duration_hours=svb_collapse.duration_days * 24,
            crash_severity=Decimal("0.15"),
            recovery_pct=Decimal("0.8"),
        )

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
        )

        n_ticks = svb_collapse.duration_days * 24 + 1
        strategy = DeterministicLendingStrategy(intents=[None] * n_ticks)

        result = await run_crisis_backtest(
            strategy=strategy,
            scenario=svb_collapse,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="ethereum",
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        assert result.result.success
        assert result.scenario_name == "svb_collapse"
        assert result.scenario_duration_days == 5

    @pytest.mark.asyncio
    async def test_run_multiple_crisis_backtests_helper(self) -> None:
        """Test the run_multiple_crisis_backtests batch helper function."""
        scenarios = [BLACK_THURSDAY, TERRA_COLLAPSE]

        # Use the longest scenario duration to size the strategy
        max_ticks = max(s.duration_days for s in scenarios) * 24 + 1
        strategy = DeterministicLendingStrategy(intents=[None] * max_ticks)

        # Use Black Thursday's data provider (covers both 7-day scenarios)
        data_provider = CrisisDataProvider(
            start_time=BLACK_THURSDAY.start_date,
            duration_hours=max(s.duration_days for s in scenarios) * 24,
            crash_severity=Decimal("0.3"),
        )

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
        )

        results = await run_multiple_crisis_backtests(
            strategy=strategy,
            scenarios=scenarios,
            backtester=backtester,
            initial_capital_usd=Decimal("10000"),
            chain="ethereum",
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        assert len(results) == 2
        assert all(r.result.success for r in results)
        assert results[0].scenario_name == "black_thursday"
        assert results[1].scenario_name == "terra_collapse"
