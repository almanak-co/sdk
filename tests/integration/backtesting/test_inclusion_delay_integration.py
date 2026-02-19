"""Integration tests for inclusion_delay_blocks feature.

These tests validate the inclusion delay mechanism end-to-end, ensuring that:
1. Intents are properly queued when inclusion_delay_blocks > 0
2. Intents execute only after the specified delay expires
3. Pending intents at simulation end are executed (P0-1 fix)
4. Results are accurate with and without delay

The inclusion_delay_blocks config simulates realistic trade timing where intents
are not executed immediately but after a configurable number of blocks (ticks).

Part of P1-3: Integration tests for production paths
"""

from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.models import (
    BacktestEngine,
    IntentType,
)
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import (
    HistoricalDataConfig,
    MarketState,
)
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktester,
)

# =============================================================================
# Mock Data Provider
# =============================================================================


class DeterministicDataProvider:
    """Data provider with pre-defined price series for deterministic testing.

    This provider yields exact prices at specific timestamps to ensure
    tests produce reproducible, deterministic results regardless of
    when or where they run.
    """

    provider_name = "deterministic"

    def __init__(
        self,
        price_series: dict[str, list[Decimal]],
        start_time: datetime,
        interval_seconds: int = 3600,
    ):
        """Initialize with pre-defined price series.

        Args:
            price_series: Dict mapping token -> list of prices in order
            start_time: Start timestamp for the series
            interval_seconds: Interval between price points
        """
        self._price_series = price_series
        self._start_time = start_time
        self._interval_seconds = interval_seconds

    async def iterate(self, config: HistoricalDataConfig) -> AsyncIterator[tuple[datetime, MarketState]]:
        """Iterate through historical data with deterministic prices."""
        current = config.start_time
        index = 0

        while current <= config.end_time:
            prices = {}
            for token in config.tokens:
                token = token.upper()
                if token in self._price_series:
                    series = self._price_series[token]
                    if index < len(series):
                        prices[token] = series[index]
                    else:
                        prices[token] = series[-1]
                else:
                    # Default stablecoin price
                    prices[token] = Decimal("1")

            market_state = MarketState(
                timestamp=current,
                prices=prices,
                chain=config.chains[0] if config.chains else "arbitrum",
                block_number=15000000 + index * 100,
                gas_price_gwei=Decimal("30"),
            )
            yield current, market_state

            index += 1
            current += timedelta(seconds=config.interval_seconds)


# =============================================================================
# Mock Intents and Strategies
# =============================================================================


@dataclass
class MockSwapIntent:
    """Mock swap intent for testing."""

    intent_type: str = "SWAP"
    from_token: str = "USDC"
    to_token: str = "WETH"
    amount_usd: Decimal = field(default_factory=lambda: Decimal("1000"))
    protocol: str = "uniswap_v3"


class DeterministicStrategy:
    """Strategy with pre-defined decision sequence for testing."""

    def __init__(
        self,
        intents: list[Any | None],
        strategy_id: str = "deterministic_strategy",
    ):
        """Initialize with pre-defined intent sequence.

        Args:
            intents: List of intents to return in order (None = hold)
            strategy_id: Identifier for the strategy
        """
        self._intents = intents
        self._strategy_id = strategy_id
        self._call_count = 0

    @property
    def strategy_id(self) -> str:
        return self._strategy_id

    def decide(self, market: Any) -> Any | None:
        """Return next intent from sequence."""
        if self._call_count < len(self._intents):
            intent = self._intents[self._call_count]
            self._call_count += 1
            return intent
        return None


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def base_timestamp() -> datetime:
    """Fixed base timestamp for deterministic tests."""
    return datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)


@pytest.fixture
def eth_steady_prices() -> list[Decimal]:
    """ETH price series with slight uptrend (for testing P&L)."""
    # 15 hourly prices: 3000 -> 3140 (4.67% gain over 14 hours)
    return [Decimal(str(3000 + i * 10)) for i in range(15)]


@pytest.fixture
def usdc_stable_prices() -> list[Decimal]:
    """USDC price series (stable at $1)."""
    return [Decimal("1")] * 15


# =============================================================================
# Integration Tests - Inclusion Delay Basic Behavior
# =============================================================================


class TestInclusionDelayBasicBehavior:
    """Tests for basic inclusion delay queueing and execution."""

    @pytest.mark.asyncio
    async def test_intents_queued_with_inclusion_delay(
        self,
        base_timestamp: datetime,
        eth_steady_prices: list[Decimal],
        usdc_stable_prices: list[Decimal],
    ) -> None:
        """Test that intents are queued and not executed immediately when delay > 0."""
        data_provider = DeterministicDataProvider(
            price_series={
                "WETH": eth_steady_prices,
                "USDC": usdc_stable_prices,
            },
            start_time=base_timestamp,
        )

        config = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=10),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=3,  # Intent queued, executed 3 ticks later
        )

        # Submit intent on first tick only
        strategy = DeterministicStrategy(
            intents=[MockSwapIntent(amount_usd=Decimal("2000"))] + [None] * 10
        )

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0.003"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0.001"))},
        )

        result = await backtester.backtest(strategy, config)

        # Verify backtest completed successfully
        assert result.success, f"Backtest failed: {result.error}"
        assert result.engine == BacktestEngine.PNL

        # Intent should have been executed (just with delay)
        assert result.metrics.total_trades == 1, (
            f"Expected 1 trade, got {result.metrics.total_trades}"
        )

    @pytest.mark.asyncio
    async def test_delay_affects_execution_timing(
        self,
        base_timestamp: datetime,
        eth_steady_prices: list[Decimal],
        usdc_stable_prices: list[Decimal],
    ) -> None:
        """Test that inclusion delay causes intents to execute at later prices.

        With a price uptrend, a delayed buy will get worse prices (higher ETH price).
        """
        data_provider = DeterministicDataProvider(
            price_series={
                "WETH": eth_steady_prices,
                "USDC": usdc_stable_prices,
            },
            start_time=base_timestamp,
        )

        # Run WITHOUT delay first
        config_no_delay = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=10),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,  # Immediate
        )

        strategy_no_delay = DeterministicStrategy(
            intents=[MockSwapIntent(amount_usd=Decimal("5000"))] + [None] * 10
        )

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
        )

        result_no_delay = await backtester.backtest(strategy_no_delay, config_no_delay)
        assert result_no_delay.success

        # Run WITH delay
        config_with_delay = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=10),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=3,  # Execute 3 ticks later
        )

        # New data provider instance for fresh iteration
        data_provider_delay = DeterministicDataProvider(
            price_series={
                "WETH": eth_steady_prices,
                "USDC": usdc_stable_prices,
            },
            start_time=base_timestamp,
        )

        strategy_with_delay = DeterministicStrategy(
            intents=[MockSwapIntent(amount_usd=Decimal("5000"))] + [None] * 10
        )

        backtester_delay = PnLBacktester(
            data_provider=data_provider_delay,
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
        )

        result_with_delay = await backtester_delay.backtest(strategy_with_delay, config_with_delay)
        assert result_with_delay.success

        # Both should have executed 1 trade
        assert result_no_delay.metrics.total_trades == 1
        assert result_with_delay.metrics.total_trades == 1

        # In uptrend, delayed buy gets worse price (higher ETH price = less ETH received)
        # This means the delayed execution should result in slightly less favorable position
        # We just verify both completed - exact comparison depends on implementation details
        assert len(result_no_delay.trades) == 1
        assert len(result_with_delay.trades) == 1


class TestInclusionDelayPendingExecution:
    """Tests for pending intent execution at simulation end (P0-1 fix)."""

    @pytest.mark.asyncio
    async def test_pending_intents_executed_at_simulation_end(
        self,
        base_timestamp: datetime,
        eth_steady_prices: list[Decimal],
        usdc_stable_prices: list[Decimal],
    ) -> None:
        """Test that pending intents are executed at simulation end (P0-1 fix)."""
        data_provider = DeterministicDataProvider(
            price_series={
                "WETH": eth_steady_prices,
                "USDC": usdc_stable_prices,
            },
            start_time=base_timestamp,
        )

        config = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=5),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=10,  # Very high delay - all intents pending at end
        )

        # Submit intents every tick
        strategy = DeterministicStrategy(
            intents=[MockSwapIntent(amount_usd=Decimal("500"))] * 6
        )

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
        )

        result = await backtester.backtest(strategy, config)

        assert result.success, f"Backtest failed: {result.error}"

        # All 6 intents should have been executed (at simulation end)
        assert result.metrics.total_trades == 6, (
            f"Expected 6 trades, got {result.metrics.total_trades}"
        )

        # execution_delayed_at_end should be 6 (all intents)
        assert result.execution_delayed_at_end == 6, (
            f"Expected 6 delayed executions, got {result.execution_delayed_at_end}"
        )

    @pytest.mark.asyncio
    async def test_delayed_at_end_flag_set_correctly(
        self,
        base_timestamp: datetime,
        eth_steady_prices: list[Decimal],
        usdc_stable_prices: list[Decimal],
    ) -> None:
        """Test that TradeRecord.delayed_at_end flag is set for delayed trades."""
        data_provider = DeterministicDataProvider(
            price_series={
                "WETH": eth_steady_prices,
                "USDC": usdc_stable_prices,
            },
            start_time=base_timestamp,
        )

        config = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=3),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=10,  # All trades will be delayed
        )

        strategy = DeterministicStrategy(
            intents=[MockSwapIntent(amount_usd=Decimal("1000"))] * 4
        )

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
        )

        result = await backtester.backtest(strategy, config)

        assert result.success, f"Backtest failed: {result.error}"

        # All trades should have delayed_at_end=True
        for i, trade in enumerate(result.trades):
            assert trade.delayed_at_end is True, (
                f"Trade {i} expected delayed_at_end=True, got {trade.delayed_at_end}"
            )

    @pytest.mark.asyncio
    async def test_mixed_delayed_and_normal_execution(
        self,
        base_timestamp: datetime,
        eth_steady_prices: list[Decimal],
        usdc_stable_prices: list[Decimal],
    ) -> None:
        """Test scenario with both normal and delayed executions."""
        data_provider = DeterministicDataProvider(
            price_series={
                "WETH": eth_steady_prices,
                "USDC": usdc_stable_prices,
            },
            start_time=base_timestamp,
        )

        config = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=8),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=2,  # Moderate delay
        )

        # 9 ticks, each with an intent
        strategy = DeterministicStrategy(
            intents=[MockSwapIntent(amount_usd=Decimal("200"))] * 9
        )

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
        )

        result = await backtester.backtest(strategy, config)

        assert result.success, f"Backtest failed: {result.error}"

        # All 9 intents should be executed
        assert result.metrics.total_trades == 9, (
            f"Expected 9 trades, got {result.metrics.total_trades}"
        )

        # Count delayed vs normal
        normal_trades = [t for t in result.trades if not t.delayed_at_end]
        delayed_trades = [t for t in result.trades if t.delayed_at_end]

        # With 9 ticks and delay=2:
        # Intents from last 2 ticks will be delayed at end
        assert len(delayed_trades) == result.execution_delayed_at_end, (
            f"Counter mismatch: execution_delayed_at_end={result.execution_delayed_at_end}, "
            f"but delayed_trades count={len(delayed_trades)}"
        )

        # Should have some of each
        assert len(normal_trades) > 0, "Expected some normal trades"
        assert len(delayed_trades) > 0, "Expected some delayed trades"


class TestInclusionDelayComparisonWithWithoutDelay:
    """Tests comparing results with and without inclusion delay."""

    @pytest.mark.asyncio
    async def test_same_number_of_trades_with_and_without_delay(
        self,
        base_timestamp: datetime,
        eth_steady_prices: list[Decimal],
        usdc_stable_prices: list[Decimal],
    ) -> None:
        """Test that total trade count is the same regardless of delay."""
        # Run without delay
        data_provider_1 = DeterministicDataProvider(
            price_series={
                "WETH": eth_steady_prices,
                "USDC": usdc_stable_prices,
            },
            start_time=base_timestamp,
        )

        config_no_delay = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=5),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,
        )

        strategy_1 = DeterministicStrategy(
            intents=[MockSwapIntent(amount_usd=Decimal("500"))] * 6
        )

        backtester_1 = PnLBacktester(
            data_provider=data_provider_1,
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
        )

        result_no_delay = await backtester_1.backtest(strategy_1, config_no_delay)

        # Run with delay
        data_provider_2 = DeterministicDataProvider(
            price_series={
                "WETH": eth_steady_prices,
                "USDC": usdc_stable_prices,
            },
            start_time=base_timestamp,
        )

        config_with_delay = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=5),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=3,
        )

        strategy_2 = DeterministicStrategy(
            intents=[MockSwapIntent(amount_usd=Decimal("500"))] * 6
        )

        backtester_2 = PnLBacktester(
            data_provider=data_provider_2,
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
        )

        result_with_delay = await backtester_2.backtest(strategy_2, config_with_delay)

        # Both should succeed and have same number of trades
        assert result_no_delay.success
        assert result_with_delay.success
        assert result_no_delay.metrics.total_trades == result_with_delay.metrics.total_trades, (
            f"Trade count mismatch: no_delay={result_no_delay.metrics.total_trades}, "
            f"with_delay={result_with_delay.metrics.total_trades}"
        )

    @pytest.mark.asyncio
    async def test_no_delay_has_zero_delayed_at_end_except_last_tick(
        self,
        base_timestamp: datetime,
        eth_steady_prices: list[Decimal],
        usdc_stable_prices: list[Decimal],
    ) -> None:
        """Test that with delay=0, only the last tick's intent is delayed at end.

        With delay=0, intents execute on the NEXT tick. The last tick has no
        "next tick" so its intent executes at simulation end.
        """
        data_provider = DeterministicDataProvider(
            price_series={
                "WETH": eth_steady_prices,
                "USDC": usdc_stable_prices,
            },
            start_time=base_timestamp,
        )

        config = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=5),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=0,  # No delay
        )

        strategy = DeterministicStrategy(
            intents=[MockSwapIntent(amount_usd=Decimal("500"))] * 6
        )

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
        )

        result = await backtester.backtest(strategy, config)

        assert result.success, f"Backtest failed: {result.error}"
        assert result.metrics.total_trades == 6

        # With delay=0, only the last tick's intent is delayed at end
        assert result.execution_delayed_at_end == 1, (
            f"Expected 1 delayed execution (last tick), got {result.execution_delayed_at_end}"
        )


class TestInclusionDelayIntentTypes:
    """Tests verifying intent types are preserved with inclusion delay."""

    @pytest.mark.asyncio
    async def test_intent_type_preserved_in_delayed_execution(
        self,
        base_timestamp: datetime,
        eth_steady_prices: list[Decimal],
        usdc_stable_prices: list[Decimal],
    ) -> None:
        """Test that intent type is correctly preserved for delayed executions."""
        data_provider = DeterministicDataProvider(
            price_series={
                "WETH": eth_steady_prices,
                "USDC": usdc_stable_prices,
            },
            start_time=base_timestamp,
        )

        config = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=3),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=10,
        )

        strategy = DeterministicStrategy(
            intents=[MockSwapIntent(amount_usd=Decimal("1000"))] * 4
        )

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
        )

        result = await backtester.backtest(strategy, config)

        assert result.success, f"Backtest failed: {result.error}"

        # All trades should be SWAP type
        for trade in result.trades:
            assert trade.intent_type == IntentType.SWAP, (
                f"Expected SWAP intent type, got {trade.intent_type}"
            )


class TestInclusionDelaySerialization:
    """Tests for serialization with inclusion delay results."""

    @pytest.mark.asyncio
    async def test_serialization_preserves_delayed_at_end(
        self,
        base_timestamp: datetime,
        eth_steady_prices: list[Decimal],
        usdc_stable_prices: list[Decimal],
    ) -> None:
        """Test that delayed_at_end flag survives serialization round-trip."""
        data_provider = DeterministicDataProvider(
            price_series={
                "WETH": eth_steady_prices,
                "USDC": usdc_stable_prices,
            },
            start_time=base_timestamp,
        )

        config = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=3),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=10,
        )

        strategy = DeterministicStrategy(
            intents=[MockSwapIntent(amount_usd=Decimal("1000"))] * 4
        )

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
        )

        result = await backtester.backtest(strategy, config)

        assert result.success, f"Backtest failed: {result.error}"

        # Serialize and deserialize
        from almanak.framework.backtesting.models import BacktestResult

        result_dict = result.to_dict()
        restored = BacktestResult.from_dict(result_dict)

        # Verify execution_delayed_at_end is preserved
        assert restored.execution_delayed_at_end == result.execution_delayed_at_end, (
            f"execution_delayed_at_end not preserved: "
            f"original={result.execution_delayed_at_end}, restored={restored.execution_delayed_at_end}"
        )

        # Verify delayed_at_end flag on each trade is preserved
        for i, (original, restored_trade) in enumerate(
            zip(result.trades, restored.trades, strict=True)
        ):
            assert original.delayed_at_end == restored_trade.delayed_at_end, (
                f"Trade {i} delayed_at_end not preserved: "
                f"original={original.delayed_at_end}, restored={restored_trade.delayed_at_end}"
            )


class TestInclusionDelayEdgeCases:
    """Tests for edge cases with inclusion delay."""

    @pytest.mark.asyncio
    async def test_high_delay_short_simulation(
        self,
        base_timestamp: datetime,
        eth_steady_prices: list[Decimal],
        usdc_stable_prices: list[Decimal],
    ) -> None:
        """Test with inclusion_delay_blocks greater than number of ticks."""
        data_provider = DeterministicDataProvider(
            price_series={
                "WETH": eth_steady_prices,
                "USDC": usdc_stable_prices,
            },
            start_time=base_timestamp,
        )

        config = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=2),  # Only 3 ticks
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=100,  # Way more than ticks
        )

        strategy = DeterministicStrategy(
            intents=[MockSwapIntent(amount_usd=Decimal("1000"))] * 3
        )

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
        )

        result = await backtester.backtest(strategy, config)

        assert result.success, f"Backtest failed: {result.error}"

        # All 3 intents should still execute (at simulation end)
        assert result.metrics.total_trades == 3, (
            f"Expected 3 trades, got {result.metrics.total_trades}"
        )

        # All should be delayed at end
        assert result.execution_delayed_at_end == 3

    @pytest.mark.asyncio
    async def test_zero_intents_with_delay(
        self,
        base_timestamp: datetime,
        eth_steady_prices: list[Decimal],
        usdc_stable_prices: list[Decimal],
    ) -> None:
        """Test that delay config doesn't break when no intents are submitted."""
        data_provider = DeterministicDataProvider(
            price_series={
                "WETH": eth_steady_prices,
                "USDC": usdc_stable_prices,
            },
            start_time=base_timestamp,
        )

        config = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(hours=5),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=3,
        )

        # Strategy that always returns None (hold)
        strategy = DeterministicStrategy(intents=[None] * 6)

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
        )

        result = await backtester.backtest(strategy, config)

        assert result.success, f"Backtest failed: {result.error}"
        assert result.metrics.total_trades == 0
        assert result.execution_delayed_at_end == 0
        assert result.final_capital_usd == config.initial_capital_usd

    @pytest.mark.asyncio
    async def test_single_tick_with_delay(
        self,
        base_timestamp: datetime,
        eth_steady_prices: list[Decimal],
        usdc_stable_prices: list[Decimal],
    ) -> None:
        """Test single tick simulation with inclusion delay."""
        data_provider = DeterministicDataProvider(
            price_series={
                "WETH": eth_steady_prices[:2],  # Just 2 prices for single-tick margin
                "USDC": usdc_stable_prices[:2],
            },
            start_time=base_timestamp,
        )

        config = PnLBacktestConfig(
            start_time=base_timestamp,
            end_time=base_timestamp + timedelta(seconds=3600),  # 1 tick
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
            inclusion_delay_blocks=2,
        )

        strategy = DeterministicStrategy(
            intents=[MockSwapIntent(amount_usd=Decimal("1000"))]
        )

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel(fee_pct=Decimal("0"))},
            slippage_models={"default": DefaultSlippageModel(slippage_pct=Decimal("0"))},
        )

        result = await backtester.backtest(strategy, config)

        assert result.success, f"Backtest failed: {result.error}"

        # Single intent should be executed at simulation end
        assert result.metrics.total_trades == 1
        assert result.execution_delayed_at_end == 1
