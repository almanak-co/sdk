"""Tests for pending intent execution at simulation end.

Tests verify that when inclusion_delay_blocks > 0:
1. Intents submitted near end of simulation are not dropped
2. Pending intents are executed with the last valid market state
3. The execution_delayed_at_end counter accurately tracks these executions
4. TradeRecord.delayed_at_end flag is set correctly for such trades
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.models import IntentType
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktester,
)


class MockDataProviderWithTicks:
    """Mock data provider that yields a configurable number of market states."""

    provider_name = "mock_ticks"

    def __init__(self, num_ticks: int = 5, start_time: datetime | None = None):
        self.num_ticks = num_ticks
        self.start_time = start_time or datetime.now(UTC)

    async def iterate(self, config: Any):
        """Yield (timestamp, market_state) tuples for each tick."""
        for i in range(self.num_ticks):
            timestamp = self.start_time + timedelta(hours=i)
            # Use the real MarketState class with properly formatted prices
            eth_price = Decimal("3000") + Decimal(i * 10)
            market_state = MarketState(
                timestamp=timestamp,
                prices={
                    "ETH": eth_price,
                    "WETH": eth_price,
                    "USDC": Decimal("1"),
                },
                chain="arbitrum",
                block_number=1000 + i,
            )
            yield timestamp, market_state


# =============================================================================
# Mock Strategy
# =============================================================================


@dataclass
class MockSwapIntent:
    """Mock swap intent for testing."""

    intent_type: str = "SWAP"
    from_token: str = "USDC"
    to_token: str = "ETH"
    amount: Decimal = field(default_factory=lambda: Decimal("1000"))
    protocol: str = "uniswap_v3"


class MockStrategy:
    """Mock strategy that generates intents on each tick."""

    def __init__(
        self,
        strategy_id: str = "test_pending_intent_strategy",
        intents_to_return: list[Any] | None = None,
    ):
        self._strategy_id = strategy_id
        self.decide_call_count = 0
        # If intents_to_return is provided, we cycle through them
        # Otherwise, return a swap intent on every call
        self._intents_to_return = intents_to_return

    @property
    def strategy_id(self) -> str:
        return self._strategy_id

    def decide(self, market: Any) -> Any:
        """Return a swap intent on each call."""
        self.decide_call_count += 1

        if self._intents_to_return is not None:
            # Return from list if available, otherwise None
            idx = self.decide_call_count - 1
            if idx < len(self._intents_to_return):
                return self._intents_to_return[idx]
            return None

        # Default: return a swap intent every time
        return MockSwapIntent(
            from_token="USDC",
            to_token="ETH",
            amount=Decimal("100"),
        )

    def get_metadata(self) -> None:
        return None


# =============================================================================
# Tests: Pending Intent Execution at Simulation End
# =============================================================================


@pytest.mark.asyncio
async def test_pending_intents_executed_at_simulation_end():
    """Test that pending intents are executed at simulation end when inclusion_delay > 0."""
    start_time = datetime.now(UTC)
    end_time = start_time + timedelta(hours=5)

    # Create data provider with 5 ticks
    data_provider = MockDataProviderWithTicks(num_ticks=5, start_time=start_time)

    # Create backtester with inclusion delay
    backtester = PnLBacktester(
        data_provider=data_provider,
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )

    # Create config with inclusion_delay_blocks = 3
    # This means intents need to wait 3 ticks before execution
    # With 5 ticks total, intents from ticks 3, 4, 5 will be pending at end
    config = PnLBacktestConfig(
        start_time=start_time,
        end_time=end_time,
        initial_capital_usd=Decimal("10000"),
        tokens=["WETH", "USDC"],
        inclusion_delay_blocks=3,
    )

    # Create strategy that returns a swap intent on every tick
    strategy = MockStrategy()

    # Run backtest
    result = await backtester.backtest(strategy, config)

    # Verify backtest completed successfully
    assert result.error is None, f"Backtest failed: {result.error}"

    # With 5 ticks and inclusion_delay_blocks=3:
    # - Tick 1: Intent queued (blocks_remaining=3)
    # - Tick 2: Intent from tick 1 (blocks_remaining=2), new intent queued
    # - Tick 3: Intent from tick 1 (blocks_remaining=1), tick 2 (remaining=2), new queued
    # - Tick 4: Intent from tick 1 executed (remaining=0), tick 2 (remaining=1), tick 3 (remaining=2), new queued
    # - Tick 5: Intent from tick 2 executed (remaining=0), others decremented, new queued
    # End of simulation: Intents from ticks 3, 4, 5 still pending -> executed at end
    # So we expect execution_delayed_at_end > 0
    assert result.execution_delayed_at_end > 0, (
        f"Expected pending intents executed at end, but got {result.execution_delayed_at_end}"
    )


@pytest.mark.asyncio
async def test_execution_delayed_at_end_counter_accuracy():
    """Test that execution_delayed_at_end counter accurately tracks delayed executions."""
    start_time = datetime.now(UTC)
    end_time = start_time + timedelta(hours=3)

    # Create data provider with 3 ticks - all intents will be pending at end
    data_provider = MockDataProviderWithTicks(num_ticks=3, start_time=start_time)

    backtester = PnLBacktester(
        data_provider=data_provider,
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )

    # With inclusion_delay_blocks=5 and only 3 ticks,
    # ALL intents will still be pending at simulation end
    config = PnLBacktestConfig(
        start_time=start_time,
        end_time=end_time,
        initial_capital_usd=Decimal("10000"),
        tokens=["WETH", "USDC"],
        inclusion_delay_blocks=5,  # More than num_ticks
    )

    strategy = MockStrategy()
    result = await backtester.backtest(strategy, config)

    assert result.error is None, f"Backtest failed: {result.error}"

    # All 3 intents should be executed at simulation end
    assert result.execution_delayed_at_end == 3, (
        f"Expected 3 intents executed at end, got {result.execution_delayed_at_end}"
    )


@pytest.mark.asyncio
async def test_no_intents_dropped_with_inclusion_delay():
    """Test that no intents are dropped when using inclusion delay."""
    start_time = datetime.now(UTC)
    end_time = start_time + timedelta(hours=5)

    data_provider = MockDataProviderWithTicks(num_ticks=5, start_time=start_time)

    backtester = PnLBacktester(
        data_provider=data_provider,
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )

    config = PnLBacktestConfig(
        start_time=start_time,
        end_time=end_time,
        initial_capital_usd=Decimal("10000"),
        tokens=["WETH", "USDC"],
        inclusion_delay_blocks=2,
    )

    strategy = MockStrategy()
    result = await backtester.backtest(strategy, config)

    assert result.error is None, f"Backtest failed: {result.error}"

    # Total trades should equal the number of decide() calls
    # (5 ticks = 5 intents, all should be executed eventually)
    total_trades = len(result.trades)

    assert total_trades == 5, (
        f"Expected 5 trades (one per tick), but got {total_trades}. Intents may have been dropped."
    )


@pytest.mark.asyncio
async def test_delayed_at_end_flag_set_on_trade_records():
    """Test that TradeRecord.delayed_at_end flag is correctly set for delayed trades."""
    start_time = datetime.now(UTC)
    end_time = start_time + timedelta(hours=3)

    # Only 3 ticks with high delay means all executed at end
    data_provider = MockDataProviderWithTicks(num_ticks=3, start_time=start_time)

    backtester = PnLBacktester(
        data_provider=data_provider,
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )

    config = PnLBacktestConfig(
        start_time=start_time,
        end_time=end_time,
        initial_capital_usd=Decimal("10000"),
        tokens=["WETH", "USDC"],
        inclusion_delay_blocks=10,  # Very high delay
    )

    strategy = MockStrategy()
    result = await backtester.backtest(strategy, config)

    assert result.error is None, f"Backtest failed: {result.error}"

    # All trades should have delayed_at_end=True
    delayed_trades = [t for t in result.trades if t.delayed_at_end]
    assert len(delayed_trades) == len(result.trades), (
        f"Expected all {len(result.trades)} trades to have delayed_at_end=True, but only {len(delayed_trades)} did."
    )


@pytest.mark.asyncio
async def test_mixed_delayed_and_normal_execution():
    """Test scenario with both normal and delayed executions."""
    start_time = datetime.now(UTC)
    end_time = start_time + timedelta(hours=6)

    # 6 ticks with delay of 2
    data_provider = MockDataProviderWithTicks(num_ticks=6, start_time=start_time)

    backtester = PnLBacktester(
        data_provider=data_provider,
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )

    config = PnLBacktestConfig(
        start_time=start_time,
        end_time=end_time,
        initial_capital_usd=Decimal("10000"),
        tokens=["WETH", "USDC"],
        inclusion_delay_blocks=2,
    )

    strategy = MockStrategy()
    result = await backtester.backtest(strategy, config)

    assert result.error is None, f"Backtest failed: {result.error}"

    # Count trades executed normally vs at end
    normal_trades = [t for t in result.trades if not t.delayed_at_end]
    delayed_trades = [t for t in result.trades if t.delayed_at_end]

    # With 6 ticks and delay=2:
    # Intent queued at tick: 1, 2, 3, 4, 5, 6
    # Executed at tick:      3, 4, 5, 6, end, end
    # So 4 normal + 2 delayed = 6 total
    assert len(result.trades) == 6, f"Expected 6 total trades, got {len(result.trades)}"
    assert result.execution_delayed_at_end == len(delayed_trades), (
        f"Counter mismatch: execution_delayed_at_end={result.execution_delayed_at_end}, "
        f"but delayed_trades count={len(delayed_trades)}"
    )

    # Verify we have some of each type
    assert len(normal_trades) > 0, "Expected some normal (non-delayed) trades"
    assert len(delayed_trades) > 0, "Expected some delayed trades"


@pytest.mark.asyncio
async def test_minimal_delayed_execution_with_zero_delay():
    """Test that with inclusion_delay_blocks=0, only the last tick's intent is delayed.

    With delay=0, intents are queued with blocks_remaining=0. They execute in the
    NEXT tick's _process_pending_intents call. The last tick's intent has no
    "next tick" so it executes at simulation end.

    Sequence with 5 ticks and delay=0:
    - Tick 1: Queue intent (blocks=0)
    - Tick 2: Execute tick 1's intent (blocks=0), queue new
    - Tick 3: Execute tick 2's intent, queue new
    - Tick 4: Execute tick 3's intent, queue new
    - Tick 5: Execute tick 4's intent, queue new
    - End: Execute tick 5's intent (delayed_at_end=True)

    Result: 4 normal executions + 1 delayed at end = 5 total trades
    """
    start_time = datetime.now(UTC)
    end_time = start_time + timedelta(hours=5)

    data_provider = MockDataProviderWithTicks(num_ticks=5, start_time=start_time)

    backtester = PnLBacktester(
        data_provider=data_provider,
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )

    config = PnLBacktestConfig(
        start_time=start_time,
        end_time=end_time,
        initial_capital_usd=Decimal("10000"),
        tokens=["WETH", "USDC"],
        inclusion_delay_blocks=0,  # No delay
    )

    strategy = MockStrategy()
    result = await backtester.backtest(strategy, config)

    assert result.error is None, f"Backtest failed: {result.error}"

    # With delay=0, only the last tick's intent is delayed at end
    # (because there's no subsequent tick to process it)
    assert result.execution_delayed_at_end == 1, (
        f"Expected 1 delayed execution (last tick's intent), got {result.execution_delayed_at_end}"
    )

    # Count trades by type
    normal_trades = [t for t in result.trades if not t.delayed_at_end]
    delayed_trades = [t for t in result.trades if t.delayed_at_end]

    assert len(normal_trades) == 4, f"Expected 4 normal trades, got {len(normal_trades)}"
    assert len(delayed_trades) == 1, f"Expected 1 delayed trade, got {len(delayed_trades)}"
    assert len(result.trades) == 5, f"Expected 5 total trades, got {len(result.trades)}"


@pytest.mark.asyncio
async def test_serialization_preserves_delayed_at_end():
    """Test that delayed_at_end flag survives serialization round-trip."""
    start_time = datetime.now(UTC)
    end_time = start_time + timedelta(hours=3)

    data_provider = MockDataProviderWithTicks(num_ticks=3, start_time=start_time)

    backtester = PnLBacktester(
        data_provider=data_provider,
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )

    config = PnLBacktestConfig(
        start_time=start_time,
        end_time=end_time,
        initial_capital_usd=Decimal("10000"),
        tokens=["WETH", "USDC"],
        inclusion_delay_blocks=10,
    )

    strategy = MockStrategy()
    result = await backtester.backtest(strategy, config)

    assert result.error is None, f"Backtest failed: {result.error}"

    # Serialize and deserialize
    from almanak.framework.backtesting.models import BacktestResult

    result_dict = result.to_dict()
    restored = BacktestResult.from_dict(result_dict)

    # Verify delayed_at_end is preserved
    assert restored.execution_delayed_at_end == result.execution_delayed_at_end, (
        f"execution_delayed_at_end not preserved: "
        f"original={result.execution_delayed_at_end}, restored={restored.execution_delayed_at_end}"
    )

    for i, (original, restored_trade) in enumerate(zip(result.trades, restored.trades, strict=True)):
        assert original.delayed_at_end == restored_trade.delayed_at_end, (
            f"Trade {i} delayed_at_end not preserved: "
            f"original={original.delayed_at_end}, restored={restored_trade.delayed_at_end}"
        )


@pytest.mark.asyncio
async def test_intent_type_preserved_in_delayed_execution():
    """Test that intent type is correctly preserved for delayed executions."""
    start_time = datetime.now(UTC)
    end_time = start_time + timedelta(hours=2)

    data_provider = MockDataProviderWithTicks(num_ticks=2, start_time=start_time)

    backtester = PnLBacktester(
        data_provider=data_provider,
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )

    config = PnLBacktestConfig(
        start_time=start_time,
        end_time=end_time,
        initial_capital_usd=Decimal("10000"),
        tokens=["WETH", "USDC"],
        inclusion_delay_blocks=5,
    )

    strategy = MockStrategy()
    result = await backtester.backtest(strategy, config)

    assert result.error is None, f"Backtest failed: {result.error}"

    # All trades should be SWAP type (from our mock intent)
    for trade in result.trades:
        assert trade.intent_type == IntentType.SWAP, f"Expected SWAP intent type, got {trade.intent_type}"


@pytest.mark.asyncio
async def test_final_pnl_includes_delayed_trade_impact():
    """Test that final PnL calculation includes the impact of delayed trade executions.

    This verifies that trades executed at simulation end (from the pending queue)
    actually affect the final portfolio value and are not silently dropped.
    """
    start_time = datetime.now(UTC)
    end_time = start_time + timedelta(hours=3)

    # Create data provider with 3 ticks - all intents will be pending at end
    data_provider = MockDataProviderWithTicks(num_ticks=3, start_time=start_time)

    backtester = PnLBacktester(
        data_provider=data_provider,
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )

    # High delay ensures all intents execute at simulation end
    config = PnLBacktestConfig(
        start_time=start_time,
        end_time=end_time,
        initial_capital_usd=Decimal("10000"),
        tokens=["WETH", "USDC"],
        inclusion_delay_blocks=10,
    )

    strategy = MockStrategy()
    result = await backtester.backtest(strategy, config)

    assert result.error is None, f"Backtest failed: {result.error}"

    # Verify trades were executed at end
    assert result.execution_delayed_at_end > 0, "Expected delayed trades at end"

    # Verify trades have non-zero amounts (proving they were executed)
    for trade in result.trades:
        # The swap intent swaps USDC -> ETH, so amount should be non-zero
        assert trade.amount_usd > 0, f"Trade {trade.timestamp} has zero amount, was it executed?"

    # Verify final capital differs from initial (trades affected portfolio)
    # This is the key test - if trades are dropped, final_capital would equal initial
    total_trade_amount = sum(t.amount_usd for t in result.trades)
    assert total_trade_amount > 0, "Expected non-zero total trade amount"

    # The metrics should reflect the executed trades
    assert result.metrics.total_trades == len(result.trades), (
        f"Metrics mismatch: total_trades={result.metrics.total_trades}, "
        f"actual trades={len(result.trades)}"
    )


@pytest.mark.asyncio
async def test_delayed_trade_pnl_affects_final_portfolio():
    """Test that delayed trades' PnL is correctly reflected in final portfolio value.

    This is a more detailed test ensuring the economic impact of delayed trades
    is properly calculated and included in the backtest result.
    """
    start_time = datetime.now(UTC)
    end_time = start_time + timedelta(hours=2)

    # 2 ticks with high delay = all intents delayed
    data_provider = MockDataProviderWithTicks(num_ticks=2, start_time=start_time)

    backtester = PnLBacktester(
        data_provider=data_provider,
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )

    config = PnLBacktestConfig(
        start_time=start_time,
        end_time=end_time,
        initial_capital_usd=Decimal("10000"),
        tokens=["WETH", "USDC"],
        inclusion_delay_blocks=5,
    )

    strategy = MockStrategy()
    result = await backtester.backtest(strategy, config)

    assert result.error is None, f"Backtest failed: {result.error}"

    # All 2 trades should be delayed at end
    assert result.execution_delayed_at_end == 2, (
        f"Expected 2 delayed trades, got {result.execution_delayed_at_end}"
    )

    # Verify each trade has positive amount
    for i, trade in enumerate(result.trades):
        assert trade.amount_usd > 0, f"Trade {i} amount is 0 (not executed properly)"
        assert trade.delayed_at_end is True, f"Trade {i} missing delayed_at_end flag"

    # Verify the total PnL is tracked in metrics
    # The exact PnL depends on the swap execution, but it should be available
    assert result.metrics.total_pnl_usd is not None, "Total PnL USD should be calculated"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "inclusion_delay_blocks,num_ticks,expected_delayed",
    [
        # Formula: normal = max(0, num_ticks - delay - 1), delayed = num_ticks - normal
        # This accounts for the fact that:
        # - Each intent queued at tick T executes at tick T + delay + 1
        # - Intents that would execute after the last tick are delayed to end

        # delay=0, 5 ticks: normal=4, delayed=1 (last tick's intent)
        (0, 5, 1),
        # delay=1, 5 ticks: normal=3, delayed=2 (ticks 4,5)
        (1, 5, 2),
        # delay=2, 5 ticks: normal=2, delayed=3 (ticks 3,4,5)
        (2, 5, 3),
        # delay=3, 5 ticks: normal=1, delayed=4 (ticks 2,3,4,5)
        (3, 5, 4),
        # delay=5, 3 ticks: normal=0, delayed=3 (all ticks)
        (5, 3, 3),
        # delay=10, 4 ticks: normal=0, delayed=4 (all ticks)
        (10, 4, 4),
        # delay=1, 2 ticks: normal=0, delayed=2 (all ticks)
        (1, 2, 2),
        # delay=0, 1 tick: normal=0, delayed=1 (only tick)
        (0, 1, 1),
    ],
    ids=[
        "delay_0_ticks_5",
        "delay_1_ticks_5",
        "delay_2_ticks_5",
        "delay_3_ticks_5",
        "delay_5_ticks_3",
        "delay_10_ticks_4",
        "delay_1_ticks_2",
        "delay_0_ticks_1",
    ],
)
async def test_various_inclusion_delay_values(
    inclusion_delay_blocks: int,
    num_ticks: int,
    expected_delayed: int,
):
    """Parametrized test verifying correct behavior with various inclusion_delay_blocks values.

    Tests the relationship between:
    - inclusion_delay_blocks: how many ticks to wait before execution
    - num_ticks: total simulation ticks
    - expected_delayed: how many intents should be executed at simulation end
    """
    start_time = datetime.now(UTC)
    end_time = start_time + timedelta(hours=num_ticks)

    data_provider = MockDataProviderWithTicks(num_ticks=num_ticks, start_time=start_time)

    backtester = PnLBacktester(
        data_provider=data_provider,
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )

    config = PnLBacktestConfig(
        start_time=start_time,
        end_time=end_time,
        initial_capital_usd=Decimal("10000"),
        tokens=["WETH", "USDC"],
        inclusion_delay_blocks=inclusion_delay_blocks,
    )

    strategy = MockStrategy()
    result = await backtester.backtest(strategy, config)

    assert result.error is None, f"Backtest failed: {result.error}"

    # Verify the expected number of delayed executions
    assert result.execution_delayed_at_end == expected_delayed, (
        f"With delay={inclusion_delay_blocks} and {num_ticks} ticks, "
        f"expected {expected_delayed} delayed executions but got {result.execution_delayed_at_end}"
    )

    # Verify total trades equals num_ticks (no intents dropped)
    assert len(result.trades) == num_ticks, (
        f"Expected {num_ticks} total trades (one per tick), got {len(result.trades)}. "
        "Some intents may have been dropped."
    )

    # Verify the delayed trades have the flag set
    delayed_trades = [t for t in result.trades if t.delayed_at_end]
    assert len(delayed_trades) == expected_delayed, (
        f"Expected {expected_delayed} trades with delayed_at_end=True, "
        f"but found {len(delayed_trades)}"
    )


@pytest.mark.asyncio
async def test_metrics_correctly_track_delayed_executions():
    """Test that BacktestResult metrics correctly count delayed executions.

    Verifies:
    - execution_delayed_at_end counter matches actual delayed trades
    - total_trades includes delayed trades
    - metrics are consistent with trade records
    """
    start_time = datetime.now(UTC)
    end_time = start_time + timedelta(hours=4)

    data_provider = MockDataProviderWithTicks(num_ticks=4, start_time=start_time)

    backtester = PnLBacktester(
        data_provider=data_provider,
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )

    # delay=2 with 4 ticks: ticks 1,2 execute normally, ticks 3,4 delayed at end
    config = PnLBacktestConfig(
        start_time=start_time,
        end_time=end_time,
        initial_capital_usd=Decimal("10000"),
        tokens=["WETH", "USDC"],
        inclusion_delay_blocks=2,
    )

    strategy = MockStrategy()
    result = await backtester.backtest(strategy, config)

    assert result.error is None, f"Backtest failed: {result.error}"

    # Count trades by type
    delayed_trades = [t for t in result.trades if t.delayed_at_end]
    normal_trades = [t for t in result.trades if not t.delayed_at_end]

    # Verify counter matches
    assert result.execution_delayed_at_end == len(delayed_trades), (
        f"Counter mismatch: execution_delayed_at_end={result.execution_delayed_at_end}, "
        f"actual delayed trades={len(delayed_trades)}"
    )

    # Verify total trades metric matches
    assert result.metrics.total_trades == len(result.trades), (
        f"Metrics total_trades={result.metrics.total_trades} "
        f"doesn't match len(trades)={len(result.trades)}"
    )

    # Verify we have both types
    assert len(normal_trades) > 0, "Expected some normal trades"
    assert len(delayed_trades) > 0, "Expected some delayed trades"

    # Verify all trades (normal + delayed) = total
    assert len(normal_trades) + len(delayed_trades) == len(result.trades), (
        "Normal + delayed should equal total trades"
    )
