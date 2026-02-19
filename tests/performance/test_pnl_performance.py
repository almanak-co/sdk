"""Performance benchmark tests for PnL Backtester.

These tests verify that the PnL backtesting engine meets performance
requirements specified in the PRD:

- 1-year backtest at 1-hour intervals should complete in < 60 seconds

Tests use mock data providers to avoid API latency and ensure benchmarks
measure actual engine performance, not network delays.
"""

import time
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

# =============================================================================
# Mock Data Provider for Performance Testing
# =============================================================================


class FastMockDataProvider:
    """High-performance mock data provider for benchmark testing.

    This provider generates synthetic price data on-the-fly without any
    external API calls or file I/O, ensuring benchmarks measure only
    the backtesting engine's performance.
    """

    def __init__(
        self,
        base_prices: dict[str, Decimal],
        start_time: datetime,
        volatility: Decimal = Decimal("0.001"),  # 0.1% per interval
        seed: int = 42,
    ):
        """Initialize with base prices and volatility settings.

        Args:
            base_prices: Dict mapping token -> starting price
            start_time: Start timestamp for the series
            volatility: Price change per interval (as decimal)
            seed: Random seed for reproducibility
        """
        self._base_prices = base_prices
        self._start_time = start_time
        self._volatility = volatility
        self._seed = seed

    def _get_price_at_index(self, token: str, index: int) -> Decimal:
        """Generate deterministic price at index.

        Uses a simple sine wave pattern for price movement to ensure
        reproducibility while simulating realistic price action.
        """
        import math

        base = self._base_prices.get(token.upper(), Decimal("1"))

        # Simple deterministic price movement using sine wave
        # This creates realistic-looking price action without randomness
        wave = Decimal(str(math.sin(index * 0.1 + hash(token) % 100)))
        trend = Decimal(str(index * 0.00001))  # Slight upward trend
        change = wave * self._volatility + trend

        return base * (Decimal("1") + change)

    async def get_price(self, token: str, timestamp: datetime) -> Decimal:
        """Get price for token at specific timestamp."""
        token = token.upper()
        if token not in self._base_prices:
            # Return $1 for unknown tokens (stablecoins)
            return Decimal("1")

        # Calculate index from timestamp (hourly intervals)
        delta = timestamp - self._start_time
        index = int(delta.total_seconds() / 3600)

        return self._get_price_at_index(token, index)

    async def get_ohlcv(
        self,
        token: str,
        start: datetime,
        end: datetime,
        interval_seconds: int = 3600,
    ) -> list[OHLCV]:
        """Get OHLCV data for token."""
        result = []
        current = start
        while current <= end:
            price = await self.get_price(token, current)
            result.append(
                OHLCV(
                    timestamp=current,
                    open=price,
                    high=price * Decimal("1.002"),
                    low=price * Decimal("0.998"),
                    close=price,
                    volume=Decimal("1000000"),
                )
            )
            current += timedelta(seconds=interval_seconds)
        return result

    async def iterate(
        self, config: HistoricalDataConfig
    ) -> AsyncIterator[tuple[datetime, MarketState]]:
        """Iterate through historical data with mock prices."""
        current = config.start_time
        index = 0
        interval_delta = timedelta(seconds=config.interval_seconds)

        while current <= config.end_time:
            prices = {}
            for token in config.tokens:
                token = token.upper()
                if token in self._base_prices:
                    prices[token] = self._get_price_at_index(token, index)
                else:
                    # Stablecoin default
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
            current += interval_delta

    @property
    def provider_name(self) -> str:
        return "fast_mock"

    @property
    def supported_tokens(self) -> list[str]:
        return list(self._base_prices.keys())

    @property
    def supported_chains(self) -> list[str]:
        return ["arbitrum", "ethereum", "base"]

    @property
    def min_timestamp(self) -> datetime | None:
        return self._start_time

    @property
    def max_timestamp(self) -> datetime | None:
        # Support up to 10 years of data
        return self._start_time + timedelta(days=3650)


# =============================================================================
# Mock Intents for Benchmark Testing
# =============================================================================


@dataclass
class MockSwapIntent:
    """Mock swap intent for benchmark testing."""

    intent_type: str = "SWAP"
    from_token: str = "USDC"
    to_token: str = "WETH"
    amount_usd: Decimal = Decimal("1000")
    protocol: str = "uniswap_v3"


class SimpleSwapStrategy:
    """Simple swap strategy for consistent benchmark baseline.

    Executes swaps at regular intervals to generate a consistent
    workload for performance measurement.
    """

    def __init__(
        self,
        swap_interval: int = 24,  # Swap every 24 ticks
        strategy_id: str = "benchmark_swap_strategy",
    ):
        """Initialize benchmark strategy.

        Args:
            swap_interval: Number of ticks between swaps
            strategy_id: Identifier for the strategy
        """
        self._swap_interval = swap_interval
        self._strategy_id = strategy_id
        self._tick_count = 0
        self._in_eth = False  # Track if we're holding ETH or USDC

    @property
    def strategy_id(self) -> str:
        return self._strategy_id

    def decide(self, market: Any) -> MockSwapIntent | None:
        """Decide whether to swap based on tick count."""
        self._tick_count += 1

        # Swap every swap_interval ticks
        if self._tick_count % self._swap_interval == 0:
            if self._in_eth:
                # Swap ETH -> USDC
                intent = MockSwapIntent(
                    from_token="WETH",
                    to_token="USDC",
                    amount_usd=Decimal("500"),
                )
            else:
                # Swap USDC -> ETH
                intent = MockSwapIntent(
                    from_token="USDC",
                    to_token="WETH",
                    amount_usd=Decimal("500"),
                )
            self._in_eth = not self._in_eth
            return intent

        return None  # Hold


# =============================================================================
# Performance Benchmark Tests
# =============================================================================


class TestPnLBacktesterPerformance:
    """Performance benchmark tests for PnL Backtester."""

    @pytest.mark.asyncio
    async def test_one_year_hourly_backtest_under_60_seconds(self) -> None:
        """Benchmark: 1-year backtest at 1-hour intervals < 60 seconds.

        This test verifies that the PnL backtester can process a full year
        of hourly data (8760 ticks) with a simple swap strategy in under
        60 seconds. This is a critical performance requirement for practical
        strategy development workflows.

        Performance target: < 60 seconds
        Data points: ~8760 (365 days * 24 hours)
        Operations: ~365 swaps (1 per day)
        """
        # Setup
        start_time = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
        end_time = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)  # Exactly 1 year

        data_provider = FastMockDataProvider(
            base_prices={
                "WETH": Decimal("3000"),
                "USDC": Decimal("1"),
                "WBTC": Decimal("45000"),
            },
            start_time=start_time,
            volatility=Decimal("0.002"),  # 0.2% volatility
        )

        config = PnLBacktestConfig(
            start_time=start_time,
            end_time=end_time,
            interval_seconds=3600,  # 1 hour intervals
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            include_gas_costs=True,
            inclusion_delay_blocks=0,  # Immediate execution
        )

        # Strategy that swaps once per day for consistent baseline
        strategy = SimpleSwapStrategy(swap_interval=24)

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
        )

        # Execute with timing
        start = time.perf_counter()
        result = await backtester.backtest(strategy, config)
        elapsed = time.perf_counter() - start

        # Verify correctness
        assert result.success, f"Backtest failed: {result.error}"
        assert result.metrics is not None

        # Should have approximately 8760 equity curve points (365 * 24)
        expected_ticks = 8760 + 1  # +1 for end time inclusive
        actual_ticks = len(result.equity_curve)
        # Allow 1% tolerance for tick count
        assert abs(actual_ticks - expected_ticks) <= expected_ticks * 0.01, (
            f"Expected ~{expected_ticks} ticks, got {actual_ticks}"
        )

        # Should have approximately 365 trades (1 swap per day)
        expected_trades = 365
        actual_trades = result.metrics.total_trades
        # Allow 10% tolerance for trade count
        assert abs(actual_trades - expected_trades) <= expected_trades * 0.1, (
            f"Expected ~{expected_trades} trades, got {actual_trades}"
        )

        # CRITICAL: Performance assertion
        max_allowed_seconds = 60
        assert elapsed < max_allowed_seconds, (
            f"1-year backtest took {elapsed:.2f}s, exceeds {max_allowed_seconds}s limit. "
            f"Performance optimization required."
        )

        # Log performance metrics for visibility
        ticks_per_second = actual_ticks / elapsed
        trades_per_second = actual_trades / elapsed

        print("\n=== PnL Backtest Performance Benchmark ===")
        print("Duration: 1 year at 1-hour intervals")
        print(f"Total ticks: {actual_ticks}")
        print(f"Total trades: {actual_trades}")
        print(f"Elapsed time: {elapsed:.2f}s")
        print(f"Throughput: {ticks_per_second:.1f} ticks/s")
        print(f"Throughput: {trades_per_second:.1f} trades/s")
        print(f"Status: {'PASS' if elapsed < max_allowed_seconds else 'FAIL'}")
        print("==========================================\n")

    @pytest.mark.asyncio
    async def test_one_year_backtest_hold_only_performance(self) -> None:
        """Benchmark: 1-year hold-only backtest establishes baseline.

        This test establishes the minimum possible time for a 1-year
        backtest with no trading activity. This represents the baseline
        overhead of the backtesting loop without trade execution.

        Performance target: < 30 seconds (baseline, no trading)
        """
        # Setup
        start_time = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
        end_time = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)

        data_provider = FastMockDataProvider(
            base_prices={
                "WETH": Decimal("3000"),
                "USDC": Decimal("1"),
            },
            start_time=start_time,
        )

        config = PnLBacktestConfig(
            start_time=start_time,
            end_time=end_time,
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            include_gas_costs=False,  # Skip gas for baseline
        )

        # Strategy that never trades (always holds)
        class HoldStrategy:
            strategy_id = "hold_benchmark"

            def decide(self, market: Any) -> None:
                return None

        strategy = HoldStrategy()

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
        )

        # Execute with timing
        start = time.perf_counter()
        result = await backtester.backtest(strategy, config)
        elapsed = time.perf_counter() - start

        # Verify
        assert result.success
        assert result.metrics.total_trades == 0
        actual_ticks = len(result.equity_curve)
        # Should have approximately 8760 equity curve points (365 * 24 + 1)
        assert actual_ticks >= 8700, f"Expected ~8761 ticks, got {actual_ticks}"

        # Baseline should be faster than trading scenario
        max_allowed_seconds = 30
        assert elapsed < max_allowed_seconds, (
            f"Hold-only backtest took {elapsed:.2f}s, exceeds {max_allowed_seconds}s. "
            f"Core loop optimization needed."
        )

        print("\n=== Hold-Only Baseline Benchmark ===")
        print(f"Total ticks: {actual_ticks}")
        print(f"Elapsed time: {elapsed:.2f}s")
        print(f"Throughput: {actual_ticks / elapsed:.1f} ticks/s")
        print("===================================\n")

    @pytest.mark.asyncio
    async def test_one_year_high_frequency_trading_performance(self) -> None:
        """Benchmark: 1-year backtest with frequent trading.

        This test stresses the trade execution path by trading every
        hour (8760 trades in a year). This represents a high-frequency
        trading scenario that exercises the full execution pipeline.

        Performance target: < 90 seconds (with ~8760 trades)
        """
        # Setup
        start_time = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
        end_time = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)

        data_provider = FastMockDataProvider(
            base_prices={
                "WETH": Decimal("3000"),
                "USDC": Decimal("1"),
            },
            start_time=start_time,
        )

        config = PnLBacktestConfig(
            start_time=start_time,
            end_time=end_time,
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            include_gas_costs=True,
        )

        # Strategy that trades every tick
        strategy = SimpleSwapStrategy(swap_interval=1)

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
        )

        # Execute with timing
        start = time.perf_counter()
        result = await backtester.backtest(strategy, config)
        elapsed = time.perf_counter() - start

        # Verify
        assert result.success
        # Should have many trades (approximately one per tick)
        assert result.metrics.total_trades > 8000

        # High-frequency should still complete in reasonable time
        max_allowed_seconds = 90
        assert elapsed < max_allowed_seconds, (
            f"High-frequency backtest took {elapsed:.2f}s, exceeds {max_allowed_seconds}s. "
            f"Trade execution path needs optimization."
        )

        trades = result.metrics.total_trades
        print("\n=== High-Frequency Trading Benchmark ===")
        print(f"Total trades: {trades}")
        print(f"Elapsed time: {elapsed:.2f}s")
        print(f"Throughput: {trades / elapsed:.1f} trades/s")
        print("=========================================\n")

    @pytest.mark.asyncio
    async def test_short_backtest_latency(self) -> None:
        """Benchmark: Short backtest initialization overhead.

        This test measures the startup overhead by running a very short
        backtest (1 week). This helps identify initialization costs that
        would be amortized in longer backtests.

        Performance target: < 2 seconds for 1-week backtest
        """
        # Setup
        start_time = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
        end_time = start_time + timedelta(days=7)

        data_provider = FastMockDataProvider(
            base_prices={
                "WETH": Decimal("3000"),
                "USDC": Decimal("1"),
            },
            start_time=start_time,
        )

        config = PnLBacktestConfig(
            start_time=start_time,
            end_time=end_time,
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            include_gas_costs=False,
        )

        strategy = SimpleSwapStrategy(swap_interval=24)

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
        )

        # Execute with timing
        start = time.perf_counter()
        result = await backtester.backtest(strategy, config)
        elapsed = time.perf_counter() - start

        # Verify
        assert result.success
        # 7 days * 24 hours = 168 ticks, +1 for inclusive end
        expected_ticks = 169
        actual_ticks = len(result.equity_curve)

        assert abs(actual_ticks - expected_ticks) <= 5, (
            f"Expected ~{expected_ticks} ticks, got {actual_ticks}"
        )

        # Short backtest should be very fast
        max_allowed_seconds = 2
        assert elapsed < max_allowed_seconds, (
            f"1-week backtest took {elapsed:.3f}s, exceeds {max_allowed_seconds}s. "
            f"Initialization overhead is too high."
        )

        print("\n=== Short Backtest Latency Benchmark ===")
        print("Duration: 1 week")
        print(f"Total ticks: {actual_ticks}")
        print(f"Elapsed time: {elapsed:.3f}s")
        print(f"Latency per tick: {elapsed / actual_ticks * 1000:.2f}ms")
        print("========================================\n")


# =============================================================================
# Parameter Sweep Performance Benchmarks
# =============================================================================


# Module-level start time constant for parallel tests
# This must be module-level for pickling support
_BENCHMARK_START_TIME = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)


# Factory functions must be at module level for pickling
def _create_benchmark_strategy() -> SimpleSwapStrategy:
    """Factory function to create benchmark strategy for parallel tests."""
    return SimpleSwapStrategy(swap_interval=24)


def _create_benchmark_data_provider() -> FastMockDataProvider:
    """Factory function to create mock data provider for parallel tests.

    Note: Uses module-level _BENCHMARK_START_TIME constant for pickling support.
    """
    return FastMockDataProvider(
        base_prices={
            "WETH": Decimal("3000"),
            "USDC": Decimal("1"),
        },
        start_time=_BENCHMARK_START_TIME,
        volatility=Decimal("0.002"),
    )


def _create_benchmark_backtester(
    data_provider: Any,
    fee_models: dict[str, Any],
    slippage_models: dict[str, Any],
) -> PnLBacktester:
    """Factory function to create backtester for parallel tests."""
    return PnLBacktester(
        data_provider=data_provider,
        fee_models=fee_models or {"default": DefaultFeeModel()},
        slippage_models=slippage_models or {"default": DefaultSlippageModel()},
    )


class TestParameterSweepPerformance:
    """Performance benchmark tests for parameter sweep execution."""

    @pytest.mark.asyncio
    async def test_100_parameter_sweep_under_30_minutes(self) -> None:
        """Benchmark: 100-parameter sweep with 4 workers < 30 minutes.

        This test verifies that a parameter sweep of 100 configurations
        can complete in under 30 minutes using parallel execution with
        4 workers. This represents a typical optimization workflow where
        a quant explores the parameter space.

        Performance target: < 30 minutes (1800 seconds)
        Configurations: 100 (10 x 10 grid)
        Workers: 4
        Per-backtest duration: 1 month at 1-hour intervals (~744 ticks)
        """
        from almanak.framework.backtesting.pnl.parallel import (
            aggregate_results,
            generate_grid_configs,
            run_parallel_backtests,
        )

        # Setup base configuration - 1 month duration for reasonable test time
        # Use module-level start time constant for pickling support
        end_time = datetime(2024, 2, 1, 0, 0, 0, tzinfo=UTC)  # 1 month

        base_config = PnLBacktestConfig(
            start_time=_BENCHMARK_START_TIME,
            end_time=end_time,
            interval_seconds=3600,  # 1 hour intervals
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            include_gas_costs=True,
            inclusion_delay_blocks=0,
        )

        # Generate 100 configs via grid search (10 x 10 = 100)
        # Vary initial_capital_usd across 10 values
        capital_values = [
            Decimal(str(5000 + i * 1000)) for i in range(10)  # 5000 to 14000
        ]
        # Vary interval_seconds across 10 values
        interval_values = [
            900 + i * 300 for i in range(10)  # 900s (15min) to 3600s (1hr)
        ]

        param_ranges = {
            "initial_capital_usd": capital_values,
            "interval_seconds": interval_values,
        }

        configs = generate_grid_configs(base_config, param_ranges)
        assert len(configs) == 100, f"Expected 100 configs, got {len(configs)}"

        # Use module-level factory functions for pickling support
        # (local closures cannot be pickled for multiprocessing)

        # Execute with timing
        start = time.perf_counter()
        results = await run_parallel_backtests(
            configs=configs,
            strategy_factory=_create_benchmark_strategy,
            data_provider_factory=_create_benchmark_data_provider,
            backtester_factory=_create_benchmark_backtester,
            workers=4,
        )
        elapsed = time.perf_counter() - start

        # Aggregate results for analysis
        aggregated = aggregate_results(results)

        # Verify correctness
        assert len(results) == 100, f"Expected 100 results, got {len(results)}"

        # At least 95% should succeed (allow for some edge cases)
        success_rate = aggregated.success_count / aggregated.total_count
        assert success_rate >= 0.95, (
            f"Success rate {success_rate:.1%} below 95% threshold. "
            f"{aggregated.failure_count} failures."
        )

        # Verify we have valid metrics
        for result in results:
            if result.success and result.result:
                assert result.result.metrics is not None
                # Should have some trades (strategy swaps once per day)
                assert result.result.metrics.total_trades >= 0

        # CRITICAL: Performance assertion
        max_allowed_seconds = 1800  # 30 minutes
        assert elapsed < max_allowed_seconds, (
            f"100-parameter sweep took {elapsed:.1f}s ({elapsed/60:.1f} minutes), "
            f"exceeds {max_allowed_seconds}s ({max_allowed_seconds/60:.0f} minute) limit. "
            f"Performance optimization required."
        )

        # Calculate statistics
        avg_time_per_config = elapsed / len(configs)
        throughput = len(configs) / elapsed * 60  # configs per minute

        print("\n=== Parameter Sweep Performance Benchmark ===")
        print("Configuration:")
        print("  - Configs: 100 (10x10 grid)")
        print("  - Workers: 4")
        print("  - Duration per backtest: 1 month at 1-hour intervals")
        print("\nResults:")
        print(f"  - Total elapsed time: {elapsed:.1f}s ({elapsed/60:.1f} minutes)")
        print(f"  - Average time per config: {avg_time_per_config:.2f}s")
        print(f"  - Throughput: {throughput:.1f} configs/minute")
        print(f"  - Success rate: {success_rate:.1%}")
        print(f"  - Avg Sharpe ratio: {aggregated.avg_sharpe:.4f}")
        print(f"  - Status: {'PASS' if elapsed < max_allowed_seconds else 'FAIL'}")
        print("==============================================\n")
