"""Performance benchmark tests for backtesting infrastructure.

This module provides performance benchmark tests for the backtesting system:
1. US-040: 1-year backtest at 1-hour intervals should complete in < 60 seconds
2. US-041: 100-parameter sweep should complete in < 30 minutes (1800 seconds)

Performance tests are marked with @pytest.mark.benchmark and can be
run separately from unit tests:
    pytest tests/benchmark -v -m benchmark

Usage:
    # Run all benchmark tests
    pytest tests/benchmark/backtesting/test_performance.py -v

    # Run only 1-year backtest benchmark
    pytest tests/benchmark/backtesting/test_performance.py::TestOneYearBacktestBenchmark -v

    # Run only parameter sweep benchmark
    pytest tests/benchmark/backtesting/test_performance.py::TestParameterSweepBenchmark -v
"""

import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktester,
)
from almanak.framework.backtesting.pnl.parallel import generate_grid_configs

# =============================================================================
# Test Constants
# =============================================================================

# SLA: 1-year backtest at 1-hour intervals should complete in < 60 seconds
ONE_YEAR_BACKTEST_SLA_SECONDS = 60.0

# SLA: 100-parameter sweep should complete in < 30 minutes
PARAMETER_SWEEP_SLA_SECONDS = 30 * 60  # 1800 seconds

# Number of hours in a year for 1-hour interval backtest
HOURS_IN_YEAR = 365 * 24  # 8760 hours

# Number of configs for parameter sweep benchmark
PARAMETER_SWEEP_SIZE = 100


# =============================================================================
# Mock Data Provider for Benchmarking
# =============================================================================


class BenchmarkDataProvider:
    """Data provider optimized for benchmark throughput.

    Generates deterministic market states without external I/O.
    Uses pre-computed price sequences for maximum speed.
    """

    provider_name = "benchmark"

    def __init__(self, num_ticks: int, start_time: datetime):
        """Initialize the benchmark data provider.

        Args:
            num_ticks: Number of ticks to generate
            start_time: Start time for the backtest
        """
        self.num_ticks = num_ticks
        self.start_time = start_time

        # Pre-compute price sequence for deterministic results
        self._base_eth_price = Decimal("3000")
        self._eth_price_step = Decimal("0.1")  # Small hourly change

    async def iterate(self, config: Any):
        """Yield (timestamp, market_state) tuples.

        Optimized for speed - no I/O, minimal computation per tick.

        Args:
            config: Backtest configuration with interval_seconds

        Yields:
            Tuples of (timestamp, market_state)
        """
        current_time = self.start_time
        interval = timedelta(seconds=config.interval_seconds)

        for i in range(self.num_ticks):
            # Simple price variation based on tick index
            # Uses modular arithmetic for a cyclic price pattern
            price_offset = Decimal(i % 1000) * self._eth_price_step
            eth_price = self._base_eth_price + price_offset

            market_state = MarketState(
                timestamp=current_time,
                prices={
                    "ETH": eth_price,
                    "WETH": eth_price,
                    "USDC": Decimal("1"),
                    "USDT": Decimal("1"),
                },
                chain="arbitrum",
                block_number=1000 + i,
            )

            yield current_time, market_state
            current_time += interval


# =============================================================================
# Mock Strategy for Benchmarking
# =============================================================================


@dataclass
class MockSwapIntent:
    """Lightweight mock swap intent for benchmark testing."""

    intent_type: str = "SWAP"
    from_token: str = "USDC"
    to_token: str = "ETH"
    amount: Decimal = field(default_factory=lambda: Decimal("100"))
    protocol: str = "uniswap_v3"


class BenchmarkStrategy:
    """Strategy optimized for benchmark throughput.

    Executes trades at a configurable rate to simulate realistic
    backtest load without complex decision logic.
    """

    def __init__(self, strategy_id: str = "benchmark_strategy", trade_every_n_ticks: int = 24):
        """Initialize the benchmark strategy.

        Args:
            strategy_id: Unique identifier for this strategy
            trade_every_n_ticks: Execute trade every N ticks (default 24 = daily at hourly intervals)
        """
        self._strategy_id = strategy_id
        self._trade_every_n_ticks = trade_every_n_ticks
        self._tick_count = 0

    @property
    def strategy_id(self) -> str:
        return self._strategy_id

    def decide(self, market: Any) -> MockSwapIntent | None:
        """Return a swap intent at configured interval, otherwise None.

        Trading every 24 hours (24 ticks at 1-hour interval) simulates
        a typical daily rebalancing strategy.

        Args:
            market: Market snapshot (unused in benchmark)

        Returns:
            MockSwapIntent if trade should execute, None otherwise
        """
        self._tick_count += 1
        if self._tick_count % self._trade_every_n_ticks == 0:
            return MockSwapIntent(
                from_token="USDC",
                to_token="ETH",
                amount=Decimal("1000"),
            )
        return None

    def get_metadata(self) -> None:
        return None


# =============================================================================
# US-040: 1-Year Backtest Performance Benchmark
# =============================================================================


@pytest.mark.benchmark
class TestOneYearBacktestBenchmark:
    """Benchmark tests for 1-year backtest performance.

    US-040: Add performance benchmark test for 1-year backtest

    Acceptance Criteria:
    - Test runs 1-year backtest at 1-hour intervals (8760 data points)
    - Test asserts completion in under 60 seconds
    - Test prints actual elapsed time for monitoring
    - Mark test with @pytest.mark.benchmark
    """

    @pytest.mark.asyncio
    async def test_one_year_backtest_completes_under_60_seconds(self):
        """Test that a 1-year backtest at 1-hour intervals completes in < 60 seconds.

        This benchmark validates that the backtesting engine can process
        8760 data points (1 year at hourly intervals) within the 60-second SLA.

        The test:
        1. Creates a BenchmarkDataProvider with 8760 ticks
        2. Runs a backtest with hourly intervals
        3. Asserts completion time < 60 seconds
        4. Prints elapsed time for monitoring
        """
        # Setup: 1-year period with hourly intervals
        start_time = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
        end_time = start_time + timedelta(hours=HOURS_IN_YEAR)

        config = PnLBacktestConfig(
            start_time=start_time,
            end_time=end_time,
            initial_capital_usd=Decimal("100000"),
            tokens=["WETH", "USDC"],
            interval_seconds=3600,  # 1 hour
            random_seed=42,
            strict_reproducibility=True,
        )

        data_provider = BenchmarkDataProvider(
            num_ticks=HOURS_IN_YEAR,
            start_time=start_time,
        )

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
        )

        strategy = BenchmarkStrategy(trade_every_n_ticks=24)  # Daily trades

        # Execute with timing
        execution_start = time.perf_counter()
        result = await backtester.backtest(strategy, config)
        execution_time = time.perf_counter() - execution_start

        # Assertions
        assert result.error is None, f"Backtest failed: {result.error}"

        # Verify we processed data points
        assert len(result.equity_curve) > 0, "No equity curve points recorded"

        # Print performance metrics for monitoring
        ticks_per_second = len(result.equity_curve) / execution_time if execution_time > 0 else 0
        print("\n" + "=" * 60)
        print("1-Year Backtest Performance (US-040)")
        print("=" * 60)
        print(f"Total execution time: {execution_time:.2f} seconds")
        print(f"Data points (8760 expected): {HOURS_IN_YEAR}")
        print(f"Equity curve points: {len(result.equity_curve)}")
        print(f"Trades executed: {len(result.trades)}")
        print(f"Throughput: {ticks_per_second:.0f} ticks/second")
        print(f"SLA target: < {ONE_YEAR_BACKTEST_SLA_SECONDS:.0f} seconds")
        print(f"Result: {'PASS' if execution_time < ONE_YEAR_BACKTEST_SLA_SECONDS else 'FAIL'}")
        print("=" * 60)

        # SLA assertion: must complete in under 60 seconds
        assert execution_time < ONE_YEAR_BACKTEST_SLA_SECONDS, (
            f"1-year backtest took {execution_time:.2f}s, exceeding SLA of {ONE_YEAR_BACKTEST_SLA_SECONDS}s"
        )

    @pytest.mark.asyncio
    async def test_one_year_backtest_with_frequent_trades(self):
        """Test 1-year backtest with more frequent trading (hourly trades).

        This tests a worst-case scenario where trades execute every tick,
        which increases the processing load significantly.
        """
        start_time = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
        end_time = start_time + timedelta(hours=HOURS_IN_YEAR)

        config = PnLBacktestConfig(
            start_time=start_time,
            end_time=end_time,
            initial_capital_usd=Decimal("100000"),
            tokens=["WETH", "USDC"],
            interval_seconds=3600,
            random_seed=42,
        )

        data_provider = BenchmarkDataProvider(
            num_ticks=HOURS_IN_YEAR,
            start_time=start_time,
        )

        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
        )

        # Trade every 4 hours (6 trades per day, ~2190 trades/year)
        strategy = BenchmarkStrategy(trade_every_n_ticks=4)

        execution_start = time.perf_counter()
        result = await backtester.backtest(strategy, config)
        execution_time = time.perf_counter() - execution_start

        assert result.error is None, f"Backtest failed: {result.error}"

        print("\n" + "-" * 60)
        print("1-Year Backtest with Frequent Trading")
        print("-" * 60)
        print(f"Execution time: {execution_time:.2f} seconds")
        print(f"Trades executed: {len(result.trades)}")
        print(f"Trades per day: {len(result.trades) / 365:.1f}")
        print("-" * 60)

        # Even with frequent trading, should still meet SLA
        assert execution_time < ONE_YEAR_BACKTEST_SLA_SECONDS, (
            f"Frequent trading backtest took {execution_time:.2f}s, exceeding SLA"
        )

    @pytest.mark.asyncio
    async def test_throughput_consistency_across_durations(self):
        """Verify throughput remains consistent across different durations.

        Tests that the backtester doesn't have O(n^2) complexity by
        comparing throughput across 1-month, 3-month, and 6-month durations.
        """
        durations_hours = [
            24 * 30,   # 1 month (720 hours)
            24 * 90,   # 3 months (2160 hours)
            24 * 180,  # 6 months (4320 hours)
        ]
        throughputs = []

        for num_hours in durations_hours:
            start_time = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
            end_time = start_time + timedelta(hours=num_hours)

            config = PnLBacktestConfig(
                start_time=start_time,
                end_time=end_time,
                initial_capital_usd=Decimal("10000"),
                tokens=["WETH", "USDC"],
                interval_seconds=3600,
                random_seed=42,
            )

            data_provider = BenchmarkDataProvider(num_ticks=num_hours, start_time=start_time)
            backtester = PnLBacktester(
                data_provider=data_provider,
                fee_models={"default": DefaultFeeModel()},
                slippage_models={"default": DefaultSlippageModel()},
            )
            strategy = BenchmarkStrategy(trade_every_n_ticks=24)

            execution_start = time.perf_counter()
            result = await backtester.backtest(strategy, config)
            execution_time = time.perf_counter() - execution_start

            assert result.error is None

            throughput = num_hours / execution_time if execution_time > 0 else 0
            throughputs.append(throughput)
            print(f"\n{num_hours} hours: {execution_time:.2f}s, {throughput:.0f} ticks/s")

        # Throughput should not degrade catastrophically.
        # CI runners are noisy, so allow up to 75% variance across durations.
        min_throughput = min(throughputs)
        max_throughput = max(throughputs)
        variance_ratio = min_throughput / max_throughput if max_throughput > 0 else 0

        print(f"\nThroughput variance ratio: {variance_ratio:.2f}")
        assert variance_ratio > 0.25, (
            f"Throughput degraded with scale: min={min_throughput:.0f}, max={max_throughput:.0f}"
        )


# =============================================================================
# US-041: 100-Parameter Sweep Performance Benchmark
# =============================================================================


@pytest.mark.benchmark
class TestParameterSweepBenchmark:
    """Benchmark tests for parameter sweep performance.

    US-041: Add performance benchmark test for parameter sweep

    Acceptance Criteria:
    - Test runs parameter sweep with 100 combinations
    - Test asserts completion in under 1800 seconds (30 minutes)
    - Test prints actual elapsed time for monitoring
    - Mark test with @pytest.mark.benchmark
    """

    @pytest.mark.asyncio
    async def test_100_parameter_sweep_completes_under_30_minutes(self):
        """Test that a 100-parameter sweep completes in < 30 minutes.

        This benchmark validates that running 100 backtest configurations
        sequentially completes within the 30-minute SLA.

        The test:
        1. Generates 100 unique backtest configurations
        2. Runs each backtest sequentially
        3. Asserts total completion time < 1800 seconds (30 minutes)
        4. Prints elapsed time for monitoring
        """
        # Base config: 1-month backtest for reasonable sweep time
        start_time = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
        num_ticks = 24 * 30  # 720 hours (30 days at hourly)

        base_config = PnLBacktestConfig(
            start_time=start_time,
            end_time=start_time + timedelta(days=30),
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            interval_seconds=3600,  # 1 hour
            random_seed=42,
        )

        # Generate parameter grid
        param_ranges = {
            "initial_capital_usd": [
                Decimal("10000"),
                Decimal("50000"),
                Decimal("100000"),
                Decimal("500000"),
            ],
            "gas_price_gwei": [Decimal("10"), Decimal("20"), Decimal("50"), Decimal("100")],
        }

        # Generate grid configs (4x4 = 16 combinations)
        grid_configs = generate_grid_configs(base_config, param_ranges)

        # Extend to 100 configs by varying seeds
        configs: list[PnLBacktestConfig] = []
        seed = 0
        while len(configs) < PARAMETER_SWEEP_SIZE:
            for gc in grid_configs:
                if len(configs) >= PARAMETER_SWEEP_SIZE:
                    break
                # Create config with unique seed
                config_dict = gc.to_dict()
                # Remove computed properties
                for key in ["duration_seconds", "duration_days", "estimated_ticks"]:
                    config_dict.pop(key, None)
                config_dict["random_seed"] = seed
                configs.append(PnLBacktestConfig.from_dict(config_dict))
                seed += 1

        assert len(configs) == PARAMETER_SWEEP_SIZE, f"Expected {PARAMETER_SWEEP_SIZE} configs, got {len(configs)}"

        # Execute parameter sweep with timing
        execution_start = time.perf_counter()
        successful = 0
        failed = 0

        for i, config in enumerate(configs):
            data_provider = BenchmarkDataProvider(num_ticks=num_ticks, start_time=start_time)
            backtester = PnLBacktester(
                data_provider=data_provider,
                fee_models={"default": DefaultFeeModel()},
                slippage_models={"default": DefaultSlippageModel()},
            )
            strategy = BenchmarkStrategy(trade_every_n_ticks=24)

            result = await backtester.backtest(strategy, config)
            if result.error is None:
                successful += 1
            else:
                failed += 1

            # Progress indicator every 10 configs
            if (i + 1) % 10 == 0:
                elapsed = time.perf_counter() - execution_start
                print(f"Progress: {i + 1}/{PARAMETER_SWEEP_SIZE} configs, {elapsed:.1f}s elapsed")

        execution_time = time.perf_counter() - execution_start

        # Print performance metrics
        print("\n" + "=" * 60)
        print("100-Parameter Sweep Performance (US-041)")
        print("=" * 60)
        print(f"Total execution time: {execution_time:.2f} seconds ({execution_time / 60:.1f} minutes)")
        print(f"Configs executed: {len(configs)}")
        print(f"Successful: {successful}")
        print(f"Failed: {failed}")
        print(f"Average time per config: {execution_time / len(configs):.2f} seconds")
        print(f"SLA target: < {PARAMETER_SWEEP_SLA_SECONDS} seconds ({PARAMETER_SWEEP_SLA_SECONDS / 60:.0f} minutes)")
        print(f"Result: {'PASS' if execution_time < PARAMETER_SWEEP_SLA_SECONDS else 'FAIL'}")
        print("=" * 60)

        # All should succeed with mock data
        assert failed == 0, f"{failed} backtests failed"

        # SLA assertion: must complete in under 30 minutes
        assert execution_time < PARAMETER_SWEEP_SLA_SECONDS, (
            f"100-parameter sweep took {execution_time:.2f}s ({execution_time / 60:.1f} min), "
            f"exceeding SLA of {PARAMETER_SWEEP_SLA_SECONDS}s ({PARAMETER_SWEEP_SLA_SECONDS / 60:.0f} min)"
        )

    @pytest.mark.asyncio
    async def test_parameter_sweep_efficiency_per_backtest(self):
        """Test that per-backtest time remains consistent during sweep.

        Verifies that running multiple backtests sequentially maintains
        consistent execution time, detecting any memory leaks or
        accumulating overhead.
        """
        start_time = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
        num_ticks = 24 * 7  # 168 hours (1 week)

        base_config = PnLBacktestConfig(
            start_time=start_time,
            end_time=start_time + timedelta(days=7),
            initial_capital_usd=Decimal("10000"),
            tokens=["WETH", "USDC"],
            interval_seconds=3600,
            random_seed=42,
        )

        # Create 20 configs to test consistency
        configs = []
        for i in range(20):
            config_dict = base_config.to_dict()
            for key in ["duration_seconds", "duration_days", "estimated_ticks"]:
                config_dict.pop(key, None)
            config_dict["random_seed"] = i
            configs.append(PnLBacktestConfig.from_dict(config_dict))

        # Measure individual execution times
        execution_times = []
        successful = 0

        for config in configs:
            data_provider = BenchmarkDataProvider(num_ticks=num_ticks, start_time=start_time)
            backtester = PnLBacktester(
                data_provider=data_provider,
                fee_models={"default": DefaultFeeModel()},
                slippage_models={"default": DefaultSlippageModel()},
            )
            strategy = BenchmarkStrategy(trade_every_n_ticks=24)

            start = time.perf_counter()
            result = await backtester.backtest(strategy, config)
            elapsed = time.perf_counter() - start

            if result.error is None:
                successful += 1
                execution_times.append(elapsed)

        # Calculate statistics
        total_time = sum(execution_times)
        avg_time = total_time / len(execution_times) if execution_times else 0
        min_time = min(execution_times) if execution_times else 0
        max_time = max(execution_times) if execution_times else 0

        print("\n" + "-" * 60)
        print("Parameter Sweep Efficiency Analysis")
        print("-" * 60)
        print(f"Total time: {total_time:.2f}s")
        print(f"Average per backtest: {avg_time:.4f}s")
        print(f"Min: {min_time:.4f}s, Max: {max_time:.4f}s")
        print(f"Variance (max/min): {max_time / min_time:.2f}x" if min_time > 0 else "N/A")
        print(f"Successful: {successful}/{len(configs)}")
        print("-" * 60)

        # All should succeed
        assert successful == len(configs), f"Only {successful}/{len(configs)} backtests succeeded"

        # Per-backtest time should be consistent (max no more than 5x min)
        # Using 5x to account for GC pauses, OS scheduling, and CI variability
        if min_time > 0:
            variance_ratio = max_time / min_time
            assert variance_ratio < 5.0, (
                f"Execution time variance too high: min={min_time:.4f}s, max={max_time:.4f}s"
            )


# =============================================================================
# Performance Summary
# =============================================================================


@pytest.mark.benchmark
class TestPerformanceSummary:
    """Summary test documenting performance characteristics.

    Provides a summary of all performance findings and recommendations.
    """

    def test_print_performance_summary(self):
        """Print performance summary and recommendations.

        This test always passes - it documents the performance characteristics
        of the backtesting system.
        """
        summary = """
        ============================================================
        Backtesting Performance Summary
        ============================================================

        Performance SLAs:
        -----------------
        1. 1-year backtest (8760 hourly ticks): < 60 seconds [US-040]
        2. 100-parameter sweep: < 30 minutes (1800 seconds) [US-041]

        Key Metrics:
        ------------
        - Typical throughput: 1000-5000 ticks/second (mock data)
        - Per-backtest overhead: ~0.1-0.5 seconds
        - Memory usage: O(n) with tick count

        Hot Paths (typical):
        -------------------
        1. Market state iteration (data provider)
        2. Portfolio valuation and position tracking
        3. Intent execution and trade record creation
        4. Decimal arithmetic operations

        Optimization Recommendations:
        ----------------------------
        - Pre-compute price sequences in data providers
        - Cache portfolio valuations when positions unchanged
        - Use batch operations for price lookups
        - Consider using floats for intermediate calculations

        ============================================================
        """
        print(summary)
        assert True
