"""Parallel backtest execution for parameter sweeps.

This module provides utilities for running multiple backtests in parallel
using a process pool. This is useful for parameter sweeps where many
independent backtests need to be run.

Key Components:
    - run_parallel_backtests: Run multiple backtests in parallel using multiprocessing
    - ParallelBacktestResult: Result container with error handling for worker processes
    - BacktestTask: Input specification for a single backtest run
    - generate_grid_configs: Generate configs using Cartesian product of parameter ranges
    - generate_random_configs: Generate configs using random sampling of parameter ranges

Example:
    from almanak.framework.backtesting.pnl.parallel import (
        run_parallel_backtests,
        generate_grid_configs,
        generate_random_configs,
    )

    # Grid search: all combinations of parameters
    param_ranges = {
        "initial_capital_usd": [Decimal("10000"), Decimal("50000")],
        "interval_seconds": [3600, 7200],
    }
    grid_configs = generate_grid_configs(base_config, param_ranges)
    # Returns 4 configs: all combinations of capital x interval

    # Random search: sample from parameter ranges
    param_ranges_continuous = {
        "gas_price_gwei": (Decimal("10"), Decimal("100")),  # Tuple = range
        "interval_seconds": [3600, 7200, 14400],  # List = discrete choices
    }
    random_configs = generate_random_configs(base_config, param_ranges_continuous, n_samples=10)

    configs = [config1, config2, config3]  # List of PnLBacktestConfig
    results = await run_parallel_backtests(
        strategy=my_strategy,
        configs=configs,
        data_provider=provider,
        workers=4,
    )

    for result in results:
        if result.success:
            print(f"Config {result.config_index}: Sharpe = {result.result.metrics.sharpe_ratio}")
        else:
            print(f"Config {result.config_index}: Failed - {result.error}")
"""

from __future__ import annotations

import asyncio
import itertools
import logging
import os
import random
import traceback
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from tqdm import tqdm

from almanak.framework.backtesting.models import BacktestResult
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


@dataclass
class BacktestTask:
    """Specification for a single backtest run.

    Attributes:
        config: Configuration for the backtest
        config_index: Index in the original config list (for result ordering)
        strategy_factory: Factory function to create strategy (for pickling)
        data_provider_factory: Factory function to create data provider (for pickling)
        backtester_factory: Factory function to create backtester (for pickling)
    """

    config: PnLBacktestConfig
    config_index: int
    strategy_factory: Callable[[], Any]
    data_provider_factory: Callable[[], Any]
    backtester_factory: Callable[[Any, dict[str, Any], dict[str, Any]], Any]
    fee_models: dict[str, Any] = field(default_factory=dict)
    slippage_models: dict[str, Any] = field(default_factory=dict)


@dataclass
class ParallelBacktestResult:
    """Result from a parallel backtest execution.

    Attributes:
        config_index: Index of the config in the original list
        config: The configuration used for this backtest
        result: BacktestResult if successful, None if failed
        success: Whether the backtest completed successfully
        error: Error message if failed
        error_traceback: Full traceback if failed
        worker_pid: Process ID of the worker that ran this backtest
        execution_time_seconds: Wall clock time for this backtest
    """

    config_index: int
    config: PnLBacktestConfig
    result: BacktestResult | None = None
    success: bool = True
    error: str | None = None
    error_traceback: str | None = None
    worker_pid: int | None = None
    execution_time_seconds: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "config_index": self.config_index,
            "config": self.config.to_dict(),
            "result": self.result.to_dict() if self.result else None,
            "success": self.success,
            "error": self.error,
            "error_traceback": self.error_traceback,
            "worker_pid": self.worker_pid,
            "execution_time_seconds": self.execution_time_seconds,
        }


def _get_default_workers() -> int:
    """Get default number of workers (CPU count - 1, minimum 1)."""
    cpu_count = os.cpu_count() or 1
    return max(1, cpu_count - 1)


def _run_single_backtest_worker(task: BacktestTask) -> ParallelBacktestResult:
    """Worker function to run a single backtest in a subprocess.

    This function is designed to be pickled and run in a separate process.
    It creates new instances of strategy, data provider, and backtester
    using the provided factory functions.

    Args:
        task: BacktestTask containing config and factory functions

    Returns:
        ParallelBacktestResult with success or error information
    """
    import asyncio
    import time

    start_time = time.time()
    worker_pid = os.getpid()

    try:
        # Create instances using factory functions
        strategy = task.strategy_factory()
        data_provider = task.data_provider_factory()
        backtester = task.backtester_factory(
            data_provider,
            task.fee_models,
            task.slippage_models,
        )

        # Run the backtest
        # Since backtest is async, we need to run it in an event loop
        result = asyncio.run(backtester.backtest(strategy, task.config))

        execution_time = time.time() - start_time
        logger.info(f"Worker {worker_pid}: Completed backtest {task.config_index} in {execution_time:.2f}s")

        return ParallelBacktestResult(
            config_index=task.config_index,
            config=task.config,
            result=result,
            success=True,
            worker_pid=worker_pid,
            execution_time_seconds=execution_time,
        )

    except Exception as e:
        execution_time = time.time() - start_time
        error_tb = traceback.format_exc()
        logger.error(f"Worker {worker_pid}: Failed backtest {task.config_index}: {e}")

        return ParallelBacktestResult(
            config_index=task.config_index,
            config=task.config,
            result=None,
            success=False,
            error=str(e),
            error_traceback=error_tb,
            worker_pid=worker_pid,
            execution_time_seconds=execution_time,
        )


async def run_parallel_backtests(
    configs: list[PnLBacktestConfig],
    strategy_factory: Callable[[], Any],
    data_provider_factory: Callable[[], Any],
    backtester_factory: Callable[[Any, dict[str, Any], dict[str, Any]], Any],
    fee_models: dict[str, Any] | None = None,
    slippage_models: dict[str, Any] | None = None,
    workers: int | None = None,
) -> list[ParallelBacktestResult]:
    """Run multiple backtests in parallel using a process pool.

    This function distributes backtest execution across multiple processes
    for improved performance on multi-core systems. Each backtest runs in
    its own process with its own instances of strategy, data provider, and
    backtester created via factory functions.

    Args:
        configs: List of PnLBacktestConfig objects to run
        strategy_factory: Factory function that returns a new strategy instance.
            Must be picklable (e.g., a module-level function).
        data_provider_factory: Factory function that returns a new data provider.
            Must be picklable (e.g., a module-level function).
        backtester_factory: Factory function that returns a new PnLBacktester.
            Takes (data_provider, fee_models, slippage_models) as arguments.
        fee_models: Optional dict of fee models to pass to backtester factory.
        slippage_models: Optional dict of slippage models to pass to backtester factory.
        workers: Number of worker processes. Defaults to CPU count - 1.

    Returns:
        List of ParallelBacktestResult in the same order as input configs.
        Each result indicates success or failure with associated data.

    Raises:
        ValueError: If configs list is empty

    Example:
        def create_strategy():
            return MyStrategy(param1=10, param2=0.5)

        def create_data_provider():
            return CoinGeckoDataProvider()

        def create_backtester(provider, fee_models, slippage_models):
            return PnLBacktester(provider, fee_models, slippage_models)

        results = await run_parallel_backtests(
            configs=[config1, config2, config3],
            strategy_factory=create_strategy,
            data_provider_factory=create_data_provider,
            backtester_factory=create_backtester,
            workers=4,
        )

    Note:
        - Factory functions must be picklable (module-level functions, not lambdas)
        - Each worker process creates its own instances to avoid sharing state
        - Results are returned in the same order as input configs
    """
    if not configs:
        raise ValueError("configs list cannot be empty")

    # Determine number of workers
    num_workers = workers if workers is not None else _get_default_workers()
    num_workers = min(num_workers, len(configs))  # Don't use more workers than configs

    logger.info(f"Starting parallel backtest with {len(configs)} configs using {num_workers} workers")

    # Create tasks
    tasks = [
        BacktestTask(
            config=config,
            config_index=i,
            strategy_factory=strategy_factory,
            data_provider_factory=data_provider_factory,
            backtester_factory=backtester_factory,
            fee_models=fee_models or {},
            slippage_models=slippage_models or {},
        )
        for i, config in enumerate(configs)
    ]

    # Track start time
    start_time = datetime.now()

    # Run in process pool
    loop = asyncio.get_event_loop()
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        # Submit all tasks
        futures = [loop.run_in_executor(executor, _run_single_backtest_worker, task) for task in tasks]

        # Wait for all to complete
        results = await asyncio.gather(*futures, return_exceptions=True)

    # Process results, handling any exceptions from gather
    processed_results: list[ParallelBacktestResult] = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            # Handle exceptions that occurred during gather
            processed_results.append(
                ParallelBacktestResult(
                    config_index=i,
                    config=configs[i],
                    result=None,
                    success=False,
                    error=str(result),
                    error_traceback="".join(traceback.format_exception(type(result), result, result.__traceback__)),
                    worker_pid=None,
                    execution_time_seconds=0.0,
                )
            )
        else:
            assert isinstance(result, ParallelBacktestResult)
            processed_results.append(result)

    # Sort by config_index to maintain input order
    processed_results.sort(key=lambda r: r.config_index)

    # Log summary
    total_time = (datetime.now() - start_time).total_seconds()
    successful = sum(1 for r in processed_results if r.success)
    failed = len(processed_results) - successful

    logger.info(f"Parallel backtest complete: {successful} succeeded, {failed} failed, total time: {total_time:.2f}s")

    return processed_results


def run_parallel_backtests_sync(
    configs: list[PnLBacktestConfig],
    strategy_factory: Callable[[], Any],
    data_provider_factory: Callable[[], Any],
    backtester_factory: Callable[[Any, dict[str, Any], dict[str, Any]], Any],
    fee_models: dict[str, Any] | None = None,
    slippage_models: dict[str, Any] | None = None,
    workers: int | None = None,
) -> list[ParallelBacktestResult]:
    """Synchronous wrapper for run_parallel_backtests.

    This is a convenience function for running parallel backtests from
    synchronous code. It creates a new event loop and runs the async
    function.

    Args:
        Same as run_parallel_backtests

    Returns:
        Same as run_parallel_backtests

    Example:
        results = run_parallel_backtests_sync(
            configs=[config1, config2],
            strategy_factory=create_strategy,
            data_provider_factory=create_data_provider,
            backtester_factory=create_backtester,
        )
    """
    return asyncio.run(
        run_parallel_backtests(
            configs=configs,
            strategy_factory=strategy_factory,
            data_provider_factory=data_provider_factory,
            backtester_factory=backtester_factory,
            fee_models=fee_models,
            slippage_models=slippage_models,
            workers=workers,
        )
    )


# Type alias for parameter ranges
# Values can be:
# - list: discrete choices (e.g., [3600, 7200, 14400])
# - tuple of two elements: continuous range (min, max) for random sampling
ParamRanges = dict[str, list[Any] | tuple[Any, Any]]


def generate_grid_configs(
    base_config: PnLBacktestConfig,
    param_ranges: ParamRanges,
) -> list[PnLBacktestConfig]:
    """Generate backtest configs using Cartesian product of parameter ranges.

    Creates all possible combinations of the specified parameter values,
    using the base config as a template for unspecified parameters.

    Args:
        base_config: Template configuration with default values
        param_ranges: Dictionary mapping config field names to lists of values.
            Each field will take on each value in its list, and all combinations
            are generated via Cartesian product.

    Returns:
        List of PnLBacktestConfig objects, one for each parameter combination.
        Order follows itertools.product ordering (rightmost parameter varies fastest).

    Raises:
        ValueError: If param_ranges is empty or contains invalid field names
        AttributeError: If a field name doesn't exist in PnLBacktestConfig

    Example:
        base = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 6, 1),
            initial_capital_usd=Decimal("10000"),
        )

        # Generate 6 configs (2 capitals x 3 intervals)
        param_ranges = {
            "initial_capital_usd": [Decimal("10000"), Decimal("50000")],
            "interval_seconds": [3600, 7200, 14400],
        }
        configs = generate_grid_configs(base, param_ranges)
        assert len(configs) == 6
    """
    if not param_ranges:
        raise ValueError("param_ranges cannot be empty")

    # Validate all field names exist in PnLBacktestConfig
    valid_fields = {f for f in vars(base_config) if not f.startswith("_")}
    for field_name in param_ranges:
        if field_name not in valid_fields:
            raise ValueError(f"Invalid field name '{field_name}'. Valid fields: {sorted(valid_fields)}")

    # Extract parameter names and value lists
    param_names = list(param_ranges.keys())
    param_values = []
    for name in param_names:
        values = param_ranges[name]
        if isinstance(values, tuple):
            # For tuples (ranges), treat as single value for grid search
            # (use generate_random_configs for sampling from ranges)
            raise ValueError(
                f"Parameter '{name}' has a tuple value (range). "
                "Grid search requires lists of discrete values. "
                "Use generate_random_configs for continuous ranges."
            )
        if not values:
            raise ValueError(f"Parameter '{name}' has empty value list")
        param_values.append(values)

    # Generate Cartesian product
    configs: list[PnLBacktestConfig] = []
    for combination in itertools.product(*param_values):
        # Create a copy of base config's data
        config_dict = base_config.to_dict()

        # Remove computed properties that shouldn't be passed to constructor
        for key in ["duration_seconds", "duration_days", "estimated_ticks"]:
            config_dict.pop(key, None)

        # Update with this combination's values
        for name, value in zip(param_names, combination, strict=True):
            config_dict[name] = value

        # Create new config
        configs.append(PnLBacktestConfig.from_dict(config_dict))

    logger.debug(f"Generated {len(configs)} grid configs from {len(param_names)} parameters: {param_names}")

    return configs


def generate_random_configs(
    base_config: PnLBacktestConfig,
    param_ranges: ParamRanges,
    n_samples: int,
    seed: int | None = None,
) -> list[PnLBacktestConfig]:
    """Generate backtest configs using random sampling from parameter ranges.

    Creates n_samples configurations by randomly sampling from the specified
    parameter ranges. Supports both discrete value lists and continuous ranges.

    Args:
        base_config: Template configuration with default values
        param_ranges: Dictionary mapping config field names to value ranges.
            - list: Discrete choices, one is randomly selected
            - tuple(min, max): Continuous range for uniform random sampling.
              For Decimal fields, samples uniformly between min and max.
              For int fields, samples uniformly between min and max (inclusive).
        n_samples: Number of random configurations to generate
        seed: Optional random seed for reproducibility

    Returns:
        List of n_samples PnLBacktestConfig objects with randomly sampled parameters.

    Raises:
        ValueError: If param_ranges is empty, n_samples < 1, or invalid field names
        AttributeError: If a field name doesn't exist in PnLBacktestConfig

    Example:
        base = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 6, 1),
            initial_capital_usd=Decimal("10000"),
        )

        # Sample 20 random configs
        param_ranges = {
            "initial_capital_usd": (Decimal("10000"), Decimal("100000")),  # Range
            "interval_seconds": [3600, 7200, 14400],  # Discrete choices
            "gas_price_gwei": (Decimal("10"), Decimal("50")),  # Range
        }
        configs = generate_random_configs(base, param_ranges, n_samples=20, seed=42)
        assert len(configs) == 20
    """
    if not param_ranges:
        raise ValueError("param_ranges cannot be empty")
    if n_samples < 1:
        raise ValueError("n_samples must be at least 1")

    # Validate all field names exist in PnLBacktestConfig
    valid_fields = {f for f in vars(base_config) if not f.startswith("_")}
    for field_name in param_ranges:
        if field_name not in valid_fields:
            raise ValueError(f"Invalid field name '{field_name}'. Valid fields: {sorted(valid_fields)}")

    # Set seed for reproducibility if provided
    rng = random.Random(seed)

    def sample_value(name: str, values: list[Any] | tuple[Any, Any]) -> Any:
        """Sample a single value from the parameter range."""
        if isinstance(values, tuple):
            # Continuous range: uniform sampling
            if len(values) != 2:
                raise ValueError(f"Parameter '{name}' tuple must have exactly 2 elements (min, max), got {len(values)}")
            min_val, max_val = values
            if isinstance(min_val, Decimal):
                # Decimal range: sample float, convert to Decimal
                sampled = rng.uniform(float(min_val), float(max_val))
                return Decimal(str(round(sampled, 6)))  # Reasonable precision
            elif isinstance(min_val, int):
                # Integer range: sample integer
                return rng.randint(min_val, max_val)
            elif isinstance(min_val, float):
                # Float range: sample float
                return rng.uniform(min_val, max_val)
            else:
                raise ValueError(
                    f"Parameter '{name}' range must be Decimal, int, or float, got {type(min_val).__name__}"
                )
        else:
            # Discrete choices: random selection
            if not values:
                raise ValueError(f"Parameter '{name}' has empty value list")
            return rng.choice(values)

    # Generate random samples
    configs: list[PnLBacktestConfig] = []
    for _ in range(n_samples):
        # Create a copy of base config's data
        config_dict = base_config.to_dict()

        # Remove computed properties that shouldn't be passed to constructor
        for key in ["duration_seconds", "duration_days", "estimated_ticks"]:
            config_dict.pop(key, None)

        # Sample and update each parameter
        for name, values in param_ranges.items():
            config_dict[name] = sample_value(name, values)

        # Create new config
        configs.append(PnLBacktestConfig.from_dict(config_dict))

    logger.debug(f"Generated {len(configs)} random configs from {len(param_ranges)} parameters (seed={seed})")

    return configs


@dataclass
class AggregatedSweepResults:
    """Aggregated results from a parameter sweep.

    Provides summary statistics and access to individual results.

    Attributes:
        results: List of all ParallelBacktestResult from the sweep
        total_count: Total number of backtests run
        success_count: Number of successful backtests
        failure_count: Number of failed backtests
        avg_sharpe: Average Sharpe ratio across successful results
        avg_return: Average total return across successful results
        best_sharpe_result: Result with highest Sharpe ratio
        best_return_result: Result with highest total return
        total_execution_time: Total wall clock time for all backtests
    """

    results: list[ParallelBacktestResult]
    total_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    avg_sharpe: Decimal = field(default_factory=lambda: Decimal("0"))
    avg_return: Decimal = field(default_factory=lambda: Decimal("0"))
    best_sharpe_result: ParallelBacktestResult | None = None
    best_return_result: ParallelBacktestResult | None = None
    total_execution_time: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "total_count": self.total_count,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "avg_sharpe": str(self.avg_sharpe),
            "avg_return": str(self.avg_return),
            "best_sharpe_config_index": self.best_sharpe_result.config_index if self.best_sharpe_result else None,
            "best_sharpe_value": str(self.best_sharpe_result.result.metrics.sharpe_ratio)
            if self.best_sharpe_result and self.best_sharpe_result.result
            else None,
            "best_return_config_index": self.best_return_result.config_index if self.best_return_result else None,
            "best_return_value": str(self.best_return_result.result.metrics.total_return_pct)
            if self.best_return_result and self.best_return_result.result
            else None,
            "total_execution_time": self.total_execution_time,
            "results": [r.to_dict() for r in self.results],
        }


def aggregate_results(results: list[ParallelBacktestResult]) -> AggregatedSweepResults:
    """Aggregate parallel backtest results into summary statistics.

    Combines multiple backtest results to provide summary metrics,
    identify best performing configurations, and calculate averages.

    Args:
        results: List of ParallelBacktestResult from run_parallel_backtests

    Returns:
        AggregatedSweepResults with summary statistics and best performers

    Example:
        results = await run_parallel_backtests(...)
        aggregated = aggregate_results(results)
        print(f"Success rate: {aggregated.success_count}/{aggregated.total_count}")
        print(f"Best Sharpe: {aggregated.best_sharpe_result.result.metrics.sharpe_ratio}")
    """
    if not results:
        return AggregatedSweepResults(results=[])

    total_count = len(results)
    successful = [r for r in results if r.success and r.result is not None]
    success_count = len(successful)
    failure_count = total_count - success_count

    # Calculate totals for execution time
    total_execution_time = sum(r.execution_time_seconds for r in results)

    # Calculate averages from successful results
    avg_sharpe = Decimal("0")
    avg_return = Decimal("0")
    best_sharpe_result: ParallelBacktestResult | None = None
    best_return_result: ParallelBacktestResult | None = None

    if successful:
        # Calculate averages
        sharpe_sum = sum(r.result.metrics.sharpe_ratio for r in successful if r.result)
        return_sum = sum(r.result.metrics.total_return_pct for r in successful if r.result)
        avg_sharpe = sharpe_sum / Decimal(success_count)
        avg_return = return_sum / Decimal(success_count)

        # Find best performers
        best_sharpe_result = max(
            successful,
            key=lambda r: r.result.metrics.sharpe_ratio if r.result else Decimal("-999"),
        )
        best_return_result = max(
            successful,
            key=lambda r: r.result.metrics.total_return_pct if r.result else Decimal("-999"),
        )

    logger.info(
        f"Aggregated {total_count} results: {success_count} succeeded, {failure_count} failed, "
        f"avg Sharpe: {avg_sharpe:.3f}, avg return: {avg_return:.2%}"
    )

    return AggregatedSweepResults(
        results=results,
        total_count=total_count,
        success_count=success_count,
        failure_count=failure_count,
        avg_sharpe=avg_sharpe,
        avg_return=avg_return,
        best_sharpe_result=best_sharpe_result,
        best_return_result=best_return_result,
        total_execution_time=total_execution_time,
    )


# Valid metrics for ranking results
RANKING_METRICS = {
    "sharpe_ratio",
    "sortino_ratio",
    "calmar_ratio",
    "total_return_pct",
    "annualized_return_pct",
    "max_drawdown_pct",
    "profit_factor",
    "win_rate",
    "net_pnl_usd",
    "total_trades",
}


def rank_results(
    results: list[ParallelBacktestResult],
    metric: str,
    ascending: bool = False,
) -> list[ParallelBacktestResult]:
    """Sort backtest results by a specified metric.

    Ranks successful backtest results by the specified performance metric.
    Failed results are placed at the end of the list.

    Args:
        results: List of ParallelBacktestResult to rank
        metric: Name of the BacktestMetrics field to sort by.
            Valid options: sharpe_ratio, sortino_ratio, calmar_ratio,
            total_return_pct, annualized_return_pct, max_drawdown_pct,
            profit_factor, win_rate, net_pnl_usd, total_trades
        ascending: If True, sort in ascending order (lower is better).
            Defaults to False (descending, higher is better).
            Use ascending=True for metrics like max_drawdown_pct where
            lower values are better.

    Returns:
        List of ParallelBacktestResult sorted by the specified metric.
        Successful results are sorted first, failed results at the end.

    Raises:
        ValueError: If metric is not a valid BacktestMetrics field

    Example:
        results = await run_parallel_backtests(...)

        # Rank by Sharpe ratio (higher is better)
        ranked_by_sharpe = rank_results(results, "sharpe_ratio")

        # Rank by max drawdown (lower is better)
        ranked_by_dd = rank_results(results, "max_drawdown_pct", ascending=True)

        # Get top 5 configs
        top_5 = ranked_by_sharpe[:5]
    """
    if metric not in RANKING_METRICS:
        raise ValueError(f"Invalid metric '{metric}'. Valid options: {sorted(RANKING_METRICS)}")

    # Separate successful and failed results
    successful = [r for r in results if r.success and r.result is not None]
    failed = [r for r in results if not r.success or r.result is None]

    def get_metric_value(r: ParallelBacktestResult) -> Decimal:
        """Extract metric value from result."""
        if not r.result:
            return Decimal("-999999") if not ascending else Decimal("999999")
        return getattr(r.result.metrics, metric)

    # Sort successful results by metric
    sorted_successful = sorted(
        successful,
        key=get_metric_value,
        reverse=not ascending,
    )

    # Combine: sorted successful first, then failed
    ranked = sorted_successful + failed

    logger.debug(f"Ranked {len(results)} results by {metric} ({'ascending' if ascending else 'descending'})")

    return ranked


async def run_parallel_backtests_with_progress(
    configs: list[PnLBacktestConfig],
    strategy_factory: Callable[[], Any],
    data_provider_factory: Callable[[], Any],
    backtester_factory: Callable[[Any, dict[str, Any], dict[str, Any]], Any],
    fee_models: dict[str, Any] | None = None,
    slippage_models: dict[str, Any] | None = None,
    workers: int | None = None,
    show_progress: bool = True,
    progress_desc: str = "Running backtests",
) -> list[ParallelBacktestResult]:
    """Run parallel backtests with tqdm progress bar.

    Enhanced version of run_parallel_backtests that displays a progress
    bar during execution. Useful for interactive sweep execution.

    Args:
        configs: List of PnLBacktestConfig objects to run
        strategy_factory: Factory function that returns a new strategy instance
        data_provider_factory: Factory function that returns a new data provider
        backtester_factory: Factory function that returns a new PnLBacktester
        fee_models: Optional dict of fee models to pass to backtester factory
        slippage_models: Optional dict of slippage models to pass to backtester factory
        workers: Number of worker processes. Defaults to CPU count - 1
        show_progress: Whether to show progress bar. Defaults to True
        progress_desc: Description text for progress bar

    Returns:
        List of ParallelBacktestResult in the same order as input configs

    Example:
        results = await run_parallel_backtests_with_progress(
            configs=configs,
            strategy_factory=create_strategy,
            data_provider_factory=create_provider,
            backtester_factory=create_backtester,
            progress_desc="Grid search",
        )
    """
    if not configs:
        raise ValueError("configs list cannot be empty")

    # Determine number of workers
    num_workers = workers if workers is not None else _get_default_workers()
    num_workers = min(num_workers, len(configs))

    logger.info(f"Starting parallel backtest with {len(configs)} configs using {num_workers} workers")

    # Create tasks
    tasks = [
        BacktestTask(
            config=config,
            config_index=i,
            strategy_factory=strategy_factory,
            data_provider_factory=data_provider_factory,
            backtester_factory=backtester_factory,
            fee_models=fee_models or {},
            slippage_models=slippage_models or {},
        )
        for i, config in enumerate(configs)
    ]

    # Track start time
    start_time = datetime.now()

    # Run in process pool with progress bar
    loop = asyncio.get_event_loop()
    processed_results: list[ParallelBacktestResult] = []

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        # Submit all tasks
        futures = [loop.run_in_executor(executor, _run_single_backtest_worker, task) for task in tasks]

        # Create progress bar if enabled
        if show_progress:
            pbar = tqdm(
                total=len(futures),
                desc=progress_desc,
                unit="backtest",
                ncols=100,
            )

        # Process results as they complete
        for coro in asyncio.as_completed(futures):
            result = await coro
            if isinstance(result, Exception):
                # Handle exceptions
                processed_results.append(
                    ParallelBacktestResult(
                        config_index=len(processed_results),
                        config=configs[len(processed_results)],
                        result=None,
                        success=False,
                        error=str(result),
                        error_traceback="".join(traceback.format_exception(type(result), result, result.__traceback__)),
                        worker_pid=None,
                        execution_time_seconds=0.0,
                    )
                )
            else:
                processed_results.append(result)

            if show_progress:
                # Update progress bar with success/fail indicator
                success_count = sum(1 for r in processed_results if r.success)
                pbar.set_postfix({"success": success_count, "fail": len(processed_results) - success_count})
                pbar.update(1)

        if show_progress:
            pbar.close()

    # Sort by config_index to maintain input order
    processed_results.sort(key=lambda r: r.config_index)

    # Log summary
    total_time = (datetime.now() - start_time).total_seconds()
    successful = sum(1 for r in processed_results if r.success)
    failed = len(processed_results) - successful

    logger.info(f"Parallel backtest complete: {successful} succeeded, {failed} failed, total time: {total_time:.2f}s")

    return processed_results


__all__ = [
    "BacktestTask",
    "ParallelBacktestResult",
    "AggregatedSweepResults",
    "run_parallel_backtests",
    "run_parallel_backtests_sync",
    "run_parallel_backtests_with_progress",
    "generate_grid_configs",
    "generate_random_configs",
    "aggregate_results",
    "rank_results",
    "RANKING_METRICS",
    "ParamRanges",
]
