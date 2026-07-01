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
        "trade_size_usd": [Decimal("100"), Decimal("500")],
        "interval_seconds": [3600, 7200],
    }
    grid_configs = generate_grid_configs(base_config, param_ranges)
    # Returns 4 configs: all combinations of trade size x interval

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


def _resolve_worker_count(config_count: int, workers: int | None) -> int:
    num_workers = workers if workers is not None else _get_default_workers()
    return min(num_workers, config_count)


def _build_backtest_tasks(
    configs: list[PnLBacktestConfig],
    strategy_factory: Callable[[], Any],
    data_provider_factory: Callable[[], Any],
    backtester_factory: Callable[[Any, dict[str, Any], dict[str, Any]], Any],
    fee_models: dict[str, Any] | None,
    slippage_models: dict[str, Any] | None,
) -> list[BacktestTask]:
    return [
        BacktestTask(
            config=config,
            config_index=index,
            strategy_factory=strategy_factory,
            data_provider_factory=data_provider_factory,
            backtester_factory=backtester_factory,
            fee_models=fee_models or {},
            slippage_models=slippage_models or {},
        )
        for index, config in enumerate(configs)
    ]


def _parallel_exception_result(
    config_index: int,
    config: PnLBacktestConfig,
    error: BaseException,
) -> ParallelBacktestResult:
    return ParallelBacktestResult(
        config_index=config_index,
        config=config,
        result=None,
        success=False,
        error=str(error),
        error_traceback="".join(traceback.format_exception(type(error), error, error.__traceback__)),
        worker_pid=None,
        execution_time_seconds=0.0,
    )


def _coerce_parallel_result(
    config_index: int,
    config: PnLBacktestConfig,
    result: ParallelBacktestResult | BaseException,
) -> ParallelBacktestResult:
    if isinstance(result, BaseException):
        return _parallel_exception_result(config_index, config, result)
    return result


def _ordered_parallel_results(results: list[ParallelBacktestResult]) -> list[ParallelBacktestResult]:
    return sorted(results, key=lambda result: result.config_index)


def _log_parallel_summary(
    results: list[ParallelBacktestResult],
    start_time: datetime,
) -> None:
    total_time = (datetime.now() - start_time).total_seconds()
    successful = sum(1 for result in results if result.success)
    failed = len(results) - successful
    logger.info(f"Parallel backtest complete: {successful} succeeded, {failed} failed, total time: {total_time:.2f}s")


async def _await_parallel_future(
    future: asyncio.Future,
    config_index: int,
    config: PnLBacktestConfig,
) -> ParallelBacktestResult:
    try:
        result = await future
    except Exception as exc:
        return _parallel_exception_result(config_index, config, exc)
    return _coerce_parallel_result(config_index, config, result)


def _progress_bar(
    show_progress: bool,
    total: int,
    progress_desc: str,
) -> Any | None:
    if not show_progress:
        return None
    return tqdm(
        total=total,
        desc=progress_desc,
        unit="backtest",
        ncols=100,
    )


def _update_progress_bar(
    pbar: Any | None,
    processed_results: list[ParallelBacktestResult],
) -> None:
    if pbar is None:
        return
    success_count = sum(1 for result in processed_results if result.success)
    pbar.set_postfix({"success": success_count, "fail": len(processed_results) - success_count})
    pbar.update(1)


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

    Factory functions must be picklable. Results are returned in input-config
    order, with worker or executor failures represented as failed results.
    """
    if not configs:
        raise ValueError("configs list cannot be empty")

    num_workers = _resolve_worker_count(len(configs), workers)
    logger.info(f"Starting parallel backtest with {len(configs)} configs using {num_workers} workers")

    tasks = _build_backtest_tasks(
        configs,
        strategy_factory,
        data_provider_factory,
        backtester_factory,
        fee_models,
        slippage_models,
    )
    start_time = datetime.now()
    loop = asyncio.get_event_loop()

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = [loop.run_in_executor(executor, _run_single_backtest_worker, task) for task in tasks]
        raw_results = await asyncio.gather(*futures, return_exceptions=True)

    processed_results = _ordered_parallel_results(
        [
            _coerce_parallel_result(index, config, result)
            for index, (config, result) in enumerate(zip(configs, raw_results, strict=True))
        ]
    )
    _log_parallel_summary(processed_results, start_time)

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

_COMPUTED_CONFIG_KEYS = ("duration_seconds", "duration_days", "estimated_ticks")


def _public_config_fields(base_config: PnLBacktestConfig) -> set[str]:
    return {field_name for field_name in vars(base_config) if not field_name.startswith("_")}


def _validate_param_fields(base_config: PnLBacktestConfig, param_ranges: ParamRanges) -> None:
    valid_fields = _public_config_fields(base_config)
    for field_name in param_ranges:
        if field_name not in valid_fields:
            raise ValueError(f"Invalid field name '{field_name}'. Valid fields: {sorted(valid_fields)}")


def _base_config_dict(base_config: PnLBacktestConfig) -> dict[str, Any]:
    config_dict = base_config.to_dict()
    for key in _COMPUTED_CONFIG_KEYS:
        config_dict.pop(key, None)
    return config_dict


def _config_with_params(
    base_config: PnLBacktestConfig,
    param_values: dict[str, Any],
) -> PnLBacktestConfig:
    config_dict = _base_config_dict(base_config)
    config_dict.update(param_values)
    return PnLBacktestConfig.from_dict(config_dict)


def _grid_values_for_param(name: str, values: list[Any] | tuple[Any, Any]) -> list[Any]:
    if isinstance(values, tuple):
        raise ValueError(
            f"Parameter '{name}' has a tuple value (range). "
            "Grid search requires lists of discrete values. "
            "Use generate_random_configs for continuous ranges."
        )
    if not values:
        raise ValueError(f"Parameter '{name}' has empty value list")
    return values


def _continuous_random_value(
    rng: random.Random,
    name: str,
    values: tuple[Any, Any],
) -> Any:
    if len(values) != 2:
        raise ValueError(f"Parameter '{name}' tuple must have exactly 2 elements (min, max), got {len(values)}")
    min_val, max_val = values
    if isinstance(min_val, Decimal):
        sampled = rng.uniform(float(min_val), float(max_val))
        return Decimal(str(round(sampled, 6)))
    if isinstance(min_val, int):
        return rng.randint(min_val, max_val)
    if isinstance(min_val, float):
        return rng.uniform(min_val, max_val)
    raise ValueError(f"Parameter '{name}' range must be Decimal, int, or float, got {type(min_val).__name__}")


def _random_value_for_param(
    rng: random.Random,
    name: str,
    values: list[Any] | tuple[Any, Any],
) -> Any:
    if isinstance(values, tuple):
        return _continuous_random_value(rng, name, values)
    if not values:
        raise ValueError(f"Parameter '{name}' has empty value list")
    return rng.choice(values)


def generate_grid_configs(
    base_config: PnLBacktestConfig,
    param_ranges: ParamRanges,
) -> list[PnLBacktestConfig]:
    """Generate configs from the Cartesian product of discrete parameter values."""
    if not param_ranges:
        raise ValueError("param_ranges cannot be empty")

    _validate_param_fields(base_config, param_ranges)

    param_names = list(param_ranges.keys())
    param_values = [_grid_values_for_param(name, param_ranges[name]) for name in param_names]
    configs = [
        _config_with_params(base_config, dict(zip(param_names, combination, strict=True)))
        for combination in itertools.product(*param_values)
    ]

    logger.debug(f"Generated {len(configs)} grid configs from {len(param_names)} parameters: {param_names}")

    return configs


def generate_random_configs(
    base_config: PnLBacktestConfig,
    param_ranges: ParamRanges,
    n_samples: int,
    seed: int | None = None,
) -> list[PnLBacktestConfig]:
    """Generate configs by seeded random sampling from discrete or range values."""
    if not param_ranges:
        raise ValueError("param_ranges cannot be empty")
    if n_samples < 1:
        raise ValueError("n_samples must be at least 1")

    _validate_param_fields(base_config, param_ranges)
    rng = random.Random(seed)
    configs = [
        _config_with_params(
            base_config,
            {name: _random_value_for_param(rng, name, values) for name, values in param_ranges.items()},
        )
        for _ in range(n_samples)
    ]

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
        f"avg Sharpe: {avg_sharpe:.3f}, avg return: {avg_return:.2f}%"
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


def _successful_backtest_results(results: list[ParallelBacktestResult]) -> list[ParallelBacktestResult]:
    return [result for result in results if result.success and result.result is not None]


def _failed_backtest_results(results: list[ParallelBacktestResult]) -> list[ParallelBacktestResult]:
    return [result for result in results if not result.success or result.result is None]


def _result_metric_value(result: ParallelBacktestResult, metric: str) -> Decimal:
    assert result.result is not None
    return getattr(result.result.metrics, metric)


def rank_results(
    results: list[ParallelBacktestResult],
    metric: str,
    ascending: bool = False,
) -> list[ParallelBacktestResult]:
    """Sort backtest results by a specified metric.

    Successful results are sorted first; failures stay at the end in their
    original relative order.
    """
    if metric not in RANKING_METRICS:
        raise ValueError(f"Invalid metric '{metric}'. Valid options: {sorted(RANKING_METRICS)}")

    sorted_successful = sorted(
        _successful_backtest_results(results),
        key=lambda result: _result_metric_value(result, metric),
        reverse=not ascending,
    )
    ranked = sorted_successful + _failed_backtest_results(results)

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

    Worker and executor failures are captured as failed results while the
    progress bar still advances and closes.
    """
    if not configs:
        raise ValueError("configs list cannot be empty")

    num_workers = _resolve_worker_count(len(configs), workers)
    logger.info(f"Starting parallel backtest with {len(configs)} configs using {num_workers} workers")

    tasks = _build_backtest_tasks(
        configs,
        strategy_factory,
        data_provider_factory,
        backtester_factory,
        fee_models,
        slippage_models,
    )
    start_time = datetime.now()
    loop = asyncio.get_event_loop()
    processed_results: list[ParallelBacktestResult] = []

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = [
            _await_parallel_future(
                loop.run_in_executor(executor, _run_single_backtest_worker, task),
                task.config_index,
                task.config,
            )
            for task in tasks
        ]
        pbar = _progress_bar(show_progress, len(futures), progress_desc)
        try:
            for coro in asyncio.as_completed(futures):
                result = await coro
                processed_results.append(result)
                _update_progress_bar(pbar, processed_results)
        finally:
            if pbar is not None:
                pbar.close()

    ordered_results = _ordered_parallel_results(processed_results)
    _log_parallel_summary(ordered_results, start_time)

    return ordered_results


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
