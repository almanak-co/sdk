"""Performance contract for the production parameter-sweep path.

The comparative PnL workloads live in ``scripts/ci/run_benchmarks.py``. This
module keeps the one distinct performance surface that the harness does not
cover: a 100-configuration sweep through the real multiprocessing API.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.engine import DefaultFeeModel, DefaultSlippageModel, PnLBacktester
from almanak.framework.backtesting.pnl.parallel import aggregate_results, generate_grid_configs, run_parallel_backtests
from scripts.ci.run_benchmarks import FastMockDataProvider, SimpleSwapStrategy
from tests.backtesting_funding import pnl_token_funding

pytestmark = pytest.mark.benchmark

_BENCHMARK_START_TIME = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
_PARAMETER_SWEEP_SIZE = 100
_PARAMETER_SWEEP_SLA_SECONDS = 30 * 60


def _create_benchmark_strategy() -> SimpleSwapStrategy:
    """Return fresh, pickle-safe strategy state for each worker task."""
    return SimpleSwapStrategy(swap_interval=24)


def _create_benchmark_data_provider() -> FastMockDataProvider:
    """Return a deterministic, network-free provider for each worker task."""
    return FastMockDataProvider(
        base_prices={"WETH": Decimal("3000"), "USDC": Decimal("1")},
        start_time=_BENCHMARK_START_TIME,
        volatility=Decimal("0.002"),
    )


def _create_benchmark_backtester(
    data_provider: Any,
    fee_models: dict[str, Any],
    slippage_models: dict[str, Any],
) -> PnLBacktester:
    """Construct the engine through the same factory contract as real sweeps."""
    return PnLBacktester(
        data_provider=data_provider,
        fee_models=fee_models or {"default": DefaultFeeModel()},
        slippage_models=slippage_models or {"default": DefaultSlippageModel()},
    )


@pytest.mark.asyncio
async def test_100_parameter_parallel_sweep_under_30_minutes() -> None:
    """A four-worker, 100-configuration monthly sweep meets its SLA."""
    base_config = PnLBacktestConfig(
        start_time=_BENCHMARK_START_TIME,
        end_time=datetime(2024, 2, 1, 0, 0, 0, tzinfo=UTC),
        interval_seconds=3600,
        token_funding=pnl_token_funding(Decimal("10000")),
        tokens=["WETH", "USDC"],
        include_gas_costs=True,
        inclusion_delay_blocks=0,
    )
    param_ranges = {
        "token_funding": [pnl_token_funding(Decimal(str(5000 + index * 1000))) for index in range(10)],
        "interval_seconds": [900 + index * 300 for index in range(10)],
    }
    configs = generate_grid_configs(base_config, param_ranges)
    assert len(configs) == _PARAMETER_SWEEP_SIZE

    started_at = time.perf_counter()
    results = await run_parallel_backtests(
        configs=configs,
        strategy_factory=_create_benchmark_strategy,
        data_provider_factory=_create_benchmark_data_provider,
        backtester_factory=_create_benchmark_backtester,
        workers=4,
    )
    elapsed = time.perf_counter() - started_at
    aggregate = aggregate_results(results)

    assert len(results) == _PARAMETER_SWEEP_SIZE
    assert aggregate.success_count == _PARAMETER_SWEEP_SIZE, (
        f"{aggregate.failure_count}/{aggregate.total_count} configurations failed; "
        "the parameter sweep must complete every configured workload"
    )
    assert all(result.result.metrics is not None for result in results if result.success and result.result)
    assert elapsed < _PARAMETER_SWEEP_SLA_SECONDS, (
        f"100-parameter sweep took {elapsed:.1f}s ({elapsed / 60:.1f} minutes), "
        f"exceeding the {_PARAMETER_SWEEP_SLA_SECONDS / 60:.0f}-minute SLA"
    )
