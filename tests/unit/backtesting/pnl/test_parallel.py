"""Unit tests for parallel PnL backtest orchestration helpers."""

from __future__ import annotations
from tests.backtesting_funding import pnl_token_funding as _pnl_token_funding

from concurrent.futures import Future
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.models import (


    BacktestEngine,
    BacktestMetrics,
    BacktestResult,
)
from almanak.framework.backtesting.pnl import parallel as parallel_module
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.parallel import (
    AggregatedSweepResults,
    ParallelBacktestResult,
    aggregate_results,
    generate_grid_configs,
    generate_random_configs,
    rank_results,
    run_parallel_backtests,
    run_parallel_backtests_with_progress,
)

START = datetime(2024, 1, 1, tzinfo=UTC)
END = datetime(2024, 1, 2, tzinfo=UTC)


def _config(
    *,
    initial_capital_usd: Decimal = Decimal("10000"),
    interval_seconds: int = 3600,
    fee_model: str = "realistic",
) -> PnLBacktestConfig:
    return PnLBacktestConfig(
        start_time=START,
        end_time=END,
        token_funding=_pnl_token_funding(initial_capital_usd),
        interval_seconds=interval_seconds,
        fee_model=fee_model,
        include_gas_costs=False,
    )


def _backtest_result(
    *,
    sharpe_ratio: Decimal,
    total_return_pct: Decimal,
    deployment_id: str = "test",
) -> BacktestResult:
    return BacktestResult(
        engine=BacktestEngine.PNL,
        deployment_id=deployment_id,
        start_time=START,
        end_time=END,
        metrics=BacktestMetrics(
            sharpe_ratio=sharpe_ratio,
            total_return_pct=total_return_pct,
            max_drawdown_pct=Decimal("0.1"),
            total_trades=3,
        ),
    )


def _parallel_result(
    index: int,
    *,
    sharpe_ratio: Decimal = Decimal("1"),
    total_return_pct: Decimal = Decimal("10"),
    success: bool = True,
    error: str | None = None,
) -> ParallelBacktestResult:
    config = _config(initial_capital_usd=Decimal("10000") + Decimal(index))
    return ParallelBacktestResult(
        config_index=index,
        config=config,
        result=_backtest_result(
            sharpe_ratio=sharpe_ratio,
            total_return_pct=total_return_pct,
            deployment_id=f"run-{index}",
        )
        if success
        else None,
        success=success,
        error=error,
        worker_pid=1000 + index,
        execution_time_seconds=1.5 + index,
    )


def _strategy_factory() -> object:
    return object()


def _data_provider_factory() -> object:
    return object()


def _backtester_factory(
    _provider: object,
    _fee_models: dict[str, Any],
    _slippage_models: dict[str, Any],
) -> object:
    return object()


class InlineExecutor:
    """Executor compatible with loop.run_in_executor, but synchronous."""

    instances: list[InlineExecutor] = []

    def __init__(self, max_workers: int) -> None:
        self.max_workers = max_workers
        self.submitted: list[tuple[Any, tuple[Any, ...]]] = []
        self.instances.append(self)

    def __enter__(self) -> InlineExecutor:
        return self

    def __exit__(self, *_exc_info: object) -> None:
        return None

    def submit(self, fn: Any, *args: Any) -> Future:
        self.submitted.append((fn, args))
        future: Future = Future()
        try:
            future.set_result(fn(*args))
        except Exception as exc:  # pragma: no cover - asserted through async wrapper
            future.set_exception(exc)
        return future


class FakeTqdm:
    instances: list[FakeTqdm] = []

    def __init__(self, *, total: int, desc: str, unit: str, ncols: int) -> None:
        self.total = total
        self.desc = desc
        self.unit = unit
        self.ncols = ncols
        self.updates = 0
        self.postfixes: list[dict[str, int]] = []
        self.closed = False
        self.instances.append(self)

    def set_postfix(self, values: dict[str, int]) -> None:
        self.postfixes.append(values)

    def update(self, amount: int) -> None:
        self.updates += amount

    def close(self) -> None:
        self.closed = True


@pytest.fixture(autouse=True)
def reset_fakes() -> None:
    InlineExecutor.instances.clear()
    FakeTqdm.instances.clear()


class TestConfigGeneration:
    def test_generate_grid_configs_cartesian_product(self) -> None:
        funding_100 = _pnl_token_funding(Decimal("100"))
        funding_200 = _pnl_token_funding(Decimal("200"))
        configs = generate_grid_configs(
            _config(),
            {
                "token_funding": [funding_100, funding_200],
                "interval_seconds": [60, 120],
            },
        )

        assert [(c.token_funding, c.interval_seconds) for c in configs] == [
            (funding_100, 60),
            (funding_100, 120),
            (funding_200, 60),
            (funding_200, 120),
        ]

    @pytest.mark.parametrize(
        ("param_ranges", "match"),
        [
            ({}, "param_ranges cannot be empty"),
            ({"missing_field": [1]}, "Invalid field name"),
            ({"interval_seconds": []}, "empty value list"),
            ({"interval_seconds": (60, 120)}, "Grid search requires lists"),
        ],
    )
    def test_generate_grid_configs_rejects_invalid_inputs(
        self,
        param_ranges: dict[str, Any],
        match: str,
    ) -> None:
        with pytest.raises(ValueError, match=match):
            generate_grid_configs(_config(), param_ranges)

    def test_generate_random_configs_seeded_sampling(self) -> None:
        funding_values = [_pnl_token_funding(Decimal("100")), _pnl_token_funding(Decimal("200"))]
        configs = generate_random_configs(
            _config(),
            {
                "token_funding": funding_values,
                "interval_seconds": (60, 120),
                "fee_model": ["zero", "realistic"],
            },
            n_samples=3,
            seed=7,
        )
        repeated = generate_random_configs(
            _config(),
            {
                "token_funding": funding_values,
                "interval_seconds": (60, 120),
                "fee_model": ["zero", "realistic"],
            },
            n_samples=3,
            seed=7,
        )

        assert [config.to_dict() for config in configs] == [config.to_dict() for config in repeated]
        assert len(configs) == 3
        assert all(config.token_funding in funding_values for config in configs)
        assert all(60 <= config.interval_seconds <= 120 for config in configs)
        assert {config.fee_model for config in configs} <= {"zero", "realistic"}

    @pytest.mark.parametrize(
        ("param_ranges", "n_samples", "match"),
        [
            ({}, 1, "param_ranges cannot be empty"),
            ({"interval_seconds": [60]}, 0, "n_samples must be at least 1"),
            ({"missing_field": [1]}, 1, "Invalid field name"),
            ({"interval_seconds": (60, 120, 180)}, 1, "exactly 2 elements"),
            ({"interval_seconds": ("low", "high")}, 1, "range must be Decimal, int, or float"),
            ({"interval_seconds": []}, 1, "empty value list"),
        ],
    )
    def test_generate_random_configs_rejects_invalid_inputs(
        self,
        param_ranges: dict[str, Any],
        n_samples: int,
        match: str,
    ) -> None:
        with pytest.raises(ValueError, match=match):
            generate_random_configs(_config(), param_ranges, n_samples=n_samples)


class TestAggregationAndRanking:
    def test_aggregate_results_counts_averages_best_and_serializes(self) -> None:
        results = [
            _parallel_result(0, sharpe_ratio=Decimal("1.5"), total_return_pct=Decimal("10")),
            _parallel_result(1, success=False, error="boom"),
            _parallel_result(2, sharpe_ratio=Decimal("2.5"), total_return_pct=Decimal("5")),
        ]

        aggregated = aggregate_results(results)
        data = aggregated.to_dict()

        assert aggregated.total_count == 3
        assert aggregated.success_count == 2
        assert aggregated.failure_count == 1
        assert aggregated.avg_sharpe == Decimal("2.0")
        assert aggregated.avg_return == Decimal("7.5")
        assert aggregated.best_sharpe_result is results[2]
        assert aggregated.best_return_result is results[0]
        assert data["best_sharpe_config_index"] == 2
        assert data["best_return_config_index"] == 0
        assert len(data["results"]) == 3

    def test_aggregate_results_empty(self) -> None:
        aggregated = aggregate_results([])

        assert aggregated == AggregatedSweepResults(results=[])
        assert aggregated.to_dict()["results"] == []

    def test_rank_results_sorts_successes_and_keeps_failures_last(self) -> None:
        low = _parallel_result(0, sharpe_ratio=Decimal("0.5"), total_return_pct=Decimal("2"))
        failed = _parallel_result(1, success=False, error="boom")
        high = _parallel_result(2, sharpe_ratio=Decimal("2.0"), total_return_pct=Decimal("4"))

        assert rank_results([low, failed, high], "sharpe_ratio") == [high, low, failed]
        assert rank_results([low, failed, high], "sharpe_ratio", ascending=True) == [low, high, failed]

    def test_rank_results_rejects_unknown_metric(self) -> None:
        with pytest.raises(ValueError, match="Invalid metric"):
            rank_results([_parallel_result(0)], "not_a_metric")


class TestParallelExecution:
    @pytest.mark.asyncio
    async def test_run_parallel_backtests_orders_results_and_captures_errors(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def worker(task: parallel_module.BacktestTask) -> ParallelBacktestResult:
            if task.config_index == 1:
                raise RuntimeError("worker boom")
            return _parallel_result(task.config_index)

        monkeypatch.setattr(parallel_module, "ProcessPoolExecutor", InlineExecutor)
        monkeypatch.setattr(parallel_module, "_run_single_backtest_worker", worker)

        results = await run_parallel_backtests(
            configs=[_config(), _config(), _config()],
            strategy_factory=_strategy_factory,
            data_provider_factory=_data_provider_factory,
            backtester_factory=_backtester_factory,
            workers=8,
        )

        assert [result.config_index for result in results] == [0, 1, 2]
        assert [result.success for result in results] == [True, False, True]
        assert results[1].error == "worker boom"
        assert InlineExecutor.instances[0].max_workers == 3

    @pytest.mark.asyncio
    async def test_run_parallel_backtests_rejects_empty_configs(self) -> None:
        with pytest.raises(ValueError, match="configs list cannot be empty"):
            await run_parallel_backtests(
                configs=[],
                strategy_factory=_strategy_factory,
                data_provider_factory=_data_provider_factory,
                backtester_factory=_backtester_factory,
            )

    @pytest.mark.asyncio
    async def test_run_parallel_backtests_with_progress_captures_future_exception(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def worker(task: parallel_module.BacktestTask) -> ParallelBacktestResult:
            if task.config_index == 1:
                raise RuntimeError("progress boom")
            return _parallel_result(task.config_index)

        monkeypatch.setattr(parallel_module, "ProcessPoolExecutor", InlineExecutor)
        monkeypatch.setattr(parallel_module, "_run_single_backtest_worker", worker)
        monkeypatch.setattr(parallel_module, "tqdm", FakeTqdm)

        results = await run_parallel_backtests_with_progress(
            configs=[_config(), _config()],
            strategy_factory=_strategy_factory,
            data_provider_factory=_data_provider_factory,
            backtester_factory=_backtester_factory,
            workers=4,
            show_progress=True,
            progress_desc="Test progress",
        )

        assert [result.config_index for result in results] == [0, 1]
        assert [result.success for result in results] == [True, False]
        assert results[1].error == "progress boom"
        assert InlineExecutor.instances[0].max_workers == 2
        assert FakeTqdm.instances[0].desc == "Test progress"
        assert FakeTqdm.instances[0].updates == 2
        assert FakeTqdm.instances[0].postfixes[-1] == {"success": 1, "fail": 1}
        assert FakeTqdm.instances[0].closed is True

    @pytest.mark.asyncio
    async def test_run_parallel_backtests_with_progress_can_hide_progress(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(parallel_module, "ProcessPoolExecutor", InlineExecutor)
        monkeypatch.setattr(
            parallel_module,
            "_run_single_backtest_worker",
            lambda task: _parallel_result(task.config_index),
        )
        monkeypatch.setattr(parallel_module, "tqdm", FakeTqdm)

        results = await run_parallel_backtests_with_progress(
            configs=[_config()],
            strategy_factory=_strategy_factory,
            data_provider_factory=_data_provider_factory,
            backtester_factory=_backtester_factory,
            show_progress=False,
        )

        assert len(results) == 1
        assert FakeTqdm.instances == []

    @pytest.mark.asyncio
    async def test_run_parallel_backtests_with_progress_rejects_empty_configs(self) -> None:
        with pytest.raises(ValueError, match="configs list cannot be empty"):
            await run_parallel_backtests_with_progress(
                configs=[],
                strategy_factory=_strategy_factory,
                data_provider_factory=_data_provider_factory,
                backtester_factory=_backtester_factory,
            )
