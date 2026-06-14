"""Unit tests for the phase helpers extracted from `sweep_backtest`.

Phase 5B.3 breaks `sweep_backtest` (previously CC=57) into module-level
helpers. These tests pin the behavioural contracts that are load-bearing:

- `_parse_sweep_params`: numeric + categorical names, empty tuple error,
  bad-format errors surface as `click.UsageError`.
- `_resolve_backtest_periods`: mutual exclusion, single-period path,
  multi-period path, missing-args error.
- `_compute_worker_count`: parallel cpu-default, async 4-default, cap at
  total_runs, explicit override.
- `_handle_sweep_dry_run`: multi-period layout + single-period layout, final
  "Dry run - no backtests executed." banner.
- `_write_sweep_json`: schema preservation (keys, `_meta`, `aggregated`,
  `best_params`, no-op on None output_path).
- `build_pnl_config`: sweep-only kwargs plumb through, pnl callers can omit
  them and receive dataclass defaults.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import click
import pytest

from almanak.framework.backtesting.models import (
    BacktestEngine,
    BacktestMetrics,
    BacktestResult,
)
from almanak.framework.cli.backtest.helpers import SweepParameter, SweepResult
from almanak.framework.cli.backtest.run_helpers import build_pnl_config
from almanak.framework.cli.backtest.sweep import (
    _compute_worker_count,
    _handle_sweep_dry_run,
    _parse_sweep_params,
    _resolve_backtest_periods,
    _SweepRunContext,
    _write_sweep_json,
)

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _make_period(name: str, y: int = 2024, m1: int = 1, m2: int = 2) -> Any:
    from almanak.framework.backtesting.pnl.periods import BacktestPeriod

    return BacktestPeriod(
        name=name,
        start=datetime(y, m1, 1, tzinfo=UTC),
        end=datetime(y, m2, 1, tzinfo=UTC),
    )


def _make_result(
    params: dict[str, str],
    sharpe: float = 1.0,
    period_name: str = "",
) -> SweepResult:
    """Build a SweepResult with a minimal BacktestResult inside."""
    metrics = BacktestMetrics(
        total_return_pct=Decimal("5.0"),
        sharpe_ratio=Decimal(str(sharpe)),
        max_drawdown_pct=Decimal("2.0"),
        win_rate=Decimal("0.6"),
        total_trades=1,
        net_pnl_usd=Decimal("123.45"),
    )
    bt = BacktestResult(
        engine=BacktestEngine.PNL,
        deployment_id="test",
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 2, 1, tzinfo=UTC),
        metrics=metrics,
    )
    return SweepResult(
        params=params,
        result=bt,
        sharpe_ratio=Decimal(str(sharpe)),
        total_return_pct=Decimal("5.0"),
        max_drawdown_pct=Decimal("2.0"),
        win_rate=Decimal("0.6"),
        total_trades=1,
        period_name=period_name,
    )


def _make_ctx(
    *,
    sweep_params: list[SweepParameter],
    combinations: list[dict[str, str]],
    periods: list[Any],
    multi: bool = False,
    output_path: Path | None = None,
    strategy: str = "demo_strat",
    chain: str = "arbitrum",
) -> _SweepRunContext:
    return _SweepRunContext(
        strategy=strategy,
        chain=chain,
        token_list=["WETH", "USDC"],
        interval=3600,
        initial_capital=10000.0,
        output_path=output_path,
        multi_period_mode=multi,
        backtest_periods=periods,
        sweep_params=sweep_params,
        combinations=combinations,
        periods_spec="2024-quarterly" if multi else None,
    )


# ---------------------------------------------------------------------------
# _parse_sweep_params
# ---------------------------------------------------------------------------


class TestParseSweepParams:
    def test_single_numeric_param(self) -> None:
        result = _parse_sweep_params(("threshold:0.01,0.02,0.03",))
        assert len(result) == 1
        assert result[0].name == "threshold"
        assert result[0].values == ["0.01", "0.02", "0.03"]

    def test_multiple_params(self) -> None:
        result = _parse_sweep_params(("window:10,20", "threshold:0.5,1.0"))
        assert len(result) == 2
        assert result[0].name == "window"
        assert result[1].name == "threshold"

    def test_categorical_values(self) -> None:
        result = _parse_sweep_params(("mode:aggressive,conservative",))
        assert result[0].values == ["aggressive", "conservative"]

    def test_whitespace_stripped(self) -> None:
        result = _parse_sweep_params(("window: 10 , 20 , 30 ",))
        assert result[0].values == ["10", "20", "30"]

    def test_empty_tuple_raises_usage_error(self) -> None:
        with pytest.raises(click.UsageError, match="At least one --param"):
            _parse_sweep_params(())

    def test_missing_colon_raises_usage_error(self) -> None:
        with pytest.raises(click.UsageError, match="Invalid parameter format"):
            _parse_sweep_params(("no_colon_here",))

    def test_empty_values_raises_usage_error(self) -> None:
        with pytest.raises(click.UsageError, match="no values"):
            _parse_sweep_params(("threshold:",))


# ---------------------------------------------------------------------------
# _resolve_backtest_periods
# ---------------------------------------------------------------------------


class TestResolveBacktestPeriods:
    def test_single_period_from_start_end(self) -> None:
        start = datetime(2024, 1, 1, tzinfo=UTC)
        end = datetime(2024, 2, 1, tzinfo=UTC)
        multi, periods = _resolve_backtest_periods(None, start, end)
        assert multi is False
        assert len(periods) == 1
        assert periods[0].name == "single"
        assert periods[0].start == start
        assert periods[0].end == end

    def test_multi_period_from_preset(self) -> None:
        multi, periods = _resolve_backtest_periods("2024-quarterly", None, None)
        assert multi is True
        assert len(periods) == 4  # 4 quarters

    def test_periods_and_start_end_conflict(self) -> None:
        start = datetime(2024, 1, 1, tzinfo=UTC)
        with pytest.raises(click.UsageError, match="Cannot use --periods together"):
            _resolve_backtest_periods("2024-quarterly", start, None)

    def test_missing_start_raises(self) -> None:
        with pytest.raises(click.UsageError, match="Either --start and --end"):
            _resolve_backtest_periods(None, None, datetime(2024, 2, 1, tzinfo=UTC))

    def test_missing_end_raises(self) -> None:
        with pytest.raises(click.UsageError, match="Either --start and --end"):
            _resolve_backtest_periods(None, datetime(2024, 1, 1, tzinfo=UTC), None)

    def test_missing_both_raises(self) -> None:
        with pytest.raises(click.UsageError, match="Either --start and --end"):
            _resolve_backtest_periods(None, None, None)

    def test_invalid_periods_spec_raises_usage_error(self) -> None:
        with pytest.raises(click.UsageError):
            _resolve_backtest_periods("not-a-real-preset", None, None)


# ---------------------------------------------------------------------------
# _compute_worker_count
# ---------------------------------------------------------------------------


class TestComputeWorkerCount:
    def test_async_default_4(self) -> None:
        assert _compute_worker_count(parallel=False, workers=None, total_runs=100) == 4

    def test_async_explicit_override(self) -> None:
        assert _compute_worker_count(parallel=False, workers=8, total_runs=100) == 8

    def test_parallel_default_from_cpu_count(self) -> None:
        with patch("os.cpu_count", return_value=16):
            # default = max(1, 16-1) = 15, capped at total_runs=100 -> 15
            assert _compute_worker_count(parallel=True, workers=None, total_runs=100) == 15

    def test_parallel_caps_at_total_runs(self) -> None:
        with patch("os.cpu_count", return_value=16):
            # default = 15, but only 3 runs -> capped at 3
            assert _compute_worker_count(parallel=True, workers=None, total_runs=3) == 3

    def test_parallel_explicit_caps_at_total_runs(self) -> None:
        # workers=16 explicit, but total_runs=4 -> capped at 4
        assert _compute_worker_count(parallel=True, workers=16, total_runs=4) == 4

    def test_parallel_explicit_under_cap(self) -> None:
        assert _compute_worker_count(parallel=True, workers=2, total_runs=20) == 2

    def test_parallel_cpu_count_none_falls_back_to_1(self) -> None:
        with patch("os.cpu_count", return_value=None):
            # max(1, 1-1) = 1
            assert _compute_worker_count(parallel=True, workers=None, total_runs=10) == 1


# ---------------------------------------------------------------------------
# _handle_sweep_dry_run
# ---------------------------------------------------------------------------


class TestHandleSweepDryRun:
    def test_single_period_dry_run_output(self, capsys: pytest.CaptureFixture[str]) -> None:
        ctx = _make_ctx(
            sweep_params=[SweepParameter(name="a", values=["1", "2"])],
            combinations=[{"a": "1"}, {"a": "2"}],
            periods=[_make_period("single")],
        )
        result = _handle_sweep_dry_run(ctx)
        assert result is True
        captured = capsys.readouterr()
        assert "Parameter combinations (dry run):" in captured.out
        assert "1. a=1" in captured.out
        assert "2. a=2" in captured.out
        assert "Dry run - no backtests executed." in captured.out

    def test_multi_period_dry_run_output(self, capsys: pytest.CaptureFixture[str]) -> None:
        ctx = _make_ctx(
            sweep_params=[SweepParameter(name="a", values=["1", "2"])],
            combinations=[{"a": "1"}, {"a": "2"}],
            periods=[_make_period("p1"), _make_period("p2", m1=3, m2=4)],
            multi=True,
        )
        result = _handle_sweep_dry_run(ctx)
        assert result is True
        captured = capsys.readouterr()
        # total runs = 2 combos x 2 periods = 4
        assert "dry run, 4 total" in captured.out
        # each combo repeated for each period
        assert "a=1  |  p1" in captured.out
        assert "a=1  |  p2" in captured.out
        assert "a=2  |  p1" in captured.out
        assert "a=2  |  p2" in captured.out

    def test_returns_true_always(self) -> None:
        ctx = _make_ctx(
            sweep_params=[SweepParameter(name="a", values=["1"])],
            combinations=[{"a": "1"}],
            periods=[_make_period("single")],
        )
        assert _handle_sweep_dry_run(ctx) is True


# ---------------------------------------------------------------------------
# _write_sweep_json
# ---------------------------------------------------------------------------


class TestWriteSweepJson:
    def test_no_op_when_output_path_is_none(self, tmp_path: Path) -> None:
        ctx = _make_ctx(
            sweep_params=[SweepParameter(name="a", values=["1"])],
            combinations=[{"a": "1"}],
            periods=[_make_period("single")],
            output_path=None,
        )
        _write_sweep_json(ctx, [_make_result({"a": "1"})])
        # Nothing was written, directory should still be empty
        assert list(tmp_path.iterdir()) == []

    def test_single_period_schema(self, tmp_path: Path) -> None:
        out = tmp_path / "results.json"
        ctx = _make_ctx(
            sweep_params=[SweepParameter(name="a", values=["1", "2"])],
            combinations=[{"a": "1"}, {"a": "2"}],
            periods=[_make_period("single")],
            output_path=out,
        )
        results = [
            _make_result({"a": "1"}, sharpe=1.5, period_name="single"),
            _make_result({"a": "2"}, sharpe=0.8, period_name="single"),
        ]
        _write_sweep_json(ctx, results)

        data = json.loads(out.read_text())
        assert set(data.keys()) >= {"sweep_config", "results", "_meta", "best_params"}
        assert data["sweep_config"]["strategy"] == "demo_strat"
        assert data["sweep_config"]["chain"] == "arbitrum"
        assert data["sweep_config"]["multi_period"] is False
        assert data["sweep_config"]["total_combinations"] == 2
        assert data["sweep_config"]["parameters"] == [{"name": "a", "values": ["1", "2"]}]
        # best_params should be the one with higher Sharpe (a=1)
        assert data["best_params"] == {"a": "1"}
        # results preserves fields
        assert len(data["results"]) == 2
        assert data["results"][0]["params"] == {"a": "1"}
        assert data["results"][0]["sharpe_ratio"] == "1.5"
        assert data["_meta"]["engine"] == "pnl"
        assert data["_meta"]["generator"] == "almanak backtest sweep"
        # No aggregated key for single-period
        assert "aggregated" not in data

    def test_multi_period_includes_aggregated_and_best(self, tmp_path: Path) -> None:
        out = tmp_path / "multi.json"
        ctx = _make_ctx(
            sweep_params=[SweepParameter(name="a", values=["1", "2"])],
            combinations=[{"a": "1"}, {"a": "2"}],
            periods=[_make_period("p1"), _make_period("p2", m1=3, m2=4)],
            multi=True,
            output_path=out,
        )
        # a=1 wins overall: avg(2.0, 1.5) = 1.75 vs a=2: avg(0.5, 0.3) = 0.4
        results = [
            _make_result({"a": "1"}, sharpe=2.0, period_name="p1"),
            _make_result({"a": "1"}, sharpe=1.5, period_name="p2"),
            _make_result({"a": "2"}, sharpe=0.5, period_name="p1"),
            _make_result({"a": "2"}, sharpe=0.3, period_name="p2"),
        ]
        _write_sweep_json(ctx, results)

        data = json.loads(out.read_text())
        assert data["sweep_config"]["multi_period"] is True
        assert len(data["sweep_config"]["periods"]) == 2
        assert "aggregated" in data
        # Aggregated sorted by avg_sharpe descending
        assert data["aggregated"][0]["params"] == {"a": "1"}
        assert data["aggregated"][1]["params"] == {"a": "2"}
        # best_params uses aggregated winner
        assert data["best_params"] == {"a": "1"}

    def test_empty_results_no_best_params(self, tmp_path: Path) -> None:
        out = tmp_path / "empty.json"
        ctx = _make_ctx(
            sweep_params=[SweepParameter(name="a", values=["1"])],
            combinations=[{"a": "1"}],
            periods=[_make_period("single")],
            output_path=out,
        )
        _write_sweep_json(ctx, [])
        data = json.loads(out.read_text())
        assert "best_params" not in data
        assert data["results"] == []


# ---------------------------------------------------------------------------
# build_pnl_config (run_helpers)
# ---------------------------------------------------------------------------


class TestBuildPnlConfig:
    def test_pnl_minimal_kwargs(self) -> None:
        """pnl caller should be able to omit sweep-only kwargs."""
        cfg = build_pnl_config(
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 2, 1, tzinfo=UTC),
            interval_seconds=3600,
            initial_capital=10000.0,
            chain="arbitrum",
            tokens=["WETH", "USDC"],
        )
        assert cfg.chain == "arbitrum"
        assert cfg.tokens == ["WETH", "USDC"]
        assert cfg.initial_capital_usd == Decimal("10000.0")
        # VIB-5088: unset gas resolves to the chain-aware registry default
        # (arbitrum: 0.1 gwei) -- the flat 30 is gone.
        assert cfg.gas_price_gwei == Decimal("0.1")
        assert cfg.gas_price_gwei_is_default is True
        # pnl-safe defaults: dataclass defaults retained when caller omits.
        assert cfg.allow_degraded_data is True  # dataclass default
        assert cfg.preflight_validation is True
        assert cfg.fail_on_preflight_error is True

    def test_sweep_extra_kwargs_propagate(self) -> None:
        cfg = build_pnl_config(
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 2, 1, tzinfo=UTC),
            interval_seconds=3600,
            initial_capital=5000.0,
            chain="base",
            tokens=["WETH"],
            allow_degraded_data=True,
            preflight_validation=False,
            fail_on_preflight_error=False,
        )
        assert cfg.chain == "base"
        assert cfg.allow_degraded_data is True
        assert cfg.preflight_validation is False
        assert cfg.fail_on_preflight_error is False

    def test_decimal_coercion(self) -> None:
        cfg = build_pnl_config(
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 2, 1, tzinfo=UTC),
            interval_seconds=3600,
            initial_capital=12345.67,
            chain="arbitrum",
            tokens=["WETH"],
            gas_price_gwei=42.5,
        )
        # Decimal via str() preserves literal float formatting; existing behaviour.
        assert cfg.initial_capital_usd == Decimal("12345.67")
        assert cfg.gas_price_gwei == Decimal("42.5")


# ---------------------------------------------------------------------------
# Phase 5B.4 extended coverage
# ---------------------------------------------------------------------------


from almanak.framework.cli.backtest.sweep import (  # noqa: E402
    _aggregate_multi_period_results,
    _display_sweep_results,
    _generate_sweep_report,
    _print_multi_period_results,
    _print_sweep_configuration,
    _run_parallel_sweep,
    _run_sweep_over_periods,
    _SweepTask,
    print_sweep_results_table,
    run_sweep_backtest,
)


class TestPrintSweepConfiguration:
    def test_single_period_banner(self, capsys: pytest.CaptureFixture[str]) -> None:
        ctx = _make_ctx(
            sweep_params=[SweepParameter(name="a", values=["1", "2"])],
            combinations=[{"a": "1"}, {"a": "2"}],
            periods=[_make_period("single")],
        )
        _print_sweep_configuration(ctx, parallel=False, effective_workers=4)
        out = capsys.readouterr().out
        assert "PARAMETER SWEEP CONFIGURATION" in out
        assert "Strategy: demo_strat" in out
        assert "Chain: arbitrum" in out
        assert "Interval: 3600s (1.0 hours)" in out
        assert "Initial Capital: $10,000.00" in out
        assert "Tokens: WETH, USDC" in out
        assert "Total combinations: 2" in out
        assert "Execution mode: Async (concurrent)" in out
        assert "Concurrency: 4" in out

    def test_multi_period_banner_lists_each_window(self, capsys: pytest.CaptureFixture[str]) -> None:
        ctx = _make_ctx(
            sweep_params=[SweepParameter(name="a", values=["1"])],
            combinations=[{"a": "1"}],
            periods=[_make_period("p1"), _make_period("p2", m1=3, m2=4)],
            multi=True,
        )
        _print_sweep_configuration(ctx, parallel=True, effective_workers=8)
        out = capsys.readouterr().out
        assert "Periods: 2024-quarterly (2 windows)" in out
        assert "- p1:" in out
        assert "- p2:" in out
        assert "Total runs: 2 (1 combinations x 2 periods)" in out
        assert "Execution mode: Parallel (multiprocessing)" in out
        assert "Workers: 8" in out

    def test_output_path_emitted_when_present(self, capsys: pytest.CaptureFixture[str]) -> None:
        out_path = Path("/tmp/sweep.json")
        ctx = _make_ctx(
            sweep_params=[SweepParameter(name="a", values=["1"])],
            combinations=[{"a": "1"}],
            periods=[_make_period("single")],
            output_path=out_path,
        )
        _print_sweep_configuration(ctx, parallel=False, effective_workers=2)
        out = capsys.readouterr().out
        assert f"Output: {out_path}" in out

    def test_output_path_absent_when_none(self, capsys: pytest.CaptureFixture[str]) -> None:
        ctx = _make_ctx(
            sweep_params=[SweepParameter(name="a", values=["1"])],
            combinations=[{"a": "1"}],
            periods=[_make_period("single")],
            output_path=None,
        )
        _print_sweep_configuration(ctx, parallel=False, effective_workers=2)
        out = capsys.readouterr().out
        assert "Output:" not in out


class TestAggregateMultiPeriodResults:
    def test_groups_by_param_combination(self) -> None:
        results = [
            _make_result({"a": "1"}, sharpe=1.0, period_name="p1"),
            _make_result({"a": "1"}, sharpe=2.0, period_name="p2"),
            _make_result({"a": "2"}, sharpe=0.5, period_name="p1"),
            _make_result({"a": "2"}, sharpe=1.5, period_name="p2"),
        ]
        aggregated = _aggregate_multi_period_results(results, [{"a": "1"}, {"a": "2"}])
        assert len(aggregated) == 2
        # Sorted by avg_sharpe desc: a=1 (avg 1.5) > a=2 (avg 1.0)
        assert aggregated[0].params == {"a": "1"}
        assert aggregated[0].avg_sharpe == 1.5
        assert aggregated[1].params == {"a": "2"}
        assert aggregated[1].avg_sharpe == 1.0

    def test_sharpe_std_zero_for_single_period(self) -> None:
        results = [_make_result({"a": "1"}, sharpe=1.5, period_name="p1")]
        aggregated = _aggregate_multi_period_results(results, [{"a": "1"}])
        assert aggregated[0].sharpe_std == 0.0

    def test_sharpe_std_nonzero_for_multi(self) -> None:
        results = [
            _make_result({"a": "1"}, sharpe=1.0, period_name="p1"),
            _make_result({"a": "1"}, sharpe=3.0, period_name="p2"),
        ]
        aggregated = _aggregate_multi_period_results(results, [{"a": "1"}])
        # sample stddev of [1.0, 3.0] with mean 2 is sqrt(((1-2)^2 + (3-2)^2)/1) = sqrt(2) ~ 1.414
        assert aggregated[0].sharpe_std == pytest.approx(1.4142, abs=0.01)

    def test_empty_results_produces_empty_aggregated(self) -> None:
        assert _aggregate_multi_period_results([], []) == []

    def test_cumulative_pnl_accumulates_across_periods(self) -> None:
        results = [
            _make_result({"a": "1"}, sharpe=1.0, period_name="p1"),
            _make_result({"a": "1"}, sharpe=2.0, period_name="p2"),
        ]
        aggregated = _aggregate_multi_period_results(results, [{"a": "1"}])
        # Each SweepResult's underlying BacktestResult has net_pnl_usd=123.45
        assert aggregated[0].cumulative_pnl == pytest.approx(2 * 123.45)


class TestPrintMultiPeriodResults:
    def test_emits_per_period_and_aggregated_sections(self, capsys: pytest.CaptureFixture[str]) -> None:
        results = [
            _make_result({"a": "1"}, sharpe=2.0, period_name="p1"),
            _make_result({"a": "1"}, sharpe=1.0, period_name="p2"),
            _make_result({"a": "2"}, sharpe=0.5, period_name="p1"),
            _make_result({"a": "2"}, sharpe=0.3, period_name="p2"),
        ]
        aggregated = _aggregate_multi_period_results(results, [{"a": "1"}, {"a": "2"}])
        params = [SweepParameter(name="a", values=["1", "2"])]
        _print_multi_period_results(results, aggregated, params)
        out = capsys.readouterr().out
        assert "PER-PERIOD DETAIL" in out
        assert "AGGREGATED RESULTS (sorted by avg Sharpe ratio)" in out
        assert "WINNER (best avg Sharpe across all periods):" in out

    def test_empty_aggregated_omits_winner_block(self, capsys: pytest.CaptureFixture[str]) -> None:
        """If aggregated is empty, no WINNER line is emitted."""
        params = [SweepParameter(name="a", values=["1"])]
        _print_multi_period_results([], [], params)
        out = capsys.readouterr().out
        assert "WINNER" not in out


class TestDisplaySweepResults:
    def test_single_period_uses_print_sweep_results_table(self, capsys: pytest.CaptureFixture[str]) -> None:
        ctx = _make_ctx(
            sweep_params=[SweepParameter(name="a", values=["1"])],
            combinations=[{"a": "1"}],
            periods=[_make_period("single")],
            multi=False,
        )
        _display_sweep_results(ctx, [_make_result({"a": "1"})])
        out = capsys.readouterr().out
        # Single-period heading (sorted by Sharpe)
        assert "PARAMETER SWEEP RESULTS (sorted by Sharpe ratio)" in out

    def test_multi_period_single_window_falls_through_to_single(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Multi-period mode with only 1 period uses single-period renderer."""
        ctx = _make_ctx(
            sweep_params=[SweepParameter(name="a", values=["1"])],
            combinations=[{"a": "1"}],
            periods=[_make_period("only")],
            multi=True,
        )
        _display_sweep_results(ctx, [_make_result({"a": "1"}, period_name="only")])
        out = capsys.readouterr().out
        assert "PARAMETER SWEEP RESULTS" in out
        assert "PER-PERIOD DETAIL" not in out

    def test_multi_period_multiple_windows_renders_aggregated(self, capsys: pytest.CaptureFixture[str]) -> None:
        ctx = _make_ctx(
            sweep_params=[SweepParameter(name="a", values=["1"])],
            combinations=[{"a": "1"}],
            periods=[_make_period("p1"), _make_period("p2", m1=3, m2=4)],
            multi=True,
        )
        _display_sweep_results(
            ctx,
            [
                _make_result({"a": "1"}, sharpe=1.0, period_name="p1"),
                _make_result({"a": "1"}, sharpe=2.0, period_name="p2"),
            ],
        )
        out = capsys.readouterr().out
        assert "PER-PERIOD DETAIL" in out
        assert "AGGREGATED RESULTS" in out


class TestRunSweepOverPeriods:
    def test_async_mode_single_period(self) -> None:
        """Async mode runs `run_parallel_sweeps` once per period."""
        ctx = _make_ctx(
            sweep_params=[SweepParameter(name="a", values=["1"])],
            combinations=[{"a": "1"}],
            periods=[_make_period("single")],
            multi=False,
        )
        fake_results = [_make_result({"a": "1"}, sharpe=1.0)]

        async def _fake_run_parallel_sweeps(*args: Any, **kwargs: Any) -> list:
            return fake_results

        with patch(
            "almanak.framework.cli.backtest.sweep.run_parallel_sweeps",
            side_effect=_fake_run_parallel_sweeps,
        ):
            results = _run_sweep_over_periods(
                ctx,
                strategy_class=MagicMock(),
                base_config={},
                data_provider=MagicMock(),
                parallel=False,
                effective_workers=4,
            )
        assert len(results) == 1
        assert results[0].period_name == "single"

    def test_parallel_mode_calls_run_parallel_sweep(self) -> None:
        """Parallel mode invokes `_run_parallel_sweep` (multiprocessing helper)."""
        ctx = _make_ctx(
            sweep_params=[SweepParameter(name="a", values=["1"])],
            combinations=[{"a": "1"}],
            periods=[_make_period("p1")],
            multi=False,
        )
        fake_results = [_make_result({"a": "1"}, sharpe=1.5)]
        with patch(
            "almanak.framework.cli.backtest.sweep._run_parallel_sweep",
            return_value=fake_results,
        ) as mock_rps:
            results = _run_sweep_over_periods(
                ctx,
                strategy_class=MagicMock(),
                base_config={"chain": "arbitrum"},
                data_provider=MagicMock(),
                parallel=True,
                effective_workers=3,
            )
        mock_rps.assert_called_once()
        assert len(results) == 1

    def test_multi_period_iterates_all_windows(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Multi-period mode emits a period banner and accumulates results."""
        ctx = _make_ctx(
            sweep_params=[SweepParameter(name="a", values=["1"])],
            combinations=[{"a": "1"}],
            periods=[_make_period("p1"), _make_period("p2", m1=3, m2=4)],
            multi=True,
        )

        async def _fake(*args: Any, **kwargs: Any) -> list:
            return [_make_result({"a": "1"}, sharpe=1.0)]

        with patch(
            "almanak.framework.cli.backtest.sweep.run_parallel_sweeps",
            side_effect=_fake,
        ):
            results = _run_sweep_over_periods(
                ctx,
                strategy_class=MagicMock(),
                base_config={},
                data_provider=MagicMock(),
                parallel=False,
                effective_workers=2,
            )
        assert len(results) == 2  # one per period
        # Period banners emitted
        out = capsys.readouterr().out
        assert "--- Period: p1" in out
        assert "--- Period: p2" in out
        # Period names set on results
        assert {r.period_name for r in results} == {"p1", "p2"}

    def test_exception_in_sweep_aborts_with_sys_exit(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Grep-asserted 'Error during sweep: {e}' + sys.exit(1)."""
        ctx = _make_ctx(
            sweep_params=[SweepParameter(name="a", values=["1"])],
            combinations=[{"a": "1"}],
            periods=[_make_period("single")],
            multi=False,
        )
        with patch(
            "almanak.framework.cli.backtest.sweep.run_parallel_sweeps",
            side_effect=RuntimeError("upstream crash"),
        ):
            with pytest.raises(SystemExit) as exc:
                _run_sweep_over_periods(
                    ctx,
                    strategy_class=MagicMock(),
                    base_config={},
                    data_provider=MagicMock(),
                    parallel=False,
                    effective_workers=4,
                )
        assert exc.value.code == 1
        assert "Error during sweep: upstream crash" in capsys.readouterr().err

    def test_preflight_validation_only_when_single_combination(self) -> None:
        """preflight_validation heuristic: total_combinations <= 1."""
        captured_configs = []

        async def _spy(*args: Any, **kwargs: Any) -> list:
            captured_configs.append(kwargs["pnl_config"])
            return []

        # 3 combinations -> preflight_validation=False
        ctx = _make_ctx(
            sweep_params=[SweepParameter(name="a", values=["1", "2", "3"])],
            combinations=[{"a": "1"}, {"a": "2"}, {"a": "3"}],
            periods=[_make_period("single")],
            multi=False,
        )
        with patch(
            "almanak.framework.cli.backtest.sweep.run_parallel_sweeps",
            side_effect=_spy,
        ):
            _run_sweep_over_periods(
                ctx,
                strategy_class=MagicMock(),
                base_config={},
                data_provider=MagicMock(),
                parallel=False,
                effective_workers=1,
            )
        assert captured_configs[0].preflight_validation is False

        captured_configs.clear()
        # 1 combination -> preflight_validation=True
        ctx = _make_ctx(
            sweep_params=[SweepParameter(name="a", values=["1"])],
            combinations=[{"a": "1"}],
            periods=[_make_period("single")],
            multi=False,
        )
        with patch(
            "almanak.framework.cli.backtest.sweep.run_parallel_sweeps",
            side_effect=_spy,
        ):
            _run_sweep_over_periods(
                ctx,
                strategy_class=MagicMock(),
                base_config={},
                data_provider=MagicMock(),
                parallel=False,
                effective_workers=1,
            )
        assert captured_configs[0].preflight_validation is True


class TestRunParallelSweep:
    """Worker exception paths in `_run_parallel_sweep`.

    #1752 FIXED: the error handler used to call
    ``BacktestResult(..., success=False, error=str(e))`` but `BacktestResult`
    defines `success` as a read-only @property, not a constructor field -- so
    any worker exception raised ``TypeError: unexpected keyword argument 'success'``
    instead of recording a failed SweepResult. The handler now sets `error=...`
    and lets `success` derive. Tests below assert the fixed contract: a failed
    worker now produces a SweepResult with a non-success BacktestResult.
    """

    def _build_pnl_config(self) -> Any:
        from almanak.framework.backtesting import PnLBacktestConfig

        return PnLBacktestConfig(
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 2, 1, tzinfo=UTC),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            chain="arbitrum",
            tokens=["WETH", "USDC"],
            gas_price_gwei=Decimal("30"),
            include_gas_costs=True,
        )

    def test_worker_exception_is_recorded_as_failed_sweep_result(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """#1752 fix: a failed worker is recorded, not re-raised.

        The error-handler previously tried to pass ``success=False`` into
        ``BacktestResult(...)`` (which has ``success`` as a @property, not a
        field), so any worker exception crashed the handler with TypeError.
        Fix: set ``error=str(e)`` and let ``success`` derive from it. The
        resulting SweepResult carries a non-success BacktestResult whose
        ``error`` string surfaces the worker failure.
        """
        pnl_config = self._build_pnl_config()

        class _FakeFuture:
            def result(self) -> Any:
                raise RuntimeError("worker died")

        class _FakeExecutor:
            def __init__(self, *a: Any, **kw: Any) -> None:
                pass

            def __enter__(self) -> _FakeExecutor:
                return self

            def __exit__(self, *a: Any) -> None:
                pass

            def submit(self, fn: Any, task: Any) -> _FakeFuture:
                return _FakeFuture()

        class StratClass:
            deployment_id = "dummy"

        with (
            patch(
                "concurrent.futures.ProcessPoolExecutor",
                _FakeExecutor,
            ),
            patch(
                "concurrent.futures.as_completed",
                lambda futures: list(futures),
            ),
        ):
            results = _run_parallel_sweep(
                strategy_class=StratClass,
                base_config={},
                pnl_config=pnl_config,
                combinations=[{"x": "1"}],
                workers=1,
                sweep_params=[SweepParameter(name="x", values=["1"])],
            )

        # The error handler produced exactly one failed SweepResult, not a raise.
        assert len(results) == 1
        sr = results[0]
        assert sr.params == {"x": "1"}
        # BacktestResult.success is a @property: `error is None` -> success.
        assert sr.result.error is not None
        assert "worker died" in sr.result.error
        assert sr.result.success is False
        # Required BacktestResult constructor fields must be populated so the
        # failed-result contract matches the successful-result contract
        # (engine tag, deployment_id sentinel, time range, metrics instance)
        # and downstream consumers of the error SweepResult never hit
        # MissingFieldError on a failure record. Chain + capital metadata
        # must propagate from pnl_config rather than falling back to the
        # BacktestResult dataclass defaults (arbitrum / 10k USD).
        assert sr.result.engine == BacktestEngine.PNL
        assert sr.result.deployment_id == "error"
        assert sr.result.start_time == pnl_config.start_time
        assert sr.result.end_time == pnl_config.end_time
        assert isinstance(sr.result.metrics, BacktestMetrics)
        assert sr.result.chain == pnl_config.chain
        assert sr.result.initial_capital_usd == pnl_config.initial_capital_usd
        assert sr.result.final_capital_usd == pnl_config.initial_capital_usd
        # Zeroed performance fields on the SweepResult wrapper.
        assert sr.sharpe_ratio == Decimal("0")
        assert sr.total_return_pct == Decimal("0")
        assert sr.max_drawdown_pct == Decimal("0")
        assert sr.win_rate == Decimal("0")
        assert sr.total_trades == 0

        # The click.echo error line is still emitted on the error path.
        err = capsys.readouterr().err
        assert "Error in worker for params" in err
        assert "worker died" in err

    def test_happy_path_all_workers_succeed(self) -> None:
        """When every worker returns a valid SweepResult, no exception path triggers."""
        pnl_config = self._build_pnl_config()
        good_results = [
            _make_result({"x": "1"}, sharpe=1.5),
            _make_result({"x": "2"}, sharpe=2.5),
        ]

        class _GoodFuture:
            def __init__(self, value: Any) -> None:
                self._value = value

            def result(self) -> Any:
                return self._value

        class _FakeExecutor:
            def __init__(self, *a: Any, **kw: Any) -> None:
                pass

            def __enter__(self) -> _FakeExecutor:
                return self

            def __exit__(self, *a: Any) -> None:
                pass

            def submit(self, fn: Any, task: Any) -> _GoodFuture:
                return _GoodFuture(good_results[task.task_index])

        class StratClass:
            deployment_id = "dummy"

        with (
            patch(
                "concurrent.futures.ProcessPoolExecutor",
                _FakeExecutor,
            ),
            patch(
                "concurrent.futures.as_completed",
                lambda futures: list(futures),
            ),
        ):
            results = _run_parallel_sweep(
                strategy_class=StratClass,
                base_config={},
                pnl_config=pnl_config,
                combinations=[{"x": "1"}, {"x": "2"}],
                workers=2,
                sweep_params=[SweepParameter(name="x", values=["1", "2"])],
            )
        assert len(results) == 2
        sharpes = sorted(float(r.sharpe_ratio) for r in results)
        assert sharpes == [1.5, 2.5]

    def test_task_construction_uses_fqcn(self) -> None:
        """Pin that the task carries the fully-qualified strategy class name."""
        pnl_config = self._build_pnl_config()
        good_result = _make_result({"x": "1"}, sharpe=1.0)

        submitted_tasks: list[Any] = []

        class _GoodFuture:
            def result(self) -> Any:
                return good_result

        class _FakeExecutor:
            def __init__(self, *a: Any, **kw: Any) -> None:
                pass

            def __enter__(self) -> _FakeExecutor:
                return self

            def __exit__(self, *a: Any) -> None:
                pass

            def submit(self, fn: Any, task: Any) -> _GoodFuture:
                submitted_tasks.append(task)
                return _GoodFuture()

        class StratClass:
            deployment_id = "dummy"

        with (
            patch(
                "concurrent.futures.ProcessPoolExecutor",
                _FakeExecutor,
            ),
            patch(
                "concurrent.futures.as_completed",
                lambda futures: list(futures),
            ),
        ):
            _run_parallel_sweep(
                strategy_class=StratClass,
                base_config={"chain": "arbitrum"},
                pnl_config=pnl_config,
                combinations=[{"x": "1"}],
                workers=1,
                sweep_params=[SweepParameter(name="x", values=["1"])],
            )
        assert len(submitted_tasks) == 1
        assert submitted_tasks[0].strategy_class_name.endswith("StratClass")
        assert submitted_tasks[0].base_config == {"chain": "arbitrum"}
        assert submitted_tasks[0].params == {"x": "1"}
        assert submitted_tasks[0].task_index == 0


class TestGenerateSweepReport:
    def test_noop_when_results_empty(self, capsys: pytest.CaptureFixture[str]) -> None:
        ctx = _make_ctx(
            sweep_params=[SweepParameter(name="a", values=["1"])],
            combinations=[{"a": "1"}],
            periods=[_make_period("single")],
        )
        _generate_sweep_report(ctx, [])
        # No output at all when empty
        assert "Generating HTML report" not in capsys.readouterr().out

    def test_single_period_picks_best_by_sharpe(self, capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
        ctx = _make_ctx(
            sweep_params=[SweepParameter(name="a", values=["1", "2"])],
            combinations=[{"a": "1"}, {"a": "2"}],
            periods=[_make_period("single")],
        )
        results = [
            _make_result({"a": "1"}, sharpe=0.5, period_name="single"),
            _make_result({"a": "2"}, sharpe=2.5, period_name="single"),
        ]
        report_result = MagicMock(success=True)
        report_result.file_path = tmp_path / "report.html"

        with patch(
            "almanak.framework.backtesting.report_generator.generate_report",
            return_value=report_result,
        ) as gen:
            _generate_sweep_report(ctx, results)

        # Best: a=2 (sharpe=2.5)
        assert gen.call_args.args[0] is results[1].result
        out = capsys.readouterr().out
        assert "Generating HTML report for best parameter combination..." in out
        assert "Best params: {'a': '2'}" in out

    def test_multi_period_picks_aggregated_winner(self, capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
        ctx = _make_ctx(
            sweep_params=[SweepParameter(name="a", values=["1", "2"])],
            combinations=[{"a": "1"}, {"a": "2"}],
            periods=[_make_period("p1"), _make_period("p2", m1=3, m2=4)],
            multi=True,
        )
        # a=1: avg(2.0, 1.8) = 1.9 (winner)
        # a=2: avg(0.5, 1.0) = 0.75
        results = [
            _make_result({"a": "1"}, sharpe=2.0, period_name="p1"),
            _make_result({"a": "1"}, sharpe=1.8, period_name="p2"),
            _make_result({"a": "2"}, sharpe=0.5, period_name="p1"),
            _make_result({"a": "2"}, sharpe=1.0, period_name="p2"),
        ]
        report_result = MagicMock(success=True)
        report_result.file_path = tmp_path / "report.html"

        with patch(
            "almanak.framework.backtesting.report_generator.generate_report",
            return_value=report_result,
        ) as gen:
            _generate_sweep_report(ctx, results)
        # Winner = a=1; within that, pick highest Sharpe (p1, sharpe=2.0)
        # generate_report(best_result.result, ...) — first positional arg
        assert gen.call_args.args[0] is results[0].result

    def test_fallback_report_path_when_no_output(self, capsys: pytest.CaptureFixture[str]) -> None:
        """When ctx.output_path is None, derives from strategy name."""
        ctx = _make_ctx(
            sweep_params=[SweepParameter(name="a", values=["1"])],
            combinations=[{"a": "1"}],
            periods=[_make_period("single")],
            output_path=None,
            strategy="my_strat",
        )
        report_result = MagicMock(success=True)
        report_result.file_path = "backtest_report_my_strat_sweep.html"

        with patch(
            "almanak.framework.backtesting.report_generator.generate_report",
            return_value=report_result,
        ) as gen:
            _generate_sweep_report(ctx, [_make_result({"a": "1"}, period_name="single")])
        kwargs = gen.call_args.kwargs
        assert kwargs["output_path"] == Path("backtest_report_my_strat_sweep.html")

    def test_fallback_sanitizes_strategy_name(self) -> None:
        ctx = _make_ctx(
            sweep_params=[SweepParameter(name="a", values=["1"])],
            combinations=[{"a": "1"}],
            periods=[_make_period("single")],
            output_path=None,
            strategy="a/b\\c",
        )
        report_result = MagicMock(success=True)
        report_result.file_path = "x"

        with patch(
            "almanak.framework.backtesting.report_generator.generate_report",
            return_value=report_result,
        ) as gen:
            _generate_sweep_report(ctx, [_make_result({"a": "1"}, period_name="single")])
        assert gen.call_args.kwargs["output_path"] == Path("backtest_report_a_b_c_sweep.html")

    def test_report_failure_prints_warning(self, capsys: pytest.CaptureFixture[str]) -> None:
        ctx = _make_ctx(
            sweep_params=[SweepParameter(name="a", values=["1"])],
            combinations=[{"a": "1"}],
            periods=[_make_period("single")],
        )
        report_result = MagicMock(success=False, error="template error")
        with patch(
            "almanak.framework.backtesting.report_generator.generate_report",
            return_value=report_result,
        ):
            _generate_sweep_report(ctx, [_make_result({"a": "1"}, period_name="single")])
        err = capsys.readouterr().err
        assert "Warning: Failed to generate report: template error" in err

    def test_output_path_with_suffix_html(self, tmp_path: Path) -> None:
        """Report path derived from `ctx.output_path.with_suffix('.html')`."""
        out = tmp_path / "sweep.json"
        ctx = _make_ctx(
            sweep_params=[SweepParameter(name="a", values=["1"])],
            combinations=[{"a": "1"}],
            periods=[_make_period("single")],
            output_path=out,
        )
        report_result = MagicMock(success=True)
        report_result.file_path = "x"

        with patch(
            "almanak.framework.backtesting.report_generator.generate_report",
            return_value=report_result,
        ) as gen:
            _generate_sweep_report(ctx, [_make_result({"a": "1"}, period_name="single")])
        assert gen.call_args.kwargs["output_path"] == out.with_suffix(".html")


class TestRunSweepBacktestCoro:
    """Unit tests for the public async `run_sweep_backtest` coroutine."""

    def _build_pnl_config(self) -> Any:
        from almanak.framework.backtesting import PnLBacktestConfig

        return PnLBacktestConfig(
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 2, 1, tzinfo=UTC),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            chain="arbitrum",
            tokens=["WETH", "USDC"],
            gas_price_gwei=Decimal("30"),
            include_gas_costs=True,
        )

    def test_sweep_sets_fallback_deployment_id(self) -> None:
        """Strategy without deployment_id gets `sweep-<params>` fallback."""
        import asyncio as _asyncio

        pnl_config = self._build_pnl_config()

        class BareStrategy:
            def __init__(self, config: dict[str, Any]) -> None:
                self.config = config

        backtest_result = _make_result({"threshold": "0.02"}, sharpe=1.0).result

        bare_instance = BareStrategy({})
        mock_backtester = MagicMock()
        mock_backtester.backtest = AsyncMock(return_value=backtest_result)
        with (
            patch(
                "almanak.framework.cli.backtest.sweep._create_backtest_strategy",
                return_value=bare_instance,
            ),
            patch(
                "almanak.framework.cli.backtest.sweep.PnLBacktester",
                return_value=mock_backtester,
            ),
        ):
            result = _asyncio.run(
                run_sweep_backtest(
                    strategy_class=BareStrategy,
                    base_config={},
                    pnl_config=pnl_config,
                    data_provider=MagicMock(),
                    params={"threshold": "0.02"},
                )
            )

        # Pin the params contract
        assert result.params == {"threshold": "0.02"}

        # Strong assertion: verify the fallback deployment_id was set on the
        # instance that was handed to PnLBacktester.backtest. Production
        # (sweep.py:88-96) derives `sweep-<k><v>` joined by `_` for multi-key.
        mock_backtester.backtest.assert_awaited_once()
        passed_strategy = mock_backtester.backtest.await_args.args[0]
        assert passed_strategy is bare_instance
        assert passed_strategy.deployment_id == "sweep-threshold0.02"

    def test_numeric_coercion_of_param_value(self) -> None:
        """Numeric string values are coerced to float in strategy_config."""
        import asyncio as _asyncio

        pnl_config = self._build_pnl_config()
        captured_configs: list[dict[str, Any]] = []

        class WithDictConfig:
            def __init__(self, config: dict[str, Any]) -> None:
                self.config = config
                self.deployment_id = "wdc"

        def _track_create(strategy_class: Any, config: dict, chain: str) -> Any:
            captured_configs.append(config)
            return WithDictConfig(config)

        backtest_result = _make_result({"a": "1"}, sharpe=1.0).result
        mock_backtester = MagicMock()
        mock_backtester.backtest = AsyncMock(return_value=backtest_result)

        with (
            patch(
                "almanak.framework.cli.backtest.sweep._create_backtest_strategy",
                side_effect=_track_create,
            ),
            patch(
                "almanak.framework.cli.backtest.sweep.PnLBacktester",
                return_value=mock_backtester,
            ),
        ):
            _asyncio.run(
                run_sweep_backtest(
                    strategy_class=WithDictConfig,
                    base_config={"base_knob": 100},
                    pnl_config=pnl_config,
                    data_provider=MagicMock(),
                    params={"numeric": "0.5", "text": "aggressive"},
                )
            )
        cfg = captured_configs[0]
        # Float-coerced
        assert cfg["numeric"] == 0.5
        # Non-numeric preserved as string
        assert cfg["text"] == "aggressive"
        # Base preserved
        assert cfg["base_knob"] == 100

    def test_strategy_without_config_dict_sets_attributes(self) -> None:
        """When strategy lacks a `.config` dict, params set as attributes."""
        import asyncio as _asyncio

        pnl_config = self._build_pnl_config()

        class NoConfigStrategy:
            # No `config` attribute at all
            deployment_id = "ncs"

            def __init__(self, config: dict[str, Any]) -> None:
                # Intentionally does NOT store config
                pass

        created: list[NoConfigStrategy] = []

        def _track_create(strategy_class: Any, config: dict, chain: str) -> Any:
            inst = NoConfigStrategy(config)
            created.append(inst)
            return inst

        backtest_result = _make_result({"a": "1"}, sharpe=1.0).result
        mock_backtester = MagicMock()
        mock_backtester.backtest = AsyncMock(return_value=backtest_result)

        with (
            patch(
                "almanak.framework.cli.backtest.sweep._create_backtest_strategy",
                side_effect=_track_create,
            ),
            patch(
                "almanak.framework.cli.backtest.sweep.PnLBacktester",
                return_value=mock_backtester,
            ),
        ):
            _asyncio.run(
                run_sweep_backtest(
                    strategy_class=NoConfigStrategy,
                    base_config={},
                    pnl_config=pnl_config,
                    data_provider=MagicMock(),
                    params={"numeric": "1.5", "text": "mode_a"},
                )
            )
        # Attributes set on the strategy instance
        inst = created[0]
        assert inst.numeric == 1.5
        assert inst.text == "mode_a"

    def test_public_deployment_id_attr_used_when_no_private(self) -> None:
        """Fallback deployment_id assignment uses public attr when no _deployment_id."""
        import asyncio as _asyncio

        pnl_config = self._build_pnl_config()

        class PlainStrategy:
            deployment_id = ""  # Empty → triggers fallback

            def __init__(self, config: dict[str, Any]) -> None:
                self.config = config

        created: list[PlainStrategy] = []

        def _track_create(strategy_class: Any, config: dict, chain: str) -> Any:
            inst = PlainStrategy(config)
            created.append(inst)
            return inst

        backtest_result = _make_result({"a": "1"}, sharpe=1.0).result
        mock_backtester = MagicMock()
        mock_backtester.backtest = AsyncMock(return_value=backtest_result)

        with (
            patch(
                "almanak.framework.cli.backtest.sweep._create_backtest_strategy",
                side_effect=_track_create,
            ),
            patch(
                "almanak.framework.cli.backtest.sweep.PnLBacktester",
                return_value=mock_backtester,
            ),
        ):
            _asyncio.run(
                run_sweep_backtest(
                    strategy_class=PlainStrategy,
                    base_config={},
                    pnl_config=pnl_config,
                    data_provider=MagicMock(),
                    params={"x": "1"},
                )
            )
        inst = created[0]
        # Falls back to public attr since no _deployment_id
        assert inst.deployment_id == "sweep-x1"

    def test_private_deployment_id_preferred(self) -> None:
        """Private `_deployment_id` attr is set when present (IntentStrategy-style)."""
        import asyncio as _asyncio

        pnl_config = self._build_pnl_config()

        class WithPrivateId:
            def __init__(self, config: dict[str, Any]) -> None:
                self.config = config
                self._deployment_id = ""

            @property
            def deployment_id(self) -> str:
                return self._deployment_id

        created: list[WithPrivateId] = []

        def _track_create(strategy_class: Any, config: dict, chain: str) -> Any:
            inst = WithPrivateId(config)
            created.append(inst)
            return inst

        backtest_result = _make_result({"a": "1"}, sharpe=1.0).result
        mock_backtester = MagicMock()
        mock_backtester.backtest = AsyncMock(return_value=backtest_result)

        with (
            patch(
                "almanak.framework.cli.backtest.sweep._create_backtest_strategy",
                side_effect=_track_create,
            ),
            patch(
                "almanak.framework.cli.backtest.sweep.PnLBacktester",
                return_value=mock_backtester,
            ),
        ):
            _asyncio.run(
                run_sweep_backtest(
                    strategy_class=WithPrivateId,
                    base_config={},
                    pnl_config=pnl_config,
                    data_provider=MagicMock(),
                    params={"threshold": "0.02"},
                )
            )
        assert created[0]._deployment_id == "sweep-threshold0.02"


class TestRunParallelSweepsCoro:
    """Cover the async `run_parallel_sweeps` coroutine."""

    def test_runs_all_combos_and_echoes_progress(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Verify async loop walks all combinations and emits progress lines."""
        import asyncio as _asyncio

        from almanak.framework.cli.backtest.sweep import run_parallel_sweeps

        call_params: list[dict[str, str]] = []

        async def _fake_run_sweep_backtest(
            *,
            strategy_class: Any,
            base_config: dict[str, Any],
            pnl_config: Any,
            data_provider: Any,
            params: dict[str, str],
            numeric_param_names: frozenset[str] = frozenset(),
            emit_ambiguity_warnings: bool = True,
        ) -> SweepResult:
            call_params.append(params)
            return _make_result(params, sharpe=1.0)

        with patch(
            "almanak.framework.cli.backtest.sweep.run_sweep_backtest",
            side_effect=_fake_run_sweep_backtest,
        ):
            results = _asyncio.run(
                run_parallel_sweeps(
                    strategy_class=MagicMock(),
                    base_config={},
                    pnl_config=MagicMock(),
                    data_provider=MagicMock(),
                    combinations=[
                        {"a": "1"},
                        {"a": "2"},
                        {"a": "3"},
                    ],
                    parallel=2,
                )
            )
        assert len(results) == 3
        assert len(call_params) == 3
        out = capsys.readouterr().out
        # Progress line per completion
        assert "Completed 1/3" in out
        assert "Completed 2/3" in out
        assert "Completed 3/3" in out

    def test_all_combinations_invoked(self) -> None:
        """Every combination is passed through to run_sweep_backtest."""
        import asyncio as _asyncio

        from almanak.framework.cli.backtest.sweep import run_parallel_sweeps

        seen_params: list[dict[str, str]] = []

        async def _record(
            *,
            strategy_class: Any,
            base_config: dict[str, Any],
            pnl_config: Any,
            data_provider: Any,
            params: dict[str, str],
            numeric_param_names: frozenset[str] = frozenset(),
            emit_ambiguity_warnings: bool = True,
        ) -> SweepResult:
            seen_params.append(params)
            return _make_result(params, sharpe=1.0)

        combos = [{"a": "1"}, {"a": "2"}, {"b": "x"}]
        with patch(
            "almanak.framework.cli.backtest.sweep.run_sweep_backtest",
            side_effect=_record,
        ):
            results = _asyncio.run(
                run_parallel_sweeps(
                    strategy_class=MagicMock(),
                    base_config={},
                    pnl_config=MagicMock(),
                    data_provider=MagicMock(),
                    combinations=combos,
                    parallel=2,
                )
            )
        # Order is not deterministic, but every combo is invoked.
        sort_key = lambda d: sorted(d.items())  # noqa: E731
        assert sorted(seen_params, key=sort_key) == sorted(combos, key=sort_key)
        assert len(results) == 3


class TestSweepTaskAndWorker:
    def test_sweep_task_fields(self) -> None:
        """`_SweepTask` dataclass preserves fields verbatim."""
        task = _SweepTask(
            strategy_class_name="mod.ClsName",
            base_config={"chain": "arbitrum"},
            pnl_config_dict={"start_time": "2024-01-01"},
            params={"a": "1"},
            task_index=7,
        )
        assert task.strategy_class_name == "mod.ClsName"
        assert task.base_config["chain"] == "arbitrum"
        assert task.params["a"] == "1"
        assert task.task_index == 7

    def _build_pnl_dict(self) -> dict[str, Any]:
        from almanak.framework.backtesting import PnLBacktestConfig

        cfg = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 2, 1, tzinfo=UTC),
            interval_seconds=3600,
            initial_capital_usd=Decimal("10000"),
            chain="arbitrum",
            tokens=["WETH", "USDC"],
            gas_price_gwei=Decimal("30"),
            include_gas_costs=True,
        )
        return cfg.to_dict()

    def test_worker_importable_strategy_class(self) -> None:
        """Strategy class resolves via `importlib.import_module`."""
        from almanak.framework.cli.backtest.sweep import _run_sweep_task_worker

        # Use a real-importable class from the standard library-ish scope:
        # `decimal.Decimal`. The worker will import `decimal` and grab `Decimal`.
        # We fake everything else to avoid running a real backtest.
        backtest_result = _make_result({"x": "1"}, sharpe=1.0).result

        task = _SweepTask(
            strategy_class_name="decimal.Decimal",
            base_config={"chain": "arbitrum"},
            pnl_config_dict=self._build_pnl_dict(),
            params={"x": "1"},
            task_index=0,
        )

        mock_backtester = MagicMock()
        mock_backtester.backtest = AsyncMock(return_value=backtest_result)

        # Fake the strategy factory and chain resolver so we don't actually
        # call Decimal(...) as a strategy.
        class _FakeStrategy:
            deployment_id = "fake"
            config: dict[str, Any] = {}

        with (
            patch(
                "almanak.framework.cli.backtest.sweep._create_backtest_strategy",
                return_value=_FakeStrategy(),
            ),
            patch(
                "almanak.framework.cli.backtest.sweep.PnLBacktester",
                return_value=mock_backtester,
            ),
            patch(
                "almanak.framework.cli.backtest.sweep.CoinGeckoDataProvider",
            ),
            patch(
                "almanak.framework.cli.run.get_default_chain",
                return_value="arbitrum",
            ),
        ):
            result = _run_sweep_task_worker(task)

        assert result.params == {"x": "1"}
        assert isinstance(result.sharpe_ratio, Decimal)

    def test_worker_import_error_falls_back_to_registry(self) -> None:
        """When the FQCN module doesn't exist, fall back to `get_strategy`."""
        from almanak.framework.cli.backtest.sweep import _run_sweep_task_worker

        backtest_result = _make_result({"a": "1"}, sharpe=1.0).result
        task = _SweepTask(
            strategy_class_name="nonexistent_module.Strategy",
            base_config={},
            pnl_config_dict=self._build_pnl_dict(),
            params={"a": "1"},
            task_index=0,
        )

        class _RegisteredClass:
            deployment_id = "reg"

        class _FakeStrategy:
            deployment_id = "fake"
            config: dict[str, Any] = {}

        mock_backtester = MagicMock()
        mock_backtester.backtest = AsyncMock(return_value=backtest_result)

        with (
            patch(
                "almanak.framework.strategies.get_strategy",
                return_value=_RegisteredClass,
            ),
            patch(
                "almanak.framework.cli.backtest.sweep._create_backtest_strategy",
                return_value=_FakeStrategy(),
            ),
            patch(
                "almanak.framework.cli.backtest.sweep.PnLBacktester",
                return_value=mock_backtester,
            ),
            patch(
                "almanak.framework.cli.backtest.sweep.CoinGeckoDataProvider",
            ),
            patch(
                "almanak.framework.cli.run.get_default_chain",
                return_value="arbitrum",
            ),
        ):
            result = _run_sweep_task_worker(task)
        assert result.params == {"a": "1"}

    def test_worker_registry_value_error_falls_back_to_mock(self) -> None:
        """Both importlib AND registry fail → MockWorkerStrategy fallback."""
        from almanak.framework.cli.backtest.sweep import _run_sweep_task_worker

        backtest_result = _make_result({"a": "1"}, sharpe=1.0).result
        task = _SweepTask(
            strategy_class_name="no_such_mod.GhostStrategy",
            base_config={},
            pnl_config_dict=self._build_pnl_dict(),
            params={"a": "1"},
            task_index=0,
        )

        class _FakeStrategy:
            deployment_id = "fake"
            config: dict[str, Any] = {}

        mock_backtester = MagicMock()
        mock_backtester.backtest = AsyncMock(return_value=backtest_result)

        with (
            patch(
                "almanak.framework.strategies.get_strategy",
                side_effect=ValueError("not registered"),
            ),
            patch(
                "almanak.framework.cli.backtest.sweep._create_backtest_strategy",
                return_value=_FakeStrategy(),
            ),
            patch(
                "almanak.framework.cli.backtest.sweep.PnLBacktester",
                return_value=mock_backtester,
            ),
            patch(
                "almanak.framework.cli.backtest.sweep.CoinGeckoDataProvider",
            ),
            patch(
                "almanak.framework.cli.run.get_default_chain",
                return_value="arbitrum",
            ),
        ):
            result = _run_sweep_task_worker(task)
        assert result.params == {"a": "1"}

    def test_worker_strips_computed_properties(self) -> None:
        """Worker removes computed properties (duration_*, estimated_ticks) before from_dict."""
        from almanak.framework.cli.backtest.sweep import _run_sweep_task_worker

        # Inject computed properties that should be stripped
        pnl_dict = self._build_pnl_dict()
        pnl_dict["duration_seconds"] = 3600
        pnl_dict["duration_days"] = 30
        pnl_dict["estimated_ticks"] = 720

        backtest_result = _make_result({"a": "1"}, sharpe=1.0).result
        task = _SweepTask(
            strategy_class_name="decimal.Decimal",
            base_config={"chain": "arbitrum"},
            pnl_config_dict=pnl_dict,
            params={"a": "1"},
            task_index=0,
        )

        class _FakeStrategy:
            deployment_id = "fake"
            config: dict[str, Any] = {}

        mock_backtester = MagicMock()
        mock_backtester.backtest = AsyncMock(return_value=backtest_result)

        with (
            patch(
                "almanak.framework.cli.backtest.sweep._create_backtest_strategy",
                return_value=_FakeStrategy(),
            ),
            patch(
                "almanak.framework.cli.backtest.sweep.PnLBacktester",
                return_value=mock_backtester,
            ),
            patch(
                "almanak.framework.cli.backtest.sweep.CoinGeckoDataProvider",
            ),
            patch(
                "almanak.framework.cli.run.get_default_chain",
                return_value="arbitrum",
            ),
        ):
            # Should NOT raise (computed properties stripped pre-from_dict)
            result = _run_sweep_task_worker(task)
        assert result.params == {"a": "1"}

    def test_worker_string_param_preserved(self) -> None:
        """Non-numeric params retain string type in strategy_config."""
        from almanak.framework.cli.backtest.sweep import _run_sweep_task_worker

        backtest_result = _make_result({"mode": "conservative"}, sharpe=1.0).result
        task = _SweepTask(
            strategy_class_name="decimal.Decimal",
            base_config={"chain": "arbitrum"},
            pnl_config_dict=self._build_pnl_dict(),
            params={"mode": "conservative"},
            task_index=0,
        )

        class _FakeStrategy:
            deployment_id = "fake"
            config: dict[str, Any] = {}

        captured_configs: list[dict[str, Any]] = []

        def _spy(cls: Any, cfg: dict, chain: str) -> Any:
            captured_configs.append(cfg)
            return _FakeStrategy()

        mock_backtester = MagicMock()
        mock_backtester.backtest = AsyncMock(return_value=backtest_result)

        with (
            patch(
                "almanak.framework.cli.backtest.sweep._create_backtest_strategy",
                side_effect=_spy,
            ),
            patch(
                "almanak.framework.cli.backtest.sweep.PnLBacktester",
                return_value=mock_backtester,
            ),
            patch(
                "almanak.framework.cli.backtest.sweep.CoinGeckoDataProvider",
            ),
            patch(
                "almanak.framework.cli.run.get_default_chain",
                return_value="arbitrum",
            ),
        ):
            _run_sweep_task_worker(task)
        assert captured_configs[0]["mode"] == "conservative"

    def test_worker_attribute_fallback_non_dict_config(self) -> None:
        """When strategy.config is not a dict, params are set as attributes."""
        from almanak.framework.cli.backtest.sweep import _run_sweep_task_worker

        class NoConfigStrategy:
            deployment_id = "ncs"
            # No config attribute

        backtest_result = _make_result({"a": "1"}, sharpe=1.0).result
        task = _SweepTask(
            strategy_class_name="decimal.Decimal",
            base_config={"chain": "arbitrum"},
            pnl_config_dict=self._build_pnl_dict(),
            params={"numeric": "0.1", "text": "xx"},
            task_index=0,
        )

        instances: list[NoConfigStrategy] = []

        def _track(cls: Any, cfg: dict, chain: str) -> Any:
            inst = NoConfigStrategy()
            instances.append(inst)
            return inst

        mock_backtester = MagicMock()
        mock_backtester.backtest = AsyncMock(return_value=backtest_result)

        with (
            patch(
                "almanak.framework.cli.backtest.sweep._create_backtest_strategy",
                side_effect=_track,
            ),
            patch(
                "almanak.framework.cli.backtest.sweep.PnLBacktester",
                return_value=mock_backtester,
            ),
            patch(
                "almanak.framework.cli.backtest.sweep.CoinGeckoDataProvider",
            ),
            patch(
                "almanak.framework.cli.run.get_default_chain",
                return_value="arbitrum",
            ),
        ):
            _run_sweep_task_worker(task)
        # attributes set on instance (covers lines 500-504)
        inst = instances[0]
        assert inst.numeric == 0.1
        assert inst.text == "xx"

    def test_worker_sets_private_deployment_id_fallback(self) -> None:
        """When deployment_id is empty and _deployment_id exists, private attr is set."""
        from almanak.framework.cli.backtest.sweep import _run_sweep_task_worker

        class WithPrivate:
            def __init__(self) -> None:
                self._deployment_id = ""
                self.config: dict[str, Any] = {}

            @property
            def deployment_id(self) -> str:
                return self._deployment_id

        backtest_result = _make_result({"a": "1"}, sharpe=1.0).result
        task = _SweepTask(
            strategy_class_name="decimal.Decimal",
            base_config={"chain": "arbitrum"},
            pnl_config_dict=self._build_pnl_dict(),
            params={"threshold": "0.02"},
            task_index=0,
        )

        instances: list[WithPrivate] = []

        def _track(cls: Any, cfg: dict, chain: str) -> Any:
            inst = WithPrivate()
            instances.append(inst)
            return inst

        mock_backtester = MagicMock()
        mock_backtester.backtest = AsyncMock(return_value=backtest_result)

        with (
            patch(
                "almanak.framework.cli.backtest.sweep._create_backtest_strategy",
                side_effect=_track,
            ),
            patch(
                "almanak.framework.cli.backtest.sweep.PnLBacktester",
                return_value=mock_backtester,
            ),
            patch(
                "almanak.framework.cli.backtest.sweep.CoinGeckoDataProvider",
            ),
            patch(
                "almanak.framework.cli.run.get_default_chain",
                return_value="arbitrum",
            ),
        ):
            _run_sweep_task_worker(task)
        # Private attr received the fallback (covers lines 508-511)
        inst = instances[0]
        assert inst._deployment_id == "sweep-threshold0.02"

    def test_worker_sets_public_deployment_id_fallback(self) -> None:
        """When no _deployment_id attr exists, public `deployment_id` is set directly."""
        from almanak.framework.cli.backtest.sweep import _run_sweep_task_worker

        class PlainStrat:
            deployment_id = ""  # empty → triggers fallback

            def __init__(self) -> None:
                self.config: dict[str, Any] = {}

        backtest_result = _make_result({"a": "1"}, sharpe=1.0).result
        task = _SweepTask(
            strategy_class_name="decimal.Decimal",
            base_config={"chain": "arbitrum"},
            pnl_config_dict=self._build_pnl_dict(),
            params={"x": "1"},
            task_index=0,
        )

        instances: list[PlainStrat] = []

        def _track(cls: Any, cfg: dict, chain: str) -> Any:
            inst = PlainStrat()
            instances.append(inst)
            return inst

        mock_backtester = MagicMock()
        mock_backtester.backtest = AsyncMock(return_value=backtest_result)

        with (
            patch(
                "almanak.framework.cli.backtest.sweep._create_backtest_strategy",
                side_effect=_track,
            ),
            patch(
                "almanak.framework.cli.backtest.sweep.PnLBacktester",
                return_value=mock_backtester,
            ),
            patch(
                "almanak.framework.cli.backtest.sweep.CoinGeckoDataProvider",
            ),
            patch(
                "almanak.framework.cli.run.get_default_chain",
                return_value="arbitrum",
            ),
        ):
            _run_sweep_task_worker(task)
        # Public attr received the fallback (covers line 513)
        inst = instances[0]
        assert inst.deployment_id == "sweep-x1"

    def test_worker_mock_strategy_class_instantiable(self) -> None:
        """MockWorkerStrategy fallback is instantiable with dict config."""
        from almanak.framework.cli.backtest.sweep import _run_sweep_task_worker

        # Both importlib (no_such_mod) AND registry raise → MockWorkerStrategy path
        backtest_result = _make_result({"a": "1"}, sharpe=1.0).result
        task = _SweepTask(
            strategy_class_name="no_such_mod.Strategy",
            base_config={},
            pnl_config_dict=self._build_pnl_dict(),
            params={"a": "1"},
            task_index=0,
        )

        captured_classes: list[Any] = []

        class _FakeStrategy:
            deployment_id = "fake"
            config: dict[str, Any] = {}

        def _capture(cls: Any, cfg: dict, chain: str) -> Any:
            captured_classes.append(cls)
            return _FakeStrategy()

        mock_backtester = MagicMock()
        mock_backtester.backtest = AsyncMock(return_value=backtest_result)

        with (
            patch(
                "almanak.framework.strategies.get_strategy",
                side_effect=ValueError("not registered"),
            ),
            patch(
                "almanak.framework.cli.backtest.sweep._create_backtest_strategy",
                side_effect=_capture,
            ),
            patch(
                "almanak.framework.cli.backtest.sweep.PnLBacktester",
                return_value=mock_backtester,
            ),
            patch(
                "almanak.framework.cli.backtest.sweep.CoinGeckoDataProvider",
            ),
            patch(
                "almanak.framework.cli.run.get_default_chain",
                return_value="arbitrum",
            ),
        ):
            _run_sweep_task_worker(task)

        # The fallback MockWorkerStrategy was passed to _create_backtest_strategy
        mock_cls = captured_classes[0]
        # Instantiate it to cover __init__ + decide (lines 476, 479)
        inst = mock_cls({"foo": "bar"})
        assert inst.config == {"foo": "bar"}
        assert inst.deployment_id == "mock-worker"
        assert inst.decide(market=None) is None  # type: ignore[arg-type]

    def test_worker_chain_resolution_priority(self) -> None:
        """base_config.chain > pnl_config_dict.chain > get_default_chain."""
        from almanak.framework.cli.backtest.sweep import _run_sweep_task_worker

        backtest_result = _make_result({"a": "1"}, sharpe=1.0).result
        pnl_dict = self._build_pnl_dict()

        # pnl_dict chain is "arbitrum"; base_config chain is "base" (should win)
        task = _SweepTask(
            strategy_class_name="decimal.Decimal",
            base_config={"chain": "base"},
            pnl_config_dict=pnl_dict,
            params={"a": "1"},
            task_index=0,
        )

        class _FakeStrategy:
            deployment_id = "fake"
            config: dict[str, Any] = {}

        mock_backtester = MagicMock()
        mock_backtester.backtest = AsyncMock(return_value=backtest_result)
        captured_chains: list[str] = []

        def _create_tracker(cls: Any, cfg: dict, chain: str) -> Any:
            captured_chains.append(chain)
            return _FakeStrategy()

        with (
            patch(
                "almanak.framework.cli.backtest.sweep._create_backtest_strategy",
                side_effect=_create_tracker,
            ),
            patch(
                "almanak.framework.cli.backtest.sweep.PnLBacktester",
                return_value=mock_backtester,
            ),
            patch(
                "almanak.framework.cli.backtest.sweep.CoinGeckoDataProvider",
            ),
            patch(
                "almanak.framework.cli.run.get_default_chain",
                return_value="optimism",
            ),
        ):
            _run_sweep_task_worker(task)
        # base_config.chain wins
        assert captured_chains == ["base"]


class TestPrintSweepResultsTable:
    def test_renders_header_and_best_block(self, capsys: pytest.CaptureFixture[str]) -> None:
        params = [SweepParameter(name="a", values=["1", "2"])]
        results = [
            _make_result({"a": "1"}, sharpe=2.0, period_name="single"),
            _make_result({"a": "2"}, sharpe=0.5, period_name="single"),
        ]
        print_sweep_results_table(results, params)
        out = capsys.readouterr().out
        assert "PARAMETER SWEEP RESULTS (sorted by Sharpe ratio)" in out
        assert "Rank" in out
        # Best-combination block is emitted
        assert "Best combination:" in out
        assert "Sharpe ratio:" in out

    def test_empty_results_no_best_block(self, capsys: pytest.CaptureFixture[str]) -> None:
        print_sweep_results_table([], [SweepParameter(name="a", values=["1"])])
        out = capsys.readouterr().out
        # Header + divider still emitted
        assert "PARAMETER SWEEP RESULTS" in out
        # But no Best combination block
        assert "Best combination:" not in out
