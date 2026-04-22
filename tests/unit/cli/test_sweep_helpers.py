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
from unittest.mock import patch

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
    _SweepRunContext,
    _compute_worker_count,
    _handle_sweep_dry_run,
    _parse_sweep_params,
    _resolve_backtest_periods,
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
        strategy_id="test",
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
        assert cfg.gas_price_gwei == Decimal("30.0")
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
