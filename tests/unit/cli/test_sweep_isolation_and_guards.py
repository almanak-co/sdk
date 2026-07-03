"""Async sweep failure isolation (VIB-5622) and inert-result guards (VIB-5623).

The default (async) sweep mode must match the ``--parallel`` path's failure
semantics: a failing combo becomes an error-carrying SweepResult, never an
abort that discards completed work. And a sweep whose every combo executed
zero trades must warn instead of silently declaring a "Best combination"
(the published-2.20.0 missing-token_funding regression produced exactly that).
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.backtesting.models import (
    BacktestEngine,
    BacktestMetrics,
    BacktestResult,
)
from almanak.framework.cli.backtest.helpers import SweepResult
from almanak.framework.cli.backtest.run_helpers import build_pnl_config
from almanak.framework.cli.backtest.sweep import (
    _failed_sweep_result,
    _run_period_sweeps_owning_provider,
    _warn_if_sweep_results_inert,
    run_parallel_sweeps,
)
from tests.backtesting_funding import pnl_token_funding


def _pnl_config():
    return build_pnl_config(
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 1, 2, tzinfo=UTC),
        interval_seconds=3600,
        chain="arbitrum",
        tokens=["WETH", "USDC"],
        token_funding=pnl_token_funding("10000"),
    )


def _sweep_result(params: dict[str, str], *, trades: int, error: str | None = None) -> SweepResult:
    return SweepResult(
        params=params,
        result=BacktestResult(
            engine=BacktestEngine.PNL,
            deployment_id="test",
            start_time=datetime(2024, 1, 1, tzinfo=UTC),
            end_time=datetime(2024, 1, 2, tzinfo=UTC),
            metrics=BacktestMetrics(),
            trades=[],
            initial_portfolio_value_usd=Decimal("0"),
            final_capital_usd=Decimal("0"),
            chain="arbitrum",
            error=error,
        ),
        sharpe_ratio=Decimal("1"),
        total_return_pct=Decimal("1"),
        max_drawdown_pct=Decimal("0"),
        win_rate=Decimal("0"),
        total_trades=trades,
    )


class TestAsyncPerComboIsolation:
    def test_one_failing_combo_does_not_abort_the_sweep(self) -> None:
        """VIB-5622: async mode records the failure and keeps the rest."""
        pnl_config = _pnl_config()
        combos = [{"a": "1"}, {"a": "2"}, {"a": "3"}]

        async def _fake_run_sweep_backtest(*, params: dict[str, str], **kwargs) -> SweepResult:
            if params == {"a": "2"}:
                raise RuntimeError("combo blew up")
            return _sweep_result(params, trades=5)

        with patch(
            "almanak.framework.cli.backtest.sweep.run_sweep_backtest",
            side_effect=_fake_run_sweep_backtest,
        ):
            results = asyncio.run(
                run_parallel_sweeps(
                    strategy_class=MagicMock(),
                    base_config={},
                    pnl_config=pnl_config,
                    data_provider=MagicMock(close=AsyncMock()),
                    combinations=combos,
                    parallel=2,
                    emit_ambiguity_warnings=False,
                )
            )

        assert len(results) == 3
        by_params = {tuple(r.params.items()): r for r in results}
        failed = by_params[(("a", "2"),)]
        assert failed.result.error == "combo blew up"
        assert failed.result.success is False
        assert failed.total_trades == 0
        for good_key in [(("a", "1"),), (("a", "3"),)]:
            assert by_params[good_key].result.error is None

    def test_failed_result_carries_run_metadata(self) -> None:
        pnl_config = _pnl_config()
        result = _failed_sweep_result({"x": "1"}, pnl_config, RuntimeError("nope"))
        assert result.result.error == "nope"
        assert result.result.chain == pnl_config.chain
        assert result.result.start_time == pnl_config.start_time
        assert result.result.end_time == pnl_config.end_time
        assert result.sharpe_ratio == Decimal("0")


class TestPeriodProviderOwnership:
    def test_provider_closed_inside_period_loop(self) -> None:
        """VIB-5621: the sweep orchestration closes the shared provider once
        per period, inside that period's event loop."""
        provider = MagicMock(close=AsyncMock())

        async def _fake_sweeps(**kwargs) -> list[SweepResult]:
            return [_sweep_result({"a": "1"}, trades=1)]

        with patch(
            "almanak.framework.cli.backtest.sweep.run_parallel_sweeps",
            side_effect=_fake_sweeps,
        ):
            results = asyncio.run(
                _run_period_sweeps_owning_provider(
                    strategy_class=MagicMock(),
                    base_config={},
                    pnl_config=_pnl_config(),
                    data_provider=provider,
                    combinations=[{"a": "1"}],
                    parallel=2,
                    numeric_param_names=frozenset(),
                )
            )
        assert len(results) == 1
        provider.close.assert_awaited_once()

    def test_provider_closed_even_when_period_fails(self) -> None:
        provider = MagicMock(close=AsyncMock())
        with patch(
            "almanak.framework.cli.backtest.sweep.run_parallel_sweeps",
            side_effect=RuntimeError("period crash"),
        ):
            with pytest.raises(RuntimeError, match="period crash"):
                asyncio.run(
                    _run_period_sweeps_owning_provider(
                        strategy_class=MagicMock(),
                        base_config={},
                        pnl_config=_pnl_config(),
                        data_provider=provider,
                        combinations=[{"a": "1"}],
                        parallel=2,
                        numeric_param_names=frozenset(),
                    )
                )
        provider.close.assert_awaited_once()


class TestInertSweepGuards:
    def test_all_zero_trades_warns(self, capsys: pytest.CaptureFixture[str]) -> None:
        """VIB-5623: an all-zero-trade sweep must not look successful."""
        results = [_sweep_result({"a": "1"}, trades=0), _sweep_result({"a": "2"}, trades=0)]
        _warn_if_sweep_results_inert(results)
        err = capsys.readouterr().err
        assert "no parameter combination executed a single trade" in err
        assert "token_funding" in err

    def test_failed_combos_warn(self, capsys: pytest.CaptureFixture[str]) -> None:
        results = [
            _sweep_result({"a": "1"}, trades=3),
            _sweep_result({"a": "2"}, trades=0, error="worker died"),
        ]
        _warn_if_sweep_results_inert(results)
        err = capsys.readouterr().err
        assert "1/2 sweep runs failed" in err
        assert "worker died" in err

    def test_healthy_results_stay_silent(self, capsys: pytest.CaptureFixture[str]) -> None:
        results = [_sweep_result({"a": "1"}, trades=3), _sweep_result({"a": "2"}, trades=1)]
        _warn_if_sweep_results_inert(results)
        assert capsys.readouterr().err == ""

    def test_empty_results_stay_silent(self, capsys: pytest.CaptureFixture[str]) -> None:
        _warn_if_sweep_results_inert([])
        assert capsys.readouterr().err == ""

    def test_empty_string_error_counts_as_failed(self, capsys: pytest.CaptureFixture[str]) -> None:
        """A message-less exception str()s to "" — still a failure (error is
        not None is the success contract), never silently ignored."""
        results = [
            _sweep_result({"a": "1"}, trades=3),
            _sweep_result({"a": "2"}, trades=0, error=""),
        ]
        _warn_if_sweep_results_inert(results)
        err = capsys.readouterr().err
        assert "1/2 sweep runs failed" in err
        assert "<no message>" in err

    def test_all_failed_runs_skip_zero_trades_warning(self, capsys: pytest.CaptureFixture[str]) -> None:
        """When every run failed, the zero-trades warning is misleading noise —
        the failure warning already tells the real story."""
        results = [
            _sweep_result({"a": "1"}, trades=0, error="boom"),
            _sweep_result({"a": "2"}, trades=0, error="boom"),
        ]
        _warn_if_sweep_results_inert(results)
        err = capsys.readouterr().err
        assert "2/2 sweep runs failed" in err
        assert "no parameter combination executed a single trade" not in err
