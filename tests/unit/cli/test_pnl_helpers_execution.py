"""Unit tests for execution helpers in `almanak.framework.cli.backtest.pnl`.

Phase 5B.2 extracts the execution-side chunks of `pnl_backtest` (warm-cache,
backtest run, benchmark comparison) into module-level helpers. These tests
exercise the behavioural contracts we must preserve:

- `_run_backtest`: exact error string + `sys.exit(1)` on failure.
- `_warm_cache`: `DataCache` returned, per-token swallow preserved (#1698),
  overall `asyncio.run` failure logs fallback text + does not raise.
- `_warm_cache_async`: per-token exception handling + outer `finally`.
- `_compute_strategy_returns`: prev_val<=0 -> Decimal("0") (preserves #1699).
- `_fetch_benchmark_returns`: awaits both providers and returns the tuple.
- `_print_benchmark_comparison`: early return when guards fail, bare-except
  emits `"Could not calculate benchmark metrics: {e}"`.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.backtesting import PnLBacktestConfig
from almanak.framework.backtesting.models import (
    BacktestEngine,
    BacktestMetrics,
    BacktestResult,
    EquityPoint,
)
from almanak.framework.cli.backtest._backtest_context import PnLBacktestContext
from almanak.framework.cli.backtest.pnl import (
    _compute_strategy_returns,
    _fetch_benchmark_returns,
    _print_benchmark_comparison,
    _run_backtest,
    _warm_cache,
    _warm_cache_async,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _make_pnl_config(tokens: list[str] | None = None) -> PnLBacktestConfig:
    return PnLBacktestConfig(
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 2, 1, tzinfo=UTC),
        interval_seconds=3600,
        initial_capital_usd=Decimal("10000"),
        chain="arbitrum",
        tokens=tokens if tokens is not None else ["WETH", "USDC"],
        gas_price_gwei=Decimal("30"),
        include_gas_costs=True,
    )


def _make_ctx(
    tokens: list[str] | None = None,
    output_path: Path | None = None,
) -> PnLBacktestContext:
    pnl_config = _make_pnl_config(tokens)
    return PnLBacktestContext(
        strategy="demo_strat",
        pnl_config=pnl_config,
        token_list=tokens if tokens is not None else ["WETH", "USDC"],
        output_path=output_path,
        loaded_from_result=False,
        start=pnl_config.start_time,
        end=pnl_config.end_time,
        interval=pnl_config.interval_seconds,
    )


def _make_result_with_equity(
    values: list[str],
    total_return_pct: str = "5.0",
) -> BacktestResult:
    equity_curve = [
        EquityPoint(
            timestamp=datetime(2024, 1, 1, i, tzinfo=UTC),
            value_usd=Decimal(v),
        )
        for i, v in enumerate(values)
    ]
    return BacktestResult(
        engine=BacktestEngine.PNL,
        strategy_id="demo",
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 2, 1, tzinfo=UTC),
        metrics=BacktestMetrics(
            total_trades=0,
            win_rate=Decimal("0.5"),
            total_return_pct=Decimal(total_return_pct),
            max_drawdown_pct=Decimal("1"),
            sharpe_ratio=Decimal("1"),
            sortino_ratio=Decimal("1"),
            calmar_ratio=Decimal("1"),
            profit_factor=Decimal("1"),
            annualized_return_pct=Decimal("10"),
            net_pnl_usd=Decimal("500"),
        ),
        equity_curve=equity_curve,
    )


@dataclass
class _FakeOHLCV:
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal | None = None


# ===========================================================================
# _run_backtest
# ===========================================================================


class TestRunBacktest:
    def test_returns_result_on_success(self) -> None:
        expected = _make_result_with_equity(["100"])
        backtester = MagicMock()
        backtester.backtest = AsyncMock(return_value=expected)

        result = _run_backtest(backtester, MagicMock(), _make_pnl_config())
        assert result is expected

    def test_prints_error_and_exits_on_failure(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        backtester = MagicMock()
        backtester.backtest = AsyncMock(side_effect=RuntimeError("boom"))

        with pytest.raises(SystemExit) as exc_info:
            _run_backtest(backtester, MagicMock(), _make_pnl_config())

        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "Error running backtest: boom" in captured.err

    def test_preserves_exact_error_string_format(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Grep-asserted string — must not change."""
        backtester = MagicMock()
        backtester.backtest = AsyncMock(side_effect=ValueError("kaboom"))

        with pytest.raises(SystemExit):
            _run_backtest(backtester, MagicMock(), _make_pnl_config())

        captured = capsys.readouterr()
        assert captured.err.startswith("Error running backtest: kaboom")


# ===========================================================================
# _warm_cache_async
# ===========================================================================


class TestWarmCacheAsync:
    def test_caches_successful_token(self, capsys: pytest.CaptureFixture[str]) -> None:
        data_provider = MagicMock()
        data_provider.close = AsyncMock()
        data_provider.get_ohlcv = AsyncMock(
            return_value=[
                _FakeOHLCV(
                    timestamp=datetime(2024, 1, 1, 0, tzinfo=UTC),
                    open=Decimal("100"),
                    high=Decimal("110"),
                    low=Decimal("90"),
                    close=Decimal("105"),
                )
            ]
        )

        cache = MagicMock()
        cache.set_batch = MagicMock(return_value=1)

        total = asyncio.run(
            _warm_cache_async(
                data_provider=data_provider,
                cache=cache,
                token_list=["WETH"],
                start=datetime(2024, 1, 1, tzinfo=UTC),
                end=datetime(2024, 2, 1, tzinfo=UTC),
                interval=3600,
                pnl_config=_make_pnl_config(),
            )
        )

        assert total == 1
        captured = capsys.readouterr()
        assert "Cached 1 data points for WETH" in captured.out
        data_provider.close.assert_awaited_once()

    def test_swallows_per_token_error(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Issue #1698 — per-token failures warn but do not abort."""
        data_provider = MagicMock()
        data_provider.close = AsyncMock()

        async def _fail(token: str, *a: Any, **kw: Any) -> list[_FakeOHLCV]:
            if token == "WETH":
                raise RuntimeError("API down")
            return []

        data_provider.get_ohlcv = AsyncMock(side_effect=_fail)
        cache = MagicMock()
        cache.set_batch = MagicMock(return_value=0)

        total = asyncio.run(
            _warm_cache_async(
                data_provider=data_provider,
                cache=cache,
                token_list=["WETH", "USDC"],
                start=datetime(2024, 1, 1, tzinfo=UTC),
                end=datetime(2024, 2, 1, tzinfo=UTC),
                interval=3600,
                pnl_config=_make_pnl_config(),
            )
        )

        assert total == 0
        captured = capsys.readouterr()
        assert "Warning: Failed to cache data for WETH: API down" in captured.err

    def test_close_runs_in_finally_even_on_outer_error(self) -> None:
        """Outer failure must still close the data provider."""
        data_provider = MagicMock()
        data_provider.close = AsyncMock()
        data_provider.get_ohlcv = AsyncMock(side_effect=RuntimeError("fatal"))

        cache = MagicMock()

        # inner swallow + finally close — even though get_ohlcv raises,
        # the per-token try/except swallows it so total==0 and close runs.
        total = asyncio.run(
            _warm_cache_async(
                data_provider=data_provider,
                cache=cache,
                token_list=["WETH"],
                start=datetime(2024, 1, 1, tzinfo=UTC),
                end=datetime(2024, 2, 1, tzinfo=UTC),
                interval=3600,
                pnl_config=_make_pnl_config(),
            )
        )
        assert total == 0
        data_provider.close.assert_awaited_once()

    def test_empty_token_list_still_closes(self) -> None:
        data_provider = MagicMock()
        data_provider.close = AsyncMock()

        total = asyncio.run(
            _warm_cache_async(
                data_provider=data_provider,
                cache=MagicMock(),
                token_list=[],
                start=datetime(2024, 1, 1, tzinfo=UTC),
                end=datetime(2024, 2, 1, tzinfo=UTC),
                interval=3600,
                pnl_config=_make_pnl_config(),
            )
        )
        assert total == 0
        data_provider.close.assert_awaited_once()


# ===========================================================================
# _warm_cache
# ===========================================================================


class TestWarmCache:
    def test_returns_cache_on_success(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        with (
            patch(
                "almanak.framework.cli.backtest.pnl._warm_cache_async",
                new=AsyncMock(return_value=42),
            ),
            patch(
                "almanak.framework.cli.backtest.pnl.CoinGeckoDataProvider"
            ),
        ):
            result = _warm_cache(
                _make_ctx(),
                start=datetime(2024, 1, 1, tzinfo=UTC),
                end=datetime(2024, 2, 1, tzinfo=UTC),
                interval=3600,
            )

        # Return shape: DataCache | None (non-None today).
        assert result is not None
        captured = capsys.readouterr()
        assert "Warming data cache..." in captured.out
        assert "Cache warming complete: 42 total data points" in captured.out

    def test_preserves_fallback_line_on_overall_failure(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Issue #1698 — overall warming failure logs fallback and does not raise."""

        async def _blow(*a: Any, **kw: Any) -> int:
            raise RuntimeError("event-loop boom")

        with (
            patch(
                "almanak.framework.cli.backtest.pnl._warm_cache_async",
                new=AsyncMock(side_effect=_blow),
            ),
            patch(
                "almanak.framework.cli.backtest.pnl.CoinGeckoDataProvider"
            ),
        ):
            result = _warm_cache(
                _make_ctx(),
                start=datetime(2024, 1, 1, tzinfo=UTC),
                end=datetime(2024, 2, 1, tzinfo=UTC),
                interval=3600,
            )

        assert result is not None
        captured = capsys.readouterr()
        assert "Warning: Cache warming failed: event-loop boom" in captured.err
        assert "Proceeding with backtest without pre-warmed cache..." in captured.out

    def test_emits_warming_banner(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        with (
            patch(
                "almanak.framework.cli.backtest.pnl._warm_cache_async",
                new=AsyncMock(return_value=0),
            ),
            patch(
                "almanak.framework.cli.backtest.pnl.CoinGeckoDataProvider"
            ),
        ):
            _warm_cache(
                _make_ctx(),
                start=datetime(2024, 1, 1, tzinfo=UTC),
                end=datetime(2024, 2, 1, tzinfo=UTC),
                interval=3600,
            )
        captured = capsys.readouterr()
        assert "Warming data cache..." in captured.out


# ===========================================================================
# _compute_strategy_returns
# ===========================================================================


class TestComputeStrategyReturns:
    def test_positive_steps(self) -> None:
        result = _make_result_with_equity(["100", "110", "121"])
        returns = _compute_strategy_returns(result.equity_curve)
        assert returns == [Decimal("110") / Decimal("100") - 1, Decimal("121") / Decimal("110") - 1]
        assert all(r > 0 for r in returns)

    def test_zero_prev_val_yields_zero(self) -> None:
        result = _make_result_with_equity(["0", "100"])
        returns = _compute_strategy_returns(result.equity_curve)
        assert returns == [Decimal("0")]

    def test_negative_prev_val_yields_zero(self) -> None:
        # Guard is prev_val > 0 — negatives also map to 0.
        result = _make_result_with_equity(["-10", "100"])
        returns = _compute_strategy_returns(result.equity_curve)
        assert returns == [Decimal("0")]

    def test_single_point_produces_empty_list(self) -> None:
        result = _make_result_with_equity(["100"])
        assert _compute_strategy_returns(result.equity_curve) == []

    def test_empty_curve_produces_empty_list(self) -> None:
        assert _compute_strategy_returns([]) == []


# ===========================================================================
# _fetch_benchmark_returns
# ===========================================================================


class TestFetchBenchmarkReturns:
    def test_returns_tuple_from_providers(self) -> None:
        fake_returns = [Decimal("0.01"), Decimal("0.02")]
        fake_total = Decimal("0.5")

        with (
            patch(
                "almanak.framework.backtesting.pnl.providers.benchmark.Benchmark"
            ) as mock_benchmark,
            patch(
                "almanak.framework.backtesting.pnl.providers.benchmark.get_benchmark_returns",
                new=AsyncMock(return_value=fake_returns),
            ),
            patch(
                "almanak.framework.backtesting.pnl.providers.benchmark.get_benchmark_total_return",
                new=AsyncMock(return_value=fake_total),
            ),
        ):
            mock_benchmark.from_string = MagicMock(return_value="ETH_HOLD")
            returns, total = asyncio.run(
                _fetch_benchmark_returns(
                    "eth_hold",
                    datetime(2024, 1, 1, tzinfo=UTC),
                    datetime(2024, 2, 1, tzinfo=UTC),
                    3600,
                )
            )

        assert returns == fake_returns
        assert total == fake_total
        mock_benchmark.from_string.assert_called_once_with("eth_hold")


# ===========================================================================
# _print_benchmark_comparison
# ===========================================================================


class TestPrintBenchmarkComparison:
    def test_noop_when_benchmark_empty(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        ctx = _make_ctx()
        _print_benchmark_comparison(
            ctx,
            _make_result_with_equity(["100", "110"]),
            benchmark="",
            start=datetime(2024, 1, 1, tzinfo=UTC),
            end=datetime(2024, 2, 1, tzinfo=UTC),
            interval=3600,
        )
        captured = capsys.readouterr()
        assert captured.out == ""
        assert captured.err == ""

    def test_noop_when_start_missing(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        ctx = _make_ctx()
        _print_benchmark_comparison(
            ctx,
            _make_result_with_equity(["100", "110"]),
            benchmark="eth_hold",
            start=None,
            end=datetime(2024, 2, 1, tzinfo=UTC),
            interval=3600,
        )
        captured = capsys.readouterr()
        assert "BENCHMARK COMPARISON" not in captured.out

    def test_noop_when_end_missing(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        ctx = _make_ctx()
        _print_benchmark_comparison(
            ctx,
            _make_result_with_equity(["100", "110"]),
            benchmark="eth_hold",
            start=datetime(2024, 1, 1, tzinfo=UTC),
            end=None,
            interval=3600,
        )
        captured = capsys.readouterr()
        assert "BENCHMARK COMPARISON" not in captured.out

    def test_renders_block_when_data_available(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        ctx = _make_ctx()
        result = _make_result_with_equity(["100", "110", "121"], total_return_pct="21")

        # benchmark_returns needs min_len>=2 along with strategy_returns.
        with patch(
            "almanak.framework.cli.backtest.pnl._fetch_benchmark_returns",
            new=AsyncMock(
                return_value=([Decimal("0.05"), Decimal("0.05")], Decimal("0.1"))
            ),
        ):
            _print_benchmark_comparison(
                ctx,
                result,
                benchmark="eth_hold",
                start=datetime(2024, 1, 1, tzinfo=UTC),
                end=datetime(2024, 2, 1, tzinfo=UTC),
                interval=3600,
            )

        captured = capsys.readouterr()
        assert "BENCHMARK COMPARISON (ETH_HOLD)" in captured.out
        assert "Benchmark Return:" in captured.out
        assert "Strategy Return:" in captured.out
        assert "Excess Return:" in captured.out
        assert "Information Ratio:" in captured.out
        assert "Beta:" in captured.out
        assert "Alpha:" in captured.out

    def test_insufficient_data_when_min_len_below_two(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """min_len = min(len(strategy_returns), len(benchmark_returns)); need >= 2."""
        ctx = _make_ctx()
        result = _make_result_with_equity(["100", "110"])

        with patch(
            "almanak.framework.cli.backtest.pnl._fetch_benchmark_returns",
            new=AsyncMock(return_value=([Decimal("0.01")], Decimal("0.02"))),
        ):
            _print_benchmark_comparison(
                ctx,
                result,
                benchmark="eth_hold",
                start=datetime(2024, 1, 1, tzinfo=UTC),
                end=datetime(2024, 2, 1, tzinfo=UTC),
                interval=3600,
            )

        captured = capsys.readouterr()
        assert "Insufficient data for benchmark comparison." in captured.out

    def test_no_equity_curve_message_when_curve_short(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        ctx = _make_ctx()
        result = _make_result_with_equity(["100"])

        with patch(
            "almanak.framework.cli.backtest.pnl._fetch_benchmark_returns",
            new=AsyncMock(return_value=([Decimal("0.01"), Decimal("0.02")], Decimal("0.02"))),
        ):
            _print_benchmark_comparison(
                ctx,
                result,
                benchmark="eth_hold",
                start=datetime(2024, 1, 1, tzinfo=UTC),
                end=datetime(2024, 2, 1, tzinfo=UTC),
                interval=3600,
            )

        captured = capsys.readouterr()
        assert "No equity curve data for benchmark comparison." in captured.out

    def test_preserves_could_not_calculate_error_string(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Issue #1699 — bare-except fallback must print 'Could not calculate...'."""
        ctx = _make_ctx()
        result = _make_result_with_equity(["100", "110"])

        with patch(
            "almanak.framework.cli.backtest.pnl._fetch_benchmark_returns",
            new=AsyncMock(side_effect=RuntimeError("provider down")),
        ):
            _print_benchmark_comparison(
                ctx,
                result,
                benchmark="eth_hold",
                start=datetime(2024, 1, 1, tzinfo=UTC),
                end=datetime(2024, 2, 1, tzinfo=UTC),
                interval=3600,
            )

        captured = capsys.readouterr()
        assert "Could not calculate benchmark metrics: provider down" in captured.out

    def test_uppercases_benchmark_name_in_heading(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        ctx = _make_ctx()
        result = _make_result_with_equity(["100"])

        with patch(
            "almanak.framework.cli.backtest.pnl._fetch_benchmark_returns",
            new=AsyncMock(return_value=([Decimal("0.01")], Decimal("0.02"))),
        ):
            _print_benchmark_comparison(
                ctx,
                result,
                benchmark="btc_hold",
                start=datetime(2024, 1, 1, tzinfo=UTC),
                end=datetime(2024, 2, 1, tzinfo=UTC),
                interval=3600,
            )

        captured = capsys.readouterr()
        assert "BENCHMARK COMPARISON (BTC_HOLD)" in captured.out
