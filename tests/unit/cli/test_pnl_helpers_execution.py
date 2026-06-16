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
    WarmCacheOutcome,
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
        deployment_id="demo",
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

    def test_prints_error_and_exits_on_failure(self, capsys: pytest.CaptureFixture[str]) -> None:
        backtester = MagicMock()
        backtester.backtest = AsyncMock(side_effect=RuntimeError("boom"))

        with pytest.raises(SystemExit) as exc_info:
            _run_backtest(backtester, MagicMock(), _make_pnl_config())

        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "Error running backtest: boom" in captured.err

    def test_preserves_exact_error_string_format(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Grep-asserted string — must not change."""
        backtester = MagicMock()
        backtester.backtest = AsyncMock(side_effect=ValueError("kaboom"))

        with pytest.raises(SystemExit):
            _run_backtest(backtester, MagicMock(), _make_pnl_config())

        captured = capsys.readouterr()
        assert captured.err.startswith("Error running backtest: kaboom")

    def test_preflight_validation_error_prints_banner_and_exits_2(self, capsys: pytest.CaptureFixture[str]) -> None:
        """A PreflightValidationError aborts loudly with exit code 2 and the remedy hint."""
        from almanak.framework.backtesting.pnl.error_handling import PreflightValidationError

        backtester = MagicMock()
        backtester.backtest = AsyncMock(
            side_effect=PreflightValidationError("No historical price data for WSTETH", error_count=1)
        )

        with pytest.raises(SystemExit) as exc_info:
            _run_backtest(backtester, MagicMock(), _make_pnl_config())

        assert exc_info.value.code == 2
        captured = capsys.readouterr()
        assert "BACKTEST ABORTED: PREFLIGHT VALIDATION FAILED" in captured.err
        assert "--allow-missing-prices" in captured.err


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

        outcome = asyncio.run(
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

        assert isinstance(outcome, WarmCacheOutcome)
        assert outcome.total_cached == 1
        assert outcome.successful_warms == 1
        assert outcome.total_tokens == 1
        captured = capsys.readouterr()
        assert "Cached 1 data points for WETH" in captured.out
        data_provider.close.assert_awaited_once()

    def test_swallows_per_token_error(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Issue #1698 — per-token failures warn but do not abort.

        The warning stderr text is preserved, and the failure is now surfaced
        via `successful_warms < total_tokens` so callers can act on it.
        """
        data_provider = MagicMock()
        data_provider.close = AsyncMock()

        async def _fail(token: str, *a: Any, **kw: Any) -> list[_FakeOHLCV]:
            if token == "WETH":
                raise RuntimeError("API down")
            return []

        data_provider.get_ohlcv = AsyncMock(side_effect=_fail)
        cache = MagicMock()
        cache.set_batch = MagicMock(return_value=0)

        outcome = asyncio.run(
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

        assert outcome.total_cached == 0
        assert outcome.successful_warms == 1  # USDC succeeded (empty data but no error)
        assert outcome.total_tokens == 2
        assert outcome.success_ratio == 0.5
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
        outcome = asyncio.run(
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
        assert outcome.total_cached == 0
        assert outcome.successful_warms == 0
        assert outcome.total_tokens == 1
        data_provider.close.assert_awaited_once()

    def test_empty_token_list_still_closes(self) -> None:
        data_provider = MagicMock()
        data_provider.close = AsyncMock()

        outcome = asyncio.run(
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
        assert outcome.total_cached == 0
        assert outcome.successful_warms == 0
        assert outcome.total_tokens == 0
        # Empty-token success_ratio defaults to 1.0 (nothing to fail on).
        assert outcome.success_ratio == 1.0
        data_provider.close.assert_awaited_once()


# ===========================================================================
# _warm_cache
# ===========================================================================


class TestWarmCache:
    def test_returns_cache_on_success(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        outcome = WarmCacheOutcome(
            total_cached=42, successful_warms=2, total_tokens=2
        )
        with (
            patch(
                "almanak.framework.cli.backtest.pnl._warm_cache_async",
                new=AsyncMock(return_value=outcome),
            ),
            patch("almanak.framework.cli.backtest.pnl.CoinGeckoDataProvider"),
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
        # Success-count now surfaced alongside total points (issue #1698).
        assert "Cache warming complete: 42 total data points" in captured.out
        assert "2/2 tokens successful" in captured.out

    def test_preserves_fallback_line_on_overall_failure(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Issue #1698 — overall warming failure logs fallback and does not raise.

        Non-strict mode preserves the original stderr + stdout lines
        byte-for-byte. Only `--strict-warm` changes the failure path.

        On overall failure we now return `None` rather than the partially
        populated `DataCache` — handing back a partial cache would
        misrepresent the `Proceeding with backtest without pre-warmed cache...`
        contract that downstream code / log scrapers rely on.
        """

        async def _blow(*a: Any, **kw: Any) -> WarmCacheOutcome:
            raise RuntimeError("event-loop boom")

        with (
            patch(
                "almanak.framework.cli.backtest.pnl._warm_cache_async",
                new=AsyncMock(side_effect=_blow),
            ),
            patch("almanak.framework.cli.backtest.pnl.CoinGeckoDataProvider"),
        ):
            result = _warm_cache(
                _make_ctx(),
                start=datetime(2024, 1, 1, tzinfo=UTC),
                end=datetime(2024, 2, 1, tzinfo=UTC),
                interval=3600,
            )

        assert result is None
        captured = capsys.readouterr()
        assert "Warning: Cache warming failed: event-loop boom" in captured.err
        assert "Proceeding with backtest without pre-warmed cache..." in captured.out

    def test_emits_warming_banner(self, capsys: pytest.CaptureFixture[str]) -> None:
        with (
            patch(
                "almanak.framework.cli.backtest.pnl._warm_cache_async",
                new=AsyncMock(
                    return_value=WarmCacheOutcome(
                        total_cached=0, successful_warms=2, total_tokens=2
                    )
                ),
            ),
            patch("almanak.framework.cli.backtest.pnl.CoinGeckoDataProvider"),
        ):
            _warm_cache(
                _make_ctx(),
                start=datetime(2024, 1, 1, tzinfo=UTC),
                end=datetime(2024, 2, 1, tzinfo=UTC),
                interval=3600,
            )
        captured = capsys.readouterr()
        assert "Warming data cache..." in captured.out

    def test_strict_mode_aborts_on_partial_warm(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Issue #1698 — `--strict-warm` must abort when any token fails."""
        import click

        outcome = WarmCacheOutcome(
            total_cached=10, successful_warms=1, total_tokens=2
        )
        with (
            patch(
                "almanak.framework.cli.backtest.pnl._warm_cache_async",
                new=AsyncMock(return_value=outcome),
            ),
            patch(
                "almanak.framework.cli.backtest.pnl.CoinGeckoDataProvider"
            ),
        ):
            with pytest.raises(click.Abort):
                _warm_cache(
                    _make_ctx(),
                    start=datetime(2024, 1, 1, tzinfo=UTC),
                    end=datetime(2024, 2, 1, tzinfo=UTC),
                    interval=3600,
                    strict=True,
                )

        captured = capsys.readouterr()
        assert "Strict warm-cache" in captured.err
        assert "1/2 tokens warmed successfully" in captured.err

    def test_strict_mode_aborts_on_overall_failure(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Issue #1698 — `--strict-warm` also aborts on overall asyncio failure."""
        import click

        async def _blow(*a: Any, **kw: Any) -> WarmCacheOutcome:
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
            with pytest.raises(click.Abort):
                _warm_cache(
                    _make_ctx(),
                    start=datetime(2024, 1, 1, tzinfo=UTC),
                    end=datetime(2024, 2, 1, tzinfo=UTC),
                    interval=3600,
                    strict=True,
                )

        captured = capsys.readouterr()
        # Original "Warning: Cache warming failed" is still emitted first
        # (preserves external log scrapers), then the strict error.
        assert "Warning: Cache warming failed: event-loop boom" in captured.err
        assert "Strict warm-cache: overall warming failed" in captured.err

    def test_strict_mode_accepts_fully_successful_warm(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """`--strict-warm` does nothing when every token succeeds."""
        outcome = WarmCacheOutcome(
            total_cached=100, successful_warms=2, total_tokens=2
        )
        with (
            patch(
                "almanak.framework.cli.backtest.pnl._warm_cache_async",
                new=AsyncMock(return_value=outcome),
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
                strict=True,
            )
        assert result is not None

    def test_non_strict_warns_on_low_success_ratio(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Issue #1698 — below the threshold, non-strict mode surfaces a warning.

        Default threshold is 50%. 1/4 = 25% triggers the warning. We only need
        `outcome.total_tokens == 4` for the ratio check — the ctx tokens list
        is not touched by `_warm_cache` after the async helper returns.
        """
        outcome = WarmCacheOutcome(
            total_cached=10, successful_warms=1, total_tokens=4
        )
        with (
            patch(
                "almanak.framework.cli.backtest.pnl._warm_cache_async",
                new=AsyncMock(return_value=outcome),
            ),
            patch(
                "almanak.framework.cli.backtest.pnl.CoinGeckoDataProvider"
            ),
        ):
            _warm_cache(
                _make_ctx(tokens=["WETH", "USDC"]),
                start=datetime(2024, 1, 1, tzinfo=UTC),
                end=datetime(2024, 2, 1, tzinfo=UTC),
                interval=3600,
            )
        captured = capsys.readouterr()
        assert "warm cache only succeeded for 1/4 tokens" in captured.err
        assert "--strict-warm" in captured.err


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
            patch("almanak.framework.backtesting.pnl.providers.benchmark.Benchmark") as mock_benchmark,
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
    def test_noop_when_benchmark_empty(self, capsys: pytest.CaptureFixture[str]) -> None:
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

    def test_noop_when_start_missing(self, capsys: pytest.CaptureFixture[str]) -> None:
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

    def test_noop_when_end_missing(self, capsys: pytest.CaptureFixture[str]) -> None:
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

    def test_renders_block_when_data_available(self, capsys: pytest.CaptureFixture[str]) -> None:
        ctx = _make_ctx()
        result = _make_result_with_equity(["100", "110", "121"], total_return_pct="21")

        # benchmark_returns needs min_len>=2 along with strategy_returns.
        with patch(
            "almanak.framework.cli.backtest.pnl._fetch_benchmark_returns",
            new=AsyncMock(return_value=([Decimal("0.05"), Decimal("0.05")], Decimal("0.1"))),
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

    def test_insufficient_data_when_min_len_below_two(self, capsys: pytest.CaptureFixture[str]) -> None:
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

    def test_no_equity_curve_message_when_curve_short(self, capsys: pytest.CaptureFixture[str]) -> None:
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
        """Issue #1699 — narrow-except fallback must print 'Could not calculate...'.

        Historically this caught bare `Exception`; we now catch a narrow tuple
        of expected data/network errors. `ValueError` is representative of the
        "bad data from provider" path and must still produce the banner line.
        """
        ctx = _make_ctx()
        result = _make_result_with_equity(["100", "110"])

        with patch(
            "almanak.framework.cli.backtest.pnl._fetch_benchmark_returns",
            new=AsyncMock(side_effect=ValueError("provider down")),
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

    def test_unexpected_exception_propagates(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Issue #1699 — exceptions outside the narrow tuple must propagate.

        A plain `RuntimeError` is NOT expected from the benchmark path and must
        now surface rather than being swallowed behind the banner line.
        """
        ctx = _make_ctx()
        result = _make_result_with_equity(["100", "110"])

        with patch(
            "almanak.framework.cli.backtest.pnl._fetch_benchmark_returns",
            new=AsyncMock(side_effect=RuntimeError("bug!")),
        ):
            with pytest.raises(RuntimeError, match="bug!"):
                _print_benchmark_comparison(
                    ctx,
                    result,
                    benchmark="eth_hold",
                    start=datetime(2024, 1, 1, tzinfo=UTC),
                    end=datetime(2024, 2, 1, tzinfo=UTC),
                    interval=3600,
                )

    def test_network_errors_in_narrow_tuple_are_caught(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Issue #1699 — TimeoutError / aiohttp.ClientError get caught.

        Python 3.12 aliases `asyncio.TimeoutError` to the builtin
        `TimeoutError`; the narrow except tuple uses the builtin (ruff
        UP041).
        """
        import aiohttp

        ctx = _make_ctx()
        result = _make_result_with_equity(["100", "110"])

        for exc in (TimeoutError("timeout"), aiohttp.ClientError("net down")):
            with patch(
                "almanak.framework.cli.backtest.pnl._fetch_benchmark_returns",
                new=AsyncMock(side_effect=exc),
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
        assert captured.out.count("Could not calculate benchmark metrics:") == 2

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


# ===========================================================================
# Phase 5B.4 extended coverage — Phase 5B.1 helpers
# ===========================================================================


from almanak.framework.backtesting import PnLBacktestConfig as _PnLConfig  # noqa: E402
from almanak.framework.backtesting.pnl.config_loader import ConfigLoadError  # noqa: E402
from almanak.framework.cli.backtest.pnl import (  # noqa: E402
    _handle_list_strategies,
    _load_config_from_result,
    _print_pnl_configuration,
    _validate_and_build_context,
)

# ---------------------------------------------------------------------------
# _handle_list_strategies
# ---------------------------------------------------------------------------


class TestHandleListStrategies:
    def test_lists_registered_strategies_sorted(self, capsys: pytest.CaptureFixture[str]) -> None:
        with patch(
            "almanak.framework.cli.backtest.pnl.list_strategies_fn",
            return_value=["beta_lp", "alpha_rsi", "gamma_arb"],
        ):
            result = _handle_list_strategies()

        assert result is True
        out = capsys.readouterr().out
        assert "Available strategies:" in out
        # Sorted alphabetically
        i_alpha = out.find("- alpha_rsi")
        i_beta = out.find("- beta_lp")
        i_gamma = out.find("- gamma_arb")
        assert 0 <= i_alpha < i_beta < i_gamma

    def test_empty_registry_shows_help_message(self, capsys: pytest.CaptureFixture[str]) -> None:
        with patch(
            "almanak.framework.cli.backtest.pnl.list_strategies_fn",
            return_value=[],
        ):
            result = _handle_list_strategies()

        assert result is True
        out = capsys.readouterr().out
        assert "No strategies registered." in out
        assert "almanak strat new --help" in out

    def test_always_returns_true(self) -> None:
        with patch(
            "almanak.framework.cli.backtest.pnl.list_strategies_fn",
            return_value=["x"],
        ):
            assert _handle_list_strategies() is True


# ---------------------------------------------------------------------------
# _load_config_from_result
# ---------------------------------------------------------------------------


def _make_load_result(
    config: _PnLConfig,
    warnings: list[str] | None = None,
    metadata: dict[str, Any] | None = None,
) -> Any:
    """Build a fake LoadConfigResult-like object."""

    @dataclass
    class _Stub:
        config: _PnLConfig
        metadata: dict[str, Any]
        warnings: list[str]

    return _Stub(
        config=config,
        metadata=metadata or {},
        warnings=warnings or [],
    )


class TestLoadConfigFromResult:
    def test_returns_config_metadata_and_true_flag(self, capsys: pytest.CaptureFixture[str]) -> None:
        cfg = _make_pnl_config()
        load_result = _make_load_result(
            cfg,
            metadata={"sdk_version": "1.0.0", "config_created_at": "2024-01-01T00:00:00Z"},
        )
        with patch(
            "almanak.framework.cli.backtest.pnl.load_config_from_result",
            return_value=load_result,
        ):
            config, meta, loaded = _load_config_from_result("results/prev.json")

        assert config is cfg
        assert meta["sdk_version"] == "1.0.0"
        assert loaded is True
        out = capsys.readouterr().out
        assert "Loading config from previous result: results/prev.json" in out
        assert "Original SDK version: 1.0.0" in out

    def test_emits_warnings_to_stderr(self, capsys: pytest.CaptureFixture[str]) -> None:
        cfg = _make_pnl_config()
        load_result = _make_load_result(cfg, warnings=["w1", "w2"])

        with patch(
            "almanak.framework.cli.backtest.pnl.load_config_from_result",
            return_value=load_result,
        ):
            _load_config_from_result("results/prev.json")

        captured = capsys.readouterr()
        assert "Warnings:" in captured.err
        assert "- w1" in captured.err
        assert "- w2" in captured.err

    def test_metadata_unknown_defaults_emitted(self, capsys: pytest.CaptureFixture[str]) -> None:
        """When metadata has non-empty dict but missing keys, 'unknown' is emitted."""
        cfg = _make_pnl_config()
        load_result = _make_load_result(cfg, metadata={"other": "x"})

        with patch(
            "almanak.framework.cli.backtest.pnl.load_config_from_result",
            return_value=load_result,
        ):
            _load_config_from_result("r.json")

        out = capsys.readouterr().out
        assert "Original SDK version: unknown" in out
        assert "Config created at: unknown" in out

    def test_file_not_found_raises_abort(self, capsys: pytest.CaptureFixture[str]) -> None:
        import click

        with patch(
            "almanak.framework.cli.backtest.pnl.load_config_from_result",
            side_effect=FileNotFoundError("/tmp/missing.json"),
        ):
            with pytest.raises(click.Abort):
                _load_config_from_result("/tmp/missing.json")

        assert "Error: /tmp/missing.json" in capsys.readouterr().err

    def test_config_load_error_raises_abort_with_prefix(self, capsys: pytest.CaptureFixture[str]) -> None:
        import click

        with patch(
            "almanak.framework.cli.backtest.pnl.load_config_from_result",
            side_effect=ConfigLoadError("schema mismatch"),
        ):
            with pytest.raises(click.Abort):
                _load_config_from_result("bad.json")

        # Grep-asserted prefix; must match original exactly.
        assert "Error loading config: schema mismatch" in capsys.readouterr().err

    def test_empty_metadata_dict_suppresses_version_line(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Empty metadata dict skips the SDK version echo (falsy guard)."""
        cfg = _make_pnl_config()
        load_result = _make_load_result(cfg, metadata={})

        with patch(
            "almanak.framework.cli.backtest.pnl.load_config_from_result",
            return_value=load_result,
        ):
            _load_config_from_result("r.json")

        out = capsys.readouterr().out
        assert "Original SDK version:" not in out


# ---------------------------------------------------------------------------
# _validate_and_build_context
# ---------------------------------------------------------------------------


class TestValidateAndBuildContext:
    def _patch_registered(self, names: list[str]) -> Any:
        return patch(
            "almanak.framework.cli.backtest.run_helpers.list_strategies_fn",
            return_value=names,
        )

    def test_missing_strategy_raises_usage_error(self) -> None:
        import click

        with pytest.raises(click.UsageError, match="--strategy"):
            _validate_and_build_context(
                strategy=None,
                start=datetime(2024, 1, 1, tzinfo=UTC),
                end=datetime(2024, 2, 1, tzinfo=UTC),
                interval=3600,
                initial_capital=10000.0,
                chain="arbitrum",
                tokens="WETH",
                gas_price=30.0,
                output=None,
                loaded_from_result=False,
                pnl_config=None,
            )

    def test_missing_start_raises_usage_error(self) -> None:
        import click

        with self._patch_registered(["demo"]):
            with pytest.raises(click.UsageError, match="--start"):
                _validate_and_build_context(
                    strategy="demo",
                    start=None,
                    end=datetime(2024, 2, 1, tzinfo=UTC),
                    interval=3600,
                    initial_capital=10000.0,
                    chain="arbitrum",
                    tokens="WETH",
                    gas_price=30.0,
                    output=None,
                    loaded_from_result=False,
                    pnl_config=None,
                )

    def test_missing_end_raises_usage_error(self) -> None:
        import click

        with self._patch_registered(["demo"]):
            with pytest.raises(click.UsageError, match="--end"):
                _validate_and_build_context(
                    strategy="demo",
                    start=datetime(2024, 1, 1, tzinfo=UTC),
                    end=None,
                    interval=3600,
                    initial_capital=10000.0,
                    chain="arbitrum",
                    tokens="WETH",
                    gas_price=30.0,
                    output=None,
                    loaded_from_result=False,
                    pnl_config=None,
                )

    def test_loaded_from_result_uses_existing_config(self) -> None:
        cfg = _make_pnl_config(tokens=["WETH", "USDC"])
        with self._patch_registered(["demo"]):
            ctx = _validate_and_build_context(
                strategy="demo",
                start=None,
                end=None,
                interval=3600,
                initial_capital=10000.0,
                chain="arbitrum",
                tokens="IGNORED",  # should be ignored since loaded_from_result
                gas_price=30.0,
                output="r.json",
                loaded_from_result=True,
                pnl_config=cfg,
            )
        assert ctx.pnl_config is cfg
        # token_list comes from the loaded config, not the CLI arg
        assert ctx.token_list == ["WETH", "USDC"]
        assert ctx.output_path == Path("r.json")
        assert ctx.loaded_from_result is True
        assert ctx.strategy == "demo"

    def test_fresh_config_built_from_cli_args(self) -> None:
        with self._patch_registered(["demo"]):
            ctx = _validate_and_build_context(
                strategy="demo",
                start=datetime(2024, 1, 1, tzinfo=UTC),
                end=datetime(2024, 2, 1, tzinfo=UTC),
                interval=3600,
                initial_capital=5000.0,
                chain="base",
                tokens="weth, usdc",
                gas_price=25.0,
                output=None,
                loaded_from_result=False,
                pnl_config=None,
            )
        assert ctx.pnl_config.chain == "base"
        assert ctx.pnl_config.initial_capital_usd == Decimal("5000.0")
        assert ctx.pnl_config.gas_price_gwei == Decimal("25.0")
        assert ctx.token_list == ["WETH", "USDC"]
        assert ctx.output_path is None
        assert ctx.loaded_from_result is False

    def test_unregistered_strategy_aborts(self) -> None:
        import click

        with self._patch_registered(["other"]):
            with pytest.raises(click.Abort):
                _validate_and_build_context(
                    strategy="ghost",
                    start=datetime(2024, 1, 1, tzinfo=UTC),
                    end=datetime(2024, 2, 1, tzinfo=UTC),
                    interval=3600,
                    initial_capital=10000.0,
                    chain="arbitrum",
                    tokens="WETH",
                    gas_price=30.0,
                    output=None,
                    loaded_from_result=False,
                    pnl_config=None,
                )

    def test_loaded_from_result_but_missing_strategy_raises_usage_error(self) -> None:
        """When pnl_config is loaded but --strategy is still missing, redundant guard fires."""
        import click

        cfg = _make_pnl_config()
        with pytest.raises(click.UsageError, match="--strategy"):
            _validate_and_build_context(
                strategy=None,
                start=None,
                end=None,
                interval=3600,
                initial_capital=10000.0,
                chain="arbitrum",
                tokens="WETH",
                gas_price=30.0,
                output=None,
                loaded_from_result=True,  # config loaded
                pnl_config=cfg,
            )


# ---------------------------------------------------------------------------
# _print_pnl_configuration
# ---------------------------------------------------------------------------


class TestPrintPnlConfiguration:
    def test_prints_banner_for_fresh_run(self, capsys: pytest.CaptureFixture[str]) -> None:
        ctx = _make_ctx()
        _print_pnl_configuration(ctx, from_result=None, warm_cache=False)
        out = capsys.readouterr().out
        assert "PNL BACKTEST CONFIGURATION" in out
        assert "Strategy: demo_strat" in out
        assert "Chain: arbitrum" in out
        assert "Warm Cache: No" in out
        # Output line absent when no output_path
        assert "Output:" not in out
        # Loaded-from line absent for fresh run
        assert "(Loaded from:" not in out

    def test_loaded_from_result_banner(self, capsys: pytest.CaptureFixture[str]) -> None:
        ctx = _make_ctx()
        ctx.loaded_from_result = True
        _print_pnl_configuration(ctx, from_result="results/prev.json", warm_cache=True)
        out = capsys.readouterr().out
        assert "(Loaded from: results/prev.json)" in out
        assert "Warm Cache: Yes" in out

    def test_output_line_emitted_when_path_set(self, capsys: pytest.CaptureFixture[str]) -> None:
        ctx = _make_ctx(output_path=Path("out.json"))
        _print_pnl_configuration(ctx, from_result=None, warm_cache=False)
        out = capsys.readouterr().out
        assert "Output: out.json" in out

    def test_tokens_joined_with_comma(self, capsys: pytest.CaptureFixture[str]) -> None:
        ctx = _make_ctx(tokens=["WETH", "USDC", "BTC"])
        _print_pnl_configuration(ctx, from_result=None, warm_cache=False)
        out = capsys.readouterr().out
        assert "Tokens: WETH, USDC, BTC" in out

    def test_interval_hours_line(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Sanity-check that interval is rendered in seconds and hours."""
        ctx = _make_ctx()
        _print_pnl_configuration(ctx, from_result=None, warm_cache=False)
        out = capsys.readouterr().out
        assert "Interval: 3600s (1.0 hours)" in out


# ===========================================================================
# Phase 5B.4 extended coverage — additional error/edge paths
# ===========================================================================


class TestFetchBenchmarkReturnsExtended:
    def test_raises_when_underlying_provider_raises(self) -> None:
        """Underlying provider failure propagates (caller wraps try/except)."""
        with (
            patch("almanak.framework.backtesting.pnl.providers.benchmark.Benchmark") as mock_benchmark,
            patch(
                "almanak.framework.backtesting.pnl.providers.benchmark.get_benchmark_returns",
                new=AsyncMock(side_effect=RuntimeError("API down")),
            ),
            patch(
                "almanak.framework.backtesting.pnl.providers.benchmark.get_benchmark_total_return",
                new=AsyncMock(return_value=Decimal("0.0")),
            ),
        ):
            mock_benchmark.from_string = MagicMock(return_value="ETH_HOLD")
            with pytest.raises(RuntimeError, match="API down"):
                asyncio.run(
                    _fetch_benchmark_returns(
                        "eth_hold",
                        datetime(2024, 1, 1, tzinfo=UTC),
                        datetime(2024, 2, 1, tzinfo=UTC),
                        3600,
                    )
                )


class TestWarmCacheAsyncExtended:
    def test_mixed_success_and_failure_tokens(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Warm-cache partial failure: some tokens succeed, others fail."""
        data_provider = MagicMock()
        data_provider.close = AsyncMock()

        async def _variable(token: str, *args: Any, **kwargs: Any) -> list[_FakeOHLCV]:
            if token == "FAIL":
                raise RuntimeError("rate-limited")
            return [
                _FakeOHLCV(
                    timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                    open=Decimal("100"),
                    high=Decimal("110"),
                    low=Decimal("90"),
                    close=Decimal("105"),
                )
            ]

        data_provider.get_ohlcv = AsyncMock(side_effect=_variable)
        cache = MagicMock()
        # 3 points cached per call
        cache.set_batch = MagicMock(return_value=3)

        outcome = asyncio.run(
            _warm_cache_async(
                data_provider=data_provider,
                cache=cache,
                token_list=["OK1", "FAIL", "OK2"],
                start=datetime(2024, 1, 1, tzinfo=UTC),
                end=datetime(2024, 2, 1, tzinfo=UTC),
                interval=3600,
                pnl_config=_make_pnl_config(),
            )
        )
        # 3 (OK1) + 0 (FAIL) + 3 (OK2) = 6; 2 successful warms out of 3 (#1698).
        assert outcome.total_cached == 6
        assert outcome.successful_warms == 2
        assert outcome.total_tokens == 3
        captured = capsys.readouterr()
        assert "Cached 3 data points for OK1" in captured.out
        assert "Cached 3 data points for OK2" in captured.out
        assert "Warning: Failed to cache data for FAIL: rate-limited" in captured.err

    def test_uses_pnl_config_as_range_fallback_when_start_none(self) -> None:
        """start=None falls through to pnl_config.start_time."""
        data_provider = MagicMock()
        data_provider.close = AsyncMock()
        data_provider.get_ohlcv = AsyncMock(return_value=[])
        cache = MagicMock()
        cache.set_batch = MagicMock(return_value=0)

        pnl_cfg = _make_pnl_config()
        asyncio.run(
            _warm_cache_async(
                data_provider=data_provider,
                cache=cache,
                token_list=["WETH"],
                start=None,
                end=None,
                interval=3600,
                pnl_config=pnl_cfg,
            )
        )

        # get_ohlcv called with (token, cfg.start_time, cfg.end_time, interval)
        data_provider.get_ohlcv.assert_awaited_once_with("WETH", pnl_cfg.start_time, pnl_cfg.end_time, 3600)

    def test_volume_extracted_when_attribute_present(self) -> None:
        data_provider = MagicMock()
        data_provider.close = AsyncMock()
        data_provider.get_ohlcv = AsyncMock(
            return_value=[
                _FakeOHLCV(
                    timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                    open=Decimal("100"),
                    high=Decimal("110"),
                    low=Decimal("90"),
                    close=Decimal("105"),
                    volume=Decimal("5000"),
                )
            ]
        )
        cache = MagicMock()
        cache.set_batch = MagicMock(return_value=1)

        asyncio.run(
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
        call_args = cache.set_batch.call_args[0][0]
        assert len(call_args) == 1
        _key, ohlcv = call_args[0]
        assert ohlcv.volume == Decimal("5000")
