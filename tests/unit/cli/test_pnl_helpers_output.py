"""Unit tests for output helpers in `almanak.framework.cli.backtest.pnl`.

Phase 5B.2 extracts the output-side chunks of `pnl_backtest` into module-level
helpers. These tests exercise:

- `_print_cache_stats`: no-op + rendered block.
- `_print_verbose_trades`: no-op guards + rendered block with correct counts.
- `_write_json_output`: schema preservation (top-level keys, `_meta`,
  `cache_stats`), no-op when `output_path is None`.
- `_chart_output_path`: extension + path derivation.
- `_generate_chart`: success + failure echo lines.
- `_generate_html_report`: success + failure echo lines.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.backtesting.models import (
    BacktestEngine,
    BacktestMetrics,
    BacktestResult,
    EquityPoint,
)
from almanak.framework.cli.backtest.pnl import (
    _chart_output_path,
    _generate_chart,
    _generate_html_report,
    _print_cache_stats,
    _print_verbose_trades,
    _write_json_output,
)

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


@dataclass
class _FakeCacheStats:
    total_entries: int = 100
    hits: int = 42
    misses: int = 10
    expired: int = 3

    def hit_rate(self) -> float:
        return 0.8

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_entries": self.total_entries,
            "hits": self.hits,
            "misses": self.misses,
            "expired": self.expired,
        }


@dataclass
class _FakeIntentType:
    value: str = "swap"


@dataclass
class _FakeTrade:
    timestamp: datetime
    intent_type: _FakeIntentType
    pnl_usd: Decimal
    fee_usd: Decimal
    gas_cost_usd: Decimal


def _make_result(trades: list[_FakeTrade] | None = None) -> BacktestResult:
    return BacktestResult(
        engine=BacktestEngine.PNL,
        strategy_id="demo",
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 2, 1, tzinfo=UTC),
        metrics=BacktestMetrics(
            total_trades=len(trades) if trades else 0,
            win_rate=Decimal("0.5"),
            total_return_pct=Decimal("5"),
            max_drawdown_pct=Decimal("1"),
            sharpe_ratio=Decimal("1"),
            sortino_ratio=Decimal("1"),
            calmar_ratio=Decimal("1"),
            profit_factor=Decimal("1"),
            annualized_return_pct=Decimal("10"),
            net_pnl_usd=Decimal("500"),
        ),
        trades=trades or [],  # type: ignore[arg-type]
        equity_curve=[
            EquityPoint(
                timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                value_usd=Decimal("10000"),
            ),
            EquityPoint(
                timestamp=datetime(2024, 1, 2, tzinfo=UTC),
                value_usd=Decimal("10500"),
            ),
        ],
    )


# ===========================================================================
# _print_cache_stats
# ===========================================================================


class TestPrintCacheStats:
    def test_noop_when_none(self, capsys: pytest.CaptureFixture[str]) -> None:
        _print_cache_stats(None)
        assert capsys.readouterr().out == ""

    def test_renders_block(self, capsys: pytest.CaptureFixture[str]) -> None:
        _print_cache_stats(_FakeCacheStats())  # type: ignore[arg-type]
        captured = capsys.readouterr()
        assert "CACHE STATISTICS" in captured.out
        assert "Total Entries: 100" in captured.out
        assert "Cache Hits: 42" in captured.out
        assert "Cache Misses: 10" in captured.out
        assert "Expired: 3" in captured.out
        assert "Hit Rate: 80.0%" in captured.out

    def test_uses_thousands_separator(self, capsys: pytest.CaptureFixture[str]) -> None:
        _print_cache_stats(_FakeCacheStats(total_entries=1234567))  # type: ignore[arg-type]
        captured = capsys.readouterr()
        assert "Total Entries: 1,234,567" in captured.out


# ===========================================================================
# _print_verbose_trades
# ===========================================================================


class TestPrintVerboseTrades:
    def test_noop_when_verbose_false(self, capsys: pytest.CaptureFixture[str]) -> None:
        trade = _FakeTrade(
            timestamp=datetime(2024, 1, 1, 12, 30, tzinfo=UTC),
            intent_type=_FakeIntentType("swap"),
            pnl_usd=Decimal("10"),
            fee_usd=Decimal("1"),
            gas_cost_usd=Decimal("2"),
        )
        _print_verbose_trades(_make_result([trade]), verbose=False)
        assert capsys.readouterr().out == ""

    def test_noop_when_no_trades(self, capsys: pytest.CaptureFixture[str]) -> None:
        _print_verbose_trades(_make_result([]), verbose=True)
        assert capsys.readouterr().out == ""

    def test_renders_trade_block(self, capsys: pytest.CaptureFixture[str]) -> None:
        trades = [
            _FakeTrade(
                timestamp=datetime(2024, 1, 1, 12, 30, tzinfo=UTC),
                intent_type=_FakeIntentType("swap"),
                pnl_usd=Decimal("100.50"),
                fee_usd=Decimal("1.23"),
                gas_cost_usd=Decimal("2.10"),
            ),
            _FakeTrade(
                timestamp=datetime(2024, 1, 2, 9, 0, tzinfo=UTC),
                intent_type=_FakeIntentType("add_liquidity"),
                pnl_usd=Decimal("-5.00"),
                fee_usd=Decimal("0.50"),
                gas_cost_usd=Decimal("1.50"),
            ),
        ]
        _print_verbose_trades(_make_result(trades), verbose=True)
        captured = capsys.readouterr()

        assert "TRADE HISTORY" in captured.out
        assert "2024-01-01 12:30" in captured.out
        assert "+$100.50" in captured.out  # positive sign prefix
        assert "2024-01-02 09:00" in captured.out
        # Negative PnL has no extra '+' prefix (bare Decimal.__format__).
        assert "$-5.00" in captured.out
        assert "add_liquidity" in captured.out

    def test_index_is_one_based(self, capsys: pytest.CaptureFixture[str]) -> None:
        trade = _FakeTrade(
            timestamp=datetime(2024, 1, 1, 12, 30, tzinfo=UTC),
            intent_type=_FakeIntentType("swap"),
            pnl_usd=Decimal("10"),
            fee_usd=Decimal("1"),
            gas_cost_usd=Decimal("2"),
        )
        _print_verbose_trades(_make_result([trade]), verbose=True)
        captured = capsys.readouterr()
        assert "  1." in captured.out


# ===========================================================================
# _write_json_output
# ===========================================================================


class TestWriteJsonOutput:
    def test_noop_when_output_path_none(self, tmp_path: Path) -> None:
        # Should not raise — no file produced.
        _write_json_output(
            _make_result(),
            output_path=None,
            benchmark="eth_hold",
            cache_stats=None,
        )
        assert list(tmp_path.iterdir()) == []

    def test_writes_expected_schema(self, tmp_path: Path) -> None:
        out = tmp_path / "result.json"
        _write_json_output(
            _make_result(),
            output_path=out,
            benchmark="eth_hold",
            cache_stats=None,
        )
        payload = json.loads(out.read_text())

        # Top-level keys from result.to_dict() must be preserved.
        assert "engine" in payload
        assert "strategy_id" in payload
        assert "metrics" in payload
        assert "equity_curve" in payload

        # _meta must be present with expected keys.
        assert payload["_meta"]["generator"] == "almanak backtest pnl"
        assert payload["_meta"]["engine"] == "pnl"
        assert payload["_meta"]["benchmark"] == "eth_hold"
        assert "generated_at" in payload["_meta"]

        # cache_stats absent when None.
        assert "cache_stats" not in payload

    def test_includes_cache_stats_when_provided(self, tmp_path: Path) -> None:
        out = tmp_path / "result.json"
        _write_json_output(
            _make_result(),
            output_path=out,
            benchmark="btc_hold",
            cache_stats=_FakeCacheStats(),  # type: ignore[arg-type]
        )
        payload = json.loads(out.read_text())
        assert "cache_stats" in payload
        assert payload["cache_stats"]["total_entries"] == 100

    def test_emits_written_line(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        out = tmp_path / "result.json"
        _write_json_output(
            _make_result(),
            output_path=out,
            benchmark="eth_hold",
            cache_stats=None,
        )
        captured = capsys.readouterr()
        assert f"Results written to: {out}" in captured.out


# ===========================================================================
# _chart_output_path
# ===========================================================================


class TestChartOutputPath:
    def test_png_suffix_by_default(self) -> None:
        assert _chart_output_path("demo", None, "png") == Path("equity_curve_demo.png")

    def test_html_suffix_when_format_html(self) -> None:
        assert _chart_output_path("demo", None, "html") == Path("equity_curve_demo.html")

    def test_case_insensitive_format(self) -> None:
        assert _chart_output_path("demo", None, "HTML") == Path("equity_curve_demo.html")
        assert _chart_output_path("demo", None, "PNG") == Path("equity_curve_demo.png")

    def test_alongside_output_path(self, tmp_path: Path) -> None:
        out = tmp_path / "foo.json"
        assert _chart_output_path("demo", out, "png") == out.with_suffix(".png")
        assert _chart_output_path("demo", out, "html") == out.with_suffix(".html")

    def test_strategy_name_sanitization(self) -> None:
        assert _chart_output_path("a/b\\c", None, "png") == Path("equity_curve_a_b_c.png")

    def test_no_strategy_uses_backtest_fallback(self) -> None:
        assert _chart_output_path(None, None, "png") == Path("equity_curve_backtest.png")


# ===========================================================================
# _generate_chart
# ===========================================================================


class TestGenerateChart:
    def test_success_path_echoes_saved_line(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        chart_result = MagicMock(success=True)
        chart_result.file_path = tmp_path / "equity.png"
        chart_result.drawdown_periods = []
        chart_result.trade_markers = []

        with patch(
            "almanak.framework.cli.backtest.pnl.save_chart",
            return_value=chart_result,
        ) as save_mock:
            _generate_chart(
                _make_result(),
                strategy="demo",
                output_path=tmp_path / "r.json",
                chart_format="png",
            )

        save_mock.assert_called_once()
        captured = capsys.readouterr()
        assert "Generating equity curve chart..." in captured.out
        assert "Chart saved to:" in captured.out

    def test_failure_path_emits_warning_on_stderr(self, capsys: pytest.CaptureFixture[str]) -> None:
        chart_result = MagicMock(success=False, error="disk full")

        with patch(
            "almanak.framework.cli.backtest.pnl.save_chart",
            return_value=chart_result,
        ):
            _generate_chart(
                _make_result(),
                strategy="demo",
                output_path=None,
                chart_format="png",
            )

        captured = capsys.readouterr()
        assert "Warning: Failed to generate chart: disk full" in captured.err

    def test_reports_drawdown_and_trade_markers(self, capsys: pytest.CaptureFixture[str]) -> None:
        chart_result = MagicMock(success=True)
        chart_result.file_path = "equity.png"
        chart_result.drawdown_periods = [1, 2]
        chart_result.trade_markers = [1, 2, 3]

        with patch(
            "almanak.framework.cli.backtest.pnl.save_chart",
            return_value=chart_result,
        ):
            _generate_chart(
                _make_result(),
                strategy="demo",
                output_path=None,
                chart_format="png",
            )

        captured = capsys.readouterr()
        assert "Highlighted 2 drawdown period(s)" in captured.out
        assert "Marked 3 trade(s)" in captured.out


# ===========================================================================
# _generate_html_report
# ===========================================================================


class TestGenerateHtmlReport:
    def test_success_path_echoes_saved_line(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        report_result = MagicMock(success=True)
        report_result.file_path = tmp_path / "report.html"

        with patch(
            "almanak.framework.backtesting.report_generator.generate_report",
            return_value=report_result,
        ) as gen_mock:
            _generate_html_report(
                _make_result(),
                strategy="demo",
                output_path=tmp_path / "r.json",
            )

        gen_mock.assert_called_once()
        captured = capsys.readouterr()
        assert "Generating HTML report..." in captured.out
        assert "Report saved to:" in captured.out

    def test_failure_path_emits_warning_on_stderr(self, capsys: pytest.CaptureFixture[str]) -> None:
        report_result = MagicMock(success=False, error="template missing")

        with patch(
            "almanak.framework.backtesting.report_generator.generate_report",
            return_value=report_result,
        ):
            _generate_html_report(
                _make_result(),
                strategy="demo",
                output_path=None,
            )

        captured = capsys.readouterr()
        assert "Warning: Failed to generate report: template missing" in captured.err

    def test_uses_fallback_name_when_no_output_path(self, capsys: pytest.CaptureFixture[str]) -> None:
        report_result = MagicMock(success=True)
        report_result.file_path = "backtest_report_demo.html"

        with patch(
            "almanak.framework.backtesting.report_generator.generate_report",
            return_value=report_result,
        ) as gen_mock:
            _generate_html_report(
                _make_result(),
                strategy="demo",
                output_path=None,
            )

        # Derived path is `backtest_report_<strategy>.html` in cwd.
        call_kwargs = gen_mock.call_args.kwargs
        assert call_kwargs["output_path"] == Path("backtest_report_demo.html")

    def test_sanitizes_strategy_name_in_fallback(self) -> None:
        report_result = MagicMock(success=True)
        report_result.file_path = "backtest_report_a_b_c.html"

        with patch(
            "almanak.framework.backtesting.report_generator.generate_report",
            return_value=report_result,
        ) as gen_mock:
            _generate_html_report(
                _make_result(),
                strategy="a/b\\c",
                output_path=None,
            )

        call_kwargs = gen_mock.call_args.kwargs
        assert call_kwargs["output_path"] == Path("backtest_report_a_b_c.html")

    def test_derives_html_from_output_path(self, tmp_path: Path) -> None:
        report_result = MagicMock(success=True)
        report_result.file_path = tmp_path / "foo.html"
        output_path = tmp_path / "foo.json"

        with patch(
            "almanak.framework.backtesting.report_generator.generate_report",
            return_value=report_result,
        ) as gen_mock:
            _generate_html_report(
                _make_result(),
                strategy="demo",
                output_path=output_path,
            )

        call_kwargs = gen_mock.call_args.kwargs
        assert call_kwargs["output_path"] == output_path.with_suffix(".html")


# ===========================================================================
# Phase 5B.4 extended coverage
# ===========================================================================


class TestWriteJsonOutputExtended:
    """Coverage gaps for `_write_json_output`."""

    def test_permission_error_on_readonly_path_propagates(self, tmp_path: Path) -> None:
        """JSON-output write surfaces PermissionError from the underlying open.

        Uses ``patch("builtins.open")`` rather than ``chmod(0o500)`` so the
        test is independent of the process UID. Root/DAC-override contexts
        (common in CI containers) bypass file-mode bits, which would make a
        filesystem-based check flaky.
        """
        out = tmp_path / "result.json"

        with (
            patch(
                "almanak.framework.cli.backtest.pnl.open",
                side_effect=PermissionError("denied"),
                create=True,
            ),
            pytest.raises(PermissionError),
        ):
            _write_json_output(
                _make_result(),
                output_path=out,
                benchmark="eth_hold",
                cache_stats=None,
            )

    def test_nonexistent_parent_raises_filenotfound(self, tmp_path: Path) -> None:
        """Writing into a missing directory surfaces FileNotFoundError."""
        out = tmp_path / "does_not_exist" / "r.json"
        with pytest.raises(FileNotFoundError):
            _write_json_output(
                _make_result(),
                output_path=out,
                benchmark="eth_hold",
                cache_stats=None,
            )

    def test_json_is_indented_two_spaces(self, tmp_path: Path) -> None:
        out = tmp_path / "r.json"
        _write_json_output(
            _make_result(),
            output_path=out,
            benchmark="eth_hold",
            cache_stats=None,
        )
        text = out.read_text()
        # Indent=2 rendering: nested value lines start with two spaces.
        assert "\n  " in text

    def test_metadata_generated_at_is_iso(self, tmp_path: Path) -> None:
        """_meta.generated_at must be ISO-8601 UTC string."""
        out = tmp_path / "r.json"
        _write_json_output(
            _make_result(),
            output_path=out,
            benchmark="eth_hold",
            cache_stats=None,
        )
        payload = json.loads(out.read_text())
        gen_at = payload["_meta"]["generated_at"]
        # Parses as ISO
        parsed = datetime.fromisoformat(gen_at)
        assert parsed.tzinfo is not None

    def test_default_str_coerces_non_json_types(self, tmp_path: Path) -> None:
        """json.dump(default=str) means Decimal/datetime serialize as strings."""
        out = tmp_path / "r.json"
        _write_json_output(
            _make_result(),
            output_path=out,
            benchmark="eth_hold",
            cache_stats=_FakeCacheStats(),  # type: ignore[arg-type]
        )
        payload = json.loads(out.read_text())
        # metrics.total_return_pct coerced via default=str → stringified
        assert isinstance(payload["metrics"]["total_return_pct"], str)


class TestGenerateChartExtended:
    """Coverage gaps for `_generate_chart`."""

    def test_empty_equity_curve_does_not_raise(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Chart generation tolerates an empty equity curve by delegating to save_chart."""

        @dataclass
        class _EmptyResult:
            equity_curve: list[Any] = None  # type: ignore[assignment]

            def __post_init__(self) -> None:
                if self.equity_curve is None:
                    self.equity_curve = []

        chart_result = MagicMock(success=False, error="no data to plot")

        with patch(
            "almanak.framework.cli.backtest.pnl.save_chart",
            return_value=chart_result,
        ) as save_mock:
            _generate_chart(
                _EmptyResult(),  # type: ignore[arg-type]
                strategy="demo",
                output_path=None,
                chart_format="png",
            )

        save_mock.assert_called_once()
        err = capsys.readouterr().err
        assert "Warning: Failed to generate chart: no data to plot" in err

    def test_html_format_produces_html_extension(self, tmp_path: Path) -> None:
        chart_result = MagicMock(success=True)
        chart_result.file_path = tmp_path / "equity.html"
        chart_result.drawdown_periods = []
        chart_result.trade_markers = []

        with patch(
            "almanak.framework.cli.backtest.pnl.save_chart",
            return_value=chart_result,
        ) as save_mock:
            _generate_chart(
                _make_result(),
                strategy="demo",
                output_path=tmp_path / "r.json",
                chart_format="html",
            )

        # save_chart called with format="html" and a .html suffix path
        call_kwargs = save_mock.call_args.kwargs
        assert call_kwargs["format"] == "html"
        assert str(call_kwargs["path"]).endswith(".html")

    def test_show_drawdown_and_show_trades_flags_true(self) -> None:
        """Flags always passed as True to save_chart (load-bearing)."""
        chart_result = MagicMock(success=True)
        chart_result.file_path = "x.png"
        chart_result.drawdown_periods = []
        chart_result.trade_markers = []

        with patch(
            "almanak.framework.cli.backtest.pnl.save_chart",
            return_value=chart_result,
        ) as save_mock:
            _generate_chart(
                _make_result(),
                strategy="demo",
                output_path=None,
                chart_format="png",
            )

        call_kwargs = save_mock.call_args.kwargs
        assert call_kwargs["show_drawdown"] is True
        assert call_kwargs["show_trades"] is True


class TestGenerateHtmlReportExtended:
    def test_permission_error_path_unaffected_by_wrapper(self, tmp_path: Path) -> None:
        """When generate_report raises, the wrapper does not swallow the error."""
        with patch(
            "almanak.framework.backtesting.report_generator.generate_report",
            side_effect=OSError("disk full"),
        ):
            with pytest.raises(OSError, match="disk full"):
                _generate_html_report(
                    _make_result(),
                    strategy="demo",
                    output_path=tmp_path / "r.json",
                )

    def test_none_strategy_falls_back_to_backtest_name(self, tmp_path: Path) -> None:
        """strategy=None falls back to 'backtest_report_backtest.html'."""
        report_result = MagicMock(success=True)
        report_result.file_path = "backtest_report_backtest.html"

        with patch(
            "almanak.framework.backtesting.report_generator.generate_report",
            return_value=report_result,
        ) as gen_mock:
            _generate_html_report(
                _make_result(),
                strategy=None,
                output_path=None,
            )

        call_kwargs = gen_mock.call_args.kwargs
        assert call_kwargs["output_path"] == Path("backtest_report_backtest.html")


class TestPrintCacheStatsExtended:
    def test_zero_counts_render_zeros(self, capsys: pytest.CaptureFixture[str]) -> None:
        stats = _FakeCacheStats(total_entries=0, hits=0, misses=0, expired=0)
        _print_cache_stats(stats)  # type: ignore[arg-type]
        out = capsys.readouterr().out
        assert "Total Entries: 0" in out
        assert "Cache Hits: 0" in out

    def test_divider_bars_emitted(self, capsys: pytest.CaptureFixture[str]) -> None:
        _print_cache_stats(_FakeCacheStats())  # type: ignore[arg-type]
        out = capsys.readouterr().out
        assert "-" * 60 in out


class TestPrintVerboseTradesExtended:
    def test_exact_format_fee_gas_line(self, capsys: pytest.CaptureFixture[str]) -> None:
        trade = _FakeTrade(
            timestamp=datetime(2024, 5, 10, 14, 25, tzinfo=UTC),
            intent_type=_FakeIntentType("swap"),
            pnl_usd=Decimal("0.00"),
            fee_usd=Decimal("0.12"),
            gas_cost_usd=Decimal("3.45"),
        )
        _print_verbose_trades(_make_result([trade]), verbose=True)
        out = capsys.readouterr().out
        # Zero PnL gets + sign (>= 0)
        assert "+$0.00" in out
        assert "(fee: $0.12, gas: $3.45)" in out

    def test_multiple_trades_numbered_correctly(self, capsys: pytest.CaptureFixture[str]) -> None:
        trades = [
            _FakeTrade(
                timestamp=datetime(2024, 1, d, 0, 0, tzinfo=UTC),
                intent_type=_FakeIntentType("swap"),
                pnl_usd=Decimal("0"),
                fee_usd=Decimal("0"),
                gas_cost_usd=Decimal("0"),
            )
            for d in (1, 2, 3)
        ]
        _print_verbose_trades(_make_result(trades), verbose=True)
        out = capsys.readouterr().out
        assert "  1." in out
        assert "  2." in out
        assert "  3." in out


class TestChartOutputPathExtended:
    def test_special_chars_in_strategy_name_sanitized(self) -> None:
        """Multiple slash/backslash characters all replaced with underscore."""
        p = _chart_output_path("foo/bar\\baz/qux", None, "html")
        assert p == Path("equity_curve_foo_bar_baz_qux.html")

    def test_empty_strategy_name_becomes_backtest(self) -> None:
        """Falsy strategy (empty string) falls through to 'backtest' fallback."""
        p = _chart_output_path("", None, "png")
        assert p == Path("equity_curve_backtest.png")

    def test_output_path_is_absolute(self, tmp_path: Path) -> None:
        """Alongside output_path preserves directory."""
        out = tmp_path / "results" / "run.json"
        out.parent.mkdir()
        chart = _chart_output_path("demo", out, "html")
        assert chart.parent == out.parent
        assert chart.name == "run.html"
