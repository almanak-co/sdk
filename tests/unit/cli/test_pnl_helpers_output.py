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

    def test_uses_thousands_separator(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        _print_cache_stats(_FakeCacheStats(total_entries=1234567))  # type: ignore[arg-type]
        captured = capsys.readouterr()
        assert "Total Entries: 1,234,567" in captured.out


# ===========================================================================
# _print_verbose_trades
# ===========================================================================


class TestPrintVerboseTrades:
    def test_noop_when_verbose_false(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        trade = _FakeTrade(
            timestamp=datetime(2024, 1, 1, 12, 30, tzinfo=UTC),
            intent_type=_FakeIntentType("swap"),
            pnl_usd=Decimal("10"),
            fee_usd=Decimal("1"),
            gas_cost_usd=Decimal("2"),
        )
        _print_verbose_trades(_make_result([trade]), verbose=False)
        assert capsys.readouterr().out == ""

    def test_noop_when_no_trades(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        _print_verbose_trades(_make_result([]), verbose=True)
        assert capsys.readouterr().out == ""

    def test_renders_trade_block(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
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

    def test_index_is_one_based(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
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

    def test_emits_written_line(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
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
    def test_success_path_echoes_saved_line(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
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

    def test_failure_path_emits_warning_on_stderr(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
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

    def test_reports_drawdown_and_trade_markers(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
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
    def test_success_path_echoes_saved_line(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
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

    def test_failure_path_emits_warning_on_stderr(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
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
        assert (
            "Warning: Failed to generate report: template missing" in captured.err
        )

    def test_uses_fallback_name_when_no_output_path(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
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
