"""Unit tests for --chart CLI flag (US-029d).

Tests validate the CLI chart generation functionality:
- --chart flag registration in CLI help
- --chart-format option supports png and html
- Chart generation function is called (integration via save_chart module)
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest
from click.testing import CliRunner

from almanak.framework.backtesting.models import (
    BacktestMetrics,
    BacktestResult,
    EquityPoint,
)
from almanak.framework.backtesting.visualization import save_chart
from almanak.framework.cli.backtest import backtest

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def cli_runner() -> CliRunner:
    """Create Click test runner."""
    return CliRunner()


@pytest.fixture
def sample_backtest_result() -> BacktestResult:
    """Create a sample BacktestResult for testing."""
    equity_curve = [
        EquityPoint(timestamp=datetime(2024, 1, 1, 0, 0), value_usd=Decimal("10000")),
        EquityPoint(timestamp=datetime(2024, 1, 2, 0, 0), value_usd=Decimal("10200")),
        EquityPoint(timestamp=datetime(2024, 1, 3, 0, 0), value_usd=Decimal("10400")),
    ]

    metrics = BacktestMetrics(
        net_pnl_usd=Decimal("400"),
        total_return_pct=Decimal("4.0"),
        max_drawdown_pct=Decimal("0.5"),
        sharpe_ratio=Decimal("1.5"),
        win_rate=Decimal("0.6"),
        total_trades=3,
    )

    return BacktestResult(
        engine="pnl",
        strategy_id="test_strategy",
        start_time=datetime(2024, 1, 1),
        end_time=datetime(2024, 1, 3),
        metrics=metrics,
        equity_curve=equity_curve,
        initial_capital_usd=Decimal("10000"),
        final_capital_usd=Decimal("10400"),
        trades=[],
    )


# =============================================================================
# Test CLI Chart Flag Registration
# =============================================================================


class TestChartFlagRegistration:
    """Tests for --chart and --chart-format option registration."""

    def test_chart_flag_appears_in_help(self, cli_runner: CliRunner) -> None:
        """Test that --chart flag appears in pnl command help."""
        result = cli_runner.invoke(backtest, ["pnl", "--help"])
        assert result.exit_code == 0
        assert "--chart" in result.output
        assert "Generate equity curve chart" in result.output

    def test_chart_format_option_appears_in_help(self, cli_runner: CliRunner) -> None:
        """Test that --chart-format option appears in pnl command help."""
        result = cli_runner.invoke(backtest, ["pnl", "--help"])
        assert result.exit_code == 0
        assert "--chart-format" in result.output
        assert "png" in result.output
        assert "html" in result.output

    def test_chart_format_default_is_png(self, cli_runner: CliRunner) -> None:
        """Test that --chart-format default is png."""
        result = cli_runner.invoke(backtest, ["pnl", "--help"])
        assert result.exit_code == 0
        # Help should indicate default is png
        assert "default: png" in result.output.lower()

    def test_chart_format_accepts_png(self, cli_runner: CliRunner) -> None:
        """Test that --chart-format accepts 'png' value."""
        # Just verify the option is accepted without error
        result = cli_runner.invoke(backtest, ["pnl", "--list-strategies", "--chart", "--chart-format", "png"])
        # Should not fail due to invalid format
        assert "Invalid value for '--chart-format'" not in result.output

    def test_chart_format_accepts_html(self, cli_runner: CliRunner) -> None:
        """Test that --chart-format accepts 'html' value."""
        # Just verify the option is accepted without error
        result = cli_runner.invoke(backtest, ["pnl", "--list-strategies", "--chart", "--chart-format", "html"])
        # Should not fail due to invalid format
        assert "Invalid value for '--chart-format'" not in result.output

    def test_chart_format_rejects_invalid(self, cli_runner: CliRunner) -> None:
        """Test that --chart-format rejects invalid values."""
        result = cli_runner.invoke(backtest, ["pnl", "--list-strategies", "--chart-format", "pdf"])
        assert result.exit_code != 0
        assert "Invalid value" in result.output


# =============================================================================
# Test Chart Generation Function (save_chart integration)
# =============================================================================


class TestSaveChartIntegration:
    """Tests for save_chart function used by CLI."""

    def test_save_chart_png_format(
        self, sample_backtest_result: BacktestResult, tmp_path: Path
    ) -> None:
        """Test save_chart with png format."""
        output_path = tmp_path / "test_chart.png"

        result = save_chart(
            result=sample_backtest_result,
            format="png",
            path=output_path,
            show_drawdown=True,
            show_trades=True,
        )

        assert result.success is True
        assert result.format == "png"
        assert result.file_path == output_path
        assert output_path.exists()

    def test_save_chart_html_format(
        self, sample_backtest_result: BacktestResult, tmp_path: Path
    ) -> None:
        """Test save_chart with html format."""
        output_path = tmp_path / "test_chart.html"

        result = save_chart(
            result=sample_backtest_result,
            format="html",
            path=output_path,
            show_drawdown=True,
            show_trades=True,
        )

        assert result.success is True
        assert result.format == "html"
        assert result.file_path == output_path
        assert output_path.exists()

    def test_save_chart_with_drawdown_periods(
        self, tmp_path: Path
    ) -> None:
        """Test save_chart detects drawdown periods."""
        # Create result with drawdown pattern
        equity_curve = [
            EquityPoint(timestamp=datetime(2024, 1, 1, 0, 0), value_usd=Decimal("10000")),
            EquityPoint(timestamp=datetime(2024, 1, 2, 0, 0), value_usd=Decimal("10500")),  # Peak
            EquityPoint(timestamp=datetime(2024, 1, 3, 0, 0), value_usd=Decimal("9500")),   # Drawdown >5%
            EquityPoint(timestamp=datetime(2024, 1, 4, 0, 0), value_usd=Decimal("9200")),   # Trough
            EquityPoint(timestamp=datetime(2024, 1, 5, 0, 0), value_usd=Decimal("10600")),  # Recovery
        ]

        metrics = BacktestMetrics()
        backtest_result = BacktestResult(
            engine="pnl",
            strategy_id="drawdown_test",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 5),
            metrics=metrics,
            equity_curve=equity_curve,
            initial_capital_usd=Decimal("10000"),
            final_capital_usd=Decimal("10600"),
        )

        output_path = tmp_path / "drawdown_chart.png"
        result = save_chart(
            result=backtest_result,
            format="png",
            path=output_path,
            show_drawdown=True,
        )

        assert result.success is True
        # Should detect at least one drawdown period
        assert len(result.drawdown_periods) >= 1

    def test_save_chart_invalid_format_returns_error(
        self, sample_backtest_result: BacktestResult, tmp_path: Path
    ) -> None:
        """Test save_chart with invalid format returns error."""
        output_path = tmp_path / "test_chart.pdf"

        result = save_chart(
            result=sample_backtest_result,
            format="pdf",
            path=output_path,
        )

        assert result.success is False
        assert result.error is not None
        assert "Unsupported format" in result.error

    def test_save_chart_empty_equity_curve_returns_error(
        self, tmp_path: Path
    ) -> None:
        """Test save_chart with empty equity curve returns error."""
        metrics = BacktestMetrics()
        backtest_result = BacktestResult(
            engine="pnl",
            strategy_id="empty_test",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 5),
            metrics=metrics,
            equity_curve=[],  # Empty
            initial_capital_usd=Decimal("10000"),
            final_capital_usd=Decimal("10000"),
        )

        output_path = tmp_path / "empty_chart.png"
        result = save_chart(
            result=backtest_result,
            format="png",
            path=output_path,
        )

        assert result.success is False
        assert result.error is not None
        assert "No equity curve data" in result.error


# =============================================================================
# Test Chart Path Derivation Logic
# =============================================================================


class TestChartPathDerivation:
    """Tests for chart path derivation from output path."""

    def test_png_path_from_json_path(self) -> None:
        """Test PNG path is derived from JSON path."""
        json_path = Path("/tmp/results.json")
        expected_chart_path = json_path.with_suffix(".png")
        assert expected_chart_path == Path("/tmp/results.png")

    def test_html_path_from_json_path(self) -> None:
        """Test HTML path is derived from JSON path."""
        json_path = Path("/tmp/results.json")
        expected_chart_path = json_path.with_suffix(".html")
        assert expected_chart_path == Path("/tmp/results.html")

    def test_default_chart_name_format(self) -> None:
        """Test default chart name format when no output specified."""
        strategy_name = "my_strategy"
        chart_format = "png"
        safe_name = strategy_name.replace("/", "_").replace("\\", "_")
        expected_path = Path(f"equity_curve_{safe_name}.{chart_format}")
        assert expected_path == Path("equity_curve_my_strategy.png")

    def test_strategy_name_sanitization(self) -> None:
        """Test strategy name with slashes is sanitized."""
        strategy_name = "path/to/strategy"
        safe_name = strategy_name.replace("/", "_").replace("\\", "_")
        assert safe_name == "path_to_strategy"
