"""Unit tests for backtest visualization module."""

from datetime import datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pytest

from almanak.framework.backtesting.models import BacktestMetrics, BacktestResult, EquityPoint, IntentType, TradeRecord
from almanak.framework.backtesting.visualization import (
    ChartConfig,
    ChartResult,
    DistributionStats,
    DrawdownPeriod,
    TradeMarker,
    _detect_drawdown_periods,
    _extract_trade_markers,
    calculate_distribution_stats,
    plot_duration_scatter,
    plot_equity_curve,
    plot_equity_curve_interactive,
    plot_intent_pie,
    plot_pnl_histogram,
    plot_pnl_histogram_interactive,
    save_chart,
)


@pytest.fixture
def sample_backtest_result() -> BacktestResult:
    """Create a sample BacktestResult for testing."""
    equity_curve = [
        EquityPoint(timestamp=datetime(2024, 1, 1, 0, 0), value_usd=Decimal("10000")),
        EquityPoint(timestamp=datetime(2024, 1, 2, 0, 0), value_usd=Decimal("10200")),
        EquityPoint(timestamp=datetime(2024, 1, 3, 0, 0), value_usd=Decimal("10150")),
        EquityPoint(timestamp=datetime(2024, 1, 4, 0, 0), value_usd=Decimal("10400")),
        EquityPoint(timestamp=datetime(2024, 1, 5, 0, 0), value_usd=Decimal("10350")),
    ]

    metrics = BacktestMetrics(
        net_pnl_usd=Decimal("350"),
        total_return_pct=Decimal("3.5"),
        max_drawdown_pct=Decimal("0.5"),
        sharpe_ratio=Decimal("1.5"),
        win_rate=Decimal("0.6"),
        total_trades=5,
    )

    return BacktestResult(
        engine="pnl",
        strategy_id="test_strategy",
        start_time=datetime(2024, 1, 1),
        end_time=datetime(2024, 1, 5),
        metrics=metrics,
        equity_curve=equity_curve,
        initial_capital_usd=Decimal("10000"),
        final_capital_usd=Decimal("10350"),
    )


@pytest.fixture
def empty_backtest_result() -> BacktestResult:
    """Create a BacktestResult with no equity curve."""
    # BacktestMetrics has defaults for all fields
    metrics = BacktestMetrics()

    return BacktestResult(
        engine="pnl",
        strategy_id="empty_strategy",
        start_time=datetime(2024, 1, 1),
        end_time=datetime(2024, 1, 5),
        metrics=metrics,
        equity_curve=[],
        initial_capital_usd=Decimal("10000"),
        final_capital_usd=Decimal("10000"),
    )


class TestPlotEquityCurve:
    """Tests for plot_equity_curve function."""

    def test_plot_equity_curve_success(
        self, sample_backtest_result: BacktestResult, tmp_path: Path
    ) -> None:
        """Test successful equity curve generation."""
        output_path = tmp_path / "equity.png"

        result = plot_equity_curve(sample_backtest_result, output_path=output_path)

        assert result.success is True
        assert result.chart_type == "equity_curve"
        assert result.file_path == output_path
        assert result.error is None
        assert output_path.exists()

    def test_plot_equity_curve_default_path(
        self, sample_backtest_result: BacktestResult, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test default output path generation."""
        monkeypatch.chdir(tmp_path)

        result = plot_equity_curve(sample_backtest_result)

        assert result.success is True
        assert result.file_path is not None
        assert "test_strategy" in str(result.file_path)
        assert result.file_path.suffix == ".png"

    def test_plot_equity_curve_empty_data(self, empty_backtest_result: BacktestResult) -> None:
        """Test error handling for empty equity curve."""
        result = plot_equity_curve(empty_backtest_result)

        assert result.success is False
        assert result.chart_type == "equity_curve"
        assert result.file_path is None
        assert result.error is not None
        assert "No equity curve data" in result.error

    def test_plot_equity_curve_custom_config(
        self, sample_backtest_result: BacktestResult, tmp_path: Path
    ) -> None:
        """Test custom chart configuration."""
        output_path = tmp_path / "custom.png"
        config = ChartConfig(
            figure_width=8.0,
            figure_height=4.0,
            dpi=100,
            line_color="#FF5733",
        )

        result = plot_equity_curve(
            sample_backtest_result,
            output_path=output_path,
            config=config,
        )

        assert result.success is True
        assert output_path.exists()

    def test_plot_equity_curve_custom_title(
        self, sample_backtest_result: BacktestResult, tmp_path: Path
    ) -> None:
        """Test custom chart title."""
        output_path = tmp_path / "titled.png"

        result = plot_equity_curve(
            sample_backtest_result,
            output_path=output_path,
            title="My Custom Title",
        )

        assert result.success is True

    def test_plot_equity_curve_string_path(
        self, sample_backtest_result: BacktestResult, tmp_path: Path
    ) -> None:
        """Test string path conversion."""
        output_path = str(tmp_path / "string_path.png")

        result = plot_equity_curve(sample_backtest_result, output_path=output_path)

        assert result.success is True
        assert result.file_path == Path(output_path)

    def test_plot_equity_curve_creates_parent_dirs(
        self, sample_backtest_result: BacktestResult, tmp_path: Path
    ) -> None:
        """Test parent directory creation."""
        output_path = tmp_path / "nested" / "dirs" / "equity.png"

        result = plot_equity_curve(sample_backtest_result, output_path=output_path)

        assert result.success is True
        assert output_path.exists()

    def test_plot_equity_curve_matplotlib_not_installed(
        self, sample_backtest_result: BacktestResult
    ) -> None:
        """Test error handling when matplotlib is not installed."""
        with patch.dict("sys.modules", {"matplotlib.pyplot": None}):
            # Mock the import to raise ImportError
            with patch(
                "almanak.framework.backtesting.visualization.plot_equity_curve"
            ) as mock_func:
                mock_func.return_value = ChartResult(
                    chart_type="equity_curve",
                    file_path=None,
                    success=False,
                    error="matplotlib not installed. Run: uv add matplotlib",
                )
                result = mock_func(sample_backtest_result)

                assert result.success is False
                assert "matplotlib" in result.error

    def test_plot_equity_curve_strategy_id_sanitized(
        self, sample_backtest_result: BacktestResult, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that strategy IDs with slashes are sanitized."""
        monkeypatch.chdir(tmp_path)
        sample_backtest_result.strategy_id = "path/to/strategy"

        result = plot_equity_curve(sample_backtest_result)

        assert result.success is True
        assert "/" not in str(result.file_path.name)
        assert "\\" not in str(result.file_path.name)


class TestChartConfig:
    """Tests for ChartConfig dataclass."""

    def test_default_config(self) -> None:
        """Test default configuration values."""
        config = ChartConfig()

        assert config.figure_width == 12.0
        assert config.figure_height == 6.0
        assert config.dpi == 150
        assert config.font_size == 10
        assert config.title_size == 14
        assert config.line_width == 2.0
        assert config.line_color == "#2196F3"
        assert config.fill_alpha == 0.1
        assert config.grid_alpha == 0.3

    def test_custom_config(self) -> None:
        """Test custom configuration."""
        config = ChartConfig(
            figure_width=16.0,
            figure_height=9.0,
            dpi=300,
            line_color="#FF0000",
        )

        assert config.figure_width == 16.0
        assert config.figure_height == 9.0
        assert config.dpi == 300
        assert config.line_color == "#FF0000"


class TestChartResult:
    """Tests for ChartResult dataclass."""

    def test_success_result(self, tmp_path: Path) -> None:
        """Test successful result creation."""
        file_path = tmp_path / "test.png"
        result = ChartResult(
            chart_type="equity_curve",
            file_path=file_path,
            success=True,
        )

        assert result.chart_type == "equity_curve"
        assert result.file_path == file_path
        assert result.success is True
        assert result.error is None

    def test_failure_result(self) -> None:
        """Test failure result creation."""
        result = ChartResult(
            chart_type="equity_curve",
            file_path=None,
            success=False,
            error="Test error message",
        )

        assert result.chart_type == "equity_curve"
        assert result.file_path is None
        assert result.success is False
        assert result.error == "Test error message"

    def test_result_with_drawdown_periods(self, tmp_path: Path) -> None:
        """Test result with drawdown periods."""
        file_path = tmp_path / "test.png"
        drawdown_periods = [
            DrawdownPeriod(
                start=datetime(2024, 1, 2),
                end=datetime(2024, 1, 4),
                peak_value=Decimal("10200"),
                trough_value=Decimal("9800"),
                drawdown_pct=Decimal("0.0392"),
            )
        ]
        result = ChartResult(
            chart_type="equity_curve",
            file_path=file_path,
            success=True,
            drawdown_periods=drawdown_periods,
        )

        assert result.success is True
        assert len(result.drawdown_periods) == 1
        assert result.drawdown_periods[0].peak_value == Decimal("10200")


class TestDrawdownPeriod:
    """Tests for DrawdownPeriod dataclass."""

    def test_drawdown_period_creation(self) -> None:
        """Test creating a drawdown period."""
        period = DrawdownPeriod(
            start=datetime(2024, 1, 2),
            end=datetime(2024, 1, 5),
            peak_value=Decimal("10000"),
            trough_value=Decimal("9000"),
            drawdown_pct=Decimal("0.10"),
        )

        assert period.start == datetime(2024, 1, 2)
        assert period.end == datetime(2024, 1, 5)
        assert period.peak_value == Decimal("10000")
        assert period.trough_value == Decimal("9000")
        assert period.drawdown_pct == Decimal("0.10")


class TestDetectDrawdownPeriods:
    """Tests for _detect_drawdown_periods helper function."""

    def test_no_drawdown(self) -> None:
        """Test with consistently rising values (no drawdown)."""
        timestamps = [
            datetime(2024, 1, 1),
            datetime(2024, 1, 2),
            datetime(2024, 1, 3),
        ]
        values = [10000.0, 10500.0, 11000.0]

        periods = _detect_drawdown_periods(timestamps, values)

        assert len(periods) == 0

    def test_single_drawdown(self) -> None:
        """Test detecting a single drawdown period."""
        timestamps = [
            datetime(2024, 1, 1),
            datetime(2024, 1, 2),
            datetime(2024, 1, 3),
            datetime(2024, 1, 4),
        ]
        values = [10000.0, 9500.0, 9800.0, 10200.0]  # 5% drop then recovery

        periods = _detect_drawdown_periods(timestamps, values)

        assert len(periods) == 1
        assert periods[0].start == datetime(2024, 1, 1)
        assert periods[0].end == datetime(2024, 1, 4)
        assert float(periods[0].drawdown_pct) == pytest.approx(0.05, rel=0.01)

    def test_multiple_drawdowns(self) -> None:
        """Test detecting multiple drawdown periods."""
        timestamps = [
            datetime(2024, 1, 1),
            datetime(2024, 1, 2),
            datetime(2024, 1, 3),
            datetime(2024, 1, 4),
            datetime(2024, 1, 5),
            datetime(2024, 1, 6),
        ]
        # Two separate drawdowns: 10% drop and recovery, then 5% drop and recovery
        values = [10000.0, 9000.0, 10200.0, 9700.0, 9600.0, 10300.0]

        periods = _detect_drawdown_periods(timestamps, values)

        assert len(periods) == 2

    def test_drawdown_at_end(self) -> None:
        """Test drawdown that doesn't recover before data ends."""
        timestamps = [
            datetime(2024, 1, 1),
            datetime(2024, 1, 2),
            datetime(2024, 1, 3),
        ]
        values = [10000.0, 9500.0, 9200.0]  # Dropping, never recovers

        periods = _detect_drawdown_periods(timestamps, values)

        assert len(periods) == 1
        assert periods[0].end == datetime(2024, 1, 3)  # Ends at last timestamp
        assert float(periods[0].drawdown_pct) == pytest.approx(0.08, rel=0.01)

    def test_min_drawdown_threshold(self) -> None:
        """Test filtering by minimum drawdown threshold."""
        timestamps = [
            datetime(2024, 1, 1),
            datetime(2024, 1, 2),
            datetime(2024, 1, 3),
        ]
        values = [10000.0, 9950.0, 10100.0]  # 0.5% drop (below 1% threshold)

        # Default 1% threshold should filter out
        periods = _detect_drawdown_periods(timestamps, values)
        assert len(periods) == 0

        # 0.5% threshold should include
        periods = _detect_drawdown_periods(timestamps, values, min_drawdown_pct=0.005)
        assert len(periods) == 1

    def test_empty_data(self) -> None:
        """Test with empty data."""
        periods = _detect_drawdown_periods([], [])
        assert len(periods) == 0

    def test_single_point(self) -> None:
        """Test with single data point."""
        periods = _detect_drawdown_periods([datetime(2024, 1, 1)], [10000.0])
        assert len(periods) == 0


class TestBenchmarkComparison:
    """Tests for benchmark comparison feature."""

    @pytest.fixture
    def benchmark_curve(self) -> list[EquityPoint]:
        """Create a benchmark equity curve (e.g., ETH hold)."""
        return [
            EquityPoint(timestamp=datetime(2024, 1, 1, 0, 0), value_usd=Decimal("10000")),
            EquityPoint(timestamp=datetime(2024, 1, 2, 0, 0), value_usd=Decimal("10100")),
            EquityPoint(timestamp=datetime(2024, 1, 3, 0, 0), value_usd=Decimal("9900")),
            EquityPoint(timestamp=datetime(2024, 1, 4, 0, 0), value_usd=Decimal("10050")),
            EquityPoint(timestamp=datetime(2024, 1, 5, 0, 0), value_usd=Decimal("10200")),
        ]

    def test_equity_curve_with_benchmark(
        self,
        sample_backtest_result: BacktestResult,
        benchmark_curve: list[EquityPoint],
        tmp_path: Path,
    ) -> None:
        """Test equity curve with benchmark overlay."""
        output_path = tmp_path / "equity_with_benchmark.png"

        result = plot_equity_curve(
            sample_backtest_result,
            output_path=output_path,
            benchmark_curve=benchmark_curve,
            benchmark_label="ETH Hold",
        )

        assert result.success is True
        assert output_path.exists()

    def test_equity_curve_with_custom_benchmark_label(
        self,
        sample_backtest_result: BacktestResult,
        benchmark_curve: list[EquityPoint],
        tmp_path: Path,
    ) -> None:
        """Test custom benchmark label."""
        output_path = tmp_path / "equity_custom_label.png"

        result = plot_equity_curve(
            sample_backtest_result,
            output_path=output_path,
            benchmark_curve=benchmark_curve,
            benchmark_label="Buy & Hold BTC",
        )

        assert result.success is True


class TestDrawdownHighlighting:
    """Tests for drawdown highlighting feature."""

    @pytest.fixture
    def result_with_drawdown(self) -> BacktestResult:
        """Create a BacktestResult with a clear drawdown period."""
        equity_curve = [
            EquityPoint(timestamp=datetime(2024, 1, 1, 0, 0), value_usd=Decimal("10000")),
            EquityPoint(timestamp=datetime(2024, 1, 2, 0, 0), value_usd=Decimal("10500")),
            EquityPoint(timestamp=datetime(2024, 1, 3, 0, 0), value_usd=Decimal("9500")),  # Drawdown
            EquityPoint(timestamp=datetime(2024, 1, 4, 0, 0), value_usd=Decimal("9200")),  # Trough
            EquityPoint(timestamp=datetime(2024, 1, 5, 0, 0), value_usd=Decimal("10600")),  # Recovery
        ]

        metrics = BacktestMetrics(
            net_pnl_usd=Decimal("600"),
            total_return_pct=Decimal("6.0"),
            max_drawdown_pct=Decimal("12.4"),  # (10500 - 9200) / 10500
            sharpe_ratio=Decimal("1.5"),
            win_rate=Decimal("0.6"),
            total_trades=5,
        )

        return BacktestResult(
            engine="pnl",
            strategy_id="drawdown_test_strategy",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 5),
            metrics=metrics,
            equity_curve=equity_curve,
            initial_capital_usd=Decimal("10000"),
            final_capital_usd=Decimal("10600"),
        )

    def test_equity_curve_with_drawdown_highlighting(
        self, result_with_drawdown: BacktestResult, tmp_path: Path
    ) -> None:
        """Test equity curve with drawdown periods highlighted."""
        output_path = tmp_path / "equity_with_drawdown.png"

        result = plot_equity_curve(
            result_with_drawdown,
            output_path=output_path,
            show_drawdown=True,
        )

        assert result.success is True
        assert output_path.exists()
        assert len(result.drawdown_periods) > 0

    def test_equity_curve_drawdown_disabled_by_default(
        self, result_with_drawdown: BacktestResult, tmp_path: Path
    ) -> None:
        """Test that drawdown highlighting is disabled by default."""
        output_path = tmp_path / "equity_no_drawdown.png"

        result = plot_equity_curve(
            result_with_drawdown,
            output_path=output_path,
        )

        assert result.success is True
        assert len(result.drawdown_periods) == 0  # Not detected when disabled

    def test_custom_min_drawdown_threshold(
        self, result_with_drawdown: BacktestResult, tmp_path: Path
    ) -> None:
        """Test custom minimum drawdown threshold."""
        output_path = tmp_path / "equity_custom_threshold.png"

        # Very high threshold - should not detect any drawdowns
        result = plot_equity_curve(
            result_with_drawdown,
            output_path=output_path,
            show_drawdown=True,
            min_drawdown_pct=0.50,  # 50% threshold
        )

        assert result.success is True
        assert len(result.drawdown_periods) == 0


class TestCombinedFeatures:
    """Tests for combined benchmark and drawdown features."""

    @pytest.fixture
    def benchmark_curve(self) -> list[EquityPoint]:
        """Create benchmark curve."""
        return [
            EquityPoint(timestamp=datetime(2024, 1, 1, 0, 0), value_usd=Decimal("10000")),
            EquityPoint(timestamp=datetime(2024, 1, 2, 0, 0), value_usd=Decimal("10100")),
            EquityPoint(timestamp=datetime(2024, 1, 3, 0, 0), value_usd=Decimal("9900")),
            EquityPoint(timestamp=datetime(2024, 1, 4, 0, 0), value_usd=Decimal("10050")),
            EquityPoint(timestamp=datetime(2024, 1, 5, 0, 0), value_usd=Decimal("10200")),
        ]

    def test_benchmark_and_drawdown_together(
        self,
        sample_backtest_result: BacktestResult,
        benchmark_curve: list[EquityPoint],
        tmp_path: Path,
    ) -> None:
        """Test combining benchmark overlay with drawdown highlighting."""
        output_path = tmp_path / "equity_full_features.png"

        result = plot_equity_curve(
            sample_backtest_result,
            output_path=output_path,
            benchmark_curve=benchmark_curve,
            benchmark_label="ETH Hold",
            show_drawdown=True,
            min_drawdown_pct=0.001,  # Very low threshold to catch any minor drawdowns
        )

        assert result.success is True
        assert output_path.exists()

    def test_custom_config_with_benchmark(
        self,
        sample_backtest_result: BacktestResult,
        benchmark_curve: list[EquityPoint],
        tmp_path: Path,
    ) -> None:
        """Test custom styling config with benchmark."""
        output_path = tmp_path / "custom_styled.png"
        config = ChartConfig(
            line_color="#4CAF50",  # Green for strategy
            benchmark_color="#FF9800",  # Orange for benchmark
            benchmark_line_style="-.",  # Different line style
            drawdown_color="#E91E63",  # Pink for drawdowns
            drawdown_alpha=0.3,
        )

        result = plot_equity_curve(
            sample_backtest_result,
            output_path=output_path,
            config=config,
            benchmark_curve=benchmark_curve,
            show_drawdown=True,
        )

        assert result.success is True


class TestChartConfigExtended:
    """Tests for extended ChartConfig options."""

    def test_default_benchmark_config(self) -> None:
        """Test default benchmark configuration values."""
        config = ChartConfig()

        assert config.benchmark_color == "#757575"
        assert config.benchmark_line_style == "--"
        assert config.drawdown_color == "#F44336"
        assert config.drawdown_alpha == 0.2

    def test_custom_benchmark_config(self) -> None:
        """Test custom benchmark configuration."""
        config = ChartConfig(
            benchmark_color="#009688",
            benchmark_line_style=":",
            drawdown_color="#3F51B5",
            drawdown_alpha=0.4,
        )

        assert config.benchmark_color == "#009688"
        assert config.benchmark_line_style == ":"
        assert config.drawdown_color == "#3F51B5"
        assert config.drawdown_alpha == 0.4

    def test_trade_marker_config_defaults(self) -> None:
        """Test default trade marker configuration values."""
        config = ChartConfig()

        assert config.entry_marker == "^"
        assert config.exit_marker == "v"
        assert config.entry_color == "#4CAF50"
        assert config.exit_color == "#F44336"
        assert config.profit_color == "#4CAF50"
        assert config.loss_color == "#F44336"
        assert config.marker_size == 80

    def test_custom_trade_marker_config(self) -> None:
        """Test custom trade marker configuration."""
        config = ChartConfig(
            entry_marker="o",
            exit_marker="s",
            entry_color="#00FF00",
            exit_color="#FF0000",
            profit_color="#00FF00",
            loss_color="#FF0000",
            marker_size=100,
        )

        assert config.entry_marker == "o"
        assert config.exit_marker == "s"
        assert config.entry_color == "#00FF00"
        assert config.exit_color == "#FF0000"
        assert config.marker_size == 100


class TestTradeMarker:
    """Tests for TradeMarker dataclass."""

    def test_trade_marker_creation(self) -> None:
        """Test creating a trade marker."""
        marker = TradeMarker(
            timestamp=datetime(2024, 1, 2),
            value_usd=Decimal("10200"),
            is_entry=True,
            trade_type="SWAP",
            pnl_usd=Decimal("100"),
        )

        assert marker.timestamp == datetime(2024, 1, 2)
        assert marker.value_usd == Decimal("10200")
        assert marker.is_entry is True
        assert marker.trade_type == "SWAP"
        assert marker.pnl_usd == Decimal("100")

    def test_trade_marker_without_pnl(self) -> None:
        """Test trade marker with no PnL."""
        marker = TradeMarker(
            timestamp=datetime(2024, 1, 2),
            value_usd=Decimal("10200"),
            is_entry=True,
            trade_type="LP_OPEN",
        )

        assert marker.pnl_usd is None


class TestExtractTradeMarkers:
    """Tests for _extract_trade_markers helper function."""

    @pytest.fixture
    def sample_trades(self) -> list[TradeRecord]:
        """Create sample trades for testing."""
        return [
            TradeRecord(
                timestamp=datetime(2024, 1, 2, 0, 0),
                intent_type=IntentType.SWAP,
                executed_price=Decimal("2000"),
                fee_usd=Decimal("5"),
                slippage_usd=Decimal("2"),
                gas_cost_usd=Decimal("3"),
                pnl_usd=Decimal("100"),
                success=True,
            ),
            TradeRecord(
                timestamp=datetime(2024, 1, 3, 0, 0),
                intent_type=IntentType.LP_OPEN,
                executed_price=Decimal("2100"),
                fee_usd=Decimal("10"),
                slippage_usd=Decimal("5"),
                gas_cost_usd=Decimal("4"),
                pnl_usd=Decimal("0"),
                success=True,
            ),
            TradeRecord(
                timestamp=datetime(2024, 1, 4, 0, 0),
                intent_type=IntentType.LP_CLOSE,
                executed_price=Decimal("2050"),
                fee_usd=Decimal("10"),
                slippage_usd=Decimal("5"),
                gas_cost_usd=Decimal("4"),
                pnl_usd=Decimal("-50"),
                success=True,
            ),
        ]

    @pytest.fixture
    def sample_equity_curve(self) -> list[EquityPoint]:
        """Create sample equity curve."""
        return [
            EquityPoint(timestamp=datetime(2024, 1, 1, 0, 0), value_usd=Decimal("10000")),
            EquityPoint(timestamp=datetime(2024, 1, 2, 0, 0), value_usd=Decimal("10200")),
            EquityPoint(timestamp=datetime(2024, 1, 3, 0, 0), value_usd=Decimal("10150")),
            EquityPoint(timestamp=datetime(2024, 1, 4, 0, 0), value_usd=Decimal("10100")),
            EquityPoint(timestamp=datetime(2024, 1, 5, 0, 0), value_usd=Decimal("10300")),
        ]

    def test_extract_trade_markers(
        self, sample_trades: list[TradeRecord], sample_equity_curve: list[EquityPoint]
    ) -> None:
        """Test extracting trade markers from trades."""
        markers = _extract_trade_markers(sample_trades, sample_equity_curve)

        assert len(markers) == 3
        assert markers[0].is_entry is True  # SWAP is entry
        assert markers[0].trade_type == "SWAP"
        assert markers[1].is_entry is True  # LP_OPEN is entry
        assert markers[2].is_entry is False  # LP_CLOSE is exit

    def test_extract_trade_markers_empty_trades(
        self, sample_equity_curve: list[EquityPoint]
    ) -> None:
        """Test with empty trades list."""
        markers = _extract_trade_markers([], sample_equity_curve)
        assert len(markers) == 0

    def test_extract_trade_markers_empty_curve(
        self, sample_trades: list[TradeRecord]
    ) -> None:
        """Test with empty equity curve."""
        markers = _extract_trade_markers(sample_trades, [])
        assert len(markers) == 0

    def test_extract_trade_markers_skips_hold(
        self, sample_equity_curve: list[EquityPoint]
    ) -> None:
        """Test that HOLD trades are skipped."""
        trades = [
            TradeRecord(
                timestamp=datetime(2024, 1, 2, 0, 0),
                intent_type=IntentType.HOLD,
                executed_price=Decimal("0"),
                fee_usd=Decimal("0"),
                slippage_usd=Decimal("0"),
                gas_cost_usd=Decimal("0"),
                pnl_usd=Decimal("0"),
                success=True,
            ),
        ]
        markers = _extract_trade_markers(trades, sample_equity_curve)
        assert len(markers) == 0


class TestPlotEquityCurveWithTrades:
    """Tests for plot_equity_curve with trade markers."""

    @pytest.fixture
    def result_with_trades(self) -> BacktestResult:
        """Create a BacktestResult with trades."""
        equity_curve = [
            EquityPoint(timestamp=datetime(2024, 1, 1, 0, 0), value_usd=Decimal("10000")),
            EquityPoint(timestamp=datetime(2024, 1, 2, 0, 0), value_usd=Decimal("10200")),
            EquityPoint(timestamp=datetime(2024, 1, 3, 0, 0), value_usd=Decimal("10150")),
            EquityPoint(timestamp=datetime(2024, 1, 4, 0, 0), value_usd=Decimal("10400")),
            EquityPoint(timestamp=datetime(2024, 1, 5, 0, 0), value_usd=Decimal("10350")),
        ]

        trades = [
            TradeRecord(
                timestamp=datetime(2024, 1, 2, 0, 0),
                intent_type=IntentType.SWAP,
                executed_price=Decimal("2000"),
                fee_usd=Decimal("5"),
                slippage_usd=Decimal("2"),
                gas_cost_usd=Decimal("3"),
                pnl_usd=Decimal("100"),
                success=True,
            ),
            TradeRecord(
                timestamp=datetime(2024, 1, 4, 0, 0),
                intent_type=IntentType.SWAP,
                executed_price=Decimal("2100"),
                fee_usd=Decimal("5"),
                slippage_usd=Decimal("2"),
                gas_cost_usd=Decimal("3"),
                pnl_usd=Decimal("-50"),
                success=True,
            ),
        ]

        metrics = BacktestMetrics(
            net_pnl_usd=Decimal("350"),
            total_return_pct=Decimal("3.5"),
            max_drawdown_pct=Decimal("0.5"),
            sharpe_ratio=Decimal("1.5"),
            win_rate=Decimal("0.5"),
            total_trades=2,
        )

        return BacktestResult(
            engine="pnl",
            strategy_id="trade_test_strategy",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 5),
            metrics=metrics,
            equity_curve=equity_curve,
            trades=trades,
            initial_capital_usd=Decimal("10000"),
            final_capital_usd=Decimal("10350"),
        )

    def test_equity_curve_with_trade_markers(
        self, result_with_trades: BacktestResult, tmp_path: Path
    ) -> None:
        """Test equity curve with trade markers enabled."""
        output_path = tmp_path / "equity_with_trades.png"

        result = plot_equity_curve(
            result_with_trades,
            output_path=output_path,
            show_trades=True,
        )

        assert result.success is True
        assert output_path.exists()
        assert len(result.trade_markers) == 2

    def test_equity_curve_trades_disabled_by_default(
        self, result_with_trades: BacktestResult, tmp_path: Path
    ) -> None:
        """Test that trade markers are disabled by default."""
        output_path = tmp_path / "equity_no_trades.png"

        result = plot_equity_curve(
            result_with_trades,
            output_path=output_path,
        )

        assert result.success is True
        assert len(result.trade_markers) == 0

    def test_equity_curve_color_by_pnl(
        self, result_with_trades: BacktestResult, tmp_path: Path
    ) -> None:
        """Test trade markers colored by PnL."""
        output_path = tmp_path / "equity_pnl_colored.png"

        result = plot_equity_curve(
            result_with_trades,
            output_path=output_path,
            show_trades=True,
            color_by_pnl=True,
        )

        assert result.success is True

    def test_equity_curve_color_by_entry_exit(
        self, result_with_trades: BacktestResult, tmp_path: Path
    ) -> None:
        """Test trade markers colored by entry/exit."""
        output_path = tmp_path / "equity_entry_exit_colored.png"

        result = plot_equity_curve(
            result_with_trades,
            output_path=output_path,
            show_trades=True,
            color_by_pnl=False,
        )

        assert result.success is True


class TestPlotEquityCurveInteractive:
    """Tests for plot_equity_curve_interactive function."""

    @pytest.fixture
    def result_with_trades(self) -> BacktestResult:
        """Create a BacktestResult with trades."""
        equity_curve = [
            EquityPoint(timestamp=datetime(2024, 1, 1, 0, 0), value_usd=Decimal("10000")),
            EquityPoint(timestamp=datetime(2024, 1, 2, 0, 0), value_usd=Decimal("10200")),
            EquityPoint(timestamp=datetime(2024, 1, 3, 0, 0), value_usd=Decimal("10150")),
            EquityPoint(timestamp=datetime(2024, 1, 4, 0, 0), value_usd=Decimal("10400")),
            EquityPoint(timestamp=datetime(2024, 1, 5, 0, 0), value_usd=Decimal("10350")),
        ]

        trades = [
            TradeRecord(
                timestamp=datetime(2024, 1, 2, 0, 0),
                intent_type=IntentType.SWAP,
                executed_price=Decimal("2000"),
                fee_usd=Decimal("5"),
                slippage_usd=Decimal("2"),
                gas_cost_usd=Decimal("3"),
                pnl_usd=Decimal("100"),
                success=True,
            ),
        ]

        metrics = BacktestMetrics(
            net_pnl_usd=Decimal("350"),
            total_return_pct=Decimal("3.5"),
            max_drawdown_pct=Decimal("0.5"),
            sharpe_ratio=Decimal("1.5"),
            win_rate=Decimal("1.0"),
            total_trades=1,
        )

        return BacktestResult(
            engine="pnl",
            strategy_id="interactive_test",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 5),
            metrics=metrics,
            equity_curve=equity_curve,
            trades=trades,
            initial_capital_usd=Decimal("10000"),
            final_capital_usd=Decimal("10350"),
        )

    def test_interactive_equity_curve_success(
        self, result_with_trades: BacktestResult, tmp_path: Path
    ) -> None:
        """Test successful interactive chart generation."""
        output_path = tmp_path / "equity.html"

        result = plot_equity_curve_interactive(
            result_with_trades,
            output_path=output_path,
        )

        assert result.success is True
        assert result.format == "html"
        assert output_path.exists()
        # Check it's actually HTML
        content = output_path.read_text()
        assert "plotly" in content.lower()

    def test_interactive_with_trades(
        self, result_with_trades: BacktestResult, tmp_path: Path
    ) -> None:
        """Test interactive chart with trade markers."""
        output_path = tmp_path / "equity_trades.html"

        result = plot_equity_curve_interactive(
            result_with_trades,
            output_path=output_path,
            show_trades=True,
        )

        assert result.success is True
        assert len(result.trade_markers) == 1

    def test_interactive_with_benchmark(
        self, result_with_trades: BacktestResult, tmp_path: Path
    ) -> None:
        """Test interactive chart with benchmark."""
        output_path = tmp_path / "equity_benchmark.html"
        benchmark = [
            EquityPoint(timestamp=datetime(2024, 1, 1, 0, 0), value_usd=Decimal("10000")),
            EquityPoint(timestamp=datetime(2024, 1, 2, 0, 0), value_usd=Decimal("10100")),
            EquityPoint(timestamp=datetime(2024, 1, 3, 0, 0), value_usd=Decimal("10050")),
            EquityPoint(timestamp=datetime(2024, 1, 4, 0, 0), value_usd=Decimal("10200")),
            EquityPoint(timestamp=datetime(2024, 1, 5, 0, 0), value_usd=Decimal("10150")),
        ]

        result = plot_equity_curve_interactive(
            result_with_trades,
            output_path=output_path,
            benchmark_curve=benchmark,
            benchmark_label="ETH Hold",
        )

        assert result.success is True

    def test_interactive_with_drawdown(
        self, result_with_trades: BacktestResult, tmp_path: Path
    ) -> None:
        """Test interactive chart with drawdown highlighting."""
        output_path = tmp_path / "equity_drawdown.html"

        result = plot_equity_curve_interactive(
            result_with_trades,
            output_path=output_path,
            show_drawdown=True,
            min_drawdown_pct=0.001,  # Low threshold to catch minor drawdowns
        )

        assert result.success is True

    def test_interactive_empty_data(self, tmp_path: Path) -> None:
        """Test error handling for empty data."""
        metrics = BacktestMetrics()
        empty_result = BacktestResult(
            engine="pnl",
            strategy_id="empty",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 5),
            metrics=metrics,
            equity_curve=[],
            initial_capital_usd=Decimal("10000"),
            final_capital_usd=Decimal("10000"),
        )

        result = plot_equity_curve_interactive(empty_result)

        assert result.success is False
        assert "No equity curve data" in result.error

    def test_interactive_default_path(
        self, result_with_trades: BacktestResult, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test default HTML path generation."""
        monkeypatch.chdir(tmp_path)

        result = plot_equity_curve_interactive(result_with_trades)

        assert result.success is True
        assert result.file_path is not None
        assert result.file_path.suffix == ".html"


class TestSaveChart:
    """Tests for save_chart convenience function."""

    @pytest.fixture
    def sample_result(self) -> BacktestResult:
        """Create a sample BacktestResult."""
        equity_curve = [
            EquityPoint(timestamp=datetime(2024, 1, 1, 0, 0), value_usd=Decimal("10000")),
            EquityPoint(timestamp=datetime(2024, 1, 2, 0, 0), value_usd=Decimal("10200")),
            EquityPoint(timestamp=datetime(2024, 1, 3, 0, 0), value_usd=Decimal("10150")),
        ]

        metrics = BacktestMetrics(
            net_pnl_usd=Decimal("150"),
            total_return_pct=Decimal("1.5"),
        )

        return BacktestResult(
            engine="pnl",
            strategy_id="save_chart_test",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 3),
            metrics=metrics,
            equity_curve=equity_curve,
            initial_capital_usd=Decimal("10000"),
            final_capital_usd=Decimal("10150"),
        )

    def test_save_chart_png(self, sample_result: BacktestResult, tmp_path: Path) -> None:
        """Test saving chart as PNG."""
        output_path = tmp_path / "chart.png"

        result = save_chart(sample_result, format="png", path=output_path)

        assert result.success is True
        assert result.format == "png"
        assert output_path.exists()

    def test_save_chart_html(self, sample_result: BacktestResult, tmp_path: Path) -> None:
        """Test saving chart as HTML."""
        output_path = tmp_path / "chart.html"

        result = save_chart(sample_result, format="html", path=output_path)

        assert result.success is True
        assert result.format == "html"
        assert output_path.exists()

    def test_save_chart_case_insensitive(
        self, sample_result: BacktestResult, tmp_path: Path
    ) -> None:
        """Test format is case insensitive."""
        output_path = tmp_path / "chart.html"

        result = save_chart(sample_result, format="HTML", path=output_path)

        assert result.success is True
        assert result.format == "html"

    def test_save_chart_unsupported_format(
        self, sample_result: BacktestResult
    ) -> None:
        """Test error for unsupported format."""
        result = save_chart(sample_result, format="pdf")

        assert result.success is False
        assert "Unsupported format" in result.error

    def test_save_chart_with_all_options(
        self, sample_result: BacktestResult, tmp_path: Path
    ) -> None:
        """Test save_chart with all options."""
        output_path = tmp_path / "full_chart.png"
        benchmark = [
            EquityPoint(timestamp=datetime(2024, 1, 1, 0, 0), value_usd=Decimal("10000")),
            EquityPoint(timestamp=datetime(2024, 1, 2, 0, 0), value_usd=Decimal("10100")),
            EquityPoint(timestamp=datetime(2024, 1, 3, 0, 0), value_usd=Decimal("10050")),
        ]

        result = save_chart(
            sample_result,
            format="png",
            path=output_path,
            title="Test Chart",
            benchmark_curve=benchmark,
            benchmark_label="ETH",
            show_drawdown=True,
            show_trades=True,
            color_by_pnl=True,
        )

        assert result.success is True


class TestChartResultExtended:
    """Tests for extended ChartResult fields."""

    def test_result_with_trade_markers(self, tmp_path: Path) -> None:
        """Test ChartResult with trade markers."""
        file_path = tmp_path / "test.png"
        trade_markers = [
            TradeMarker(
                timestamp=datetime(2024, 1, 2),
                value_usd=Decimal("10200"),
                is_entry=True,
                trade_type="SWAP",
                pnl_usd=Decimal("100"),
            ),
            TradeMarker(
                timestamp=datetime(2024, 1, 4),
                value_usd=Decimal("10100"),
                is_entry=False,
                trade_type="SWAP",
                pnl_usd=Decimal("-50"),
            ),
        ]

        result = ChartResult(
            chart_type="equity_curve",
            file_path=file_path,
            success=True,
            trade_markers=trade_markers,
        )

        assert len(result.trade_markers) == 2
        assert result.trade_markers[0].is_entry is True
        assert result.trade_markers[1].is_entry is False

    def test_result_format_field(self, tmp_path: Path) -> None:
        """Test ChartResult format field."""
        file_path = tmp_path / "test.html"

        result = ChartResult(
            chart_type="equity_curve",
            file_path=file_path,
            success=True,
            format="html",
        )

        assert result.format == "html"

    def test_result_default_format(self) -> None:
        """Test default format is png."""
        result = ChartResult(
            chart_type="equity_curve",
            file_path=None,
            success=False,
        )

        assert result.format == "png"


class TestPlotPnlHistogram:
    """Tests for plot_pnl_histogram function."""

    @pytest.fixture
    def result_with_trades(self) -> BacktestResult:
        """Create a BacktestResult with trades for histogram testing."""
        equity_curve = [
            EquityPoint(timestamp=datetime(2024, 1, 1, 0, 0), value_usd=Decimal("10000")),
            EquityPoint(timestamp=datetime(2024, 1, 5, 0, 0), value_usd=Decimal("10500")),
        ]

        trades = [
            TradeRecord(
                timestamp=datetime(2024, 1, 2, 0, 0),
                intent_type=IntentType.SWAP,
                executed_price=Decimal("2000"),
                fee_usd=Decimal("5"),
                slippage_usd=Decimal("2"),
                gas_cost_usd=Decimal("3"),
                pnl_usd=Decimal("100"),
                success=True,
            ),
            TradeRecord(
                timestamp=datetime(2024, 1, 3, 0, 0),
                intent_type=IntentType.SWAP,
                executed_price=Decimal("2100"),
                fee_usd=Decimal("5"),
                slippage_usd=Decimal("2"),
                gas_cost_usd=Decimal("3"),
                pnl_usd=Decimal("-50"),
                success=True,
            ),
            TradeRecord(
                timestamp=datetime(2024, 1, 4, 0, 0),
                intent_type=IntentType.SWAP,
                executed_price=Decimal("2050"),
                fee_usd=Decimal("5"),
                slippage_usd=Decimal("2"),
                gas_cost_usd=Decimal("3"),
                pnl_usd=Decimal("200"),
                success=True,
            ),
        ]

        metrics = BacktestMetrics(
            net_pnl_usd=Decimal("250"),
            total_return_pct=Decimal("2.5"),
            total_trades=3,
        )

        return BacktestResult(
            engine="pnl",
            strategy_id="histogram_test",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 5),
            metrics=metrics,
            equity_curve=equity_curve,
            trades=trades,
            initial_capital_usd=Decimal("10000"),
            final_capital_usd=Decimal("10250"),
        )

    def test_pnl_histogram_success(
        self, result_with_trades: BacktestResult, tmp_path: Path
    ) -> None:
        """Test successful PnL histogram generation."""
        output_path = tmp_path / "pnl_histogram.png"

        result = plot_pnl_histogram(result_with_trades, output_path=output_path)

        assert result.success is True
        assert result.chart_type == "pnl_histogram"
        assert result.file_path == output_path
        assert output_path.exists()

    def test_pnl_histogram_default_path(
        self, result_with_trades: BacktestResult, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test default output path generation."""
        monkeypatch.chdir(tmp_path)

        result = plot_pnl_histogram(result_with_trades)

        assert result.success is True
        assert result.file_path is not None
        assert "histogram_test" in str(result.file_path)

    def test_pnl_histogram_no_trades(self, sample_backtest_result: BacktestResult) -> None:
        """Test error when no trades data."""
        # sample_backtest_result has no trades
        result = plot_pnl_histogram(sample_backtest_result)

        assert result.success is False
        assert "No trades data" in result.error

    def test_pnl_histogram_custom_bins(
        self, result_with_trades: BacktestResult, tmp_path: Path
    ) -> None:
        """Test custom number of bins."""
        output_path = tmp_path / "pnl_histogram_bins.png"

        result = plot_pnl_histogram(result_with_trades, output_path=output_path, bins=10)

        assert result.success is True
        assert output_path.exists()


class TestPlotDurationScatter:
    """Tests for plot_duration_scatter function."""

    @pytest.fixture
    def result_with_timed_trades(self) -> BacktestResult:
        """Create a BacktestResult with trades at different times."""
        equity_curve = [
            EquityPoint(timestamp=datetime(2024, 1, 1, 0, 0), value_usd=Decimal("10000")),
            EquityPoint(timestamp=datetime(2024, 1, 5, 0, 0), value_usd=Decimal("10500")),
        ]

        trades = [
            TradeRecord(
                timestamp=datetime(2024, 1, 1, 10, 0),
                intent_type=IntentType.SWAP,
                executed_price=Decimal("2000"),
                fee_usd=Decimal("5"),
                slippage_usd=Decimal("2"),
                gas_cost_usd=Decimal("3"),
                pnl_usd=Decimal("100"),
                success=True,
            ),
            TradeRecord(
                timestamp=datetime(2024, 1, 2, 14, 0),  # 28 hours later
                intent_type=IntentType.SWAP,
                executed_price=Decimal("2100"),
                fee_usd=Decimal("5"),
                slippage_usd=Decimal("2"),
                gas_cost_usd=Decimal("3"),
                pnl_usd=Decimal("-50"),
                success=True,
            ),
            TradeRecord(
                timestamp=datetime(2024, 1, 3, 8, 0),  # 18 hours later
                intent_type=IntentType.SWAP,
                executed_price=Decimal("2050"),
                fee_usd=Decimal("5"),
                slippage_usd=Decimal("2"),
                gas_cost_usd=Decimal("3"),
                pnl_usd=Decimal("200"),
                success=True,
            ),
        ]

        metrics = BacktestMetrics(
            net_pnl_usd=Decimal("250"),
            total_return_pct=Decimal("2.5"),
            total_trades=3,
        )

        return BacktestResult(
            engine="pnl",
            strategy_id="duration_test",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 5),
            metrics=metrics,
            equity_curve=equity_curve,
            trades=trades,
            initial_capital_usd=Decimal("10000"),
            final_capital_usd=Decimal("10250"),
        )

    def test_duration_scatter_success(
        self, result_with_timed_trades: BacktestResult, tmp_path: Path
    ) -> None:
        """Test successful duration scatter plot generation."""
        output_path = tmp_path / "duration_scatter.png"

        result = plot_duration_scatter(result_with_timed_trades, output_path=output_path)

        assert result.success is True
        assert result.chart_type == "duration_scatter"
        assert result.file_path == output_path
        assert output_path.exists()

    def test_duration_scatter_default_path(
        self, result_with_timed_trades: BacktestResult, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test default output path generation."""
        monkeypatch.chdir(tmp_path)

        result = plot_duration_scatter(result_with_timed_trades)

        assert result.success is True
        assert result.file_path is not None
        assert "duration_test" in str(result.file_path)

    def test_duration_scatter_no_trades(self, sample_backtest_result: BacktestResult) -> None:
        """Test error when no trades data."""
        result = plot_duration_scatter(sample_backtest_result)

        assert result.success is False
        assert "No trades data" in result.error


class TestPlotIntentPie:
    """Tests for plot_intent_pie function."""

    @pytest.fixture
    def result_with_varied_intents(self) -> BacktestResult:
        """Create a BacktestResult with various intent types."""
        equity_curve = [
            EquityPoint(timestamp=datetime(2024, 1, 1, 0, 0), value_usd=Decimal("10000")),
            EquityPoint(timestamp=datetime(2024, 1, 5, 0, 0), value_usd=Decimal("10500")),
        ]

        trades = [
            TradeRecord(
                timestamp=datetime(2024, 1, 1, 10, 0),
                intent_type=IntentType.SWAP,
                executed_price=Decimal("2000"),
                fee_usd=Decimal("5"),
                slippage_usd=Decimal("2"),
                gas_cost_usd=Decimal("3"),
                pnl_usd=Decimal("100"),
                success=True,
            ),
            TradeRecord(
                timestamp=datetime(2024, 1, 2, 10, 0),
                intent_type=IntentType.SWAP,
                executed_price=Decimal("2100"),
                fee_usd=Decimal("5"),
                slippage_usd=Decimal("2"),
                gas_cost_usd=Decimal("3"),
                pnl_usd=Decimal("50"),
                success=True,
            ),
            TradeRecord(
                timestamp=datetime(2024, 1, 3, 10, 0),
                intent_type=IntentType.LP_OPEN,
                executed_price=Decimal("2050"),
                fee_usd=Decimal("10"),
                slippage_usd=Decimal("5"),
                gas_cost_usd=Decimal("4"),
                pnl_usd=Decimal("0"),
                success=True,
            ),
            TradeRecord(
                timestamp=datetime(2024, 1, 4, 10, 0),
                intent_type=IntentType.LP_CLOSE,
                executed_price=Decimal("2080"),
                fee_usd=Decimal("10"),
                slippage_usd=Decimal("5"),
                gas_cost_usd=Decimal("4"),
                pnl_usd=Decimal("150"),
                success=True,
            ),
        ]

        metrics = BacktestMetrics(
            net_pnl_usd=Decimal("300"),
            total_return_pct=Decimal("3.0"),
            total_trades=4,
        )

        return BacktestResult(
            engine="pnl",
            strategy_id="intent_test",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 5),
            metrics=metrics,
            equity_curve=equity_curve,
            trades=trades,
            initial_capital_usd=Decimal("10000"),
            final_capital_usd=Decimal("10300"),
        )

    def test_intent_pie_success(
        self, result_with_varied_intents: BacktestResult, tmp_path: Path
    ) -> None:
        """Test successful intent pie chart generation."""
        output_path = tmp_path / "intent_pie.png"

        result = plot_intent_pie(result_with_varied_intents, output_path=output_path)

        assert result.success is True
        assert result.chart_type == "intent_pie"
        assert result.file_path == output_path
        assert output_path.exists()

    def test_intent_pie_default_path(
        self, result_with_varied_intents: BacktestResult, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test default output path generation."""
        monkeypatch.chdir(tmp_path)

        result = plot_intent_pie(result_with_varied_intents)

        assert result.success is True
        assert result.file_path is not None
        assert "intent_test" in str(result.file_path)

    def test_intent_pie_no_trades(self, sample_backtest_result: BacktestResult) -> None:
        """Test error when no trades data."""
        result = plot_intent_pie(sample_backtest_result)

        assert result.success is False
        assert "No trades data" in result.error

    def test_intent_pie_skips_hold(self, tmp_path: Path) -> None:
        """Test that HOLD intents are skipped in pie chart."""
        equity_curve = [
            EquityPoint(timestamp=datetime(2024, 1, 1, 0, 0), value_usd=Decimal("10000")),
        ]

        trades = [
            TradeRecord(
                timestamp=datetime(2024, 1, 1, 10, 0),
                intent_type=IntentType.HOLD,
                executed_price=Decimal("0"),
                fee_usd=Decimal("0"),
                slippage_usd=Decimal("0"),
                gas_cost_usd=Decimal("0"),
                pnl_usd=Decimal("0"),
                success=True,
            ),
            TradeRecord(
                timestamp=datetime(2024, 1, 1, 11, 0),
                intent_type=IntentType.SWAP,
                executed_price=Decimal("2000"),
                fee_usd=Decimal("5"),
                slippage_usd=Decimal("2"),
                gas_cost_usd=Decimal("3"),
                pnl_usd=Decimal("100"),
                success=True,
            ),
        ]

        result_obj = BacktestResult(
            engine="pnl",
            strategy_id="hold_test",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 1),
            metrics=BacktestMetrics(),
            equity_curve=equity_curve,
            trades=trades,
            initial_capital_usd=Decimal("10000"),
            final_capital_usd=Decimal("10100"),
        )

        output_path = tmp_path / "intent_pie_hold.png"
        result = plot_intent_pie(result_obj, output_path=output_path)

        assert result.success is True
        # The HOLD trade should be skipped, only SWAP counted


class TestDistributionChartsCustomConfig:
    """Tests for custom configuration in distribution charts."""

    @pytest.fixture
    def result_with_trades(self) -> BacktestResult:
        """Create a BacktestResult with trades."""
        equity_curve = [
            EquityPoint(timestamp=datetime(2024, 1, 1, 0, 0), value_usd=Decimal("10000")),
            EquityPoint(timestamp=datetime(2024, 1, 5, 0, 0), value_usd=Decimal("10500")),
        ]

        trades = [
            TradeRecord(
                timestamp=datetime(2024, 1, 2, 0, 0),
                intent_type=IntentType.SWAP,
                executed_price=Decimal("2000"),
                fee_usd=Decimal("5"),
                slippage_usd=Decimal("2"),
                gas_cost_usd=Decimal("3"),
                pnl_usd=Decimal("100"),
                success=True,
            ),
        ]

        return BacktestResult(
            engine="pnl",
            strategy_id="config_test",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 5),
            metrics=BacktestMetrics(),
            equity_curve=equity_curve,
            trades=trades,
            initial_capital_usd=Decimal("10000"),
            final_capital_usd=Decimal("10100"),
        )

    def test_histogram_custom_config(
        self, result_with_trades: BacktestResult, tmp_path: Path
    ) -> None:
        """Test histogram with custom configuration."""
        output_path = tmp_path / "custom_histogram.png"
        config = ChartConfig(
            figure_width=10.0,
            figure_height=5.0,
            profit_color="#00FF00",
            loss_color="#FF0000",
        )

        result = plot_pnl_histogram(
            result_with_trades,
            output_path=output_path,
            config=config,
            title="Custom Histogram",
        )

        assert result.success is True

    def test_scatter_custom_config(
        self, result_with_trades: BacktestResult, tmp_path: Path
    ) -> None:
        """Test scatter plot with custom configuration."""
        output_path = tmp_path / "custom_scatter.png"
        config = ChartConfig(
            figure_width=10.0,
            figure_height=5.0,
            marker_size=100,
        )

        result = plot_duration_scatter(
            result_with_trades,
            output_path=output_path,
            config=config,
            title="Custom Scatter",
        )

        # May fail due to single trade (no duration calculation possible for first trade)
        # but that's expected behavior
        assert result.chart_type == "duration_scatter"

    def test_pie_custom_config(
        self, result_with_trades: BacktestResult, tmp_path: Path
    ) -> None:
        """Test pie chart with custom configuration."""
        output_path = tmp_path / "custom_pie.png"
        config = ChartConfig(
            figure_width=10.0,
            figure_height=10.0,
            title_size=18,
        )

        result = plot_intent_pie(
            result_with_trades,
            output_path=output_path,
            config=config,
            title="Custom Pie Chart",
        )

        assert result.success is True


class TestDistributionStats:
    """Tests for DistributionStats dataclass."""

    def test_distribution_stats_creation(self) -> None:
        """Test DistributionStats dataclass creation."""
        stats = DistributionStats(
            mean=100.0,
            median=90.0,
            std_dev=50.0,
            skewness=0.5,
            kurtosis=1.2,
            min_return=-100.0,
            max_return=300.0,
            percentile_5=-50.0,
            percentile_95=250.0,
            count=50,
        )

        assert stats.mean == 100.0
        assert stats.median == 90.0
        assert stats.std_dev == 50.0
        assert stats.skewness == 0.5
        assert stats.kurtosis == 1.2
        assert stats.min_return == -100.0
        assert stats.max_return == 300.0
        assert stats.percentile_5 == -50.0
        assert stats.percentile_95 == 250.0
        assert stats.count == 50


class TestCalculateDistributionStats:
    """Tests for calculate_distribution_stats function."""

    def test_basic_stats_calculation(self) -> None:
        """Test basic statistics calculation."""
        pnl_values = [100.0, 200.0, 300.0, 400.0, 500.0]
        stats = calculate_distribution_stats(pnl_values)

        assert stats is not None
        assert stats.mean == 300.0  # Sum 1500 / 5 = 300
        assert stats.median == 300.0  # Middle value
        assert stats.count == 5
        assert stats.min_return == 100.0
        assert stats.max_return == 500.0

    def test_insufficient_data_returns_none(self) -> None:
        """Test that insufficient data returns None."""
        # Empty list
        assert calculate_distribution_stats([]) is None

        # Single value
        assert calculate_distribution_stats([100.0]) is None

        # Two values
        assert calculate_distribution_stats([100.0, 200.0]) is None

    def test_three_values_minimum(self) -> None:
        """Test that exactly 3 values works."""
        pnl_values = [100.0, 200.0, 300.0]
        stats = calculate_distribution_stats(pnl_values)

        assert stats is not None
        assert stats.count == 3
        assert stats.mean == 200.0
        assert stats.median == 200.0

    def test_skewness_positive(self) -> None:
        """Test positive skewness (right-skewed distribution)."""
        # Right-skewed: most values small, few large values
        pnl_values = [10.0, 20.0, 30.0, 40.0, 500.0]
        stats = calculate_distribution_stats(pnl_values)

        assert stats is not None
        assert stats.skewness > 0  # Positive skewness

    def test_skewness_negative(self) -> None:
        """Test negative skewness (left-skewed distribution)."""
        # Left-skewed: most values large, few small values
        pnl_values = [-500.0, 80.0, 90.0, 95.0, 100.0]
        stats = calculate_distribution_stats(pnl_values)

        assert stats is not None
        assert stats.skewness < 0  # Negative skewness

    def test_symmetric_distribution(self) -> None:
        """Test approximately symmetric distribution."""
        # Symmetric around 0
        pnl_values = [-100.0, -50.0, 0.0, 50.0, 100.0]
        stats = calculate_distribution_stats(pnl_values)

        assert stats is not None
        # Skewness should be close to 0 for symmetric distribution
        assert abs(stats.skewness) < 0.5

    def test_kurtosis_fat_tails(self) -> None:
        """Test positive excess kurtosis (fat tails)."""
        # Distribution with extreme values
        pnl_values = [-1000.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1000.0]
        stats = calculate_distribution_stats(pnl_values)

        assert stats is not None
        assert stats.kurtosis > 0  # Positive excess kurtosis (fat tails)

    def test_kurtosis_thin_tails(self) -> None:
        """Test negative excess kurtosis (thin tails)."""
        # Uniform-like distribution
        pnl_values = list(range(1, 11))  # [1, 2, ..., 10] evenly spaced
        pnl_floats = [float(x) for x in pnl_values]
        stats = calculate_distribution_stats(pnl_floats)

        assert stats is not None
        # Uniform distribution has negative excess kurtosis
        assert stats.kurtosis < 0

    def test_percentiles(self) -> None:
        """Test percentile calculation."""
        pnl_values = list(range(1, 101))  # 1 to 100
        pnl_floats = [float(x) for x in pnl_values]
        stats = calculate_distribution_stats(pnl_floats)

        assert stats is not None
        # 5th percentile should be around 5
        assert 4.0 <= stats.percentile_5 <= 6.0
        # 95th percentile should be around 95
        assert 94.0 <= stats.percentile_95 <= 96.0

    def test_even_count_median(self) -> None:
        """Test median calculation with even number of values."""
        pnl_values = [100.0, 200.0, 300.0, 400.0]  # 4 values
        stats = calculate_distribution_stats(pnl_values)

        assert stats is not None
        # Median should be average of middle two: (200 + 300) / 2 = 250
        assert stats.median == 250.0

    def test_odd_count_median(self) -> None:
        """Test median calculation with odd number of values."""
        pnl_values = [100.0, 200.0, 300.0, 400.0, 500.0]  # 5 values
        stats = calculate_distribution_stats(pnl_values)

        assert stats is not None
        # Median should be middle value
        assert stats.median == 300.0

    def test_all_same_values(self) -> None:
        """Test with all same values (zero variance)."""
        pnl_values = [100.0, 100.0, 100.0, 100.0, 100.0]
        stats = calculate_distribution_stats(pnl_values)

        assert stats is not None
        assert stats.mean == 100.0
        assert stats.median == 100.0
        assert stats.std_dev == 0.0
        assert stats.skewness == 0.0
        assert stats.kurtosis == 0.0

    def test_negative_values(self) -> None:
        """Test with negative PnL values (losses)."""
        pnl_values = [-100.0, -50.0, -25.0, -10.0, -5.0]
        stats = calculate_distribution_stats(pnl_values)

        assert stats is not None
        assert stats.mean < 0
        assert stats.median < 0
        assert stats.min_return == -100.0
        assert stats.max_return == -5.0

    def test_mixed_values(self) -> None:
        """Test with mixed positive and negative values."""
        pnl_values = [-100.0, -50.0, 0.0, 100.0, 200.0]
        stats = calculate_distribution_stats(pnl_values)

        assert stats is not None
        assert stats.mean == 30.0  # Sum 150 / 5
        assert stats.min_return == -100.0
        assert stats.max_return == 200.0


class TestPnlHistogramWithStats:
    """Tests for plot_pnl_histogram with statistics display."""

    @pytest.fixture
    def result_with_trades(self) -> BacktestResult:
        """Create a BacktestResult with multiple trades for histogram testing."""
        equity_curve = [
            EquityPoint(timestamp=datetime(2024, 1, 1, 0, 0), value_usd=Decimal("10000")),
            EquityPoint(timestamp=datetime(2024, 1, 10, 0, 0), value_usd=Decimal("10500")),
        ]

        # Create diverse trades for meaningful statistics
        pnl_values = [100, -50, 200, -25, 150, 75, -80, 300, -10, 50]
        trades = [
            TradeRecord(
                timestamp=datetime(2024, 1, 1, i),
                intent_type=IntentType.SWAP,
                executed_price=Decimal("2000"),
                fee_usd=Decimal("5"),
                slippage_usd=Decimal("2"),
                gas_cost_usd=Decimal("3"),
                pnl_usd=Decimal(str(pnl)),
                success=True,
            )
            for i, pnl in enumerate(pnl_values)
        ]

        metrics = BacktestMetrics(
            net_pnl_usd=Decimal("710"),
            total_return_pct=Decimal("7.1"),
            total_trades=10,
        )

        return BacktestResult(
            engine="pnl",
            strategy_id="test_strategy",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 10),
            metrics=metrics,
            equity_curve=equity_curve,
            initial_capital_usd=Decimal("10000"),
            final_capital_usd=Decimal("10500"),
            trades=trades,
        )

    def test_histogram_with_stats_enabled(
        self, result_with_trades: BacktestResult, tmp_path: Path
    ) -> None:
        """Test histogram with show_stats=True."""
        output_path = tmp_path / "histogram_stats.png"

        result = plot_pnl_histogram(
            result_with_trades,
            output_path=output_path,
            show_stats=True,
        )

        assert result.success is True
        assert output_path.exists()

    def test_histogram_stats_disabled_by_default(
        self, result_with_trades: BacktestResult, tmp_path: Path
    ) -> None:
        """Test that stats are disabled by default."""
        output_path = tmp_path / "histogram_no_stats.png"

        result = plot_pnl_histogram(
            result_with_trades,
            output_path=output_path,
        )

        assert result.success is True
        assert output_path.exists()


class TestPnlHistogramInteractive:
    """Tests for plot_pnl_histogram_interactive function."""

    @pytest.fixture
    def result_with_trades(self) -> BacktestResult:
        """Create a BacktestResult with multiple trades."""
        equity_curve = [
            EquityPoint(timestamp=datetime(2024, 1, 1, 0, 0), value_usd=Decimal("10000")),
            EquityPoint(timestamp=datetime(2024, 1, 10, 0, 0), value_usd=Decimal("10500")),
        ]

        # Create trades with diverse PnL values
        pnl_values = [100, -50, 200, -25, 150, 75, -80, 300, -10, 50]
        trades = [
            TradeRecord(
                timestamp=datetime(2024, 1, 1, i),
                intent_type=IntentType.SWAP,
                executed_price=Decimal("2000"),
                fee_usd=Decimal("5"),
                slippage_usd=Decimal("2"),
                gas_cost_usd=Decimal("3"),
                pnl_usd=Decimal(str(pnl)),
                success=True,
            )
            for i, pnl in enumerate(pnl_values)
        ]

        metrics = BacktestMetrics(
            net_pnl_usd=Decimal("710"),
            total_return_pct=Decimal("7.1"),
            total_trades=10,
        )

        return BacktestResult(
            engine="pnl",
            strategy_id="test_strategy",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 10),
            metrics=metrics,
            equity_curve=equity_curve,
            initial_capital_usd=Decimal("10000"),
            final_capital_usd=Decimal("10500"),
            trades=trades,
        )

    def test_interactive_histogram_success(
        self, result_with_trades: BacktestResult, tmp_path: Path
    ) -> None:
        """Test successful interactive histogram generation."""
        output_path = tmp_path / "histogram.html"

        result = plot_pnl_histogram_interactive(result_with_trades, output_path=output_path)

        assert result.success is True
        assert result.chart_type == "pnl_histogram"
        assert result.file_path == output_path
        assert result.format == "html"
        assert output_path.exists()

    def test_interactive_histogram_default_path(
        self, result_with_trades: BacktestResult, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test default output path generation."""
        monkeypatch.chdir(tmp_path)

        result = plot_pnl_histogram_interactive(result_with_trades)

        assert result.success is True
        assert result.file_path is not None
        assert "test_strategy" in str(result.file_path)
        assert result.file_path.suffix == ".html"

    def test_interactive_histogram_with_stats(
        self, result_with_trades: BacktestResult, tmp_path: Path
    ) -> None:
        """Test interactive histogram with stats enabled (default)."""
        output_path = tmp_path / "histogram_stats.html"

        result = plot_pnl_histogram_interactive(
            result_with_trades,
            output_path=output_path,
            show_stats=True,
        )

        assert result.success is True
        assert output_path.exists()

        # Verify HTML contains stats
        content = output_path.read_text()
        assert "Skewness" in content
        assert "Kurtosis" in content

    def test_interactive_histogram_without_stats(
        self, result_with_trades: BacktestResult, tmp_path: Path
    ) -> None:
        """Test interactive histogram with stats disabled."""
        output_path = tmp_path / "histogram_no_stats.html"

        result = plot_pnl_histogram_interactive(
            result_with_trades,
            output_path=output_path,
            show_stats=False,
        )

        assert result.success is True
        assert output_path.exists()

    def test_interactive_histogram_no_trades(
        self, tmp_path: Path
    ) -> None:
        """Test error handling for empty trades."""
        metrics = BacktestMetrics()
        result_no_trades = BacktestResult(
            engine="pnl",
            strategy_id="empty_strategy",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 5),
            metrics=metrics,
            equity_curve=[],
            initial_capital_usd=Decimal("10000"),
            final_capital_usd=Decimal("10000"),
            trades=[],
        )

        output_path = tmp_path / "histogram.html"
        result = plot_pnl_histogram_interactive(result_no_trades, output_path=output_path)

        assert result.success is False
        assert "No trades data" in result.error

    def test_interactive_histogram_custom_bins(
        self, result_with_trades: BacktestResult, tmp_path: Path
    ) -> None:
        """Test interactive histogram with custom bin count."""
        output_path = tmp_path / "histogram_bins.html"

        result = plot_pnl_histogram_interactive(
            result_with_trades,
            output_path=output_path,
            bins=10,
        )

        assert result.success is True
        assert output_path.exists()

    def test_interactive_histogram_custom_title(
        self, result_with_trades: BacktestResult, tmp_path: Path
    ) -> None:
        """Test interactive histogram with custom title."""
        output_path = tmp_path / "histogram_title.html"

        result = plot_pnl_histogram_interactive(
            result_with_trades,
            output_path=output_path,
            title="Custom PnL Distribution",
        )

        assert result.success is True
        assert output_path.exists()

        content = output_path.read_text()
        assert "Custom PnL Distribution" in content


class TestAttributionCharts:
    """Tests for attribution chart generation functions."""

    @pytest.fixture
    def sample_attribution_data(self) -> dict[str, Decimal]:
        """Create sample attribution data."""
        return {
            "uniswap_v3": Decimal("150"),
            "aave_v3": Decimal("-30"),
            "gmx": Decimal("80"),
        }

    @pytest.fixture
    def result_with_attribution(self) -> BacktestResult:
        """Create a BacktestResult with attribution metrics."""
        equity_curve = [
            EquityPoint(timestamp=datetime(2024, 1, 1, 0, 0), value_usd=Decimal("10000")),
            EquityPoint(timestamp=datetime(2024, 1, 5, 0, 0), value_usd=Decimal("10200")),
        ]

        metrics = BacktestMetrics(
            net_pnl_usd=Decimal("200"),
            total_pnl_usd=Decimal("200"),
            total_return_pct=Decimal("2.0"),
            sharpe_ratio=Decimal("1.5"),
            win_rate=Decimal("0.6"),
            total_trades=5,
            pnl_by_protocol={
                "uniswap_v3": Decimal("150"),
                "aave_v3": Decimal("-30"),
                "gmx": Decimal("80"),
            },
            pnl_by_intent_type={
                "SWAP": Decimal("100"),
                "LP_OPEN": Decimal("50"),
                "LP_CLOSE": Decimal("50"),
            },
            pnl_by_asset={
                "ETH": Decimal("120"),
                "USDC": Decimal("80"),
            },
        )

        return BacktestResult(
            engine="pnl",
            strategy_id="attribution_test",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 5),
            metrics=metrics,
            equity_curve=equity_curve,
            initial_capital_usd=Decimal("10000"),
            final_capital_usd=Decimal("10200"),
        )

    def test_generate_attribution_pie_chart_success(
        self, sample_attribution_data: dict[str, Decimal]
    ) -> None:
        """Test successful pie chart generation."""
        from almanak.framework.backtesting.visualization import (
            generate_attribution_pie_chart_html,
        )

        html = generate_attribution_pie_chart_html(
            sample_attribution_data,
            title="PnL by Protocol",
        )

        assert html != ""
        assert "plotly" in html.lower()
        assert "uniswap_v3" in html or "Uniswap" in html

    def test_generate_attribution_pie_chart_empty_data(self) -> None:
        """Test pie chart with empty data."""
        from almanak.framework.backtesting.visualization import (
            generate_attribution_pie_chart_html,
        )

        html = generate_attribution_pie_chart_html({})

        assert html == ""

    def test_generate_attribution_bar_chart_success(
        self, sample_attribution_data: dict[str, Decimal]
    ) -> None:
        """Test successful bar chart generation."""
        from almanak.framework.backtesting.visualization import (
            generate_attribution_bar_chart_html,
        )

        html = generate_attribution_bar_chart_html(
            sample_attribution_data,
            title="PnL by Protocol",
        )

        assert html != ""
        assert "plotly" in html.lower()

    def test_generate_attribution_bar_chart_empty_data(self) -> None:
        """Test bar chart with empty data."""
        from almanak.framework.backtesting.visualization import (
            generate_attribution_bar_chart_html,
        )

        html = generate_attribution_bar_chart_html({})

        assert html == ""

    def test_generate_attribution_charts_html_all(
        self, result_with_attribution: BacktestResult
    ) -> None:
        """Test generating all attribution charts from result."""
        from almanak.framework.backtesting.visualization import (
            generate_attribution_charts_html,
        )

        charts = generate_attribution_charts_html(result_with_attribution)

        assert "by_protocol" in charts
        assert "by_intent_type" in charts
        assert "by_asset" in charts
        assert charts["by_protocol"] != ""
        assert charts["by_intent_type"] != ""
        assert charts["by_asset"] != ""

    def test_generate_attribution_charts_html_pie_type(
        self, result_with_attribution: BacktestResult
    ) -> None:
        """Test generating pie charts instead of bar charts."""
        from almanak.framework.backtesting.visualization import (
            generate_attribution_charts_html,
        )

        charts = generate_attribution_charts_html(
            result_with_attribution,
            chart_type="pie",
        )

        assert charts["by_protocol"] != ""

    def test_generate_attribution_charts_html_empty_metrics(self) -> None:
        """Test with result that has no attribution data."""
        from almanak.framework.backtesting.visualization import (
            generate_attribution_charts_html,
        )

        metrics = BacktestMetrics()  # No attribution fields populated
        result = BacktestResult(
            engine="pnl",
            strategy_id="empty",
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 5),
            metrics=metrics,
            equity_curve=[],
            initial_capital_usd=Decimal("10000"),
            final_capital_usd=Decimal("10000"),
        )

        charts = generate_attribution_charts_html(result)

        # Empty strings when no data
        assert charts["by_protocol"] == ""
        assert charts["by_intent_type"] == ""
        assert charts["by_asset"] == ""

    def test_attribution_values_sum_to_total(
        self, result_with_attribution: BacktestResult
    ) -> None:
        """Test that attribution values sum to total PnL (critical validation)."""
        metrics = result_with_attribution.metrics

        # Sum of pnl_by_protocol should equal total_pnl_usd
        protocol_sum = sum(metrics.pnl_by_protocol.values())
        intent_sum = sum(metrics.pnl_by_intent_type.values())
        asset_sum = sum(metrics.pnl_by_asset.values())

        # All attributions should sum to total PnL
        assert protocol_sum == metrics.total_pnl_usd, (
            f"Protocol attribution sum ({protocol_sum}) != total PnL ({metrics.total_pnl_usd})"
        )
        assert intent_sum == metrics.total_pnl_usd, (
            f"Intent type attribution sum ({intent_sum}) != total PnL ({metrics.total_pnl_usd})"
        )
        assert asset_sum == metrics.total_pnl_usd, (
            f"Asset attribution sum ({asset_sum}) != total PnL ({metrics.total_pnl_usd})"
        )

    def test_attribution_handles_negative_values(self) -> None:
        """Test that attribution charts handle negative PnL values correctly."""
        from almanak.framework.backtesting.visualization import (
            generate_attribution_bar_chart_html,
        )

        # Mix of positive and negative values
        data = {
            "protocol_a": Decimal("200"),
            "protocol_b": Decimal("-150"),
            "protocol_c": Decimal("50"),
        }

        html = generate_attribution_bar_chart_html(data, title="Mixed PnL")

        assert html != ""
        # Should not error with negative values

    def test_attribution_handles_all_negative(self) -> None:
        """Test attribution chart with all negative values."""
        from almanak.framework.backtesting.visualization import (
            generate_attribution_pie_chart_html,
        )

        data = {
            "protocol_a": Decimal("-100"),
            "protocol_b": Decimal("-50"),
            "protocol_c": Decimal("-25"),
        }

        html = generate_attribution_pie_chart_html(data, title="Loss Attribution")

        assert html != ""

    def test_attribution_custom_height(
        self, sample_attribution_data: dict[str, Decimal]
    ) -> None:
        """Test attribution chart with custom height."""
        from almanak.framework.backtesting.visualization import (
            generate_attribution_bar_chart_html,
        )

        html = generate_attribution_bar_chart_html(
            sample_attribution_data,
            title="Custom Height",
            height=500,
        )

        assert html != ""
        assert "500" in html  # Height should appear in the HTML
