"""Tests for Plot Generator Module.

This test suite covers:
- PlotConfig and PlotResult dataclass creation
- PlotGenerator initialization and directory creation
- Price, RSI, and WETH figure contracts with optional matplotlib
- Summary grid dispatch, layout, and overflow behavior
- Error handling when matplotlib is not available
- Error handling when no data is provided
"""

import builtins
import tempfile
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from almanak.framework.data.interfaces import OHLCVCandle
from almanak.framework.data.qa.reporting.plots import PlotConfig, PlotGenerator, PlotResult
from almanak.framework.data.qa.test_definitions.rsi import RSIDataPoint

# =============================================================================
# Helper Functions
# =============================================================================


def create_candle(
    timestamp: datetime,
    close_price: float = 100.0,
) -> OHLCVCandle:
    """Create an OHLCVCandle for testing."""
    return OHLCVCandle(
        timestamp=timestamp,
        open=Decimal(str(close_price - 5.0)),
        high=Decimal(str(close_price + 5.0)),
        low=Decimal(str(close_price - 10.0)),
        close=Decimal(str(close_price)),
        volume=None,
    )


def create_candles(
    start: datetime,
    count: int,
    start_price: float = 100.0,
    price_change: float = 1.0,
    interval_hours: int = 4,
) -> list[OHLCVCandle]:
    """Create a series of OHLCVCandle objects for testing."""
    candles = []
    current = start
    price = start_price
    for _ in range(count):
        candles.append(create_candle(timestamp=current, close_price=price))
        current = current + timedelta(hours=interval_hours)
        price += price_change
    return candles


def create_rsi_history(count: int = 20, base_rsi: float = 50.0) -> list[RSIDataPoint]:
    """Create a list of RSI data points for testing."""
    history = []
    for i in range(count):
        # Oscillate around the base RSI
        rsi = base_rsi + (10 * ((-1) ** i))
        rsi = max(0, min(100, rsi))  # Clamp to valid range
        history.append(RSIDataPoint(index=i, rsi=rsi))
    return history


class MockWETHPricePoint:
    """Mock WETH price point for testing."""

    def __init__(self, timestamp: datetime, price_weth: float):
        self.timestamp = timestamp
        self.price_weth = price_weth


def create_weth_prices(
    start: datetime,
    count: int,
    start_price: float = 0.05,
    price_change: float = 0.001,
    interval_hours: int = 4,
) -> list[MockWETHPricePoint]:
    """Create a list of WETH price points for testing."""
    prices = []
    current = start
    price = start_price
    for _ in range(count):
        prices.append(MockWETHPricePoint(timestamp=current, price_weth=price))
        current = current + timedelta(hours=interval_hours)
        price += price_change
    return prices


@pytest.fixture
def captured_pyplot(monkeypatch: pytest.MonkeyPatch) -> Iterator[tuple[Any, list[Any]]]:
    """Capture figures passed to pyplot.close so their rendered state can be asserted."""
    pyplot = pytest.importorskip("matplotlib.pyplot")
    figures: list[Any] = []
    close = pyplot.close
    monkeypatch.setattr(pyplot, "close", figures.append)

    yield pyplot, figures

    for figure in figures:
        close(figure)


def assert_common_axis_style(ax: Any, config: PlotConfig) -> None:
    """Assert styling shared by standalone plots."""
    assert all(label.get_fontsize() == config.font_size for label in ax.get_xticklabels())
    assert any(line.get_visible() and line.get_alpha() == 0.3 for line in ax.get_xgridlines())


# =============================================================================
# PlotConfig Tests
# =============================================================================


class TestPlotConfig:
    """Tests for PlotConfig dataclass."""

    def test_default_config(self) -> None:
        """Test default PlotConfig values."""
        config = PlotConfig()
        assert config.dark_theme is False
        assert config.figure_width == 10.0
        assert config.figure_height == 6.0
        assert config.dpi == 150
        assert config.font_size == 10
        assert config.title_size == 12
        assert config.line_width == 1.5
        assert config.rsi_oversold == 30.0
        assert config.rsi_overbought == 70.0

    def test_custom_config(self) -> None:
        """Test custom PlotConfig values."""
        config = PlotConfig(
            dark_theme=True,
            figure_width=12.0,
            figure_height=8.0,
            dpi=200,
            font_size=12,
            title_size=14,
            line_width=2.0,
            rsi_oversold=25.0,
            rsi_overbought=75.0,
        )
        assert config.dark_theme is True
        assert config.figure_width == 12.0
        assert config.figure_height == 8.0
        assert config.dpi == 200
        assert config.font_size == 12
        assert config.title_size == 14
        assert config.line_width == 2.0
        assert config.rsi_oversold == 25.0
        assert config.rsi_overbought == 75.0


# =============================================================================
# PlotResult Tests
# =============================================================================


class TestPlotResult:
    """Tests for PlotResult dataclass."""

    def test_successful_result(self) -> None:
        """Test creating a successful PlotResult."""
        result = PlotResult(
            token="ETH",
            plot_type="price",
            file_path=Path("/tmp/eth_price.png"),
            success=True,
        )
        assert result.token == "ETH"
        assert result.plot_type == "price"
        assert result.file_path == Path("/tmp/eth_price.png")
        assert result.success is True
        assert result.error is None

    def test_failed_result(self) -> None:
        """Test creating a failed PlotResult."""
        result = PlotResult(
            token="ETH",
            plot_type="price",
            file_path=None,
            success=False,
            error="matplotlib not installed",
        )
        assert result.token == "ETH"
        assert result.plot_type == "price"
        assert result.file_path is None
        assert result.success is False
        assert result.error == "matplotlib not installed"


# =============================================================================
# PlotGenerator Initialization Tests
# =============================================================================


class TestPlotGeneratorInit:
    """Tests for PlotGenerator initialization."""

    def test_init_creates_directory(self) -> None:
        """Test that PlotGenerator creates output directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "plots"
            assert not output_dir.exists()

            generator = PlotGenerator(output_dir=output_dir)

            assert output_dir.exists()
            assert generator.output_dir == output_dir
            assert isinstance(generator.config, PlotConfig)

    def test_init_with_custom_config(self) -> None:
        """Test PlotGenerator with custom config."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "plots"
            custom_config = PlotConfig(dpi=300, dark_theme=True)

            generator = PlotGenerator(output_dir=output_dir, config=custom_config)

            assert generator.config.dpi == 300
            assert generator.config.dark_theme is True


# =============================================================================
# Price Plot Tests
# =============================================================================


class TestCreatePricePlot:
    """Tests for create_price_plot method."""

    def test_price_plot_no_data(self, tmp_path: Path) -> None:
        """Test price plot with no candle data."""
        pytest.importorskip("matplotlib.pyplot")
        generator = PlotGenerator(output_dir=tmp_path)

        result = generator.create_price_plot(token="ETH", candles=[], quote="USD")

        assert result == PlotResult(
            token="ETH",
            plot_type="price",
            file_path=None,
            success=False,
            error="No candle data provided",
        )

    def test_price_plot_preserves_figure_contract(
        self,
        tmp_path: Path,
        captured_pyplot: tuple[Any, list[Any]],
    ) -> None:
        """Test the complete standalone price figure contract."""
        _, figures = captured_pyplot
        config = PlotConfig(figure_width=8, figure_height=4, dpi=100, font_size=11, title_size=13, line_width=2)
        generator = PlotGenerator(output_dir=tmp_path, config=config)
        candles = create_candles(datetime(2025, 1, 1, tzinfo=UTC), 3)
        original = list(candles)

        result = generator.create_price_plot("ETH", candles, "USD")

        assert result == PlotResult(
            token="ETH",
            plot_type="price",
            file_path=tmp_path / "eth_price_usd.png",
            success=True,
        )
        assert result.file_path is not None and result.file_path.is_file()
        assert candles == original
        assert len(figures) == 1
        figure = figures[0]
        assert tuple(figure.get_size_inches()) == pytest.approx((8, 4))
        assert len(figure.axes) == 1
        ax = figure.axes[0]
        assert ax.get_title() == "ETH Price (USD)"
        assert ax.title.get_fontsize() == 13
        assert ax.title.get_fontweight() == "bold"
        assert ax.get_xlabel() == "Time"
        assert ax.get_ylabel() == "Price (USD)"
        assert len(ax.lines) == 1
        line = ax.lines[0]
        assert list(line.get_xdata()) == [candle.timestamp for candle in candles]
        assert list(line.get_ydata()) == [100.0, 101.0, 102.0]
        assert line.get_label() == "ETH/USD"
        assert line.get_linewidth() == 2
        assert line.get_color() == "#2196F3"
        assert len(ax.collections) == 1
        assert ax.collections[0].get_alpha() == 0.1
        assert ax.get_legend_handles_labels()[1] == ["ETH/USD"]
        assert all(label.get_rotation() == 45 for label in ax.get_xticklabels())
        assert all(label.get_horizontalalignment() == "right" for label in ax.get_xticklabels())
        assert_common_axis_style(ax, config)

    def test_price_plot_custom_title(self, tmp_path: Path) -> None:
        """Test custom price titles replace only the default title."""
        pyplot = pytest.importorskip("matplotlib.pyplot")
        generator = PlotGenerator(output_dir=tmp_path)

        result = generator.create_price_plot(
            "ETH",
            create_candles(datetime(2025, 1, 1, tzinfo=UTC), 1),
            "EUR",
            title="Custom price",
        )

        assert result.success is True
        assert result.file_path == tmp_path / "eth_price_eur.png"
        pyplot.close("all")

    def test_price_plot_malformed_data_error_is_preserved(self, tmp_path: Path) -> None:
        """Test malformed candles remain a failed result with the source error."""
        pytest.importorskip("matplotlib.pyplot")
        generator = PlotGenerator(output_dir=tmp_path)

        result = generator.create_price_plot("ETH", [object()], "USD")

        assert result == PlotResult(
            token="ETH",
            plot_type="price",
            file_path=None,
            success=False,
            error="'object' object has no attribute 'timestamp'",
        )


# =============================================================================
# RSI Plot Tests
# =============================================================================


class TestCreateRSIPlot:
    """Tests for create_rsi_plot method."""

    def test_rsi_plot_no_data(self, tmp_path: Path) -> None:
        """Test RSI plot with no history data."""
        pytest.importorskip("matplotlib.pyplot")
        generator = PlotGenerator(output_dir=tmp_path)

        result = generator.create_rsi_plot(token="ETH", rsi_history=[])

        assert result == PlotResult(
            token="ETH",
            plot_type="rsi",
            file_path=None,
            success=False,
            error="No RSI history data provided",
        )

    def test_rsi_plot_preserves_figure_contract(
        self,
        tmp_path: Path,
        captured_pyplot: tuple[Any, list[Any]],
    ) -> None:
        """Test the complete standalone RSI figure contract."""
        _, figures = captured_pyplot
        config = PlotConfig(font_size=11, title_size=13, line_width=2, rsi_oversold=25, rsi_overbought=75)
        generator = PlotGenerator(output_dir=tmp_path, config=config)
        history = create_rsi_history(3)
        original = list(history)

        result = generator.create_rsi_plot("ETH", history)

        assert result == PlotResult(
            token="ETH",
            plot_type="rsi",
            file_path=tmp_path / "eth_rsi.png",
            success=True,
        )
        assert result.file_path is not None and result.file_path.is_file()
        assert history == original
        assert len(figures) == 1
        ax = figures[0].axes[0]
        assert ax.get_title() == "ETH RSI Indicator"
        assert ax.get_xlabel() == "Period"
        assert ax.get_ylabel() == "RSI"
        assert ax.get_ylim() == (0.0, 100.0)
        assert len(ax.lines) == 4
        rsi_line, oversold, overbought, midpoint = ax.lines
        assert list(rsi_line.get_xdata()) == [0, 1, 2]
        assert list(rsi_line.get_ydata()) == [60.0, 40.0, 60.0]
        assert rsi_line.get_label() == "RSI"
        assert rsi_line.get_color() == "#9C27B0"
        assert list(oversold.get_ydata()) == [25, 25]
        assert oversold.get_label() == "Oversold (25)"
        assert oversold.get_color() == "#4CAF50"
        assert oversold.get_linestyle() == "--"
        assert list(overbought.get_ydata()) == [75, 75]
        assert overbought.get_label() == "Overbought (75)"
        assert overbought.get_color() == "#F44336"
        assert list(midpoint.get_ydata()) == [50, 50]
        assert midpoint.get_color() == "#9E9E9E"
        assert midpoint.get_linestyle() == ":"
        assert midpoint.get_alpha() == 0.5
        assert len(ax.collections) == 3
        assert [collection.get_alpha() for collection in ax.collections] == [0.1, 0.1, 0.1]
        assert ax.get_legend_handles_labels()[1] == [
            "RSI",
            "Oversold (25)",
            "Overbought (75)",
            "Neutral Zone",
        ]
        assert_common_axis_style(ax, config)

    def test_rsi_plot_custom_title(self, tmp_path: Path) -> None:
        """Test custom RSI titles replace only the default title."""
        pyplot = pytest.importorskip("matplotlib.pyplot")
        generator = PlotGenerator(output_dir=tmp_path)

        result = generator.create_rsi_plot("ETH", create_rsi_history(1), title="Custom RSI")

        assert result.success is True
        pyplot.close("all")

    def test_rsi_plot_malformed_data_error_is_preserved(self, tmp_path: Path) -> None:
        """Test malformed RSI points remain a failed result with the source error."""
        pytest.importorskip("matplotlib.pyplot")
        generator = PlotGenerator(output_dir=tmp_path)

        result = generator.create_rsi_plot("ETH", [object()])

        assert result == PlotResult(
            token="ETH",
            plot_type="rsi",
            file_path=None,
            success=False,
            error="'object' object has no attribute 'index'",
        )


# =============================================================================
# WETH Price Plot Tests
# =============================================================================


class TestCreateWETHPricePlot:
    """Tests for create_weth_price_plot method."""

    def test_weth_price_plot_no_data(self, tmp_path: Path) -> None:
        """Test WETH price plot with no data."""
        pytest.importorskip("matplotlib.pyplot")
        generator = PlotGenerator(output_dir=tmp_path)

        result = generator.create_weth_price_plot(token="LINK", weth_prices=[])

        assert result == PlotResult(
            token="LINK",
            plot_type="weth_price",
            file_path=None,
            success=False,
            error="No WETH price data provided",
        )

    def test_weth_price_plot_preserves_figure_contract(
        self,
        tmp_path: Path,
        captured_pyplot: tuple[Any, list[Any]],
    ) -> None:
        """Test the complete standalone WETH price figure contract."""
        _, figures = captured_pyplot
        config = PlotConfig(font_size=11, title_size=13, line_width=2)
        generator = PlotGenerator(output_dir=tmp_path, config=config)
        prices = create_weth_prices(datetime(2025, 1, 1, tzinfo=UTC), 3)
        original = list(prices)

        result = generator.create_weth_price_plot("LINK", prices)

        assert result == PlotResult(
            token="LINK",
            plot_type="weth_price",
            file_path=tmp_path / "link_price_weth.png",
            success=True,
        )
        assert result.file_path is not None and result.file_path.is_file()
        assert prices == original
        assert len(figures) == 1
        ax = figures[0].axes[0]
        assert ax.get_title() == "LINK Price (WETH)"
        assert ax.get_xlabel() == "Time"
        assert ax.get_ylabel() == "Price (WETH)"
        assert len(ax.lines) == 1
        line = ax.lines[0]
        assert list(line.get_xdata()) == [price.timestamp for price in prices]
        assert list(line.get_ydata()) == pytest.approx([0.05, 0.051, 0.052])
        assert line.get_label() == "LINK/WETH"
        assert line.get_linewidth() == 2
        assert line.get_color() == "#FF9800"
        assert len(ax.collections) == 1
        assert ax.collections[0].get_alpha() == 0.1
        assert ax.get_legend_handles_labels()[1] == ["LINK/WETH"]
        assert all(label.get_rotation() == 45 for label in ax.get_xticklabels())
        assert all(label.get_horizontalalignment() == "right" for label in ax.get_xticklabels())
        assert_common_axis_style(ax, config)

    def test_weth_price_plot_custom_title(self, tmp_path: Path) -> None:
        """Test custom WETH price titles replace only the default title."""
        pyplot = pytest.importorskip("matplotlib.pyplot")
        generator = PlotGenerator(output_dir=tmp_path)

        result = generator.create_weth_price_plot(
            "LINK",
            create_weth_prices(datetime(2025, 1, 1, tzinfo=UTC), 1),
            title="Custom WETH price",
        )

        assert result.success is True
        pyplot.close("all")

    def test_weth_price_plot_malformed_data_error_is_preserved(self, tmp_path: Path) -> None:
        """Test malformed WETH points remain a failed result with the source error."""
        pytest.importorskip("matplotlib.pyplot")
        generator = PlotGenerator(output_dir=tmp_path)

        result = generator.create_weth_price_plot("LINK", [object()])

        assert result == PlotResult(
            token="LINK",
            plot_type="weth_price",
            file_path=None,
            success=False,
            error="'object' object has no attribute 'timestamp'",
        )


# =============================================================================
# Summary Grid Tests
# =============================================================================


class TestCreateSummaryGrid:
    """Tests for create_summary_grid method."""

    def test_summary_grid_no_plots(self, tmp_path: Path) -> None:
        """Test summary grid with no plots provided."""
        pytest.importorskip("matplotlib.pyplot")
        generator = PlotGenerator(output_dir=tmp_path)

        result = generator.create_summary_grid(plots=[])

        assert result == PlotResult(
            token="summary",
            plot_type="grid",
            file_path=None,
            success=False,
            error="No plots provided",
        )

    def test_summary_grid_preserves_every_subplot_contract(
        self,
        tmp_path: Path,
        captured_pyplot: tuple[Any, list[Any]],
    ) -> None:
        """Test price, WETH, RSI, empty, unknown, and unused grid cells."""
        _, figures = captured_pyplot
        generator = PlotGenerator(output_dir=tmp_path)
        start = datetime(2025, 1, 1, tzinfo=UTC)
        plots: list[tuple[str, list, str]] = [
            ("ETH", create_candles(start, 3), "price"),
            ("LINK", create_weth_prices(start, 3), "weth_price"),
            ("AAVE", create_rsi_history(3), "rsi"),
            ("EMPTY", [], "price"),
            ("OTHER", [object()], "unknown"),
        ]
        original = [(token, list(data), plot_type) for token, data, plot_type in plots]

        result = generator.create_summary_grid(plots, rows=2, cols=3)

        assert result == PlotResult(
            token="summary",
            plot_type="grid",
            file_path=tmp_path / "summary_grid.png",
            success=True,
        )
        assert result.file_path is not None and result.file_path.is_file()
        assert plots == original
        assert len(figures) == 1
        figure = figures[0]
        assert tuple(figure.get_size_inches()) == pytest.approx((18, 6))
        assert figure._suptitle.get_text() == "QA Data Summary"
        assert figure._suptitle.get_fontsize() == 14
        assert figure._suptitle.get_fontweight() == "bold"
        assert len(figure.axes) == 6

        price_ax, weth_ax, rsi_ax, empty_ax, unknown_ax, unused_ax = figure.axes
        assert price_ax.get_title() == "ETH/USD"
        assert len(price_ax.lines) == 1
        assert price_ax.lines[0].get_color() == "#2196F3"
        assert price_ax.lines[0].get_linewidth() == pytest.approx(1.05)
        assert len(price_ax.collections) == 1
        assert price_ax.xaxis.get_major_locator().__class__.__name__ == "MaxNLocator"

        assert weth_ax.get_title() == "LINK/WETH"
        assert len(weth_ax.lines) == 1
        assert weth_ax.lines[0].get_color() == "#FF9800"
        assert len(weth_ax.collections) == 1
        assert weth_ax.xaxis.get_major_locator().__class__.__name__ == "MaxNLocator"

        assert rsi_ax.get_title() == "AAVE RSI"
        assert rsi_ax.get_ylim() == (0.0, 100.0)
        assert len(rsi_ax.lines) == 3
        assert rsi_ax.lines[0].get_color() == "#9C27B0"
        assert rsi_ax.lines[1].get_color() == "#4CAF50"
        assert rsi_ax.lines[2].get_color() == "#F44336"
        assert len(rsi_ax.collections) == 1

        assert empty_ax.axison is False
        assert [text.get_text() for text in empty_ax.texts] == ["EMPTY\nNo data"]
        assert unknown_ax.axison is False
        assert [text.get_text() for text in unknown_ax.texts] == ["OTHER\nUnknown type: unknown"]
        assert unused_ax.get_visible() is False

    def test_summary_grid_truncates_over_capacity_and_uses_custom_title(
        self,
        tmp_path: Path,
        captured_pyplot: tuple[Any, list[Any]],
    ) -> None:
        """Pin the existing successful truncation behavior when the grid is full."""
        _, figures = captured_pyplot
        generator = PlotGenerator(output_dir=tmp_path)
        start = datetime(2025, 1, 1, tzinfo=UTC)
        plots = [
            ("FIRST", create_candles(start, 1), "price"),
            ("DROPPED", create_candles(start, 1), "price"),
        ]
        original = [(token, list(data), plot_type) for token, data, plot_type in plots]

        result = generator.create_summary_grid(plots, rows=1, cols=1, title="Custom summary")

        assert result.success is True
        assert plots == original
        assert len(figures) == 1
        figure = figures[0]
        assert figure._suptitle.get_text() == "Custom summary"
        assert len(figure.axes) == 1
        assert figure.axes[0].get_title() == "FIRST/USD"

    def test_summary_grid_malformed_data_error_is_preserved(self, tmp_path: Path) -> None:
        """Test malformed recognized plot data remains a failed result."""
        pyplot = pytest.importorskip("matplotlib.pyplot")
        generator = PlotGenerator(output_dir=tmp_path)

        result = generator.create_summary_grid([("ETH", [object()], "price")], rows=1, cols=1)

        assert result == PlotResult(
            token="summary",
            plot_type="grid",
            file_path=None,
            success=False,
            error="'object' object has no attribute 'timestamp'",
        )
        pyplot.close("all")

    def test_summary_grid_invalid_dimensions_error_is_preserved(self, tmp_path: Path) -> None:
        """Test invalid grid dimensions remain a failed result with Matplotlib's error."""
        pytest.importorskip("matplotlib.pyplot")
        generator = PlotGenerator(output_dir=tmp_path)

        result = generator.create_summary_grid([("ETH", [object()], "price")], rows=0, cols=1)

        assert result == PlotResult(
            token="summary",
            plot_type="grid",
            file_path=None,
            success=False,
            error="Number of rows must be a positive integer, not 0",
        )


# =============================================================================
# Integration Tests (with matplotlib if available)
# =============================================================================


class TestPlotGeneratorIntegration:
    """Integration tests that actually create plots (if matplotlib available)."""

    @pytest.fixture
    def generator(self):  # type: ignore[misc]
        """Create a PlotGenerator with temp directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield PlotGenerator(output_dir=Path(tmpdir))

    def test_plot_result_types(self) -> None:
        """Test that PlotResult has correct types."""
        result = PlotResult(
            token="ETH",
            plot_type="price",
            file_path=Path("/tmp/test.png"),
            success=True,
        )
        assert isinstance(result.token, str)
        assert isinstance(result.plot_type, str)
        assert isinstance(result.file_path, Path)
        assert isinstance(result.success, bool)

    def test_empty_data_handling(self) -> None:
        """Test that all plot methods handle empty data gracefully."""
        pytest.importorskip("matplotlib.pyplot")
        with tempfile.TemporaryDirectory() as tmpdir:
            generator = PlotGenerator(output_dir=Path(tmpdir))

            # All should fail gracefully with empty data
            price_result = generator.create_price_plot("ETH", [], "USD")
            rsi_result = generator.create_rsi_plot("ETH", [])
            weth_result = generator.create_weth_price_plot("LINK", [])
            grid_result = generator.create_summary_grid([])

            assert price_result.success is False
            assert rsi_result.success is False
            assert weth_result.success is False
            assert grid_result.success is False

    @pytest.mark.parametrize(
        ("method_name", "args", "expected_token", "expected_plot_type"),
        [
            ("create_price_plot", ("ETH", [object()], "USD"), "ETH", "price"),
            ("create_rsi_plot", ("ETH", [object()]), "ETH", "rsi"),
            ("create_weth_price_plot", ("LINK", [object()]), "LINK", "weth_price"),
            ("create_summary_grid", ([("ETH", [object()], "price")],), "summary", "grid"),
        ],
    )
    def test_matplotlib_import_error_contract(
        self,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
        method_name: str,
        args: tuple[Any, ...],
        expected_token: str,
        expected_plot_type: str,
    ) -> None:
        """Test every public plot method's real optional-dependency boundary."""
        generator = PlotGenerator(output_dir=tmp_path)
        real_import = builtins.__import__

        def import_without_pyplot(name: str, *import_args: Any, **import_kwargs: Any) -> Any:
            if name == "matplotlib.pyplot":
                raise ImportError("matplotlib intentionally unavailable")
            return real_import(name, *import_args, **import_kwargs)

        with caplog.at_level("ERROR"), patch("builtins.__import__", side_effect=import_without_pyplot):
            result = getattr(generator, method_name)(*args)

        assert result == PlotResult(
            token=expected_token,
            plot_type=expected_plot_type,
            file_path=None,
            success=False,
            error="matplotlib not installed",
        )
        assert caplog.records[-1].getMessage() == "matplotlib not installed. Run: pip install 'almanak[backtest]'"

    @pytest.mark.parametrize(
        ("method_name", "args", "expected_token", "expected_plot_type"),
        [
            ("create_price_plot", ("ETH", [], "USD"), "ETH", "price"),
            ("create_rsi_plot", ("ETH", []), "ETH", "rsi"),
            ("create_weth_price_plot", ("LINK", []), "LINK", "weth_price"),
            ("create_summary_grid", ([],), "summary", "grid"),
        ],
    )
    def test_missing_matplotlib_takes_precedence_over_empty_data(
        self,
        tmp_path: Path,
        method_name: str,
        args: tuple[Any, ...],
        expected_token: str,
        expected_plot_type: str,
    ) -> None:
        """Pin import-first behavior for base installs given empty plot data."""
        generator = PlotGenerator(output_dir=tmp_path)
        real_import = builtins.__import__

        def import_without_pyplot(name: str, *import_args: Any, **import_kwargs: Any) -> Any:
            if name == "matplotlib.pyplot":
                raise ImportError("matplotlib intentionally unavailable")
            return real_import(name, *import_args, **import_kwargs)

        with patch("builtins.__import__", side_effect=import_without_pyplot):
            result = getattr(generator, method_name)(*args)

        assert result == PlotResult(
            token=expected_token,
            plot_type=expected_plot_type,
            file_path=None,
            success=False,
            error="matplotlib not installed",
        )


__all__ = [
    "TestPlotConfig",
    "TestPlotResult",
    "TestPlotGeneratorInit",
    "TestCreatePricePlot",
    "TestCreateRSIPlot",
    "TestCreateWETHPricePlot",
    "TestCreateSummaryGrid",
    "TestPlotGeneratorIntegration",
]
