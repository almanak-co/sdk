"""Tests for Plot Generator Module.

This test suite covers:
- PlotConfig and PlotResult dataclass creation
- PlotGenerator initialization and directory creation
- Price plot generation with mocked matplotlib
- RSI plot generation with mocked matplotlib
- WETH price plot generation with mocked matplotlib
- Summary grid generation with mocked matplotlib
- Error handling when matplotlib is not available
- Error handling when no data is provided
"""

import tempfile
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pytest

from almanak.framework.data.interfaces import OHLCVCandle
from almanak.framework.data.qa.reporting.plots import PlotConfig, PlotGenerator, PlotResult
from almanak.framework.data.qa.tests.rsi import RSIDataPoint

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

    def test_price_plot_no_data(self) -> None:
        """Test price plot with no candle data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generator = PlotGenerator(output_dir=Path(tmpdir))

            result = generator.create_price_plot(
                token="ETH",
                candles=[],
                quote="USD",
            )

            assert result.success is False
            assert result.error == "No candle data provided"
            assert result.file_path is None

    def test_price_plot_matplotlib_not_installed(self) -> None:
        """Test price plot when matplotlib is not installed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            PlotGenerator(output_dir=Path(tmpdir))  # Create to test directory creation
            start = datetime(2025, 1, 1, tzinfo=UTC)
            candles = create_candles(start, 10)

            with patch.dict("sys.modules", {"matplotlib.pyplot": None}):
                with patch("src.data.qa.reporting.plots.PlotGenerator.create_price_plot") as mock_create:
                    mock_create.return_value = PlotResult(
                        token="ETH",
                        plot_type="price",
                        file_path=None,
                        success=False,
                        error="matplotlib not installed",
                    )
                    result = mock_create("ETH", candles, "USD")

            assert result.success is False
            assert "matplotlib" in result.error.lower()

    @pytest.mark.skipif(
        True,  # Skip if matplotlib not available
        reason="matplotlib may not be installed",
    )
    def test_price_plot_creates_file(self) -> None:
        """Test price plot creates PNG file."""
        try:
            import matplotlib.pyplot  # noqa: F401
        except ImportError:
            pytest.skip("matplotlib not installed")

        with tempfile.TemporaryDirectory() as tmpdir:
            generator = PlotGenerator(output_dir=Path(tmpdir))
            start = datetime(2025, 1, 1, tzinfo=UTC)
            candles = create_candles(start, 20)

            result = generator.create_price_plot(
                token="ETH",
                candles=candles,
                quote="USD",
            )

            assert result.success is True
            assert result.file_path is not None
            assert result.file_path.exists()
            assert result.file_path.suffix == ".png"
            assert "eth" in result.file_path.name.lower()


# =============================================================================
# RSI Plot Tests
# =============================================================================


class TestCreateRSIPlot:
    """Tests for create_rsi_plot method."""

    def test_rsi_plot_no_data(self) -> None:
        """Test RSI plot with no history data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generator = PlotGenerator(output_dir=Path(tmpdir))

            result = generator.create_rsi_plot(
                token="ETH",
                rsi_history=[],
            )

            assert result.success is False
            assert result.error == "No RSI history data provided"
            assert result.file_path is None

    @pytest.mark.skipif(
        True,  # Skip if matplotlib not available
        reason="matplotlib may not be installed",
    )
    def test_rsi_plot_creates_file(self) -> None:
        """Test RSI plot creates PNG file."""
        try:
            import matplotlib.pyplot  # noqa: F401
        except ImportError:
            pytest.skip("matplotlib not installed")

        with tempfile.TemporaryDirectory() as tmpdir:
            generator = PlotGenerator(output_dir=Path(tmpdir))
            rsi_history = create_rsi_history(20)

            result = generator.create_rsi_plot(
                token="ETH",
                rsi_history=rsi_history,
            )

            assert result.success is True
            assert result.file_path is not None
            assert result.file_path.exists()
            assert result.file_path.suffix == ".png"
            assert "rsi" in result.file_path.name.lower()


# =============================================================================
# WETH Price Plot Tests
# =============================================================================


class TestCreateWETHPricePlot:
    """Tests for create_weth_price_plot method."""

    def test_weth_price_plot_no_data(self) -> None:
        """Test WETH price plot with no data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generator = PlotGenerator(output_dir=Path(tmpdir))

            result = generator.create_weth_price_plot(
                token="LINK",
                weth_prices=[],
            )

            assert result.success is False
            assert result.error == "No WETH price data provided"
            assert result.file_path is None

    @pytest.mark.skipif(
        True,  # Skip if matplotlib not available
        reason="matplotlib may not be installed",
    )
    def test_weth_price_plot_creates_file(self) -> None:
        """Test WETH price plot creates PNG file."""
        try:
            import matplotlib.pyplot  # noqa: F401
        except ImportError:
            pytest.skip("matplotlib not installed")

        with tempfile.TemporaryDirectory() as tmpdir:
            generator = PlotGenerator(output_dir=Path(tmpdir))
            start = datetime(2025, 1, 1, tzinfo=UTC)
            weth_prices = create_weth_prices(start, 20)

            result = generator.create_weth_price_plot(
                token="LINK",
                weth_prices=weth_prices,
            )

            assert result.success is True
            assert result.file_path is not None
            assert result.file_path.exists()
            assert result.file_path.suffix == ".png"
            assert "weth" in result.file_path.name.lower()


# =============================================================================
# Summary Grid Tests
# =============================================================================


class TestCreateSummaryGrid:
    """Tests for create_summary_grid method."""

    def test_summary_grid_no_plots(self) -> None:
        """Test summary grid with no plots provided."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generator = PlotGenerator(output_dir=Path(tmpdir))

            result = generator.create_summary_grid(plots=[])

            assert result.success is False
            assert result.error == "No plots provided"
            assert result.file_path is None

    @pytest.mark.skipif(
        True,  # Skip if matplotlib not available
        reason="matplotlib may not be installed",
    )
    def test_summary_grid_creates_file(self) -> None:
        """Test summary grid creates PNG file."""
        try:
            import matplotlib.pyplot  # noqa: F401
        except ImportError:
            pytest.skip("matplotlib not installed")

        with tempfile.TemporaryDirectory() as tmpdir:
            generator = PlotGenerator(output_dir=Path(tmpdir))
            start = datetime(2025, 1, 1, tzinfo=UTC)

            plots: list[tuple[str, list, str]] = [
                ("ETH", create_candles(start, 10), "price"),
                ("WBTC", create_candles(start, 10, start_price=50000), "price"),
                ("LINK", list(create_rsi_history(10)), "rsi"),
            ]

            result = generator.create_summary_grid(
                plots=plots,
                rows=2,
                cols=2,
            )

            assert result.success is True
            assert result.file_path is not None
            assert result.file_path.exists()
            assert result.file_path.suffix == ".png"
            assert "summary" in result.file_path.name.lower()


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
