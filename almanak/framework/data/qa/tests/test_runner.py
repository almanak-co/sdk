"""Unit tests for QA Runner.

Tests the QARunner class which orchestrates all QA tests and generates reports.
All test dependencies are mocked to isolate unit test behavior.
"""

import tempfile
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.data.qa.config import QAConfig, QAThresholds
from almanak.framework.data.qa.runner import QAReport, QARunner, TestDuration
from almanak.framework.data.qa.tests.cex_historical import CEXHistoricalResult
from almanak.framework.data.qa.tests.cex_spot import CEXSpotResult
from almanak.framework.data.qa.tests.dex_historical import DEXHistoricalResult, WETHPricePoint
from almanak.framework.data.qa.tests.dex_spot import DEXSpotResult
from almanak.framework.data.qa.tests.rsi import RSIDataPoint, RSIResult


@pytest.fixture
def qa_config() -> QAConfig:
    """Create a test QA configuration."""
    return QAConfig(
        chain="arbitrum",
        historical_days=7,
        timeframe="4h",
        rsi_period=14,
        thresholds=QAThresholds(
            min_confidence=0.8,
            max_price_impact_bps=100,
            max_gap_hours=8.0,
            max_stale_seconds=120,
        ),
        popular_tokens=["ETH", "WBTC"],
        additional_tokens=["LINK"],
        dex_tokens=["USDC", "LINK"],
    )


@pytest.fixture
def temp_output_dir() -> Path:
    """Create a temporary output directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def mock_cex_spot_results() -> list[CEXSpotResult]:
    """Create mock CEX spot price results."""
    return [
        CEXSpotResult(
            token="ETH",
            price_usd=Decimal("2500.00"),
            confidence=0.95,
            timestamp=datetime.now(),
            is_fresh=True,
            passed=True,
            error=None,
        ),
        CEXSpotResult(
            token="WBTC",
            price_usd=Decimal("45000.00"),
            confidence=0.92,
            timestamp=datetime.now(),
            is_fresh=True,
            passed=True,
            error=None,
        ),
    ]


@pytest.fixture
def mock_dex_spot_results() -> list[DEXSpotResult]:
    """Create mock DEX spot price results."""
    return [
        DEXSpotResult(
            token="USDC",
            best_dex="uniswap_v3",
            price_weth=Decimal("0.0004"),
            amount_out=Decimal("1000.00"),
            price_impact_bps=5,
            passed=True,
            error=None,
        ),
        DEXSpotResult(
            token="LINK",
            best_dex="enso",
            price_weth=Decimal("0.005"),
            amount_out=Decimal("80.00"),
            price_impact_bps=15,
            passed=True,
            error=None,
        ),
    ]


@pytest.fixture
def mock_cex_historical_results() -> list[CEXHistoricalResult]:
    """Create mock CEX historical results."""
    return [
        CEXHistoricalResult(
            token="ETH",
            candles=[],
            total_candles=42,
            expected_candles=42,
            missing_count=0,
            max_gap_hours=4.0,
            price_range=(Decimal("2400"), Decimal("2600")),
            passed=True,
            error=None,
        ),
    ]


@pytest.fixture
def mock_dex_historical_results() -> list[DEXHistoricalResult]:
    """Create mock DEX historical results."""
    return [
        DEXHistoricalResult(
            token="LINK",
            weth_prices=[
                WETHPricePoint(timestamp=datetime.now(), price_weth=Decimal("0.005")),
            ],
            total_points=42,
            passed=True,
            error=None,
        ),
    ]


@pytest.fixture
def mock_rsi_results() -> list[RSIResult]:
    """Create mock RSI results."""
    return [
        RSIResult(
            token="ETH",
            current_rsi=55.0,
            signal="Neutral",
            rsi_history=[RSIDataPoint(index=0, rsi=55.0)],
            min_rsi=40.0,
            max_rsi=65.0,
            avg_rsi=52.5,
            passed=True,
            error=None,
        ),
    ]


class TestTestDuration:
    """Tests for TestDuration dataclass."""

    def test_start_stop(self) -> None:
        """Test duration tracking with start and stop."""
        duration = TestDuration(category="test")
        duration.start()
        # Simulate some work
        duration.stop()

        assert duration.start_time > 0
        assert duration.end_time >= duration.start_time
        assert duration.duration_seconds >= 0

    def test_default_values(self) -> None:
        """Test default values for TestDuration."""
        duration = TestDuration(category="test")

        assert duration.category == "test"
        assert duration.start_time == 0.0
        assert duration.end_time == 0.0
        assert duration.duration_seconds == 0.0


class TestQAReport:
    """Tests for QAReport dataclass."""

    def test_empty_report_properties(self, qa_config: QAConfig) -> None:
        """Test properties of empty report."""
        report = QAReport(config=qa_config)

        assert report.total_tests == 0
        assert report.passed_tests == 0
        assert report.failed_tests == 0
        assert report.passed is False

    def test_report_with_results(
        self,
        qa_config: QAConfig,
        mock_cex_spot_results: list[CEXSpotResult],
        mock_dex_spot_results: list[DEXSpotResult],
    ) -> None:
        """Test report properties with some results."""
        report = QAReport(
            config=qa_config,
            cex_spot_results=mock_cex_spot_results,
            dex_spot_results=mock_dex_spot_results,
        )

        assert report.total_tests == 4
        assert report.passed_tests == 4
        assert report.failed_tests == 0

    def test_report_with_failures(
        self,
        qa_config: QAConfig,
        mock_cex_spot_results: list[CEXSpotResult],
    ) -> None:
        """Test report properties with some failures."""
        # Create a failing result
        failing_result = CEXSpotResult(
            token="FAIL",
            price_usd=None,
            confidence=None,
            timestamp=None,
            is_fresh=False,
            passed=False,
            error="Test error",
        )

        report = QAReport(
            config=qa_config,
            cex_spot_results=mock_cex_spot_results + [failing_result],
        )

        assert report.total_tests == 3
        assert report.passed_tests == 2
        assert report.failed_tests == 1


class TestQARunner:
    """Tests for QARunner class."""

    def test_init_creates_directories(
        self,
        qa_config: QAConfig,
        temp_output_dir: Path,
    ) -> None:
        """Test that runner creates output directories."""
        runner = QARunner(
            config=qa_config,
            output_dir=temp_output_dir,
            skip_plots=False,
        )

        assert runner.output_dir.exists()
        assert runner.plots_dir.exists()

    def test_init_skip_plots(
        self,
        qa_config: QAConfig,
        temp_output_dir: Path,
    ) -> None:
        """Test that runner skips plots directory creation when skip_plots=True."""
        # Create runner with skip_plots
        runner = QARunner(
            config=qa_config,
            output_dir=temp_output_dir,
            skip_plots=True,
        )

        assert runner.output_dir.exists()
        assert runner.skip_plots is True

    @pytest.mark.asyncio
    async def test_run_all_orchestrates_tests(
        self,
        qa_config: QAConfig,
        temp_output_dir: Path,
        mock_cex_spot_results: list[CEXSpotResult],
        mock_dex_spot_results: list[DEXSpotResult],
        mock_cex_historical_results: list[CEXHistoricalResult],
        mock_dex_historical_results: list[DEXHistoricalResult],
        mock_rsi_results: list[RSIResult],
    ) -> None:
        """Test that run_all orchestrates all tests correctly."""
        runner = QARunner(
            config=qa_config,
            output_dir=temp_output_dir,
            skip_plots=True,
        )

        # Mock all test classes
        with (
            patch("src.data.qa.runner.CEXSpotPriceTest") as mock_cex_spot,
            patch("src.data.qa.runner.DEXSpotPriceTest") as mock_dex_spot,
            patch("src.data.qa.runner.CEXHistoricalTest") as mock_cex_hist,
            patch("src.data.qa.runner.DEXHistoricalTest") as mock_dex_hist,
            patch("src.data.qa.runner.RSITest") as mock_rsi,
        ):
            # Configure mocks
            mock_cex_spot.return_value.__aenter__ = AsyncMock(return_value=mock_cex_spot.return_value)
            mock_cex_spot.return_value.__aexit__ = AsyncMock(return_value=None)
            mock_cex_spot.return_value.run = AsyncMock(return_value=mock_cex_spot_results)

            mock_dex_spot.return_value.__aenter__ = AsyncMock(return_value=mock_dex_spot.return_value)
            mock_dex_spot.return_value.__aexit__ = AsyncMock(return_value=None)
            mock_dex_spot.return_value.run = AsyncMock(return_value=mock_dex_spot_results)

            mock_cex_hist.return_value.__aenter__ = AsyncMock(return_value=mock_cex_hist.return_value)
            mock_cex_hist.return_value.__aexit__ = AsyncMock(return_value=None)
            mock_cex_hist.return_value.run = AsyncMock(return_value=mock_cex_historical_results)

            mock_dex_hist.return_value.__aenter__ = AsyncMock(return_value=mock_dex_hist.return_value)
            mock_dex_hist.return_value.__aexit__ = AsyncMock(return_value=None)
            mock_dex_hist.return_value.run = AsyncMock(return_value=mock_dex_historical_results)

            mock_rsi.return_value.__aenter__ = AsyncMock(return_value=mock_rsi.return_value)
            mock_rsi.return_value.__aexit__ = AsyncMock(return_value=None)
            mock_rsi.return_value.run = AsyncMock(return_value=mock_rsi_results)

            # Run all tests
            report = await runner.run_all()

            # Verify all tests were run
            mock_cex_spot.return_value.run.assert_called_once()
            mock_dex_spot.return_value.run.assert_called_once()
            mock_cex_hist.return_value.run.assert_called_once()
            mock_dex_hist.return_value.run.assert_called_once()
            mock_rsi.return_value.run.assert_called_once()

            # Verify report
            assert report.passed is True
            assert report.total_tests == 7
            assert report.passed_tests == 7
            assert report.failed_tests == 0
            assert report.report_path is not None
            assert report.report_path.exists()
            assert report.total_duration_seconds > 0

    @pytest.mark.asyncio
    async def test_run_all_tracks_durations(
        self,
        qa_config: QAConfig,
        temp_output_dir: Path,
    ) -> None:
        """Test that run_all tracks durations for each test category."""
        runner = QARunner(
            config=qa_config,
            output_dir=temp_output_dir,
            skip_plots=True,
        )

        with (
            patch("src.data.qa.runner.CEXSpotPriceTest") as mock_cex_spot,
            patch("src.data.qa.runner.DEXSpotPriceTest") as mock_dex_spot,
            patch("src.data.qa.runner.CEXHistoricalTest") as mock_cex_hist,
            patch("src.data.qa.runner.DEXHistoricalTest") as mock_dex_hist,
            patch("src.data.qa.runner.RSITest") as mock_rsi,
        ):
            # Configure minimal mocks
            for mock in [mock_cex_spot, mock_dex_spot, mock_cex_hist, mock_dex_hist, mock_rsi]:
                mock.return_value.__aenter__ = AsyncMock(return_value=mock.return_value)
                mock.return_value.__aexit__ = AsyncMock(return_value=None)
                mock.return_value.run = AsyncMock(return_value=[])

            report = await runner.run_all()

            # Verify durations tracked
            assert "cex_spot" in report.durations
            assert "dex_spot" in report.durations
            assert "cex_historical" in report.durations
            assert "dex_historical" in report.durations
            assert "rsi" in report.durations

            for _category, duration in report.durations.items():
                assert duration.duration_seconds >= 0

    @pytest.mark.asyncio
    async def test_run_all_generates_report(
        self,
        qa_config: QAConfig,
        temp_output_dir: Path,
    ) -> None:
        """Test that run_all generates a Markdown report."""
        runner = QARunner(
            config=qa_config,
            output_dir=temp_output_dir,
            skip_plots=True,
        )

        with (
            patch("src.data.qa.runner.CEXSpotPriceTest") as mock_cex_spot,
            patch("src.data.qa.runner.DEXSpotPriceTest") as mock_dex_spot,
            patch("src.data.qa.runner.CEXHistoricalTest") as mock_cex_hist,
            patch("src.data.qa.runner.DEXHistoricalTest") as mock_dex_hist,
            patch("src.data.qa.runner.RSITest") as mock_rsi,
        ):
            for mock in [mock_cex_spot, mock_dex_spot, mock_cex_hist, mock_dex_hist, mock_rsi]:
                mock.return_value.__aenter__ = AsyncMock(return_value=mock.return_value)
                mock.return_value.__aexit__ = AsyncMock(return_value=None)
                mock.return_value.run = AsyncMock(return_value=[])

            report = await runner.run_all()

            # Verify report generated
            assert report.report_path is not None
            assert report.report_path.exists()
            assert report.report_path.suffix == ".md"

            # Verify report-latest.md also exists
            latest_path = temp_output_dir / "report-latest.md"
            assert latest_path.exists()

    @pytest.mark.asyncio
    async def test_run_all_handles_failures(
        self,
        qa_config: QAConfig,
        temp_output_dir: Path,
    ) -> None:
        """Test that run_all correctly handles test failures."""
        runner = QARunner(
            config=qa_config,
            output_dir=temp_output_dir,
            skip_plots=True,
        )

        # Create results with one failure
        failing_result = CEXSpotResult(
            token="FAIL",
            price_usd=None,
            confidence=None,
            timestamp=None,
            is_fresh=False,
            passed=False,
            error="Test error",
        )

        passing_result = CEXSpotResult(
            token="PASS",
            price_usd=Decimal("100"),
            confidence=0.9,
            timestamp=datetime.now(),
            is_fresh=True,
            passed=True,
            error=None,
        )

        with (
            patch("src.data.qa.runner.CEXSpotPriceTest") as mock_cex_spot,
            patch("src.data.qa.runner.DEXSpotPriceTest") as mock_dex_spot,
            patch("src.data.qa.runner.CEXHistoricalTest") as mock_cex_hist,
            patch("src.data.qa.runner.DEXHistoricalTest") as mock_dex_hist,
            patch("src.data.qa.runner.RSITest") as mock_rsi,
        ):
            # CEX spot returns mixed results
            mock_cex_spot.return_value.__aenter__ = AsyncMock(return_value=mock_cex_spot.return_value)
            mock_cex_spot.return_value.__aexit__ = AsyncMock(return_value=None)
            mock_cex_spot.return_value.run = AsyncMock(return_value=[passing_result, failing_result])

            # Others return empty
            for mock in [mock_dex_spot, mock_cex_hist, mock_dex_hist, mock_rsi]:
                mock.return_value.__aenter__ = AsyncMock(return_value=mock.return_value)
                mock.return_value.__aexit__ = AsyncMock(return_value=None)
                mock.return_value.run = AsyncMock(return_value=[])

            report = await runner.run_all()

            # Verify failure detected
            assert report.passed is False
            assert report.total_tests == 2
            assert report.passed_tests == 1
            assert report.failed_tests == 1

    @pytest.mark.asyncio
    async def test_generate_plots_called_when_enabled(
        self,
        qa_config: QAConfig,
        temp_output_dir: Path,
        mock_cex_historical_results: list[CEXHistoricalResult],
        mock_rsi_results: list[RSIResult],
    ) -> None:
        """Test that plots are generated when skip_plots=False."""
        runner = QARunner(
            config=qa_config,
            output_dir=temp_output_dir,
            skip_plots=False,
        )

        with (
            patch("src.data.qa.runner.CEXSpotPriceTest") as mock_cex_spot,
            patch("src.data.qa.runner.DEXSpotPriceTest") as mock_dex_spot,
            patch("src.data.qa.runner.CEXHistoricalTest") as mock_cex_hist,
            patch("src.data.qa.runner.DEXHistoricalTest") as mock_dex_hist,
            patch("src.data.qa.runner.RSITest") as mock_rsi,
            patch("src.data.qa.runner.PlotGenerator") as mock_plot_gen,
        ):
            # Configure test mocks
            mock_cex_spot.return_value.__aenter__ = AsyncMock(return_value=mock_cex_spot.return_value)
            mock_cex_spot.return_value.__aexit__ = AsyncMock(return_value=None)
            mock_cex_spot.return_value.run = AsyncMock(return_value=[])

            mock_dex_spot.return_value.__aenter__ = AsyncMock(return_value=mock_dex_spot.return_value)
            mock_dex_spot.return_value.__aexit__ = AsyncMock(return_value=None)
            mock_dex_spot.return_value.run = AsyncMock(return_value=[])

            mock_cex_hist.return_value.__aenter__ = AsyncMock(return_value=mock_cex_hist.return_value)
            mock_cex_hist.return_value.__aexit__ = AsyncMock(return_value=None)
            mock_cex_hist.return_value.run = AsyncMock(return_value=mock_cex_historical_results)

            mock_dex_hist.return_value.__aenter__ = AsyncMock(return_value=mock_dex_hist.return_value)
            mock_dex_hist.return_value.__aexit__ = AsyncMock(return_value=None)
            mock_dex_hist.return_value.run = AsyncMock(return_value=[])

            mock_rsi.return_value.__aenter__ = AsyncMock(return_value=mock_rsi.return_value)
            mock_rsi.return_value.__aexit__ = AsyncMock(return_value=None)
            mock_rsi.return_value.run = AsyncMock(return_value=mock_rsi_results)

            # Configure plot generator mock
            mock_plot_instance = MagicMock()
            mock_plot_gen.return_value = mock_plot_instance

            await runner.run_all()

            # Verify plot generator was created and used
            mock_plot_gen.assert_called_once_with(output_dir=runner.plots_dir)

    @pytest.mark.asyncio
    async def test_generate_plots_skipped_when_disabled(
        self,
        qa_config: QAConfig,
        temp_output_dir: Path,
    ) -> None:
        """Test that plots are skipped when skip_plots=True."""
        runner = QARunner(
            config=qa_config,
            output_dir=temp_output_dir,
            skip_plots=True,
        )

        with (
            patch("src.data.qa.runner.CEXSpotPriceTest") as mock_cex_spot,
            patch("src.data.qa.runner.DEXSpotPriceTest") as mock_dex_spot,
            patch("src.data.qa.runner.CEXHistoricalTest") as mock_cex_hist,
            patch("src.data.qa.runner.DEXHistoricalTest") as mock_dex_hist,
            patch("src.data.qa.runner.RSITest") as mock_rsi,
            patch("src.data.qa.runner.PlotGenerator") as mock_plot_gen,
        ):
            for mock in [mock_cex_spot, mock_dex_spot, mock_cex_hist, mock_dex_hist, mock_rsi]:
                mock.return_value.__aenter__ = AsyncMock(return_value=mock.return_value)
                mock.return_value.__aexit__ = AsyncMock(return_value=None)
                mock.return_value.run = AsyncMock(return_value=[])

            await runner.run_all()

            # Verify plot generator was NOT called
            mock_plot_gen.assert_not_called()


class TestQAReportIntegration:
    """Integration tests for QAReport with real data structures."""

    def test_full_report_properties(self, qa_config: QAConfig) -> None:
        """Test QAReport with all result types populated."""
        report = QAReport(
            config=qa_config,
            cex_spot_results=[
                CEXSpotResult(
                    token="ETH",
                    price_usd=Decimal("2500"),
                    confidence=0.95,
                    timestamp=datetime.now(),
                    is_fresh=True,
                    passed=True,
                    error=None,
                ),
                CEXSpotResult(
                    token="BTC",
                    price_usd=None,
                    confidence=None,
                    timestamp=None,
                    is_fresh=False,
                    passed=False,
                    error="Unavailable",
                ),
            ],
            dex_spot_results=[
                DEXSpotResult(
                    token="USDC",
                    best_dex="uniswap",
                    price_weth=Decimal("0.0004"),
                    amount_out=Decimal("1000"),
                    price_impact_bps=5,
                    passed=True,
                    error=None,
                ),
            ],
            cex_historical_results=[
                CEXHistoricalResult(
                    token="ETH",
                    candles=[],
                    total_candles=42,
                    expected_candles=42,
                    missing_count=0,
                    max_gap_hours=4.0,
                    price_range=None,
                    passed=True,
                    error=None,
                ),
            ],
            dex_historical_results=[
                DEXHistoricalResult(
                    token="LINK",
                    weth_prices=[],
                    total_points=0,
                    passed=False,
                    error="No data",
                ),
            ],
            rsi_results=[
                RSIResult(
                    token="ETH",
                    current_rsi=50.0,
                    signal="Neutral",
                    passed=True,
                ),
            ],
            total_duration_seconds=10.5,
        )

        assert report.total_tests == 6
        assert report.passed_tests == 4
        assert report.failed_tests == 2
        assert report.total_duration_seconds == 10.5
