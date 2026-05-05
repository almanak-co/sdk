"""Tests for Report Generator.

This module provides unit tests for the ReportGenerator class,
verifying Markdown report generation with various test result scenarios.
"""

import tempfile
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest

from almanak.framework.data.qa.config import QAConfig, QAThresholds
from almanak.framework.data.qa.reporting.generator import ReportGenerator, ReportSummary
from almanak.framework.data.qa.tests.cex_historical import CEXHistoricalResult
from almanak.framework.data.qa.tests.cex_spot import CEXSpotResult
from almanak.framework.data.qa.tests.dex_historical import DEXHistoricalResult, WETHPricePoint
from almanak.framework.data.qa.tests.dex_spot import DEXSpotResult
from almanak.framework.data.qa.tests.rsi import RSIDataPoint, RSIResult


@pytest.fixture
def temp_output_dir():
    """Create a temporary directory for test output."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_config():
    """Create a sample QA configuration."""
    return QAConfig(
        chain="arbitrum",
        historical_days=30,
        timeframe="4h",
        rsi_period=14,
        thresholds=QAThresholds(
            min_confidence=0.8,
            max_price_impact_bps=100,
            max_gap_hours=8.0,
            max_stale_seconds=120,
        ),
        popular_tokens=["ETH", "WBTC", "USDC"],
        additional_tokens=["LINK", "ARB"],
        dex_tokens=["USDC", "LINK"],
    )


@pytest.fixture
def sample_cex_spot_results():
    """Create sample CEX spot test results."""
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
        CEXSpotResult(
            token="USDC",
            price_usd=Decimal("1.00"),
            confidence=0.70,
            timestamp=datetime.now(),
            is_fresh=True,
            passed=False,
            error="Low confidence: 0.70 (min: 0.8)",
        ),
    ]


@pytest.fixture
def sample_dex_spot_results():
    """Create sample DEX spot test results."""
    return [
        DEXSpotResult(
            token="USDC",
            best_dex="uniswap_v3",
            price_weth=Decimal("0.0004"),
            amount_out=Decimal("1000"),
            price_impact_bps=5,
            passed=True,
            error=None,
        ),
        DEXSpotResult(
            token="LINK",
            best_dex="enso",
            price_weth=Decimal("0.006"),
            amount_out=Decimal("66.67"),
            price_impact_bps=150,
            passed=False,
            error="High price impact: 150 bps (max: 100 bps)",
        ),
    ]


@pytest.fixture
def sample_cex_historical_results():
    """Create sample CEX historical test results."""
    return [
        CEXHistoricalResult(
            token="ETH",
            candles=[],  # Empty for tests
            total_candles=180,
            expected_candles=180,
            missing_count=0,
            max_gap_hours=4.0,
            price_range=(Decimal("2400.00"), Decimal("2600.00")),
            passed=True,
            error=None,
        ),
        CEXHistoricalResult(
            token="WBTC",
            candles=[],
            total_candles=150,
            expected_candles=180,
            missing_count=30,
            max_gap_hours=12.0,
            price_range=(Decimal("42000.00"), Decimal("48000.00")),
            passed=False,
            error="Max gap 12.0h exceeds threshold (8.0h)",
        ),
    ]


@pytest.fixture
def sample_dex_historical_results():
    """Create sample DEX historical test results."""
    return [
        DEXHistoricalResult(
            token="LINK",
            weth_prices=[
                WETHPricePoint(timestamp=datetime.now(), price_weth=Decimal("0.005")),
                WETHPricePoint(timestamp=datetime.now(), price_weth=Decimal("0.006")),
            ],
            total_points=180,
            passed=True,
            error=None,
            note="Derived from CEX data with WETH conversion",
        ),
        DEXHistoricalResult(
            token="ARB",
            weth_prices=[],
            total_points=0,
            passed=False,
            error="No WETH prices could be derived",
            note="Derived from CEX data with WETH conversion",
        ),
    ]


@pytest.fixture
def sample_rsi_results():
    """Create sample RSI test results."""
    return [
        RSIResult(
            token="ETH",
            current_rsi=55.5,
            signal="Neutral",
            rsi_history=[
                RSIDataPoint(index=0, rsi=45.0),
                RSIDataPoint(index=1, rsi=55.5),
            ],
            min_rsi=35.0,
            max_rsi=65.0,
            avg_rsi=50.0,
            passed=True,
            error=None,
        ),
        RSIResult(
            token="WBTC",
            current_rsi=25.0,
            signal="Oversold",
            rsi_history=[
                RSIDataPoint(index=0, rsi=30.0),
                RSIDataPoint(index=1, rsi=25.0),
            ],
            min_rsi=20.0,
            max_rsi=45.0,
            avg_rsi=32.5,
            passed=True,
            error=None,
        ),
    ]


class TestReportSummary:
    """Tests for ReportSummary dataclass."""

    def test_overall_passed_all_pass(self):
        """Test overall_passed when all tests pass."""
        summary = ReportSummary(
            total_tests=10,
            passed_tests=10,
            failed_tests=0,
            cex_spot_passed=2,
            cex_spot_total=2,
            dex_spot_passed=2,
            dex_spot_total=2,
            cex_historical_passed=2,
            cex_historical_total=2,
            dex_historical_passed=2,
            dex_historical_total=2,
            rsi_passed=2,
            rsi_total=2,
        )
        assert summary.overall_passed is True

    def test_overall_passed_some_fail(self):
        """Test overall_passed when some tests fail."""
        summary = ReportSummary(
            total_tests=10,
            passed_tests=8,
            failed_tests=2,
            cex_spot_passed=2,
            cex_spot_total=2,
            dex_spot_passed=1,
            dex_spot_total=2,
            cex_historical_passed=2,
            cex_historical_total=2,
            dex_historical_passed=1,
            dex_historical_total=2,
            rsi_passed=2,
            rsi_total=2,
        )
        assert summary.overall_passed is False

    def test_pass_rate(self):
        """Test pass_rate calculation."""
        summary = ReportSummary(
            total_tests=10,
            passed_tests=8,
            failed_tests=2,
            cex_spot_passed=2,
            cex_spot_total=2,
            dex_spot_passed=1,
            dex_spot_total=2,
            cex_historical_passed=2,
            cex_historical_total=2,
            dex_historical_passed=1,
            dex_historical_total=2,
            rsi_passed=2,
            rsi_total=2,
        )
        assert summary.pass_rate == 80.0

    def test_pass_rate_zero_tests(self):
        """Test pass_rate with zero total tests."""
        summary = ReportSummary(
            total_tests=0,
            passed_tests=0,
            failed_tests=0,
            cex_spot_passed=0,
            cex_spot_total=0,
            dex_spot_passed=0,
            dex_spot_total=0,
            cex_historical_passed=0,
            cex_historical_total=0,
            dex_historical_passed=0,
            dex_historical_total=0,
            rsi_passed=0,
            rsi_total=0,
        )
        assert summary.pass_rate == 0.0


class TestReportGenerator:
    """Tests for ReportGenerator class."""

    def test_init_creates_output_dir(self, temp_output_dir):
        """Test that __init__ creates output directory."""
        output_dir = temp_output_dir / "new_dir"
        assert not output_dir.exists()

        generator = ReportGenerator(output_dir=output_dir)

        assert output_dir.exists()
        assert generator.output_dir == output_dir
        assert generator.plots_dir == output_dir / "plots"

    def test_generate_report_creates_files(
        self,
        temp_output_dir,
        sample_config,
        sample_cex_spot_results,
        sample_dex_spot_results,
        sample_cex_historical_results,
        sample_dex_historical_results,
        sample_rsi_results,
    ):
        """Test that generate_report creates report files."""
        generator = ReportGenerator(output_dir=temp_output_dir)
        timestamp = datetime(2024, 1, 15, 10, 30, 0)

        report_path = generator.generate_report(
            cex_spot_results=sample_cex_spot_results,
            dex_spot_results=sample_dex_spot_results,
            cex_historical_results=sample_cex_historical_results,
            dex_historical_results=sample_dex_historical_results,
            rsi_results=sample_rsi_results,
            config=sample_config,
            duration_seconds=123.45,
            timestamp=timestamp,
        )

        # Check timestamped report exists
        assert report_path.exists()
        assert report_path.name == "report_20240115_103000.md"

        # Check latest report exists
        latest_path = temp_output_dir / "report-latest.md"
        assert latest_path.exists()

        # Check content is identical
        assert report_path.read_text() == latest_path.read_text()

    def test_report_contains_header(
        self,
        temp_output_dir,
        sample_config,
        sample_cex_spot_results,
        sample_dex_spot_results,
        sample_cex_historical_results,
        sample_dex_historical_results,
        sample_rsi_results,
    ):
        """Test that report contains header section."""
        generator = ReportGenerator(output_dir=temp_output_dir)
        timestamp = datetime(2024, 1, 15, 10, 30, 0)

        report_path = generator.generate_report(
            cex_spot_results=sample_cex_spot_results,
            dex_spot_results=sample_dex_spot_results,
            cex_historical_results=sample_cex_historical_results,
            dex_historical_results=sample_dex_historical_results,
            rsi_results=sample_rsi_results,
            config=sample_config,
            duration_seconds=123.45,
            timestamp=timestamp,
        )

        content = report_path.read_text()

        assert "# Data QA Report" in content
        assert "2024-01-15 10:30:00" in content
        assert "arbitrum" in content
        assert "123.45 seconds" in content

    def test_report_contains_executive_summary(
        self,
        temp_output_dir,
        sample_config,
        sample_cex_spot_results,
        sample_dex_spot_results,
        sample_cex_historical_results,
        sample_dex_historical_results,
        sample_rsi_results,
    ):
        """Test that report contains executive summary."""
        generator = ReportGenerator(output_dir=temp_output_dir)

        report_path = generator.generate_report(
            cex_spot_results=sample_cex_spot_results,
            dex_spot_results=sample_dex_spot_results,
            cex_historical_results=sample_cex_historical_results,
            dex_historical_results=sample_dex_historical_results,
            rsi_results=sample_rsi_results,
            config=sample_config,
            duration_seconds=100.0,
        )

        content = report_path.read_text()

        assert "## Executive Summary" in content
        assert "CEX Spot Prices" in content
        assert "DEX Spot Prices" in content
        assert "CEX Historical" in content
        assert "DEX Historical" in content
        assert "RSI Indicators" in content

    def test_report_contains_cex_spot_section(
        self,
        temp_output_dir,
        sample_config,
        sample_cex_spot_results,
        sample_dex_spot_results,
        sample_cex_historical_results,
        sample_dex_historical_results,
        sample_rsi_results,
    ):
        """Test that report contains CEX spot prices section."""
        generator = ReportGenerator(output_dir=temp_output_dir)

        report_path = generator.generate_report(
            cex_spot_results=sample_cex_spot_results,
            dex_spot_results=sample_dex_spot_results,
            cex_historical_results=sample_cex_historical_results,
            dex_historical_results=sample_dex_historical_results,
            rsi_results=sample_rsi_results,
            config=sample_config,
            duration_seconds=100.0,
        )

        content = report_path.read_text()

        assert "## 1. CEX Spot Prices" in content
        assert "ETH" in content
        assert "$2,500.00" in content
        assert "0.95" in content

    def test_report_contains_dex_spot_section(
        self,
        temp_output_dir,
        sample_config,
        sample_cex_spot_results,
        sample_dex_spot_results,
        sample_cex_historical_results,
        sample_dex_historical_results,
        sample_rsi_results,
    ):
        """Test that report contains DEX spot prices section."""
        generator = ReportGenerator(output_dir=temp_output_dir)

        report_path = generator.generate_report(
            cex_spot_results=sample_cex_spot_results,
            dex_spot_results=sample_dex_spot_results,
            cex_historical_results=sample_cex_historical_results,
            dex_historical_results=sample_dex_historical_results,
            rsi_results=sample_rsi_results,
            config=sample_config,
            duration_seconds=100.0,
        )

        content = report_path.read_text()

        assert "## 2. DEX Spot Prices" in content
        assert "uniswap_v3" in content
        assert "enso" in content

    def test_report_contains_historical_cex_section(
        self,
        temp_output_dir,
        sample_config,
        sample_cex_spot_results,
        sample_dex_spot_results,
        sample_cex_historical_results,
        sample_dex_historical_results,
        sample_rsi_results,
    ):
        """Test that report contains historical CEX section."""
        generator = ReportGenerator(output_dir=temp_output_dir)

        report_path = generator.generate_report(
            cex_spot_results=sample_cex_spot_results,
            dex_spot_results=sample_dex_spot_results,
            cex_historical_results=sample_cex_historical_results,
            dex_historical_results=sample_dex_historical_results,
            rsi_results=sample_rsi_results,
            config=sample_config,
            duration_seconds=100.0,
        )

        content = report_path.read_text()

        assert "## 3. Historical CEX Data" in content
        assert "### Price Charts" in content
        assert "eth_price_usd.png" in content

    def test_report_contains_historical_dex_section(
        self,
        temp_output_dir,
        sample_config,
        sample_cex_spot_results,
        sample_dex_spot_results,
        sample_cex_historical_results,
        sample_dex_historical_results,
        sample_rsi_results,
    ):
        """Test that report contains historical DEX section."""
        generator = ReportGenerator(output_dir=temp_output_dir)

        report_path = generator.generate_report(
            cex_spot_results=sample_cex_spot_results,
            dex_spot_results=sample_dex_spot_results,
            cex_historical_results=sample_cex_historical_results,
            dex_historical_results=sample_dex_historical_results,
            rsi_results=sample_rsi_results,
            config=sample_config,
            duration_seconds=100.0,
        )

        content = report_path.read_text()

        assert "## 4. Historical DEX Data" in content
        assert "### WETH Price Charts" in content
        assert "Derived from CEX data with WETH conversion" in content

    def test_report_contains_rsi_section(
        self,
        temp_output_dir,
        sample_config,
        sample_cex_spot_results,
        sample_dex_spot_results,
        sample_cex_historical_results,
        sample_dex_historical_results,
        sample_rsi_results,
    ):
        """Test that report contains RSI section."""
        generator = ReportGenerator(output_dir=temp_output_dir)

        report_path = generator.generate_report(
            cex_spot_results=sample_cex_spot_results,
            dex_spot_results=sample_dex_spot_results,
            cex_historical_results=sample_cex_historical_results,
            dex_historical_results=sample_dex_historical_results,
            rsi_results=sample_rsi_results,
            config=sample_config,
            duration_seconds=100.0,
        )

        content = report_path.read_text()

        assert "## 5. RSI Indicators" in content
        assert "### RSI Charts" in content
        assert "55.50" in content
        assert "Neutral" in content
        assert "Oversold" in content

    def test_report_contains_failures_section(
        self,
        temp_output_dir,
        sample_config,
        sample_cex_spot_results,
        sample_dex_spot_results,
        sample_cex_historical_results,
        sample_dex_historical_results,
        sample_rsi_results,
    ):
        """Test that report contains failures section."""
        generator = ReportGenerator(output_dir=temp_output_dir)

        report_path = generator.generate_report(
            cex_spot_results=sample_cex_spot_results,
            dex_spot_results=sample_dex_spot_results,
            cex_historical_results=sample_cex_historical_results,
            dex_historical_results=sample_dex_historical_results,
            rsi_results=sample_rsi_results,
            config=sample_config,
            duration_seconds=100.0,
        )

        content = report_path.read_text()

        assert "## Failures" in content
        assert "CEX Spot" in content
        assert "USDC" in content
        assert "Low confidence" in content

    def test_report_no_failures_message(
        self,
        temp_output_dir,
        sample_config,
    ):
        """Test that report shows 'no failures' when all pass."""
        generator = ReportGenerator(output_dir=temp_output_dir)

        # All passing results
        cex_spot = [
            CEXSpotResult(
                token="ETH",
                price_usd=Decimal("2500"),
                confidence=0.95,
                timestamp=datetime.now(),
                is_fresh=True,
                passed=True,
                error=None,
            ),
        ]

        report_path = generator.generate_report(
            cex_spot_results=cex_spot,
            dex_spot_results=[],
            cex_historical_results=[],
            dex_historical_results=[],
            rsi_results=[],
            config=sample_config,
            duration_seconds=10.0,
        )

        content = report_path.read_text()

        assert "No failures detected" in content

    def test_calculate_summary(
        self,
        temp_output_dir,
        sample_cex_spot_results,
        sample_dex_spot_results,
        sample_cex_historical_results,
        sample_dex_historical_results,
        sample_rsi_results,
    ):
        """Test summary calculation."""
        generator = ReportGenerator(output_dir=temp_output_dir)

        summary = generator._calculate_summary(
            sample_cex_spot_results,
            sample_dex_spot_results,
            sample_cex_historical_results,
            sample_dex_historical_results,
            sample_rsi_results,
        )

        # CEX spot: 2 pass, 1 fail (3 total)
        assert summary.cex_spot_passed == 2
        assert summary.cex_spot_total == 3

        # DEX spot: 1 pass, 1 fail (2 total)
        assert summary.dex_spot_passed == 1
        assert summary.dex_spot_total == 2

        # CEX historical: 1 pass, 1 fail (2 total)
        assert summary.cex_historical_passed == 1
        assert summary.cex_historical_total == 2

        # DEX historical: 1 pass, 1 fail (2 total)
        assert summary.dex_historical_passed == 1
        assert summary.dex_historical_total == 2

        # RSI: 2 pass, 0 fail (2 total)
        assert summary.rsi_passed == 2
        assert summary.rsi_total == 2

        # Total: 7 pass, 4 fail (11 total)
        assert summary.total_tests == 11
        assert summary.passed_tests == 7
        assert summary.failed_tests == 4

    def test_status_indicator(self, temp_output_dir):
        """Test status indicator generation."""
        generator = ReportGenerator(output_dir=temp_output_dir)

        assert generator._status_indicator(5, 5) == "PASS"
        assert generator._status_indicator(3, 5) == "FAIL"
        assert generator._status_indicator(0, 0) == "N/A"

    def test_empty_results(self, temp_output_dir, sample_config):
        """Test report generation with empty results."""
        generator = ReportGenerator(output_dir=temp_output_dir)

        report_path = generator.generate_report(
            cex_spot_results=[],
            dex_spot_results=[],
            cex_historical_results=[],
            dex_historical_results=[],
            rsi_results=[],
            config=sample_config,
            duration_seconds=0.5,
        )

        content = report_path.read_text()

        assert "No CEX spot price tests were run" in content
        assert "No DEX spot price tests were run" in content
        assert "No CEX historical tests were run" in content
        assert "No DEX historical tests were run" in content
        assert "No RSI tests were run" in content

    def test_error_escaping_in_failures(self, temp_output_dir, sample_config):
        """Test that pipe characters in errors are escaped."""
        generator = ReportGenerator(output_dir=temp_output_dir)

        cex_spot = [
            CEXSpotResult(
                token="TEST",
                price_usd=None,
                confidence=None,
                timestamp=None,
                is_fresh=False,
                passed=False,
                error="Error with | pipe | characters",
            ),
        ]

        report_path = generator.generate_report(
            cex_spot_results=cex_spot,
            dex_spot_results=[],
            cex_historical_results=[],
            dex_historical_results=[],
            rsi_results=[],
            config=sample_config,
            duration_seconds=1.0,
        )

        content = report_path.read_text()

        # Pipe characters should be escaped
        assert "Error with \\| pipe \\| characters" in content
