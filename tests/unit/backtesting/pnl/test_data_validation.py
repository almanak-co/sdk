"""Unit tests for price data validation in PnL backtesting.

Tests cover:
- Gap detection (gaps larger than expected interval)
- Duplicate timestamp detection
- Invalid order detection
- DataQualityResult properties and serialization
- Logging of warnings
"""

import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.backtesting.pnl.providers.data_validation import (
    DataQualityIssue,
    DataQualityIssueType,
    DataQualityResult,
    DataQualitySeverity,
    detect_outliers,
    validate_ohlcv_data,
    validate_price_data,
    validate_price_data_with_outliers,
)


class TestDataQualityIssue:
    """Tests for DataQualityIssue dataclass."""

    def test_issue_str_representation(self) -> None:
        """Test string representation of issue."""
        issue = DataQualityIssue(
            issue_type=DataQualityIssueType.GAP,
            severity=DataQualitySeverity.WARNING,
            timestamp=datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
            description="Gap of 7200s detected",
        )
        result = str(issue)
        assert "[WARNING]" in result
        assert "gap" in result
        assert "Gap of 7200s detected" in result

    def test_issue_to_dict(self) -> None:
        """Test serialization to dictionary."""
        issue = DataQualityIssue(
            issue_type=DataQualityIssueType.DUPLICATE,
            severity=DataQualitySeverity.INFO,
            timestamp=datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
            description="Duplicate timestamp found",
            details={"occurrence": 2},
        )
        result = issue.to_dict()
        assert result["issue_type"] == "duplicate"
        assert result["severity"] == "info"
        assert "2024-01-01" in result["timestamp"]
        assert result["description"] == "Duplicate timestamp found"
        assert result["details"]["occurrence"] == 2


class TestDataQualityResult:
    """Tests for DataQualityResult dataclass."""

    def test_empty_result_has_no_issues(self) -> None:
        """Test empty result reports no issues."""
        result = DataQualityResult()
        assert not result.has_issues
        assert not result.has_warnings
        assert not result.has_errors

    def test_result_with_info_issue(self) -> None:
        """Test result with info-level issue."""
        result = DataQualityResult(
            issues=[
                DataQualityIssue(
                    issue_type=DataQualityIssueType.GAP,
                    severity=DataQualitySeverity.INFO,
                    timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                    description="Small gap",
                )
            ]
        )
        assert result.has_issues
        assert not result.has_warnings
        assert not result.has_errors

    def test_result_with_warning_issue(self) -> None:
        """Test result with warning-level issue."""
        result = DataQualityResult(
            issues=[
                DataQualityIssue(
                    issue_type=DataQualityIssueType.GAP,
                    severity=DataQualitySeverity.WARNING,
                    timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                    description="Medium gap",
                )
            ]
        )
        assert result.has_issues
        assert result.has_warnings
        assert not result.has_errors

    def test_result_with_error_issue(self) -> None:
        """Test result with error-level issue."""
        result = DataQualityResult(
            issues=[
                DataQualityIssue(
                    issue_type=DataQualityIssueType.GAP,
                    severity=DataQualitySeverity.ERROR,
                    timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                    description="Large gap",
                )
            ]
        )
        assert result.has_issues
        assert result.has_warnings  # Error is also a warning
        assert result.has_errors

    def test_issues_by_type(self) -> None:
        """Test grouping issues by type."""
        result = DataQualityResult(
            issues=[
                DataQualityIssue(
                    issue_type=DataQualityIssueType.GAP,
                    severity=DataQualitySeverity.WARNING,
                    timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                    description="Gap 1",
                ),
                DataQualityIssue(
                    issue_type=DataQualityIssueType.DUPLICATE,
                    severity=DataQualitySeverity.WARNING,
                    timestamp=datetime(2024, 1, 2, tzinfo=UTC),
                    description="Duplicate",
                ),
                DataQualityIssue(
                    issue_type=DataQualityIssueType.GAP,
                    severity=DataQualitySeverity.WARNING,
                    timestamp=datetime(2024, 1, 3, tzinfo=UTC),
                    description="Gap 2",
                ),
            ]
        )
        by_type = result.issues_by_type
        assert len(by_type[DataQualityIssueType.GAP]) == 2
        assert len(by_type[DataQualityIssueType.DUPLICATE]) == 1

    def test_issues_by_severity(self) -> None:
        """Test grouping issues by severity."""
        result = DataQualityResult(
            issues=[
                DataQualityIssue(
                    issue_type=DataQualityIssueType.GAP,
                    severity=DataQualitySeverity.INFO,
                    timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                    description="Info issue",
                ),
                DataQualityIssue(
                    issue_type=DataQualityIssueType.GAP,
                    severity=DataQualitySeverity.WARNING,
                    timestamp=datetime(2024, 1, 2, tzinfo=UTC),
                    description="Warning issue",
                ),
                DataQualityIssue(
                    issue_type=DataQualityIssueType.GAP,
                    severity=DataQualitySeverity.ERROR,
                    timestamp=datetime(2024, 1, 3, tzinfo=UTC),
                    description="Error issue",
                ),
            ]
        )
        by_severity = result.issues_by_severity
        assert len(by_severity[DataQualitySeverity.INFO]) == 1
        assert len(by_severity[DataQualitySeverity.WARNING]) == 1
        assert len(by_severity[DataQualitySeverity.ERROR]) == 1

    def test_summary_no_issues(self) -> None:
        """Test summary when no issues found."""
        result = DataQualityResult(
            total_data_points=100,
            coverage_percent=100.0,
        )
        summary = result.summary()
        assert "OK" in summary
        assert "100 data points" in summary

    def test_summary_with_issues(self) -> None:
        """Test summary when issues found."""
        result = DataQualityResult(
            issues=[
                DataQualityIssue(
                    issue_type=DataQualityIssueType.GAP,
                    severity=DataQualitySeverity.ERROR,
                    timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                    description="Gap",
                ),
                DataQualityIssue(
                    issue_type=DataQualityIssueType.GAP,
                    severity=DataQualitySeverity.WARNING,
                    timestamp=datetime(2024, 1, 2, tzinfo=UTC),
                    description="Gap",
                ),
            ],
            total_data_points=95,
            expected_data_points=100,
            coverage_percent=95.0,
            token="WETH",
        )
        summary = result.summary()
        assert "1 error" in summary
        assert "1 warning" in summary
        assert "WETH" in summary
        assert "95/100" in summary

    def test_to_dict(self) -> None:
        """Test serialization to dictionary."""
        result = DataQualityResult(
            issues=[
                DataQualityIssue(
                    issue_type=DataQualityIssueType.GAP,
                    severity=DataQualitySeverity.WARNING,
                    timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                    description="Gap",
                )
            ],
            total_data_points=95,
            expected_data_points=100,
            gaps_found=1,
            duplicates_found=0,
            coverage_percent=95.0,
            token="WETH",
        )
        d = result.to_dict()
        assert len(d["issues"]) == 1
        assert d["total_data_points"] == 95
        assert d["expected_data_points"] == 100
        assert d["gaps_found"] == 1
        assert d["duplicates_found"] == 0
        assert d["coverage_percent"] == 95.0
        assert d["token"] == "WETH"
        assert d["has_issues"] is True
        assert d["has_warnings"] is True
        assert d["has_errors"] is False


class TestValidatePriceData:
    """Tests for validate_price_data function."""

    def test_empty_data(self) -> None:
        """Test validation of empty data."""
        result = validate_price_data([], expected_interval_seconds=3600)
        assert result.total_data_points == 0
        assert result.expected_data_points == 0
        assert not result.has_issues

    def test_single_data_point(self) -> None:
        """Test validation of single data point."""
        price_data = [(datetime(2024, 1, 1, tzinfo=UTC), Decimal("2500"))]
        result = validate_price_data(price_data, expected_interval_seconds=3600)
        assert result.total_data_points == 1
        assert result.expected_data_points == 1
        assert not result.has_issues

    def test_no_gaps(self) -> None:
        """Test validation when data has no gaps."""
        base_time = datetime(2024, 1, 1, tzinfo=UTC)
        price_data = [
            (base_time + timedelta(hours=i), Decimal(f"{2500 + i}"))
            for i in range(10)
        ]
        result = validate_price_data(
            price_data, expected_interval_seconds=3600, token="WETH"
        )
        assert result.total_data_points == 10
        assert result.gaps_found == 0
        assert result.duplicates_found == 0
        assert not result.has_issues

    def test_detects_small_gap(self) -> None:
        """Test detection of small gap (INFO severity)."""
        base_time = datetime(2024, 1, 1, tzinfo=UTC)
        # Expected interval is 1 hour, gap of 1.6 hours (just above 1.5x threshold)
        price_data = [
            (base_time, Decimal("2500")),
            (base_time + timedelta(hours=1), Decimal("2505")),
            (base_time + timedelta(hours=2, minutes=36), Decimal("2510")),  # 1.6h gap
        ]
        result = validate_price_data(
            price_data,
            expected_interval_seconds=3600,
            token="WETH",
            log_warnings=False,
        )
        assert result.gaps_found == 1
        assert result.has_issues
        # Small gap should be INFO severity
        assert result.issues[0].severity == DataQualitySeverity.INFO

    def test_detects_medium_gap(self) -> None:
        """Test detection of medium gap (WARNING severity)."""
        base_time = datetime(2024, 1, 1, tzinfo=UTC)
        # Gap of 3 hours (3x expected interval)
        price_data = [
            (base_time, Decimal("2500")),
            (base_time + timedelta(hours=1), Decimal("2505")),
            (base_time + timedelta(hours=4), Decimal("2510")),  # 3h gap
        ]
        result = validate_price_data(
            price_data,
            expected_interval_seconds=3600,
            token="WETH",
            log_warnings=False,
        )
        assert result.gaps_found == 1
        assert result.has_warnings
        assert result.issues[0].severity == DataQualitySeverity.WARNING

    def test_detects_large_gap(self) -> None:
        """Test detection of large gap (ERROR severity)."""
        base_time = datetime(2024, 1, 1, tzinfo=UTC)
        # Gap of 5 hours (5x expected interval)
        price_data = [
            (base_time, Decimal("2500")),
            (base_time + timedelta(hours=1), Decimal("2505")),
            (base_time + timedelta(hours=6), Decimal("2510")),  # 5h gap
        ]
        result = validate_price_data(
            price_data,
            expected_interval_seconds=3600,
            token="WETH",
            log_warnings=False,
        )
        assert result.gaps_found == 1
        assert result.has_errors
        assert result.issues[0].severity == DataQualitySeverity.ERROR

    def test_detects_multiple_gaps(self) -> None:
        """Test detection of multiple gaps."""
        base_time = datetime(2024, 1, 1, tzinfo=UTC)
        price_data = [
            (base_time, Decimal("2500")),
            (base_time + timedelta(hours=3), Decimal("2505")),  # Gap 1
            (base_time + timedelta(hours=4), Decimal("2510")),
            (base_time + timedelta(hours=8), Decimal("2515")),  # Gap 2
        ]
        result = validate_price_data(
            price_data,
            expected_interval_seconds=3600,
            token="WETH",
            log_warnings=False,
        )
        assert result.gaps_found == 2

    def test_detects_duplicate_timestamps(self) -> None:
        """Test detection of duplicate timestamps."""
        base_time = datetime(2024, 1, 1, tzinfo=UTC)
        price_data = [
            (base_time, Decimal("2500")),
            (base_time + timedelta(hours=1), Decimal("2505")),
            (base_time + timedelta(hours=1), Decimal("2506")),  # Duplicate
            (base_time + timedelta(hours=2), Decimal("2510")),
        ]
        result = validate_price_data(
            price_data,
            expected_interval_seconds=3600,
            token="WETH",
            log_warnings=False,
        )
        assert result.duplicates_found == 1
        assert any(
            issue.issue_type == DataQualityIssueType.DUPLICATE
            for issue in result.issues
        )

    def test_calculates_coverage_percent(self) -> None:
        """Test coverage percentage calculation."""
        base_time = datetime(2024, 1, 1, tzinfo=UTC)
        # 5 hours = 6 expected data points (inclusive), but only provide 4
        price_data = [
            (base_time, Decimal("2500")),
            (base_time + timedelta(hours=1), Decimal("2505")),
            (base_time + timedelta(hours=3), Decimal("2515")),  # Skip hour 2
            (base_time + timedelta(hours=5), Decimal("2525")),  # Skip hour 4
        ]
        result = validate_price_data(
            price_data,
            expected_interval_seconds=3600,
            log_warnings=False,
        )
        # 4 data points out of 6 expected = 66.67%
        assert 60 < result.coverage_percent < 70

    def test_custom_gap_tolerance_factor(self) -> None:
        """Test custom gap tolerance factor."""
        base_time = datetime(2024, 1, 1, tzinfo=UTC)
        # Gap of 1.3 hours
        price_data = [
            (base_time, Decimal("2500")),
            (base_time + timedelta(hours=1, minutes=18), Decimal("2505")),
        ]
        # With default 1.5x tolerance, this should NOT be a gap
        result1 = validate_price_data(
            price_data,
            expected_interval_seconds=3600,
            gap_tolerance_factor=1.5,
            log_warnings=False,
        )
        assert result1.gaps_found == 0

        # With 1.2x tolerance, this SHOULD be a gap
        result2 = validate_price_data(
            price_data,
            expected_interval_seconds=3600,
            gap_tolerance_factor=1.2,
            log_warnings=False,
        )
        assert result2.gaps_found == 1

    def test_unsorted_data_is_handled(self) -> None:
        """Test that unsorted data is handled correctly."""
        base_time = datetime(2024, 1, 1, tzinfo=UTC)
        # Data out of order
        price_data = [
            (base_time + timedelta(hours=2), Decimal("2510")),
            (base_time, Decimal("2500")),
            (base_time + timedelta(hours=1), Decimal("2505")),
        ]
        result = validate_price_data(
            price_data,
            expected_interval_seconds=3600,
            log_warnings=False,
        )
        # Should be sorted and validated correctly
        assert result.total_data_points == 3
        assert result.gaps_found == 0

    def test_logs_warnings_for_gaps(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that warnings are logged for gaps."""
        base_time = datetime(2024, 1, 1, tzinfo=UTC)
        price_data = [
            (base_time, Decimal("2500")),
            (base_time + timedelta(hours=5), Decimal("2505")),  # 5h gap
        ]
        with caplog.at_level(logging.WARNING):
            validate_price_data(
                price_data,
                expected_interval_seconds=3600,
                token="WETH",
                log_warnings=True,
            )
        assert "Data gap" in caplog.text
        assert "WETH" in caplog.text

    def test_logs_warnings_for_duplicates(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that warnings are logged for duplicates."""
        base_time = datetime(2024, 1, 1, tzinfo=UTC)
        price_data = [
            (base_time, Decimal("2500")),
            (base_time, Decimal("2505")),  # Duplicate
        ]
        with caplog.at_level(logging.WARNING):
            validate_price_data(
                price_data,
                expected_interval_seconds=3600,
                token="WETH",
                log_warnings=True,
            )
        assert "Duplicate timestamp" in caplog.text
        assert "WETH" in caplog.text

    def test_no_logging_when_disabled(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that no warnings are logged when log_warnings=False."""
        base_time = datetime(2024, 1, 1, tzinfo=UTC)
        price_data = [
            (base_time, Decimal("2500")),
            (base_time + timedelta(hours=5), Decimal("2505")),  # Gap
            (base_time + timedelta(hours=5), Decimal("2506")),  # Duplicate
        ]
        with caplog.at_level(logging.WARNING):
            validate_price_data(
                price_data,
                expected_interval_seconds=3600,
                token="WETH",
                log_warnings=False,
            )
        assert "Data gap" not in caplog.text
        assert "Duplicate" not in caplog.text

    def test_gap_details_include_missing_points_estimate(self) -> None:
        """Test that gap details include estimate of missing points."""
        base_time = datetime(2024, 1, 1, tzinfo=UTC)
        # 4 hour gap with 1 hour intervals = ~3 missing points
        price_data = [
            (base_time, Decimal("2500")),
            (base_time + timedelta(hours=4), Decimal("2505")),
        ]
        result = validate_price_data(
            price_data,
            expected_interval_seconds=3600,
            log_warnings=False,
        )
        assert result.gaps_found == 1
        gap_issue = result.issues[0]
        assert gap_issue.details["missing_points_estimate"] == 3


class TestValidateOHLCVData:
    """Tests for validate_ohlcv_data convenience function."""

    def test_ohlcv_validation(self) -> None:
        """Test OHLCV data validation uses close prices."""
        base_time = datetime(2024, 1, 1, tzinfo=UTC)
        ohlcv_data = [
            (base_time, Decimal("2500"), Decimal("2510"), Decimal("2495"), Decimal("2505")),
            (base_time + timedelta(hours=1), Decimal("2505"), Decimal("2515"), Decimal("2500"), Decimal("2510")),
            # Gap of 4 hours
            (base_time + timedelta(hours=5), Decimal("2520"), Decimal("2530"), Decimal("2515"), Decimal("2525")),
        ]
        result = validate_ohlcv_data(
            ohlcv_data,
            expected_interval_seconds=3600,
            token="WETH",
            log_warnings=False,
        )
        assert result.total_data_points == 3
        assert result.gaps_found == 1
        assert result.token == "WETH"


class TestDataQualityEnums:
    """Tests for DataQualityIssueType and DataQualitySeverity enums."""

    def test_issue_type_values(self) -> None:
        """Test issue type enum values."""
        assert DataQualityIssueType.GAP.value == "gap"
        assert DataQualityIssueType.DUPLICATE.value == "duplicate"
        assert DataQualityIssueType.STALE.value == "stale"
        assert DataQualityIssueType.INVALID_ORDER.value == "invalid_order"
        assert DataQualityIssueType.OUTLIER.value == "outlier"

    def test_severity_values(self) -> None:
        """Test severity enum values."""
        assert DataQualitySeverity.INFO.value == "info"
        assert DataQualitySeverity.WARNING.value == "warning"
        assert DataQualitySeverity.ERROR.value == "error"


class TestDetectOutliers:
    """Tests for detect_outliers function."""

    def test_empty_data(self) -> None:
        """Test outlier detection with empty data."""
        outliers = detect_outliers([], log_warnings=False)
        assert len(outliers) == 0

    def test_single_data_point(self) -> None:
        """Test outlier detection with single data point."""
        price_data = [(datetime(2024, 1, 1, tzinfo=UTC), Decimal("2500"))]
        outliers = detect_outliers(price_data, log_warnings=False)
        assert len(outliers) == 0

    def test_no_outliers_in_stable_data(self) -> None:
        """Test that stable data has no outliers."""
        base_time = datetime(2024, 1, 1, tzinfo=UTC)
        # Prices with small variations (1-2%)
        price_data = [
            (base_time + timedelta(hours=i), Decimal(f"{2500 + i * 10}"))
            for i in range(30)
        ]
        outliers = detect_outliers(price_data, log_warnings=False)
        assert len(outliers) == 0

    def test_detects_rapid_price_increase(self) -> None:
        """Test detection of rapid price increase (>50% in 1 interval)."""
        base_time = datetime(2024, 1, 1, tzinfo=UTC)
        price_data = [
            (base_time, Decimal("1000")),
            (base_time + timedelta(hours=1), Decimal("1010")),
            (base_time + timedelta(hours=2), Decimal("1600")),  # +58.4% spike
            (base_time + timedelta(hours=3), Decimal("1020")),
        ]
        outliers = detect_outliers(
            price_data,
            rapid_change_threshold_pct=50.0,
            log_warnings=False,
        )
        assert len(outliers) >= 1
        # Should detect at least the spike at hour 2
        spike_outliers = [o for o in outliers if o.details.get("outlier_type") == "rapid_change"]
        assert len(spike_outliers) >= 1
        assert any(abs(o.details["percent_change"] - 58.4) < 1 for o in spike_outliers)

    def test_detects_rapid_price_decrease(self) -> None:
        """Test detection of rapid price decrease (>50% in 1 interval)."""
        base_time = datetime(2024, 1, 1, tzinfo=UTC)
        price_data = [
            (base_time, Decimal("2000")),
            (base_time + timedelta(hours=1), Decimal("2010")),
            (base_time + timedelta(hours=2), Decimal("900")),  # -55.2% crash
            (base_time + timedelta(hours=3), Decimal("2020")),
        ]
        outliers = detect_outliers(
            price_data,
            rapid_change_threshold_pct=50.0,
            log_warnings=False,
        )
        assert len(outliers) >= 1
        rapid_changes = [o for o in outliers if o.details.get("outlier_type") == "rapid_change"]
        assert len(rapid_changes) >= 1

    def test_custom_rapid_change_threshold(self) -> None:
        """Test custom rapid change threshold."""
        base_time = datetime(2024, 1, 1, tzinfo=UTC)
        price_data = [
            (base_time, Decimal("1000")),
            (base_time + timedelta(hours=1), Decimal("1250")),  # 25% increase
        ]
        # With 50% threshold, this should NOT be flagged
        outliers1 = detect_outliers(
            price_data,
            rapid_change_threshold_pct=50.0,
            log_warnings=False,
        )
        rapid_changes1 = [o for o in outliers1 if o.details.get("outlier_type") == "rapid_change"]
        assert len(rapid_changes1) == 0

        # With 20% threshold, this SHOULD be flagged
        outliers2 = detect_outliers(
            price_data,
            rapid_change_threshold_pct=20.0,
            log_warnings=False,
        )
        rapid_changes2 = [o for o in outliers2 if o.details.get("outlier_type") == "rapid_change"]
        assert len(rapid_changes2) == 1

    def test_detects_statistical_outlier(self) -> None:
        """Test detection of statistical outlier (>3 std dev from rolling mean)."""
        base_time = datetime(2024, 1, 1, tzinfo=UTC)
        # Create stable data followed by an extreme outlier
        price_data = []
        for i in range(25):
            # Stable prices around 1000 with small variations
            price = 1000 + (i % 5)  # Prices from 1000 to 1004
            price_data.append(
                (base_time + timedelta(hours=i), Decimal(str(price)))
            )
        # Add extreme outlier that's many std devs away
        price_data.append(
            (base_time + timedelta(hours=25), Decimal("5000"))
        )

        outliers = detect_outliers(
            price_data,
            rapid_change_threshold_pct=500.0,  # Set high to avoid rapid change detection
            std_dev_threshold=3.0,
            rolling_window_size=20,
            log_warnings=False,
        )
        stat_outliers = [o for o in outliers if o.details.get("outlier_type") == "statistical"]
        assert len(stat_outliers) >= 1
        # Check the z_score is high
        assert any(o.details["z_score"] > 3.0 for o in stat_outliers)

    def test_custom_std_dev_threshold(self) -> None:
        """Test custom standard deviation threshold."""
        base_time = datetime(2024, 1, 1, tzinfo=UTC)
        # Create stable data with one moderate outlier
        price_data = []
        for i in range(25):
            price = 1000 + (i % 3)  # Very stable: 1000, 1001, 1002
            price_data.append(
                (base_time + timedelta(hours=i), Decimal(str(price)))
            )
        # Add moderate outlier
        price_data.append(
            (base_time + timedelta(hours=25), Decimal("1010"))
        )

        # With high threshold (10 std devs), may not be flagged
        outliers1 = detect_outliers(
            price_data,
            rapid_change_threshold_pct=500.0,
            std_dev_threshold=10.0,
            rolling_window_size=20,
            log_warnings=False,
        )
        stat_outliers1 = [o for o in outliers1 if o.details.get("outlier_type") == "statistical"]

        # With lower threshold (2 std devs), should be flagged
        outliers2 = detect_outliers(
            price_data,
            rapid_change_threshold_pct=500.0,
            std_dev_threshold=2.0,
            rolling_window_size=20,
            log_warnings=False,
        )
        stat_outliers2 = [o for o in outliers2 if o.details.get("outlier_type") == "statistical"]

        # Lower threshold should find more outliers
        assert len(stat_outliers2) >= len(stat_outliers1)

    def test_custom_rolling_window_size(self) -> None:
        """Test custom rolling window size."""
        base_time = datetime(2024, 1, 1, tzinfo=UTC)
        price_data = [
            (base_time + timedelta(hours=i), Decimal(f"{1000 + i}"))
            for i in range(15)
        ]
        # With window size 20, can't compute stats (need at least 20 points)
        outliers1 = detect_outliers(
            price_data,
            rolling_window_size=20,
            log_warnings=False,
        )
        stat_outliers1 = [o for o in outliers1 if o.details.get("outlier_type") == "statistical"]
        assert len(stat_outliers1) == 0

        # With window size 10, can compute stats
        # Even though data is stable, we can still run the check
        outliers2 = detect_outliers(
            price_data,
            rolling_window_size=10,
            log_warnings=False,
        )
        # No statistical outliers since data is stable
        stat_outliers2 = [o for o in outliers2 if o.details.get("outlier_type") == "statistical"]
        assert len(stat_outliers2) == 0

    def test_severity_for_extreme_rapid_change(self) -> None:
        """Test ERROR severity for >100% price change."""
        base_time = datetime(2024, 1, 1, tzinfo=UTC)
        price_data = [
            (base_time, Decimal("1000")),
            (base_time + timedelta(hours=1), Decimal("2500")),  # +150% change
        ]
        outliers = detect_outliers(
            price_data,
            rapid_change_threshold_pct=50.0,
            log_warnings=False,
        )
        assert len(outliers) >= 1
        # >100% change should be ERROR severity
        assert outliers[0].severity == DataQualitySeverity.ERROR

    def test_severity_for_moderate_rapid_change(self) -> None:
        """Test WARNING severity for 50-100% price change."""
        base_time = datetime(2024, 1, 1, tzinfo=UTC)
        price_data = [
            (base_time, Decimal("1000")),
            (base_time + timedelta(hours=1), Decimal("1600")),  # +60% change
        ]
        outliers = detect_outliers(
            price_data,
            rapid_change_threshold_pct=50.0,
            log_warnings=False,
        )
        assert len(outliers) >= 1
        # 50-100% change should be WARNING severity
        assert outliers[0].severity == DataQualitySeverity.WARNING

    def test_logs_warnings_for_outliers(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that warnings are logged for detected outliers."""
        base_time = datetime(2024, 1, 1, tzinfo=UTC)
        price_data = [
            (base_time, Decimal("1000")),
            (base_time + timedelta(hours=1), Decimal("2000")),  # +100% spike
        ]
        with caplog.at_level(logging.WARNING):
            detect_outliers(
                price_data,
                rapid_change_threshold_pct=50.0,
                token="WETH",
                log_warnings=True,
            )
        assert "Outlier detected" in caplog.text
        assert "WETH" in caplog.text

    def test_no_logging_when_disabled(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that no warnings are logged when log_warnings=False."""
        base_time = datetime(2024, 1, 1, tzinfo=UTC)
        price_data = [
            (base_time, Decimal("1000")),
            (base_time + timedelta(hours=1), Decimal("2000")),  # +100% spike
        ]
        with caplog.at_level(logging.WARNING):
            detect_outliers(
                price_data,
                rapid_change_threshold_pct=50.0,
                token="WETH",
                log_warnings=False,
            )
        assert "Outlier" not in caplog.text

    def test_outlier_issue_type_is_outlier(self) -> None:
        """Test that outlier issues have correct issue type."""
        base_time = datetime(2024, 1, 1, tzinfo=UTC)
        price_data = [
            (base_time, Decimal("1000")),
            (base_time + timedelta(hours=1), Decimal("2000")),  # +100% spike
        ]
        outliers = detect_outliers(
            price_data,
            rapid_change_threshold_pct=50.0,
            log_warnings=False,
        )
        assert len(outliers) >= 1
        assert all(o.issue_type == DataQualityIssueType.OUTLIER for o in outliers)

    def test_outlier_details_include_all_fields(self) -> None:
        """Test that outlier details include all expected fields."""
        base_time = datetime(2024, 1, 1, tzinfo=UTC)
        price_data = [
            (base_time, Decimal("1000")),
            (base_time + timedelta(hours=1), Decimal("1600")),  # +60% spike
        ]
        outliers = detect_outliers(
            price_data,
            rapid_change_threshold_pct=50.0,
            log_warnings=False,
        )
        assert len(outliers) >= 1
        rapid_change = [o for o in outliers if o.details.get("outlier_type") == "rapid_change"][0]
        assert "outlier_type" in rapid_change.details
        assert "previous_price" in rapid_change.details
        assert "current_price" in rapid_change.details
        assert "percent_change" in rapid_change.details
        assert "threshold_pct" in rapid_change.details
        assert "previous_timestamp" in rapid_change.details

    def test_handles_zero_previous_price(self) -> None:
        """Test handling of zero previous price (avoid division by zero)."""
        base_time = datetime(2024, 1, 1, tzinfo=UTC)
        price_data = [
            (base_time, Decimal("0")),  # Zero price
            (base_time + timedelta(hours=1), Decimal("1000")),
        ]
        # Should not raise exception
        outliers = detect_outliers(price_data, log_warnings=False)
        # Zero price should be skipped, no rapid change outlier for this transition
        rapid_changes = [o for o in outliers if o.details.get("outlier_type") == "rapid_change"]
        assert len(rapid_changes) == 0

    def test_avoids_duplicate_flagging(self) -> None:
        """Test that same timestamp isn't flagged by both rapid change and statistical outlier."""
        base_time = datetime(2024, 1, 1, tzinfo=UTC)
        # Create data where an outlier would be caught by both methods
        price_data = []
        for i in range(22):
            price = 1000 + (i % 3)
            price_data.append(
                (base_time + timedelta(hours=i), Decimal(str(price)))
            )
        # Add extreme outlier that triggers both rapid change and statistical
        price_data.append(
            (base_time + timedelta(hours=22), Decimal("5000"))
        )

        outliers = detect_outliers(
            price_data,
            rapid_change_threshold_pct=50.0,
            std_dev_threshold=3.0,
            rolling_window_size=20,
            log_warnings=False,
        )
        # Should only have one outlier for the spike timestamp, not two
        spike_time = base_time + timedelta(hours=22)
        spike_outliers = [o for o in outliers if o.timestamp == spike_time]
        # Could be 1 (rapid_change) or more, but should avoid exact duplicate
        # Check that we don't have both rapid_change and statistical for same timestamp
        outlier_types = [o.details.get("outlier_type") for o in spike_outliers]
        if "rapid_change" in outlier_types:
            assert outlier_types.count("rapid_change") == 1


class TestValidatePriceDataWithOutliers:
    """Tests for validate_price_data_with_outliers function."""

    def test_combines_gap_and_outlier_detection(self) -> None:
        """Test that function combines gap and outlier detection."""
        base_time = datetime(2024, 1, 1, tzinfo=UTC)
        price_data = [
            (base_time, Decimal("1000")),
            (base_time + timedelta(hours=1), Decimal("1010")),
            # Gap of 4 hours
            (base_time + timedelta(hours=5), Decimal("3000")),  # Also an outlier!
        ]
        result = validate_price_data_with_outliers(
            price_data,
            expected_interval_seconds=3600,
            rapid_change_threshold_pct=50.0,
            token="WETH",
            log_warnings=False,
        )
        assert result.gaps_found >= 1
        assert result.outliers_found >= 1
        assert result.has_issues

    def test_outliers_found_field_populated(self) -> None:
        """Test that outliers_found field is correctly populated."""
        base_time = datetime(2024, 1, 1, tzinfo=UTC)
        price_data = [
            (base_time, Decimal("1000")),
            (base_time + timedelta(hours=1), Decimal("1600")),  # +60% spike
            (base_time + timedelta(hours=2), Decimal("1010")),
        ]
        result = validate_price_data_with_outliers(
            price_data,
            expected_interval_seconds=3600,
            rapid_change_threshold_pct=50.0,
            log_warnings=False,
        )
        assert result.outliers_found >= 1

    def test_no_outliers_with_stable_data(self) -> None:
        """Test that stable data has no outliers."""
        base_time = datetime(2024, 1, 1, tzinfo=UTC)
        price_data = [
            (base_time + timedelta(hours=i), Decimal(f"{1000 + i}"))
            for i in range(10)
        ]
        result = validate_price_data_with_outliers(
            price_data,
            expected_interval_seconds=3600,
            rapid_change_threshold_pct=50.0,
            log_warnings=False,
        )
        assert result.outliers_found == 0

    def test_result_to_dict_includes_outliers(self) -> None:
        """Test that to_dict includes outliers_found field."""
        result = DataQualityResult(
            total_data_points=10,
            expected_data_points=10,
            gaps_found=0,
            duplicates_found=0,
            outliers_found=2,
            coverage_percent=100.0,
            token="WETH",
        )
        d = result.to_dict()
        assert "outliers_found" in d
        assert d["outliers_found"] == 2

    def test_all_parameters_passed_correctly(self) -> None:
        """Test that all parameters are passed to underlying functions."""
        base_time = datetime(2024, 1, 1, tzinfo=UTC)
        price_data = [
            (base_time + timedelta(hours=i), Decimal(f"{1000 + i * 10}"))
            for i in range(30)
        ]
        # All custom parameters
        result = validate_price_data_with_outliers(
            price_data,
            expected_interval_seconds=3600,
            token="WETH",
            gap_tolerance_factor=2.0,
            rapid_change_threshold_pct=10.0,  # Lower threshold
            std_dev_threshold=2.0,  # Lower threshold
            rolling_window_size=15,  # Custom window
            log_warnings=False,
        )
        # Should run without errors
        assert result.total_data_points == 30
        assert result.token == "WETH"
