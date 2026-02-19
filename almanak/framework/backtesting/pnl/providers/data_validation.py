"""Data quality validation for price data in PnL backtesting.

This module provides functions to validate price data quality before
running backtests. It detects common data quality issues like gaps,
duplicates, and suspicious price changes.

Key Components:
    - DataQualityIssue: Single data quality issue found during validation
    - DataQualityResult: Aggregated result of data quality validation
    - validate_price_data: Main function to validate price data series

Example:
    from almanak.framework.backtesting.pnl.providers.data_validation import (
        validate_price_data,
        DataQualityResult,
    )
    from datetime import datetime, timedelta

    # List of (timestamp, price) tuples
    price_data = [
        (datetime(2024, 1, 1, 0, 0), Decimal("2500.00")),
        (datetime(2024, 1, 1, 1, 0), Decimal("2505.00")),
        (datetime(2024, 1, 1, 2, 0), Decimal("2510.00")),
        # Gap: missing 3:00
        (datetime(2024, 1, 1, 4, 0), Decimal("2520.00")),
    ]

    result = validate_price_data(
        price_data,
        expected_interval_seconds=3600,
        token="WETH",
    )

    if result.has_issues:
        print(f"Found {len(result.issues)} data quality issues:")
        for issue in result.issues:
            print(f"  - {issue}")
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


class DataQualityIssueType(Enum):
    """Types of data quality issues that can be detected.

    Attributes:
        GAP: Missing data point(s) between expected timestamps
        DUPLICATE: Same timestamp appears multiple times
        STALE: Data is older than expected (outdated)
        INVALID_ORDER: Timestamps are not in ascending order
        OUTLIER: Suspicious price value that may be erroneous
    """

    GAP = "gap"
    DUPLICATE = "duplicate"
    STALE = "stale"
    INVALID_ORDER = "invalid_order"
    OUTLIER = "outlier"


class DataQualitySeverity(Enum):
    """Severity level of data quality issues.

    Attributes:
        INFO: Informational, may not affect results
        WARNING: May affect result accuracy
        ERROR: Likely to affect result accuracy significantly
    """

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


@dataclass
class DataQualityIssue:
    """Single data quality issue found during validation.

    Attributes:
        issue_type: Type of data quality issue
        severity: Severity level of the issue
        timestamp: Timestamp where the issue was detected
        description: Human-readable description of the issue
        details: Additional details about the issue
    """

    issue_type: DataQualityIssueType
    severity: DataQualitySeverity
    timestamp: datetime
    description: str
    details: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        """Return human-readable string representation."""
        return (
            f"[{self.severity.value.upper()}] {self.issue_type.value}: "
            f"{self.description} at {self.timestamp.isoformat()}"
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "issue_type": self.issue_type.value,
            "severity": self.severity.value,
            "timestamp": self.timestamp.isoformat(),
            "description": self.description,
            "details": self.details,
        }


@dataclass
class DataQualityResult:
    """Aggregated result of data quality validation.

    Contains all issues found during validation along with summary statistics.

    Attributes:
        issues: List of data quality issues found
        total_data_points: Total number of data points validated
        expected_data_points: Expected number of data points based on interval
        gaps_found: Number of gaps detected
        duplicates_found: Number of duplicate timestamps found
        outliers_found: Number of price outliers detected
        coverage_percent: Percentage of expected data points present
        token: Token symbol that was validated (optional)
    """

    issues: list[DataQualityIssue] = field(default_factory=list)
    total_data_points: int = 0
    expected_data_points: int = 0
    gaps_found: int = 0
    duplicates_found: int = 0
    outliers_found: int = 0
    coverage_percent: float = 100.0
    token: str | None = None

    @property
    def has_issues(self) -> bool:
        """Check if any issues were found."""
        return len(self.issues) > 0

    @property
    def has_warnings(self) -> bool:
        """Check if any warning-level or higher issues were found."""
        return any(issue.severity in (DataQualitySeverity.WARNING, DataQualitySeverity.ERROR) for issue in self.issues)

    @property
    def has_errors(self) -> bool:
        """Check if any error-level issues were found."""
        return any(issue.severity == DataQualitySeverity.ERROR for issue in self.issues)

    @property
    def issues_by_type(self) -> dict[DataQualityIssueType, list[DataQualityIssue]]:
        """Group issues by type."""
        result: dict[DataQualityIssueType, list[DataQualityIssue]] = {}
        for issue in self.issues:
            if issue.issue_type not in result:
                result[issue.issue_type] = []
            result[issue.issue_type].append(issue)
        return result

    @property
    def issues_by_severity(
        self,
    ) -> dict[DataQualitySeverity, list[DataQualityIssue]]:
        """Group issues by severity."""
        result: dict[DataQualitySeverity, list[DataQualityIssue]] = {}
        for issue in self.issues:
            if issue.severity not in result:
                result[issue.severity] = []
            result[issue.severity].append(issue)
        return result

    def summary(self) -> str:
        """Return a human-readable summary of validation results."""
        if not self.has_issues:
            return f"Data quality OK: {self.total_data_points} data points, {self.coverage_percent:.1f}% coverage"

        error_count = len([i for i in self.issues if i.severity == DataQualitySeverity.ERROR])
        warning_count = len([i for i in self.issues if i.severity == DataQualitySeverity.WARNING])
        info_count = len([i for i in self.issues if i.severity == DataQualitySeverity.INFO])

        parts = []
        if error_count:
            parts.append(f"{error_count} error(s)")
        if warning_count:
            parts.append(f"{warning_count} warning(s)")
        if info_count:
            parts.append(f"{info_count} info")

        token_str = f" for {self.token}" if self.token else ""
        return (
            f"Data quality issues{token_str}: {', '.join(parts)}. "
            f"{self.total_data_points}/{self.expected_data_points} data points "
            f"({self.coverage_percent:.1f}% coverage)"
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "issues": [issue.to_dict() for issue in self.issues],
            "total_data_points": self.total_data_points,
            "expected_data_points": self.expected_data_points,
            "gaps_found": self.gaps_found,
            "duplicates_found": self.duplicates_found,
            "outliers_found": self.outliers_found,
            "coverage_percent": self.coverage_percent,
            "token": self.token,
            "has_issues": self.has_issues,
            "has_warnings": self.has_warnings,
            "has_errors": self.has_errors,
        }


def validate_price_data(
    price_data: list[tuple[datetime, Decimal]],
    expected_interval_seconds: int,
    token: str | None = None,
    gap_tolerance_factor: float = 1.5,
    log_warnings: bool = True,
) -> DataQualityResult:
    """Validate price data for quality issues.

    Checks for:
    - Gaps: Missing data points (gaps larger than expected interval × tolerance)
    - Duplicates: Same timestamp appearing multiple times
    - Invalid ordering: Timestamps not in ascending order

    Args:
        price_data: List of (timestamp, price) tuples, should be sorted by timestamp
        expected_interval_seconds: Expected interval between data points in seconds
        token: Optional token symbol for logging context
        gap_tolerance_factor: Factor to multiply expected interval to determine
            when a gap is "too large". Default 1.5 means gaps >1.5x expected
            interval are flagged.
        log_warnings: Whether to log warnings for issues found. Default True.

    Returns:
        DataQualityResult containing all issues found and summary statistics

    Example:
        price_data = [
            (datetime(2024, 1, 1, 0, 0), Decimal("2500.00")),
            (datetime(2024, 1, 1, 1, 0), Decimal("2505.00")),
            (datetime(2024, 1, 1, 4, 0), Decimal("2520.00")),  # Gap!
        ]

        result = validate_price_data(
            price_data,
            expected_interval_seconds=3600,  # 1 hour
            token="WETH",
        )

        if result.gaps_found > 0:
            print(f"Found {result.gaps_found} gaps in data")
    """
    result = DataQualityResult(token=token)
    issues: list[DataQualityIssue] = []

    if not price_data:
        result.total_data_points = 0
        result.expected_data_points = 0
        result.coverage_percent = 0.0
        return result

    result.total_data_points = len(price_data)

    # Sort by timestamp if not already sorted
    sorted_data = sorted(price_data, key=lambda x: x[0])

    # Calculate expected data points based on time range
    if len(sorted_data) >= 2:
        time_range = (sorted_data[-1][0] - sorted_data[0][0]).total_seconds()
        result.expected_data_points = int(time_range / expected_interval_seconds) + 1
    else:
        result.expected_data_points = result.total_data_points

    # Check for duplicates using a set
    seen_timestamps: set[datetime] = set()
    duplicates_found = 0

    for timestamp, _ in sorted_data:
        if timestamp in seen_timestamps:
            duplicates_found += 1
            issues.append(
                DataQualityIssue(
                    issue_type=DataQualityIssueType.DUPLICATE,
                    severity=DataQualitySeverity.WARNING,
                    timestamp=timestamp,
                    description="Duplicate timestamp found",
                    details={"occurrence": duplicates_found + 1},
                )
            )
            if log_warnings:
                token_str = f" for {token}" if token else ""
                logger.warning(f"Duplicate timestamp{token_str}: {timestamp.isoformat()}")
        seen_timestamps.add(timestamp)

    result.duplicates_found = duplicates_found

    # Check for gaps between consecutive data points
    expected_interval = timedelta(seconds=expected_interval_seconds)
    max_allowed_gap = expected_interval * gap_tolerance_factor
    gaps_found = 0

    # Check ordering and gaps
    prev_timestamp: datetime | None = None
    for timestamp, _ in sorted_data:
        if prev_timestamp is not None:
            gap = timestamp - prev_timestamp

            # Check for invalid ordering (should not happen if sorted)
            if gap < timedelta(0):
                issues.append(
                    DataQualityIssue(
                        issue_type=DataQualityIssueType.INVALID_ORDER,
                        severity=DataQualitySeverity.ERROR,
                        timestamp=timestamp,
                        description="Timestamp is before previous timestamp",
                        details={
                            "previous_timestamp": prev_timestamp.isoformat(),
                            "gap_seconds": gap.total_seconds(),
                        },
                    )
                )
                if log_warnings:
                    token_str = f" for {token}" if token else ""
                    logger.error(
                        f"Invalid timestamp order{token_str}: "
                        f"{timestamp.isoformat()} is before {prev_timestamp.isoformat()}"
                    )

            # Check for gaps larger than expected
            elif gap > max_allowed_gap:
                gaps_found += 1
                gap_seconds = gap.total_seconds()
                expected_seconds = expected_interval_seconds
                missing_points = int(gap_seconds / expected_seconds) - 1

                # Determine severity based on gap size
                if gap_seconds > expected_seconds * 4:
                    severity = DataQualitySeverity.ERROR
                elif gap_seconds > expected_seconds * 2:
                    severity = DataQualitySeverity.WARNING
                else:
                    severity = DataQualitySeverity.INFO

                issues.append(
                    DataQualityIssue(
                        issue_type=DataQualityIssueType.GAP,
                        severity=severity,
                        timestamp=prev_timestamp,
                        description=(
                            f"Gap of {gap_seconds:.0f}s detected "
                            f"(expected {expected_seconds}s, "
                            f"~{missing_points} missing point(s))"
                        ),
                        details={
                            "gap_start": prev_timestamp.isoformat(),
                            "gap_end": timestamp.isoformat(),
                            "gap_seconds": gap_seconds,
                            "expected_interval_seconds": expected_seconds,
                            "missing_points_estimate": missing_points,
                        },
                    )
                )
                if log_warnings:
                    token_str = f" for {token}" if token else ""
                    logger.warning(
                        f"Data gap{token_str}: {gap_seconds:.0f}s gap "
                        f"from {prev_timestamp.isoformat()} to {timestamp.isoformat()} "
                        f"(~{missing_points} missing point(s))"
                    )

        prev_timestamp = timestamp

    result.gaps_found = gaps_found
    result.issues = issues

    # Calculate coverage percentage
    if result.expected_data_points > 0:
        result.coverage_percent = (result.total_data_points / result.expected_data_points) * 100
    else:
        result.coverage_percent = 100.0 if result.total_data_points > 0 else 0.0

    # Log summary if issues were found
    if log_warnings and result.has_issues:
        logger.info(result.summary())

    return result


def detect_outliers(
    price_data: list[tuple[datetime, Decimal]],
    rapid_change_threshold_pct: float = 50.0,
    std_dev_threshold: float = 3.0,
    rolling_window_size: int = 20,
    token: str | None = None,
    log_warnings: bool = True,
) -> list[DataQualityIssue]:
    """Detect price outliers that may indicate bad data.

    This function detects two types of outliers:
    1. Rapid price changes: Price changes exceeding a threshold percentage
       between consecutive data points (default: >50% change in 1 interval)
    2. Statistical outliers: Prices that deviate more than a threshold number
       of standard deviations from a rolling mean (default: >3 std dev)

    Args:
        price_data: List of (timestamp, price) tuples, should be sorted by timestamp
        rapid_change_threshold_pct: Maximum allowed percentage change between
            consecutive data points. Default 50.0 means >50% change is flagged.
        std_dev_threshold: Number of standard deviations from rolling mean
            to consider a price an outlier. Default 3.0.
        rolling_window_size: Number of data points for calculating rolling
            statistics. Default 20.
        token: Optional token symbol for logging context
        log_warnings: Whether to log warnings for outliers found. Default True.

    Returns:
        List of DataQualityIssue objects for each outlier detected

    Example:
        price_data = [
            (datetime(2024, 1, 1, 0, 0), Decimal("2500.00")),
            (datetime(2024, 1, 1, 1, 0), Decimal("2505.00")),
            (datetime(2024, 1, 1, 2, 0), Decimal("5000.00")),  # Outlier!
            (datetime(2024, 1, 1, 3, 0), Decimal("2510.00")),
        ]

        outliers = detect_outliers(price_data, token="WETH")
        for outlier in outliers:
            print(f"Outlier: {outlier}")
    """
    issues: list[DataQualityIssue] = []

    if not price_data or len(price_data) < 2:
        return issues

    # Sort by timestamp if not already sorted
    sorted_data = sorted(price_data, key=lambda x: x[0])
    prices = [float(price) for _, price in sorted_data]
    timestamps = [ts for ts, _ in sorted_data]

    # 1. Detect rapid price changes (>threshold% change between consecutive points)
    for i in range(1, len(prices)):
        prev_price = prices[i - 1]
        curr_price = prices[i]

        # Skip if previous price is zero (can't calculate percentage change)
        if prev_price == 0:
            continue

        pct_change = abs((curr_price - prev_price) / prev_price) * 100

        if pct_change > rapid_change_threshold_pct:
            # Determine severity based on magnitude
            if pct_change > 100:
                severity = DataQualitySeverity.ERROR
            elif pct_change > rapid_change_threshold_pct:
                severity = DataQualitySeverity.WARNING
            else:
                severity = DataQualitySeverity.INFO

            issue = DataQualityIssue(
                issue_type=DataQualityIssueType.OUTLIER,
                severity=severity,
                timestamp=timestamps[i],
                description=(f"Rapid price change: {pct_change:.1f}% change from {prev_price:.2f} to {curr_price:.2f}"),
                details={
                    "outlier_type": "rapid_change",
                    "previous_price": prev_price,
                    "current_price": curr_price,
                    "percent_change": pct_change,
                    "threshold_pct": rapid_change_threshold_pct,
                    "previous_timestamp": timestamps[i - 1].isoformat(),
                },
            )
            issues.append(issue)

            if log_warnings:
                token_str = f" for {token}" if token else ""
                logger.warning(
                    f"Outlier detected{token_str}: {pct_change:.1f}% price change "
                    f"at {timestamps[i].isoformat()} "
                    f"(from {prev_price:.2f} to {curr_price:.2f})"
                )

    # 2. Detect statistical outliers (>N standard deviations from rolling mean)
    if len(prices) >= rolling_window_size:
        # Calculate rolling mean and standard deviation
        for i in range(rolling_window_size, len(prices)):
            window = prices[i - rolling_window_size : i]
            mean = sum(window) / len(window)
            variance = sum((x - mean) ** 2 for x in window) / len(window)
            std_dev = variance**0.5

            # Skip if standard deviation is too small (all same values)
            if std_dev < 1e-10:
                continue

            curr_price = prices[i]
            z_score = abs(curr_price - mean) / std_dev

            if z_score > std_dev_threshold:
                # Determine severity based on z-score
                if z_score > 5:
                    severity = DataQualitySeverity.ERROR
                elif z_score > std_dev_threshold:
                    severity = DataQualitySeverity.WARNING
                else:
                    severity = DataQualitySeverity.INFO

                # Check if this timestamp already has a rapid change issue
                # to avoid duplicate reporting
                timestamp_already_flagged = any(
                    iss.timestamp == timestamps[i] and iss.details.get("outlier_type") == "rapid_change"
                    for iss in issues
                )

                if not timestamp_already_flagged:
                    issue = DataQualityIssue(
                        issue_type=DataQualityIssueType.OUTLIER,
                        severity=severity,
                        timestamp=timestamps[i],
                        description=(
                            f"Statistical outlier: price {curr_price:.2f} is "
                            f"{z_score:.1f} std devs from rolling mean {mean:.2f}"
                        ),
                        details={
                            "outlier_type": "statistical",
                            "price": curr_price,
                            "rolling_mean": mean,
                            "rolling_std_dev": std_dev,
                            "z_score": z_score,
                            "threshold_std_dev": std_dev_threshold,
                            "window_size": rolling_window_size,
                        },
                    )
                    issues.append(issue)

                    if log_warnings:
                        token_str = f" for {token}" if token else ""
                        logger.warning(
                            f"Statistical outlier{token_str}: price {curr_price:.2f} "
                            f"at {timestamps[i].isoformat()} is {z_score:.1f} "
                            f"std devs from rolling mean {mean:.2f}"
                        )

    return issues


def validate_price_data_with_outliers(
    price_data: list[tuple[datetime, Decimal]],
    expected_interval_seconds: int,
    token: str | None = None,
    gap_tolerance_factor: float = 1.5,
    rapid_change_threshold_pct: float = 50.0,
    std_dev_threshold: float = 3.0,
    rolling_window_size: int = 20,
    log_warnings: bool = True,
) -> DataQualityResult:
    """Validate price data for quality issues including outlier detection.

    This is a convenience function that combines validate_price_data() with
    detect_outliers() for comprehensive data quality validation.

    Args:
        price_data: List of (timestamp, price) tuples, should be sorted by timestamp
        expected_interval_seconds: Expected interval between data points in seconds
        token: Optional token symbol for logging context
        gap_tolerance_factor: Factor for gap detection threshold. Default 1.5.
        rapid_change_threshold_pct: Max allowed % change between points. Default 50.0.
        std_dev_threshold: Std devs from rolling mean for outlier. Default 3.0.
        rolling_window_size: Window size for rolling statistics. Default 20.
        log_warnings: Whether to log warnings. Default True.

    Returns:
        DataQualityResult containing all issues found including outliers

    Example:
        result = validate_price_data_with_outliers(
            price_data,
            expected_interval_seconds=3600,
            token="WETH",
        )

        if result.outliers_found > 0:
            print(f"Found {result.outliers_found} price outliers")
    """
    # First, run standard validation
    result = validate_price_data(
        price_data,
        expected_interval_seconds,
        token=token,
        gap_tolerance_factor=gap_tolerance_factor,
        log_warnings=log_warnings,
    )

    # Then detect outliers
    outlier_issues = detect_outliers(
        price_data,
        rapid_change_threshold_pct=rapid_change_threshold_pct,
        std_dev_threshold=std_dev_threshold,
        rolling_window_size=rolling_window_size,
        token=token,
        log_warnings=log_warnings,
    )

    # Add outlier issues to result
    result.issues.extend(outlier_issues)
    result.outliers_found = len(outlier_issues)

    # Re-log summary if we added new issues
    if log_warnings and outlier_issues:
        logger.info(result.summary())

    return result


def validate_ohlcv_data(
    ohlcv_data: list[tuple[datetime, Decimal, Decimal, Decimal, Decimal]],
    expected_interval_seconds: int,
    token: str | None = None,
    gap_tolerance_factor: float = 1.5,
    log_warnings: bool = True,
) -> DataQualityResult:
    """Validate OHLCV data for quality issues.

    This is a convenience wrapper around validate_price_data that accepts
    OHLCV tuples (timestamp, open, high, low, close) and uses the close
    price for validation.

    Args:
        ohlcv_data: List of (timestamp, open, high, low, close) tuples
        expected_interval_seconds: Expected interval between data points
        token: Optional token symbol for logging context
        gap_tolerance_factor: Factor for gap detection threshold
        log_warnings: Whether to log warnings

    Returns:
        DataQualityResult containing all issues found

    Example:
        ohlcv_data = [
            (datetime(2024, 1, 1, 0, 0), Decimal("2500"), Decimal("2510"),
             Decimal("2495"), Decimal("2505")),
            ...
        ]
        result = validate_ohlcv_data(ohlcv_data, 3600, token="WETH")
    """
    # Extract (timestamp, close_price) pairs
    price_data = [(ts, close) for ts, _, _, _, close in ohlcv_data]
    return validate_price_data(
        price_data,
        expected_interval_seconds,
        token=token,
        gap_tolerance_factor=gap_tolerance_factor,
        log_warnings=log_warnings,
    )


__all__ = [
    "DataQualityIssue",
    "DataQualityIssueType",
    "DataQualityResult",
    "DataQualitySeverity",
    "detect_outliers",
    "validate_ohlcv_data",
    "validate_price_data",
    "validate_price_data_with_outliers",
]
