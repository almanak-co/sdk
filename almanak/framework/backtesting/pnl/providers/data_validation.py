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


@dataclass(frozen=True)
class _OutlierPoint:
    timestamp: datetime
    price: Decimal
    price_float: float | None

    @property
    def is_valid(self) -> bool:
        return self.price_float is not None


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

        parts = _severity_summary_parts(_count_issues_by_severity(self.issues))
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


def _count_issues_by_severity(issues: list[DataQualityIssue]) -> dict[DataQualitySeverity, int]:
    return {severity: sum(1 for issue in issues if issue.severity == severity) for severity in DataQualitySeverity}


def _severity_summary_parts(counts: dict[DataQualitySeverity, int]) -> list[str]:
    parts: list[str] = []
    if counts[DataQualitySeverity.ERROR]:
        parts.append(f"{counts[DataQualitySeverity.ERROR]} error(s)")
    if counts[DataQualitySeverity.WARNING]:
        parts.append(f"{counts[DataQualitySeverity.WARNING]} warning(s)")
    if counts[DataQualitySeverity.INFO]:
        parts.append(f"{counts[DataQualitySeverity.INFO]} info")
    return parts


def validate_price_data(  # noqa: C901
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
    if expected_interval_seconds <= 0:
        raise ValueError("expected_interval_seconds must be positive")

    result = DataQualityResult(token=token)

    if not price_data:
        result.total_data_points = 0
        result.expected_data_points = 0
        result.coverage_percent = 0.0
        return result

    result.total_data_points = len(price_data)
    sorted_data = sorted(price_data, key=lambda x: x[0])
    issues = _detect_invalid_order(price_data, token, log_warnings)

    duplicate_issues, duplicates_found, unique_timestamps = _detect_duplicate_timestamps(
        sorted_data, token, log_warnings
    )
    gap_issues = _detect_gaps(sorted_data, expected_interval_seconds, gap_tolerance_factor, token, log_warnings)

    issues.extend(duplicate_issues)
    issues.extend(gap_issues)
    result.expected_data_points = _expected_data_points(sorted_data, expected_interval_seconds)
    result.duplicates_found = duplicates_found
    result.gaps_found = len(gap_issues)
    result.issues = issues
    result.coverage_percent = _coverage_percent(len(unique_timestamps), result.expected_data_points)

    # Log summary if issues were found
    if log_warnings and result.has_issues:
        logger.info(result.summary())

    return result


def _detect_invalid_order(
    price_data: list[tuple[datetime, Decimal]],
    token: str | None,
    log_warnings: bool,
) -> list[DataQualityIssue]:
    issues: list[DataQualityIssue] = []
    prev_timestamp: datetime | None = None
    for timestamp, _ in price_data:
        if prev_timestamp is not None and timestamp < prev_timestamp:
            issue = _invalid_order_issue(prev_timestamp, timestamp)
            issues.append(issue)
            if log_warnings:
                _log_invalid_order(token, prev_timestamp, timestamp)
        prev_timestamp = timestamp
    return issues


def _invalid_order_issue(prev_timestamp: datetime, timestamp: datetime) -> DataQualityIssue:
    gap = timestamp - prev_timestamp
    return DataQualityIssue(
        issue_type=DataQualityIssueType.INVALID_ORDER,
        severity=DataQualitySeverity.ERROR,
        timestamp=timestamp,
        description="Timestamp is before previous timestamp",
        details={
            "previous_timestamp": prev_timestamp.isoformat(),
            "gap_seconds": gap.total_seconds(),
        },
    )


def _log_invalid_order(token: str | None, prev_timestamp: datetime, timestamp: datetime) -> None:
    token_str = f" for {token}" if token else ""
    logger.error(f"Invalid timestamp order{token_str}: {timestamp.isoformat()} is before {prev_timestamp.isoformat()}")


def _detect_duplicate_timestamps(
    sorted_data: list[tuple[datetime, Decimal]],
    token: str | None,
    log_warnings: bool,
) -> tuple[list[DataQualityIssue], int, set[datetime]]:
    issues: list[DataQualityIssue] = []
    seen_timestamps: set[datetime] = set()
    occurrence_counts: dict[datetime, int] = {}

    for timestamp, _ in sorted_data:
        occurrence_counts[timestamp] = occurrence_counts.get(timestamp, 0) + 1
        if timestamp in seen_timestamps:
            issues.append(_duplicate_timestamp_issue(timestamp, occurrence_counts[timestamp]))
            if log_warnings:
                _log_duplicate_timestamp(token, timestamp)
        seen_timestamps.add(timestamp)

    return issues, len(issues), seen_timestamps


def _duplicate_timestamp_issue(timestamp: datetime, occurrence: int) -> DataQualityIssue:
    return DataQualityIssue(
        issue_type=DataQualityIssueType.DUPLICATE,
        severity=DataQualitySeverity.WARNING,
        timestamp=timestamp,
        description="Duplicate timestamp found",
        details={"occurrence": occurrence},
    )


def _log_duplicate_timestamp(token: str | None, timestamp: datetime) -> None:
    token_str = f" for {token}" if token else ""
    logger.warning(f"Duplicate timestamp{token_str}: {timestamp.isoformat()}")


def _detect_gaps(
    sorted_data: list[tuple[datetime, Decimal]],
    expected_interval_seconds: int,
    gap_tolerance_factor: float,
    token: str | None,
    log_warnings: bool,
) -> list[DataQualityIssue]:
    expected_interval = timedelta(seconds=expected_interval_seconds)
    max_allowed_gap = expected_interval * gap_tolerance_factor
    issues: list[DataQualityIssue] = []

    prev_timestamp: datetime | None = None
    for timestamp, _ in sorted_data:
        if prev_timestamp is not None:
            issue = _gap_issue(prev_timestamp, timestamp, expected_interval_seconds, max_allowed_gap)
            if issue is not None:
                issues.append(issue)
                if log_warnings:
                    _log_gap(token, issue)
        prev_timestamp = timestamp
    return issues


def _gap_issue(
    prev_timestamp: datetime,
    timestamp: datetime,
    expected_interval_seconds: int,
    max_allowed_gap: timedelta,
) -> DataQualityIssue | None:
    gap = timestamp - prev_timestamp
    if gap <= max_allowed_gap:
        return None

    gap_seconds = gap.total_seconds()
    missing_points = int(gap_seconds / expected_interval_seconds) - 1
    return DataQualityIssue(
        issue_type=DataQualityIssueType.GAP,
        severity=_gap_severity(gap_seconds, expected_interval_seconds),
        timestamp=prev_timestamp,
        description=(
            f"Gap of {gap_seconds:.0f}s detected "
            f"(expected {expected_interval_seconds}s, "
            f"~{missing_points} missing point(s))"
        ),
        details={
            "gap_start": prev_timestamp.isoformat(),
            "gap_end": timestamp.isoformat(),
            "gap_seconds": gap_seconds,
            "expected_interval_seconds": expected_interval_seconds,
            "missing_points_estimate": missing_points,
        },
    )


def _gap_severity(gap_seconds: float, expected_seconds: int) -> DataQualitySeverity:
    if gap_seconds > expected_seconds * 4:
        return DataQualitySeverity.ERROR
    if gap_seconds > expected_seconds * 2:
        return DataQualitySeverity.WARNING
    return DataQualitySeverity.INFO


def _log_gap(token: str | None, issue: DataQualityIssue) -> None:
    token_str = f" for {token}" if token else ""
    logger.warning(
        f"Data gap{token_str}: {issue.details['gap_seconds']:.0f}s gap "
        f"from {issue.details['gap_start']} to {issue.details['gap_end']} "
        f"(~{issue.details['missing_points_estimate']} missing point(s))"
    )


def _expected_data_points(sorted_data: list[tuple[datetime, Decimal]], expected_interval_seconds: int) -> int:
    if len(sorted_data) < 2:
        return len(sorted_data)
    time_range = (sorted_data[-1][0] - sorted_data[0][0]).total_seconds()
    return int(time_range / expected_interval_seconds) + 1


def _coverage_percent(unique_data_points: int, expected_data_points: int) -> float:
    if expected_data_points <= 0:
        return 100.0 if unique_data_points > 0 else 0.0
    return min((unique_data_points / expected_data_points) * 100, 100.0)


def detect_outliers(  # noqa: C901
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

    if rolling_window_size <= 0:
        raise ValueError("rolling_window_size must be positive")

    if not price_data or len(price_data) < 2:
        return issues

    # Sort by timestamp if not already sorted
    sorted_data = sorted(price_data, key=lambda x: x[0])
    points, issues = _build_outlier_points(sorted_data, token, log_warnings)
    issues.extend(
        _detect_rapid_change_outliers(
            points,
            rapid_change_threshold_pct=rapid_change_threshold_pct,
            token=token,
            log_warnings=log_warnings,
        )
    )
    issues.extend(
        _detect_statistical_outliers(
            points,
            std_dev_threshold=std_dev_threshold,
            rolling_window_size=rolling_window_size,
            token=token,
            log_warnings=log_warnings,
            existing_issues=issues,
        )
    )

    return issues


def _build_outlier_points(
    sorted_data: list[tuple[datetime, Decimal]],
    token: str | None,
    log_warnings: bool,
) -> tuple[list[_OutlierPoint], list[DataQualityIssue]]:
    points: list[_OutlierPoint] = []
    issues: list[DataQualityIssue] = []

    for timestamp, price in sorted_data:
        if _is_valid_price(price):
            points.append(_OutlierPoint(timestamp=timestamp, price=price, price_float=float(price)))
            continue
        points.append(_OutlierPoint(timestamp=timestamp, price=price, price_float=None))
        issue = _invalid_price_issue(timestamp, price)
        issues.append(issue)
        if log_warnings:
            _log_invalid_price(token, issue)

    return points, issues


def _is_valid_price(price: Decimal) -> bool:
    return price.is_finite() and price > Decimal("0")


def _invalid_price_issue(timestamp: datetime, price: Decimal) -> DataQualityIssue:
    return DataQualityIssue(
        issue_type=DataQualityIssueType.OUTLIER,
        severity=DataQualitySeverity.ERROR,
        timestamp=timestamp,
        description=f"Invalid price value: {price}",
        details={
            "outlier_type": "invalid_price",
            "price": str(price),
        },
    )


def _log_invalid_price(token: str | None, issue: DataQualityIssue) -> None:
    token_str = f" for {token}" if token else ""
    logger.warning(f"Invalid price{token_str}: {issue.details['price']} at {issue.timestamp.isoformat()}")


def _detect_rapid_change_outliers(
    points: list[_OutlierPoint],
    rapid_change_threshold_pct: float,
    token: str | None,
    log_warnings: bool,
) -> list[DataQualityIssue]:
    issues: list[DataQualityIssue] = []
    for i in range(1, len(points)):
        previous = points[i - 1]
        current = points[i]
        if previous.price_float is None or current.price_float is None:
            continue

        pct_change = abs((current.price_float - previous.price_float) / previous.price_float) * 100
        if pct_change > rapid_change_threshold_pct:
            issue = _rapid_change_issue(previous, current, pct_change, rapid_change_threshold_pct)
            issues.append(issue)
            if log_warnings:
                _log_rapid_change(token, issue)
    return issues


def _rapid_change_issue(
    previous: _OutlierPoint,
    current: _OutlierPoint,
    pct_change: float,
    rapid_change_threshold_pct: float,
) -> DataQualityIssue:
    assert previous.price_float is not None
    assert current.price_float is not None
    return DataQualityIssue(
        issue_type=DataQualityIssueType.OUTLIER,
        severity=DataQualitySeverity.ERROR if pct_change > 100 else DataQualitySeverity.WARNING,
        timestamp=current.timestamp,
        description=(
            f"Rapid price change: {pct_change:.1f}% change from {previous.price_float:.2f} to {current.price_float:.2f}"
        ),
        details={
            "outlier_type": "rapid_change",
            "previous_price": previous.price_float,
            "current_price": current.price_float,
            "percent_change": pct_change,
            "threshold_pct": rapid_change_threshold_pct,
            "previous_timestamp": previous.timestamp.isoformat(),
        },
    )


def _log_rapid_change(token: str | None, issue: DataQualityIssue) -> None:
    token_str = f" for {token}" if token else ""
    logger.warning(
        f"Outlier detected{token_str}: {issue.details['percent_change']:.1f}% price change "
        f"at {issue.timestamp.isoformat()} "
        f"(from {issue.details['previous_price']:.2f} to {issue.details['current_price']:.2f})"
    )


def _detect_statistical_outliers(
    points: list[_OutlierPoint],
    std_dev_threshold: float,
    rolling_window_size: int,
    token: str | None,
    log_warnings: bool,
    existing_issues: list[DataQualityIssue],
) -> list[DataQualityIssue]:
    if len(points) < rolling_window_size:
        return []

    issues: list[DataQualityIssue] = []
    for i in range(rolling_window_size, len(points)):
        current = points[i]
        window = points[i - rolling_window_size : i]
        stats = _window_stats(window)
        if current.price_float is None or stats is None:
            continue

        mean, std_dev = stats
        z_score = abs(current.price_float - mean) / std_dev
        if z_score <= std_dev_threshold or _has_rapid_change_issue(current.timestamp, existing_issues):
            continue

        issue = _statistical_outlier_issue(current, mean, std_dev, z_score, std_dev_threshold, rolling_window_size)
        issues.append(issue)
        if log_warnings:
            _log_statistical_outlier(token, issue)
    return issues


def _window_stats(window: list[_OutlierPoint]) -> tuple[float, float] | None:
    prices = [point.price_float for point in window]
    if any(price is None for price in prices):
        return None

    typed_prices = [price for price in prices if price is not None]
    mean = sum(typed_prices) / len(typed_prices)
    variance = sum((price - mean) ** 2 for price in typed_prices) / len(typed_prices)
    std_dev = variance**0.5
    if std_dev < 1e-10:
        return None
    return mean, std_dev


def _has_rapid_change_issue(timestamp: datetime, issues: list[DataQualityIssue]) -> bool:
    return any(issue.timestamp == timestamp and issue.details.get("outlier_type") == "rapid_change" for issue in issues)


def _statistical_outlier_issue(
    current: _OutlierPoint,
    mean: float,
    std_dev: float,
    z_score: float,
    std_dev_threshold: float,
    rolling_window_size: int,
) -> DataQualityIssue:
    assert current.price_float is not None
    return DataQualityIssue(
        issue_type=DataQualityIssueType.OUTLIER,
        severity=DataQualitySeverity.ERROR if z_score > 5 else DataQualitySeverity.WARNING,
        timestamp=current.timestamp,
        description=(
            f"Statistical outlier: price {current.price_float:.2f} is "
            f"{z_score:.1f} std devs from rolling mean {mean:.2f}"
        ),
        details={
            "outlier_type": "statistical",
            "price": current.price_float,
            "rolling_mean": mean,
            "rolling_std_dev": std_dev,
            "z_score": z_score,
            "threshold_std_dev": std_dev_threshold,
            "window_size": rolling_window_size,
        },
    )


def _log_statistical_outlier(token: str | None, issue: DataQualityIssue) -> None:
    token_str = f" for {token}" if token else ""
    logger.warning(
        f"Statistical outlier{token_str}: price {issue.details['price']:.2f} "
        f"at {issue.timestamp.isoformat()} is {issue.details['z_score']:.1f} "
        f"std devs from rolling mean {issue.details['rolling_mean']:.2f}"
    )


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
