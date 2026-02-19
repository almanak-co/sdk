"""A/B Testing Framework for strategy variant comparison.

This module provides A/B testing functionality that allows:
- Creating tests with two strategy variants and configurable split ratios
- Allocating capital according to split ratio
- Tracking performance metrics for each variant
- Statistical comparison of variant performance
- Ending tests and selecting winners

Usage:
    from almanak.framework.testing.ab_test import ABTestManager, ABTestConfig

    # Create an A/B test manager
    manager = ABTestManager(strategy_id="my_strategy")

    # Create a new A/B test
    result = manager.create_ab_test(
        variant_a="v_baseline",
        variant_b="v_experimental",
        split_ratio=0.5,  # 50/50 split
        total_capital_usd=Decimal("100000"),
    )

    # Update metrics as variants run
    manager.update_variant_metrics("a", pnl_usd=Decimal("100"), trades=5)
    manager.update_variant_metrics("b", pnl_usd=Decimal("150"), trades=5)

    # Get comparison with statistical analysis
    comparison = manager.compare()

    # End test and select winner
    winner = manager.end_test(select_winner="variant_b")
"""

import logging
import math
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from ..api.timeline import TimelineEvent, TimelineEventType, add_event
from ..models.strategy_version import PerformanceMetrics

logger = logging.getLogger(__name__)


class ABTestStatus(StrEnum):
    """Status of an A/B test."""

    # Test not yet started
    PENDING = "PENDING"

    # Test is actively running
    RUNNING = "RUNNING"

    # Test is completed, winner selected
    COMPLETED = "COMPLETED"

    # Test was cancelled without selecting a winner
    CANCELLED = "CANCELLED"

    # Test reached inconclusive results
    INCONCLUSIVE = "INCONCLUSIVE"


class ABTestEventType(StrEnum):
    """Types of A/B test-specific events."""

    AB_TEST_CREATED = "AB_TEST_CREATED"
    AB_TEST_STARTED = "AB_TEST_STARTED"
    AB_TEST_METRICS_UPDATED = "AB_TEST_METRICS_UPDATED"
    AB_TEST_COMPARISON_UPDATED = "AB_TEST_COMPARISON_UPDATED"
    AB_TEST_ENDED = "AB_TEST_ENDED"
    AB_TEST_WINNER_SELECTED = "AB_TEST_WINNER_SELECTED"
    AB_TEST_CANCELLED = "AB_TEST_CANCELLED"


@dataclass
class ABTestConfig:
    """Configuration for an A/B test.

    Attributes:
        split_ratio: Proportion of capital allocated to variant A (0-1).
                     Variant B gets (1 - split_ratio).
        min_sample_size: Minimum number of trades per variant for statistical validity
        confidence_level: Statistical confidence level (e.g., 0.95 for 95%)
        emit_events: Whether to emit timeline events
        auto_end_on_significance: Automatically end test when statistically significant
        max_duration_hours: Maximum test duration in hours (0 = no limit)
    """

    split_ratio: float = 0.5
    min_sample_size: int = 30
    confidence_level: float = 0.95
    emit_events: bool = True
    auto_end_on_significance: bool = False
    max_duration_hours: int = 0

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        if not 0.0 < self.split_ratio < 1.0:
            raise ValueError(f"split_ratio must be between 0 and 1 (exclusive), got {self.split_ratio}")
        if self.min_sample_size < 1:
            raise ValueError(f"min_sample_size must be at least 1, got {self.min_sample_size}")
        if not 0.5 <= self.confidence_level < 1.0:
            raise ValueError(f"confidence_level must be between 0.5 and 1.0, got {self.confidence_level}")
        if self.max_duration_hours < 0:
            raise ValueError(f"max_duration_hours cannot be negative, got {self.max_duration_hours}")

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "split_ratio": self.split_ratio,
            "min_sample_size": self.min_sample_size,
            "confidence_level": self.confidence_level,
            "emit_events": self.emit_events,
            "auto_end_on_significance": self.auto_end_on_significance,
            "max_duration_hours": self.max_duration_hours,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ABTestConfig":
        """Create from dictionary."""
        return cls(
            split_ratio=data.get("split_ratio", 0.5),
            min_sample_size=data.get("min_sample_size", 30),
            confidence_level=data.get("confidence_level", 0.95),
            emit_events=data.get("emit_events", True),
            auto_end_on_significance=data.get("auto_end_on_significance", False),
            max_duration_hours=data.get("max_duration_hours", 0),
        )


@dataclass
class VariantMetrics:
    """Performance metrics tracked for a single variant.

    Extends PerformanceMetrics with variant-specific tracking.

    Attributes:
        variant_id: The version ID this variant represents
        variant_name: Human-readable name ("A" or "B")
        capital_allocated_usd: Capital allocated to this variant
        metrics: The underlying performance metrics
        trade_count: Number of trades executed
        error_count: Number of errors encountered
        is_control: Whether this is the control (baseline) variant
        measurement_start: When metrics collection started
    """

    variant_id: str
    variant_name: str
    capital_allocated_usd: Decimal
    metrics: PerformanceMetrics
    trade_count: int = 0
    error_count: int = 0
    is_control: bool = False
    measurement_start: datetime | None = None

    # Running statistics for variance calculation
    _pnl_sum: Decimal = field(default=Decimal("0"), repr=False)
    _pnl_sum_squares: Decimal = field(default=Decimal("0"), repr=False)

    def __post_init__(self) -> None:
        """Set default values after initialization."""
        if self.measurement_start is None:
            self.measurement_start = datetime.now(UTC)

    @property
    def error_rate(self) -> Decimal:
        """Calculate the error rate."""
        if self.trade_count == 0:
            return Decimal("0")
        return Decimal(str(self.error_count)) / Decimal(str(self.trade_count))

    @property
    def duration_seconds(self) -> int:
        """Get the duration of metrics collection."""
        if not self.measurement_start:
            return 0
        return int((datetime.now(UTC) - self.measurement_start).total_seconds())

    @property
    def average_pnl_per_trade(self) -> Decimal:
        """Calculate average PnL per trade."""
        if self.trade_count == 0:
            return Decimal("0")
        return self.metrics.net_pnl_usd / Decimal(str(self.trade_count))

    @property
    def pnl_variance(self) -> Decimal:
        """Calculate PnL variance using Welford's online algorithm."""
        if self.trade_count < 2:
            return Decimal("0")
        n = Decimal(str(self.trade_count))
        mean = self._pnl_sum / n
        # Variance = (sum_squares - n * mean^2) / (n - 1)
        variance = (self._pnl_sum_squares - n * mean * mean) / (n - Decimal("1"))
        return max(Decimal("0"), variance)  # Ensure non-negative

    @property
    def pnl_std_dev(self) -> Decimal:
        """Calculate PnL standard deviation."""
        variance = self.pnl_variance
        if variance == Decimal("0"):
            return Decimal("0")
        # Use float for sqrt, then convert back
        return Decimal(str(math.sqrt(float(variance))))

    def record_trade_pnl(self, pnl_usd: Decimal) -> None:
        """Record a trade PnL for variance tracking.

        Args:
            pnl_usd: PnL from a single trade
        """
        self._pnl_sum += pnl_usd
        self._pnl_sum_squares += pnl_usd * pnl_usd

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "variant_id": self.variant_id,
            "variant_name": self.variant_name,
            "capital_allocated_usd": str(self.capital_allocated_usd),
            "metrics": self.metrics.to_dict(),
            "trade_count": self.trade_count,
            "error_count": self.error_count,
            "is_control": self.is_control,
            "measurement_start": self.measurement_start.isoformat() if self.measurement_start else None,
            "error_rate": str(self.error_rate),
            "duration_seconds": self.duration_seconds,
            "average_pnl_per_trade": str(self.average_pnl_per_trade),
            "pnl_variance": str(self.pnl_variance),
            "pnl_std_dev": str(self.pnl_std_dev),
            "_pnl_sum": str(self._pnl_sum),
            "_pnl_sum_squares": str(self._pnl_sum_squares),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "VariantMetrics":
        """Create from dictionary."""
        instance = cls(
            variant_id=data["variant_id"],
            variant_name=data["variant_name"],
            capital_allocated_usd=Decimal(data["capital_allocated_usd"]),
            metrics=PerformanceMetrics.from_dict(data["metrics"]),
            trade_count=data.get("trade_count", 0),
            error_count=data.get("error_count", 0),
            is_control=data.get("is_control", False),
            measurement_start=datetime.fromisoformat(data["measurement_start"])
            if data.get("measurement_start")
            else None,
        )
        # Restore running statistics
        instance._pnl_sum = Decimal(data.get("_pnl_sum", "0"))
        instance._pnl_sum_squares = Decimal(data.get("_pnl_sum_squares", "0"))
        return instance


@dataclass
class StatisticalResult:
    """Result of statistical comparison between variants.

    Uses Welch's t-test for comparing means with unequal variances.

    Attributes:
        t_statistic: The t-statistic from Welch's t-test
        degrees_of_freedom: Degrees of freedom for the t-test
        p_value: Two-tailed p-value
        confidence_interval_lower: Lower bound of confidence interval for difference
        confidence_interval_upper: Upper bound of confidence interval for difference
        is_significant: Whether the difference is statistically significant
        effect_size: Cohen's d effect size (if calculable)
    """

    t_statistic: float
    degrees_of_freedom: float
    p_value: float
    confidence_interval_lower: float
    confidence_interval_upper: float
    is_significant: bool
    effect_size: float | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "t_statistic": self.t_statistic,
            "degrees_of_freedom": self.degrees_of_freedom,
            "p_value": self.p_value,
            "confidence_interval_lower": self.confidence_interval_lower,
            "confidence_interval_upper": self.confidence_interval_upper,
            "is_significant": self.is_significant,
            "effect_size": self.effect_size,
        }


@dataclass
class VariantComparison:
    """Comparison results between variant A and variant B.

    Provides detailed comparison of performance metrics with
    statistical significance analysis.

    Attributes:
        variant_a_metrics: Metrics for variant A (control)
        variant_b_metrics: Metrics for variant B (treatment)
        mean_difference_usd: Difference in average PnL (B - A)
        relative_improvement: Percentage improvement of B over A
        pnl_statistical_result: Statistical analysis of PnL difference
        recommended_winner: Recommended winner based on analysis
        recommendation_reason: Explanation for the recommendation
        has_sufficient_data: Whether both variants have enough trades
    """

    variant_a_metrics: VariantMetrics
    variant_b_metrics: VariantMetrics
    mean_difference_usd: Decimal = Decimal("0")
    relative_improvement: Decimal | None = None
    pnl_statistical_result: StatisticalResult | None = None
    recommended_winner: str | None = None
    recommendation_reason: str = ""
    has_sufficient_data: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "variant_a_metrics": self.variant_a_metrics.to_dict(),
            "variant_b_metrics": self.variant_b_metrics.to_dict(),
            "mean_difference_usd": str(self.mean_difference_usd),
            "relative_improvement": str(self.relative_improvement) if self.relative_improvement is not None else None,
            "pnl_statistical_result": self.pnl_statistical_result.to_dict() if self.pnl_statistical_result else None,
            "recommended_winner": self.recommended_winner,
            "recommendation_reason": self.recommendation_reason,
            "has_sufficient_data": self.has_sufficient_data,
        }


@dataclass
class ABTest:
    """Represents an A/B test between two strategy variants.

    Tracks the full state of an ongoing or completed A/B test.

    Attributes:
        test_id: Unique identifier for this test
        strategy_id: Strategy being tested
        variant_a_id: Version ID of variant A (control)
        variant_b_id: Version ID of variant B (treatment)
        status: Current test status
        config: Test configuration
        created_at: When the test was created
        started_at: When the test started running
        ended_at: When the test ended
        variant_a_metrics: Performance metrics for variant A
        variant_b_metrics: Performance metrics for variant B
        total_capital_usd: Total capital across both variants
        winner: Selected winner ("variant_a" or "variant_b")
        comparison_history: History of comparisons made
    """

    test_id: str
    strategy_id: str
    variant_a_id: str
    variant_b_id: str
    status: ABTestStatus = ABTestStatus.PENDING
    config: ABTestConfig = field(default_factory=ABTestConfig)
    created_at: datetime | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    variant_a_metrics: VariantMetrics | None = None
    variant_b_metrics: VariantMetrics | None = None
    total_capital_usd: Decimal = Decimal("0")
    winner: str | None = None
    comparison_history: list[dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Generate test ID and set created_at if not provided."""
        if not self.test_id:
            ts = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
            self.test_id = f"abtest_{self.strategy_id}_{ts}"
        if self.created_at is None:
            self.created_at = datetime.now(UTC)

    @property
    def variant_a_capital_usd(self) -> Decimal:
        """Calculate capital allocated to variant A."""
        return self.total_capital_usd * Decimal(str(self.config.split_ratio))

    @property
    def variant_b_capital_usd(self) -> Decimal:
        """Calculate capital allocated to variant B."""
        return self.total_capital_usd * (Decimal("1") - Decimal(str(self.config.split_ratio)))

    @property
    def duration_seconds(self) -> int:
        """Get the duration of the test."""
        if not self.started_at:
            return 0
        end = self.ended_at or datetime.now(UTC)
        return int((end - self.started_at).total_seconds())

    @property
    def is_expired(self) -> bool:
        """Check if test has exceeded max duration."""
        if self.config.max_duration_hours == 0:
            return False
        max_seconds = self.config.max_duration_hours * 3600
        return self.duration_seconds >= max_seconds

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "test_id": self.test_id,
            "strategy_id": self.strategy_id,
            "variant_a_id": self.variant_a_id,
            "variant_b_id": self.variant_b_id,
            "status": self.status.value,
            "config": self.config.to_dict(),
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "variant_a_metrics": self.variant_a_metrics.to_dict() if self.variant_a_metrics else None,
            "variant_b_metrics": self.variant_b_metrics.to_dict() if self.variant_b_metrics else None,
            "total_capital_usd": str(self.total_capital_usd),
            "winner": self.winner,
            "comparison_history": self.comparison_history,
            "variant_a_capital_usd": str(self.variant_a_capital_usd),
            "variant_b_capital_usd": str(self.variant_b_capital_usd),
            "duration_seconds": self.duration_seconds,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ABTest":
        """Create from dictionary."""
        variant_a_metrics = None
        if data.get("variant_a_metrics"):
            variant_a_metrics = VariantMetrics.from_dict(data["variant_a_metrics"])

        variant_b_metrics = None
        if data.get("variant_b_metrics"):
            variant_b_metrics = VariantMetrics.from_dict(data["variant_b_metrics"])

        return cls(
            test_id=data["test_id"],
            strategy_id=data["strategy_id"],
            variant_a_id=data["variant_a_id"],
            variant_b_id=data["variant_b_id"],
            status=ABTestStatus(data["status"]),
            config=ABTestConfig.from_dict(data.get("config", {})),
            created_at=datetime.fromisoformat(data["created_at"]) if data.get("created_at") else None,
            started_at=datetime.fromisoformat(data["started_at"]) if data.get("started_at") else None,
            ended_at=datetime.fromisoformat(data["ended_at"]) if data.get("ended_at") else None,
            variant_a_metrics=variant_a_metrics,
            variant_b_metrics=variant_b_metrics,
            total_capital_usd=Decimal(data.get("total_capital_usd", "0")),
            winner=data.get("winner"),
            comparison_history=data.get("comparison_history", []),
        )


@dataclass
class ABTestResult:
    """Result of an A/B test comparison or action.

    Attributes:
        success: Whether the action succeeded
        comparison: Current comparison data
        error: Error message if failed
        message: Human-readable status message
    """

    success: bool
    comparison: VariantComparison | None = None
    error: str | None = None
    message: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "success": self.success,
            "comparison": self.comparison.to_dict() if self.comparison else None,
            "error": self.error,
            "message": self.message,
        }


@dataclass
class CreateTestResult:
    """Result of creating an A/B test.

    Attributes:
        success: Whether test creation succeeded
        test_id: Unique ID of the created test
        test: The created ABTest object
        error: Error message if failed
    """

    success: bool
    test_id: str = ""
    test: ABTest | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "success": self.success,
            "test_id": self.test_id,
            "test": self.test.to_dict() if self.test else None,
            "error": self.error,
        }


@dataclass
class EndTestResult:
    """Result of ending an A/B test.

    Attributes:
        success: Whether ending succeeded
        winner: The selected winner ("variant_a" or "variant_b")
        final_comparison: Final comparison at test end
        error: Error message if failed
    """

    success: bool
    winner: str | None = None
    final_comparison: VariantComparison | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "success": self.success,
            "winner": self.winner,
            "final_comparison": self.final_comparison.to_dict() if self.final_comparison else None,
            "error": self.error,
        }


# Type alias for A/B test callbacks
ABTestCallback = Callable[[ABTest], None]


def _welch_t_test(
    mean_a: float,
    mean_b: float,
    var_a: float,
    var_b: float,
    n_a: int,
    n_b: int,
) -> tuple[float, float]:
    """Perform Welch's t-test for comparing means with unequal variances.

    Args:
        mean_a: Mean of sample A
        mean_b: Mean of sample B
        var_a: Variance of sample A
        var_b: Variance of sample B
        n_a: Sample size of A
        n_b: Sample size of B

    Returns:
        Tuple of (t_statistic, degrees_of_freedom)
    """
    # Standard error of the difference
    se_a = var_a / n_a if n_a > 0 else 0
    se_b = var_b / n_b if n_b > 0 else 0
    se_diff = math.sqrt(se_a + se_b) if (se_a + se_b) > 0 else 1e-10

    # T-statistic
    t_stat = (mean_b - mean_a) / se_diff

    # Welch-Satterthwaite degrees of freedom
    if se_a + se_b > 0:
        numerator = (se_a + se_b) ** 2
        denominator = (se_a**2 / (n_a - 1) if n_a > 1 else 0) + (se_b**2 / (n_b - 1) if n_b > 1 else 0)
        df = numerator / denominator if denominator > 0 else 1
    else:
        df = max(n_a + n_b - 2, 1)

    return t_stat, df


def _t_cdf(t: float, df: float) -> float:
    """Approximate the cumulative distribution function of the t-distribution.

    Uses a simple approximation based on the normal distribution for large df.

    Args:
        t: t-statistic
        df: degrees of freedom

    Returns:
        Approximate CDF value
    """
    # For large df, t-distribution approaches normal
    # Use approximation: t * (1 - 1/(4*df)) / sqrt(1 + t^2/(2*df))
    if df > 100:
        # Use normal approximation
        z = t
        return 0.5 * (1 + math.erf(z / math.sqrt(2)))

    # For smaller df, use a rougher approximation
    # This is not perfect but avoids scipy dependency
    x = df / (df + t * t)
    # Incomplete beta function approximation
    if t < 0:
        return 0.5 * x ** (df / 2)
    else:
        return 1 - 0.5 * x ** (df / 2)


def _t_critical_value(alpha: float, df: float) -> float:
    """Get approximate critical t-value for a given alpha and df.

    Args:
        alpha: Significance level (two-tailed, so use alpha/2)
        df: Degrees of freedom

    Returns:
        Approximate critical t-value
    """
    # Common critical values (two-tailed)
    # This is a simplification - in production, use scipy.stats
    critical_values: dict[tuple[float, int], float] = {
        (0.05, 10): 2.228,
        (0.05, 20): 2.086,
        (0.05, 30): 2.042,
        (0.05, 60): 2.000,
        (0.05, 120): 1.980,
        (0.01, 10): 3.169,
        (0.01, 20): 2.845,
        (0.01, 30): 2.750,
        (0.01, 60): 2.660,
        (0.01, 120): 2.617,
    }

    # Find closest match
    df_int = int(round(df))
    df_int = min(max(df_int, 10), 120)

    closest_df = min([10, 20, 30, 60, 120], key=lambda x: abs(x - df_int))

    if alpha <= 0.01:
        return critical_values.get((0.01, closest_df), 2.576)
    else:
        return critical_values.get((0.05, closest_df), 1.96)


class ABTestManager:
    """Manages A/B tests for strategy variant comparison.

    This class handles the full lifecycle of an A/B test:
    1. Create test with two variants and split ratio
    2. Allocate capital according to ratio
    3. Track performance metrics for each variant
    4. Provide statistical comparison
    5. End test and select winner

    Attributes:
        strategy_id: ID of the strategy being tested
        test: Current active test (if any)
    """

    def __init__(
        self,
        strategy_id: str,
        on_test_start: ABTestCallback | None = None,
        on_test_end: ABTestCallback | None = None,
        on_comparison: ABTestCallback | None = None,
        chain: str = "unknown",
    ) -> None:
        """Initialize the A/B test manager.

        Args:
            strategy_id: ID of the strategy being tested
            on_test_start: Callback when test starts
            on_test_end: Callback when test ends
            on_comparison: Callback when comparison is made
            chain: Blockchain network for event emission
        """
        self.strategy_id = strategy_id
        self._chain = chain

        # Callbacks
        self._on_test_start = on_test_start
        self._on_test_end = on_test_end
        self._on_comparison = on_comparison

        # Current test state
        self.test: ABTest | None = None

        logger.info(f"ABTestManager initialized for strategy {strategy_id}")

    def create_ab_test(
        self,
        variant_a: str,
        variant_b: str,
        split_ratio: float = 0.5,
        total_capital_usd: Decimal | None = None,
        config: ABTestConfig | None = None,
    ) -> CreateTestResult:
        """Create a new A/B test between two variants.

        Args:
            variant_a: Version ID for variant A (control/baseline)
            variant_b: Version ID for variant B (treatment/experimental)
            split_ratio: Proportion of capital for variant A (0-1)
            total_capital_usd: Total capital to allocate
            config: Optional test configuration

        Returns:
            CreateTestResult with test details
        """
        # Validate inputs
        if not variant_a:
            return CreateTestResult(success=False, error="variant_a is required")
        if not variant_b:
            return CreateTestResult(success=False, error="variant_b is required")
        if variant_a == variant_b:
            return CreateTestResult(success=False, error="variant_a and variant_b must be different")

        # Check for existing test
        if self.test and self.test.status == ABTestStatus.RUNNING:
            return CreateTestResult(
                success=False,
                error="An A/B test is already running. End it first before creating a new one.",
            )

        # Use provided config or create one with split_ratio
        if config is None:
            try:
                config = ABTestConfig(split_ratio=split_ratio)
            except ValueError as e:
                return CreateTestResult(success=False, error=str(e))
        else:
            # Override split_ratio in config if explicitly provided
            if split_ratio != 0.5:
                try:
                    config = ABTestConfig(
                        split_ratio=split_ratio,
                        min_sample_size=config.min_sample_size,
                        confidence_level=config.confidence_level,
                        emit_events=config.emit_events,
                        auto_end_on_significance=config.auto_end_on_significance,
                        max_duration_hours=config.max_duration_hours,
                    )
                except ValueError as e:
                    return CreateTestResult(success=False, error=str(e))

        # Create the test
        self.test = ABTest(
            test_id="",  # Will be generated in __post_init__
            strategy_id=self.strategy_id,
            variant_a_id=variant_a,
            variant_b_id=variant_b,
            config=config,
            total_capital_usd=total_capital_usd or Decimal("0"),
        )

        # Initialize variant metrics
        now = datetime.now(UTC)
        self.test.variant_a_metrics = VariantMetrics(
            variant_id=variant_a,
            variant_name="A",
            capital_allocated_usd=self.test.variant_a_capital_usd,
            metrics=PerformanceMetrics(),
            is_control=True,
            measurement_start=now,
        )

        self.test.variant_b_metrics = VariantMetrics(
            variant_id=variant_b,
            variant_name="B",
            capital_allocated_usd=self.test.variant_b_capital_usd,
            metrics=PerformanceMetrics(),
            is_control=False,
            measurement_start=now,
        )

        # Start the test
        self.test.status = ABTestStatus.RUNNING
        self.test.started_at = now

        # Emit creation event
        self._emit_event(
            ABTestEventType.AB_TEST_CREATED,
            f"A/B test created: {variant_a} vs {variant_b}, split {split_ratio:.0%}/{1 - split_ratio:.0%}",
            {
                "test_id": self.test.test_id,
                "variant_a_id": variant_a,
                "variant_b_id": variant_b,
                "split_ratio": split_ratio,
                "variant_a_capital_usd": str(self.test.variant_a_capital_usd),
                "variant_b_capital_usd": str(self.test.variant_b_capital_usd),
            },
        )

        logger.info(
            f"A/B test {self.test.test_id} created: "
            f"{variant_a} ({split_ratio:.0%}) vs {variant_b} ({1 - split_ratio:.0%})"
        )

        # Call start callback
        if self._on_test_start:
            try:
                self._on_test_start(self.test)
            except Exception as e:
                logger.error(f"Test start callback failed: {e}")

        return CreateTestResult(
            success=True,
            test_id=self.test.test_id,
            test=self.test,
        )

    def update_variant_metrics(
        self,
        variant: str,
        pnl_usd: Decimal | None = None,
        trades: int | None = None,
        errors: int | None = None,
        drawdown: Decimal | None = None,
        sharpe: Decimal | None = None,
        win_rate: Decimal | None = None,
        trade_pnl: Decimal | None = None,
    ) -> bool:
        """Update metrics for a specific variant.

        Args:
            variant: "a" or "b" to identify the variant
            pnl_usd: Net PnL in USD
            trades: Number of trades
            errors: Number of errors
            drawdown: Max drawdown
            sharpe: Sharpe ratio
            win_rate: Win rate (0-1)
            trade_pnl: Single trade PnL for variance tracking

        Returns:
            True if update succeeded, False otherwise
        """
        if not self.test or self.test.status != ABTestStatus.RUNNING:
            logger.warning("Cannot update metrics: no running test")
            return False

        # Select the right variant metrics
        variant_lower = variant.lower()
        if variant_lower == "a":
            metrics = self.test.variant_a_metrics
        elif variant_lower == "b":
            metrics = self.test.variant_b_metrics
        else:
            logger.warning(f"Invalid variant: {variant}, must be 'a' or 'b'")
            return False

        if not metrics:
            return False

        # Update metrics
        if pnl_usd is not None:
            metrics.metrics.net_pnl_usd = pnl_usd
            metrics.metrics.total_pnl_usd = pnl_usd  # Simplified
        if trades is not None:
            metrics.trade_count = trades
            metrics.metrics.total_trades = trades
        if errors is not None:
            metrics.error_count = errors
        if drawdown is not None:
            metrics.metrics.max_drawdown = drawdown
        if sharpe is not None:
            metrics.metrics.sharpe_ratio = sharpe
        if win_rate is not None:
            metrics.metrics.win_rate = win_rate
        if trade_pnl is not None:
            metrics.record_trade_pnl(trade_pnl)

        # Emit metrics update event
        self._emit_event(
            ABTestEventType.AB_TEST_METRICS_UPDATED,
            f"Variant {variant.upper()} metrics updated",
            {
                "variant": variant.upper(),
                "pnl_usd": str(metrics.metrics.net_pnl_usd),
                "trade_count": metrics.trade_count,
            },
        )

        return True

    def compare(self) -> VariantComparison:
        """Compare performance between variant A and variant B.

        Performs statistical analysis to determine if there is a
        significant difference between variants.

        Returns:
            VariantComparison with metrics and statistical analysis
        """
        if not self.test or not self.test.variant_a_metrics or not self.test.variant_b_metrics:
            # Return empty comparison
            return VariantComparison(
                variant_a_metrics=VariantMetrics(
                    variant_id="",
                    variant_name="A",
                    capital_allocated_usd=Decimal("0"),
                    metrics=PerformanceMetrics(),
                    is_control=True,
                ),
                variant_b_metrics=VariantMetrics(
                    variant_id="",
                    variant_name="B",
                    capital_allocated_usd=Decimal("0"),
                    metrics=PerformanceMetrics(),
                    is_control=False,
                ),
                recommendation_reason="No test data available",
            )

        metrics_a = self.test.variant_a_metrics
        metrics_b = self.test.variant_b_metrics
        config = self.test.config

        # Check if we have sufficient data
        has_sufficient_data = (
            metrics_a.trade_count >= config.min_sample_size and metrics_b.trade_count >= config.min_sample_size
        )

        # Calculate mean difference
        mean_a = float(metrics_a.average_pnl_per_trade)
        mean_b = float(metrics_b.average_pnl_per_trade)
        mean_difference = Decimal(str(mean_b - mean_a))

        # Calculate relative improvement
        relative_improvement: Decimal | None = None
        if mean_a != 0:
            relative_improvement = Decimal(str((mean_b - mean_a) / abs(mean_a)))

        # Perform statistical analysis if we have enough data
        statistical_result: StatisticalResult | None = None
        if has_sufficient_data:
            var_a = float(metrics_a.pnl_variance)
            var_b = float(metrics_b.pnl_variance)
            n_a = metrics_a.trade_count
            n_b = metrics_b.trade_count

            # Perform Welch's t-test
            t_stat, df = _welch_t_test(mean_a, mean_b, var_a, var_b, n_a, n_b)

            # Calculate p-value (two-tailed)
            p_value = 2 * (1 - _t_cdf(abs(t_stat), df))

            # Calculate confidence interval for the difference
            alpha = 1 - config.confidence_level
            t_critical = _t_critical_value(alpha, df)
            se_diff = math.sqrt(var_a / n_a + var_b / n_b) if n_a > 0 and n_b > 0 else 0
            ci_lower = (mean_b - mean_a) - t_critical * se_diff
            ci_upper = (mean_b - mean_a) + t_critical * se_diff

            # Check significance
            is_significant = p_value < alpha

            # Calculate effect size (Cohen's d)
            pooled_std = math.sqrt((var_a + var_b) / 2) if (var_a + var_b) > 0 else 1
            effect_size = (mean_b - mean_a) / pooled_std if pooled_std > 0 else 0

            statistical_result = StatisticalResult(
                t_statistic=t_stat,
                degrees_of_freedom=df,
                p_value=p_value,
                confidence_interval_lower=ci_lower,
                confidence_interval_upper=ci_upper,
                is_significant=is_significant,
                effect_size=effect_size,
            )

        # Determine recommendation
        recommended_winner: str | None = None
        recommendation_reason = ""

        if not has_sufficient_data:
            recommendation_reason = (
                f"Insufficient data: A has {metrics_a.trade_count} trades, "
                f"B has {metrics_b.trade_count} trades, "
                f"minimum required is {config.min_sample_size}"
            )
        elif statistical_result:
            if statistical_result.is_significant:
                if mean_b > mean_a:
                    recommended_winner = "variant_b"
                    recommendation_reason = (
                        f"Variant B significantly outperforms A "
                        f"(p={statistical_result.p_value:.4f}, "
                        f"effect size={statistical_result.effect_size:.3f})"
                    )
                else:
                    recommended_winner = "variant_a"
                    recommendation_reason = (
                        f"Variant A significantly outperforms B "
                        f"(p={statistical_result.p_value:.4f}, "
                        f"effect size={statistical_result.effect_size:.3f})"
                    )
            else:
                recommendation_reason = f"No significant difference detected (p={statistical_result.p_value:.4f})"

        # Create comparison
        comparison = VariantComparison(
            variant_a_metrics=metrics_a,
            variant_b_metrics=metrics_b,
            mean_difference_usd=mean_difference,
            relative_improvement=relative_improvement,
            pnl_statistical_result=statistical_result,
            recommended_winner=recommended_winner,
            recommendation_reason=recommendation_reason,
            has_sufficient_data=has_sufficient_data,
        )

        # Record comparison in history
        self.test.comparison_history.append(
            {
                "timestamp": datetime.now(UTC).isoformat(),
                "comparison": comparison.to_dict(),
            }
        )

        # Emit comparison event
        self._emit_event(
            ABTestEventType.AB_TEST_COMPARISON_UPDATED,
            f"A/B test comparison: {recommendation_reason}",
            {
                "has_sufficient_data": has_sufficient_data,
                "mean_difference_usd": str(mean_difference),
                "recommended_winner": recommended_winner,
            },
        )

        # Call comparison callback
        if self._on_comparison:
            try:
                self._on_comparison(self.test)
            except Exception as e:
                logger.error(f"Comparison callback failed: {e}")

        # Auto-end if configured and significant
        if (
            config.auto_end_on_significance
            and statistical_result
            and statistical_result.is_significant
            and recommended_winner
        ):
            logger.info("Auto-ending test due to statistical significance")
            self.end_test(select_winner=recommended_winner)

        return comparison

    def end_test(self, select_winner: str | None = None) -> EndTestResult:
        """End the A/B test and optionally select a winner.

        Args:
            select_winner: "variant_a" or "variant_b" to select winner,
                          or None to end without selecting

        Returns:
            EndTestResult with final comparison
        """
        if not self.test:
            return EndTestResult(success=False, error="No test to end")

        if self.test.status not in (ABTestStatus.RUNNING, ABTestStatus.PENDING):
            return EndTestResult(
                success=False,
                error=f"Cannot end test in status {self.test.status.value}",
            )

        # Validate winner selection
        valid_winners = {"variant_a", "variant_b", None}
        if select_winner not in valid_winners:
            return EndTestResult(
                success=False,
                error=f"Invalid winner: {select_winner}, must be 'variant_a', 'variant_b', or None",
            )

        # Get final comparison
        final_comparison = self.compare()

        # Update test state
        self.test.ended_at = datetime.now(UTC)
        self.test.winner = select_winner

        if select_winner:
            self.test.status = ABTestStatus.COMPLETED
            status_description = f"Winner: {select_winner}"
        elif not final_comparison.has_sufficient_data:
            self.test.status = ABTestStatus.INCONCLUSIVE
            status_description = "Inconclusive - insufficient data"
        else:
            self.test.status = ABTestStatus.COMPLETED
            status_description = "Completed without winner selection"

        # Emit end event
        self._emit_event(
            ABTestEventType.AB_TEST_ENDED,
            f"A/B test ended: {status_description}",
            {
                "test_id": self.test.test_id,
                "status": self.test.status.value,
                "winner": select_winner,
                "duration_seconds": self.test.duration_seconds,
            },
        )

        if select_winner:
            self._emit_event(
                ABTestEventType.AB_TEST_WINNER_SELECTED,
                f"Winner selected: {select_winner}",
                {
                    "winner": select_winner,
                    "winner_version_id": (
                        self.test.variant_a_id if select_winner == "variant_a" else self.test.variant_b_id
                    ),
                },
            )

        logger.info(f"A/B test {self.test.test_id} ended: {status_description}")

        # Call end callback
        if self._on_test_end:
            try:
                self._on_test_end(self.test)
            except Exception as e:
                logger.error(f"Test end callback failed: {e}")

        return EndTestResult(
            success=True,
            winner=select_winner,
            final_comparison=final_comparison,
        )

    def cancel_test(self) -> EndTestResult:
        """Cancel the A/B test without selecting a winner.

        Returns:
            EndTestResult indicating cancellation
        """
        if not self.test:
            return EndTestResult(success=False, error="No test to cancel")

        if self.test.status not in (ABTestStatus.RUNNING, ABTestStatus.PENDING):
            return EndTestResult(
                success=False,
                error=f"Cannot cancel test in status {self.test.status.value}",
            )

        # Update test state
        self.test.ended_at = datetime.now(UTC)
        self.test.status = ABTestStatus.CANCELLED

        # Emit cancellation event
        self._emit_event(
            ABTestEventType.AB_TEST_CANCELLED,
            "A/B test cancelled",
            {
                "test_id": self.test.test_id,
                "duration_seconds": self.test.duration_seconds,
            },
        )

        logger.info(f"A/B test {self.test.test_id} cancelled")

        return EndTestResult(
            success=True,
            winner=None,
            final_comparison=self.compare() if self.test.variant_a_metrics else None,
        )

    def get_status(self) -> dict[str, Any]:
        """Get the current status of the A/B test.

        Returns:
            Dictionary with test status
        """
        if not self.test:
            return {
                "has_active_test": False,
                "strategy_id": self.strategy_id,
            }

        comparison = None
        if self.test.variant_a_metrics and self.test.variant_b_metrics:
            comparison = self.compare()

        return {
            "has_active_test": True,
            "test_id": self.test.test_id,
            "strategy_id": self.strategy_id,
            "status": self.test.status.value,
            "variant_a_id": self.test.variant_a_id,
            "variant_b_id": self.test.variant_b_id,
            "split_ratio": self.test.config.split_ratio,
            "duration_seconds": self.test.duration_seconds,
            "variant_a_metrics": self.test.variant_a_metrics.to_dict() if self.test.variant_a_metrics else None,
            "variant_b_metrics": self.test.variant_b_metrics.to_dict() if self.test.variant_b_metrics else None,
            "comparison": comparison.to_dict() if comparison else None,
            "winner": self.test.winner,
        }

    def _emit_event(
        self,
        event_type: ABTestEventType,
        description: str,
        details: dict[str, Any],
    ) -> None:
        """Emit an A/B test-related timeline event.

        Args:
            event_type: Type of A/B test event
            description: Human-readable description
            details: Additional event details
        """
        if not self.test or not self.test.config.emit_events:
            return

        event = TimelineEvent(
            timestamp=datetime.now(UTC),
            event_type=TimelineEventType.CUSTOM,
            description=description,
            strategy_id=self.strategy_id,
            chain=self._chain,
            details={
                "ab_test_event_type": event_type.value,
                "test_id": self.test.test_id if self.test else None,
                **details,
            },
        )

        add_event(event)
        logger.debug(f"A/B test event emitted: {event_type.value} - {description}")

    def to_dict(self) -> dict[str, Any]:
        """Export the manager state for persistence.

        Returns:
            Dictionary containing manager state
        """
        return {
            "strategy_id": self.strategy_id,
            "chain": self._chain,
            "test": self.test.to_dict() if self.test else None,
        }

    @classmethod
    def from_dict(
        cls,
        data: dict[str, Any],
        on_test_start: ABTestCallback | None = None,
        on_test_end: ABTestCallback | None = None,
        on_comparison: ABTestCallback | None = None,
    ) -> "ABTestManager":
        """Restore a manager from persisted state.

        Args:
            data: Dictionary with manager data
            on_test_start: Optional test start callback
            on_test_end: Optional test end callback
            on_comparison: Optional comparison callback

        Returns:
            ABTestManager instance with restored state
        """
        manager = cls(
            strategy_id=data["strategy_id"],
            on_test_start=on_test_start,
            on_test_end=on_test_end,
            on_comparison=on_comparison,
            chain=data.get("chain", "unknown"),
        )

        # Restore test state
        if data.get("test"):
            manager.test = ABTest.from_dict(data["test"])

        return manager


__all__ = [
    "ABTest",
    "ABTestConfig",
    "ABTestManager",
    "ABTestStatus",
    "ABTestResult",
    "VariantMetrics",
    "VariantComparison",
    "StatisticalResult",
    "ABTestEventType",
    "ABTestCallback",
    "CreateTestResult",
    "EndTestResult",
]
