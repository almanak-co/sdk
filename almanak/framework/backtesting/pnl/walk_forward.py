"""Walk-forward optimization for backtest parameter tuning.

Walk-forward optimization is a technique that addresses overfitting in
backtesting by:
1. Splitting data into multiple train/test windows
2. Optimizing parameters on training windows
3. Testing with optimal parameters on out-of-sample test windows
4. Aggregating out-of-sample results for realistic performance estimates

This module provides:
    - split_walk_forward: Generate train/test window splits
    - WalkForwardWindow: Data structure for window definitions
    - WalkForwardConfig: Configuration for walk-forward optimization

Key Concepts:
    - train_size: Duration of training window (for parameter optimization)
    - test_size: Duration of test window (for out-of-sample evaluation)
    - step: How far to advance between splits (controls overlap)

Example:
    from almanak.framework.backtesting.pnl.walk_forward import (
        split_walk_forward,
        WalkForwardConfig,
    )
    from datetime import datetime, timedelta

    config = WalkForwardConfig(
        train_size=timedelta(days=90),
        test_size=timedelta(days=30),
        step=timedelta(days=30),
    )

    windows = split_walk_forward(
        start_date=datetime(2023, 1, 1),
        end_date=datetime(2024, 1, 1),
        config=config,
    )

    for window in windows:
        print(f"Train: {window.train_start} to {window.train_end}")
        print(f"Test:  {window.test_start} to {window.test_end}")
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from almanak.framework.backtesting.models import BacktestResult
    from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
    from almanak.framework.backtesting.pnl.optuna_tuner import (
        OptimizationResult,
        OptunaParamRanges,
    )

logger = logging.getLogger(__name__)


@dataclass
class WalkForwardWindow:
    """A single train/test window for walk-forward optimization.

    Represents one split in the walk-forward process, containing both
    the training period (for parameter optimization) and the test period
    (for out-of-sample evaluation).

    Attributes:
        window_index: Sequential index of this window (0-based)
        train_start: Start datetime of training period
        train_end: End datetime of training period
        test_start: Start datetime of test period
        test_end: End datetime of test period

    Note:
        Typically train_end == test_start (no gap), but this is not enforced
        to support more complex splitting strategies.
    """

    window_index: int
    train_start: datetime
    train_end: datetime
    test_start: datetime
    test_end: datetime

    def __post_init__(self) -> None:
        """Validate window configuration."""
        if self.train_start >= self.train_end:
            raise ValueError(f"train_start ({self.train_start}) must be before train_end ({self.train_end})")
        if self.test_start >= self.test_end:
            raise ValueError(f"test_start ({self.test_start}) must be before test_end ({self.test_end})")
        if self.train_end > self.test_start:
            raise ValueError(
                f"train_end ({self.train_end}) must not be after test_start ({self.test_start}) to prevent data leakage"
            )

    @property
    def train_duration(self) -> timedelta:
        """Duration of the training period."""
        return self.train_end - self.train_start

    @property
    def test_duration(self) -> timedelta:
        """Duration of the test period."""
        return self.test_end - self.test_start

    @property
    def gap_duration(self) -> timedelta:
        """Gap between training and test periods (can be 0)."""
        return self.test_start - self.train_end

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "window_index": self.window_index,
            "train_start": self.train_start.isoformat(),
            "train_end": self.train_end.isoformat(),
            "test_start": self.test_start.isoformat(),
            "test_end": self.test_end.isoformat(),
            "train_duration_days": self.train_duration.days,
            "test_duration_days": self.test_duration.days,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WalkForwardWindow:
        """Deserialize from dictionary."""
        return cls(
            window_index=data["window_index"],
            train_start=datetime.fromisoformat(data["train_start"]),
            train_end=datetime.fromisoformat(data["train_end"]),
            test_start=datetime.fromisoformat(data["test_start"]),
            test_end=datetime.fromisoformat(data["test_end"]),
        )

    def to_tuple(self) -> tuple[datetime, datetime, datetime, datetime]:
        """Return window as (train_start, train_end, test_start, test_end) tuple."""
        return (self.train_start, self.train_end, self.test_start, self.test_end)


@dataclass
class WalkForwardConfig:
    """Configuration for walk-forward optimization.

    Controls how the data is split into train/test windows for
    walk-forward validation.

    Attributes:
        train_size: Duration of each training window
        test_size: Duration of each test window
        step: How far to advance between windows. Controls overlap:
            - step == test_size: No overlap (anchored walk-forward)
            - step < test_size: Overlapping test windows (rolling)
            - step > test_size: Gaps between windows
        gap: Optional gap between training and test windows.
            Useful for strategies that need time to implement.
            Default is 0 (no gap).
        min_windows: Minimum number of windows required.
            Raises error if fewer windows would be generated.
            Default is 2.

    Example:
        # Non-overlapping (anchored) walk-forward
        config = WalkForwardConfig(
            train_size=timedelta(days=90),
            test_size=timedelta(days=30),
            step=timedelta(days=30),
        )

        # Overlapping (rolling) walk-forward
        config = WalkForwardConfig(
            train_size=timedelta(days=90),
            test_size=timedelta(days=30),
            step=timedelta(days=7),  # Weekly steps
        )

        # With implementation gap
        config = WalkForwardConfig(
            train_size=timedelta(days=90),
            test_size=timedelta(days=30),
            step=timedelta(days=30),
            gap=timedelta(days=1),  # 1-day implementation lag
        )
    """

    train_size: timedelta
    test_size: timedelta
    step: timedelta | None = None
    gap: timedelta = field(default_factory=lambda: timedelta(0))
    min_windows: int = 2

    def __post_init__(self) -> None:
        """Validate configuration and set defaults."""
        if self.train_size.total_seconds() <= 0:
            raise ValueError(f"train_size must be positive, got {self.train_size}")
        if self.test_size.total_seconds() <= 0:
            raise ValueError(f"test_size must be positive, got {self.test_size}")
        if self.step is None:
            # Default step = test_size (no overlap)
            self.step = self.test_size
        if self.step.total_seconds() <= 0:
            raise ValueError(f"step must be positive, got {self.step}")
        if self.gap.total_seconds() < 0:
            raise ValueError(f"gap must be non-negative, got {self.gap}")
        if self.min_windows < 1:
            raise ValueError(f"min_windows must be at least 1, got {self.min_windows}")

    @property
    def window_size(self) -> timedelta:
        """Total duration of train + gap + test for one window."""
        return self.train_size + self.gap + self.test_size

    @property
    def is_overlapping(self) -> bool:
        """Whether test windows overlap (step < test_size)."""
        assert self.step is not None
        return self.step < self.test_size

    @property
    def is_anchored(self) -> bool:
        """Whether windows are anchored (step == test_size, no overlap)."""
        return self.step == self.test_size

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "train_size_seconds": self.train_size.total_seconds(),
            "test_size_seconds": self.test_size.total_seconds(),
            "step_seconds": self.step.total_seconds() if self.step else None,
            "gap_seconds": self.gap.total_seconds(),
            "min_windows": self.min_windows,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WalkForwardConfig:
        """Deserialize from dictionary."""
        return cls(
            train_size=timedelta(seconds=data["train_size_seconds"]),
            test_size=timedelta(seconds=data["test_size_seconds"]),
            step=timedelta(seconds=data["step_seconds"]) if data.get("step_seconds") else None,
            gap=timedelta(seconds=data.get("gap_seconds", 0)),
            min_windows=data.get("min_windows", 2),
        )

    @classmethod
    def from_days(
        cls,
        train_days: int,
        test_days: int,
        step_days: int | None = None,
        gap_days: int = 0,
        min_windows: int = 2,
    ) -> WalkForwardConfig:
        """Create config from day counts (convenience factory).

        Args:
            train_days: Training window size in days
            test_days: Test window size in days
            step_days: Step size in days (default: test_days)
            gap_days: Gap between train and test in days (default: 0)
            min_windows: Minimum number of windows (default: 2)

        Returns:
            WalkForwardConfig instance

        Example:
            # 90-day train, 30-day test, non-overlapping
            config = WalkForwardConfig.from_days(90, 30)

            # 90-day train, 30-day test, weekly rolling
            config = WalkForwardConfig.from_days(90, 30, step_days=7)
        """
        return cls(
            train_size=timedelta(days=train_days),
            test_size=timedelta(days=test_days),
            step=timedelta(days=step_days) if step_days is not None else None,
            gap=timedelta(days=gap_days),
            min_windows=min_windows,
        )


def _resolve_walk_forward_config(
    config: WalkForwardConfig | None,
    train_size: timedelta | None,
    test_size: timedelta | None,
    step: timedelta | None,
    gap: timedelta | None,
    min_windows: int | None,
) -> WalkForwardConfig:
    if config is not None:
        return config
    if train_size is None or test_size is None:
        raise ValueError("Either 'config' or both 'train_size' and 'test_size' must be provided")
    return WalkForwardConfig(
        train_size=train_size,
        test_size=test_size,
        step=step,
        gap=gap if gap is not None else timedelta(0),
        min_windows=min_windows if min_windows is not None else 2,
    )


def _validate_walk_forward_date_range(
    start_date: datetime,
    end_date: datetime,
    config: WalkForwardConfig,
) -> None:
    if start_date >= end_date:
        raise ValueError(f"start_date ({start_date}) must be before end_date ({end_date})")

    total_duration = end_date - start_date
    window_size = config.window_size
    if total_duration < window_size:
        raise ValueError(
            f"Date range ({total_duration.days} days) is shorter than "
            f"one window ({window_size.days} days = "
            f"{config.train_size.days} train + {config.gap.days} gap + "
            f"{config.test_size.days} test)"
        )


def _build_walk_forward_windows(
    start_date: datetime,
    end_date: datetime,
    config: WalkForwardConfig,
) -> list[WalkForwardWindow]:
    windows: list[WalkForwardWindow] = []
    window_index = 0
    current_train_start = start_date

    while True:
        train_end = current_train_start + config.train_size
        test_start = train_end + config.gap
        test_end = test_start + config.test_size

        if test_end > end_date:
            break

        windows.append(
            WalkForwardWindow(
                window_index=window_index,
                train_start=current_train_start,
                train_end=train_end,
                test_start=test_start,
                test_end=test_end,
            )
        )

        assert config.step is not None
        current_train_start += config.step
        window_index += 1

    return windows


def _validate_min_walk_forward_windows(
    windows: list[WalkForwardWindow],
    min_windows: int,
) -> None:
    if len(windows) >= min_windows:
        return
    raise ValueError(
        f"Only {len(windows)} windows can be created, but min_windows={min_windows}. "
        f"Either extend the date range, reduce train_size/test_size, "
        f"or lower min_windows."
    )


def _log_walk_forward_windows(windows: list[WalkForwardWindow], config: WalkForwardConfig) -> None:
    logger.info(
        f"Generated {len(windows)} walk-forward windows: "
        f"train={config.train_size.days}d, test={config.test_size.days}d, "
        f"step={config.step.days if config.step else 0}d, gap={config.gap.days}d"
    )


def split_walk_forward(
    start_date: datetime,
    end_date: datetime,
    config: WalkForwardConfig | None = None,
    train_size: timedelta | None = None,
    test_size: timedelta | None = None,
    step: timedelta | None = None,
    gap: timedelta | None = None,
    min_windows: int | None = None,
) -> list[WalkForwardWindow]:
    """Generate train/test window splits for walk-forward optimization.

    If ``config`` is provided, it owns all sizing fields and individual
    ``train_size`` / ``test_size`` / ``step`` / ``gap`` / ``min_windows``
    arguments are ignored.
    """
    resolved_config = _resolve_walk_forward_config(
        config,
        train_size,
        test_size,
        step,
        gap,
        min_windows,
    )
    _validate_walk_forward_date_range(start_date, end_date, resolved_config)
    windows = _build_walk_forward_windows(start_date, end_date, resolved_config)
    _validate_min_walk_forward_windows(windows, resolved_config.min_windows)
    _log_walk_forward_windows(windows, resolved_config)

    return windows


def split_walk_forward_tuples(
    start_date: datetime,
    end_date: datetime,
    config: WalkForwardConfig | None = None,
    train_size: timedelta | None = None,
    test_size: timedelta | None = None,
    step: timedelta | None = None,
    gap: timedelta | None = None,
    min_windows: int | None = None,
) -> list[tuple[datetime, datetime, datetime, datetime]]:
    """Generate walk-forward splits as tuples.

    Convenience function that returns splits as raw tuples instead of
    WalkForwardWindow objects, matching the acceptance criteria format.

    Args:
        Same as split_walk_forward

    Returns:
        List of (train_start, train_end, test_start, test_end) tuples

    Example:
        splits = split_walk_forward_tuples(
            start_date=datetime(2023, 1, 1),
            end_date=datetime(2024, 1, 1),
            train_size=timedelta(days=90),
            test_size=timedelta(days=30),
        )

        for train_start, train_end, test_start, test_end in splits:
            print(f"Train: {train_start} - {train_end}")
            print(f"Test:  {test_start} - {test_end}")
    """
    windows = split_walk_forward(
        start_date=start_date,
        end_date=end_date,
        config=config,
        train_size=train_size,
        test_size=test_size,
        step=step,
        gap=gap,
        min_windows=min_windows,
    )
    return [w.to_tuple() for w in windows]


# =============================================================================
# Walk-Forward Optimization Results
# =============================================================================


@dataclass
class WalkForwardWindowResult:
    """Result from a single walk-forward window.

    Contains both the optimization result from the training period and
    the out-of-sample backtest result from the test period.

    Attributes:
        window: The window definition (train/test periods)
        optimization_result: Result from parameter optimization on training data
        test_result: Backtest result from applying optimal params to test data
        train_objective_value: Best objective value achieved on training data
        test_objective_value: Objective value achieved on test data
        objective_metric: Name of the metric being optimized
    """

    window: WalkForwardWindow
    optimization_result: OptimizationResult
    test_result: BacktestResult
    train_objective_value: float
    test_objective_value: float
    objective_metric: str

    @property
    def overfitting_ratio(self) -> float:
        """Calculate overfitting ratio: train/test performance.

        A ratio significantly > 1 indicates overfitting to training data.
        A ratio close to 1 indicates good generalization.

        Returns:
            Ratio of training to test performance.
            Returns 0 if test performance is 0 to avoid division by zero.
        """
        if self.test_objective_value == 0:
            return 0.0 if self.train_objective_value == 0 else float("inf")
        return self.train_objective_value / self.test_objective_value

    @property
    def generalization_score(self) -> float:
        """Calculate generalization score (1 - normalized overfitting).

        Score close to 1.0 indicates good generalization.
        Score close to 0.0 indicates poor generalization (overfitting).
        Negative scores indicate severe overfitting.

        Returns:
            Generalization score between -inf and 1.0
        """
        if self.train_objective_value == 0:
            return 1.0 if self.test_objective_value >= 0 else 0.0
        return min(1.0, self.test_objective_value / self.train_objective_value)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "window": self.window.to_dict(),
            "optimization_result": self.optimization_result.to_dict(),
            "test_result": self.test_result.to_dict(),
            "train_objective_value": self.train_objective_value,
            "test_objective_value": self.test_objective_value,
            "objective_metric": self.objective_metric,
            "overfitting_ratio": self.overfitting_ratio,
            "generalization_score": self.generalization_score,
        }


@dataclass
class ParameterStability:
    """Analysis of parameter stability across walk-forward windows.

    Tracks how optimal parameters change across windows to detect
    instability. High variance indicates the strategy is sensitive
    to the time period used for optimization.

    Attributes:
        param_name: Name of the parameter being analyzed
        values: List of optimal values found in each window
        mean: Mean value across windows
        std: Standard deviation across windows
        variance: Variance across windows
        cv: Coefficient of variation (std/mean) - normalized measure
        min_value: Minimum optimal value found
        max_value: Maximum optimal value found
        is_stable: Whether the parameter is considered stable (CV < threshold)
        stability_threshold: CV threshold used for stability check
    """

    param_name: str
    values: list[Any]
    mean: float
    std: float
    variance: float
    cv: float  # Coefficient of variation = std / |mean|
    min_value: Any
    max_value: Any
    is_stable: bool
    stability_threshold: float = 0.3  # Default CV threshold for stability

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        # Convert Decimal values to strings for JSON serialization
        serialized_values = [str(v) if isinstance(v, Decimal) else v for v in self.values]
        serialized_min = str(self.min_value) if isinstance(self.min_value, Decimal) else self.min_value
        serialized_max = str(self.max_value) if isinstance(self.max_value, Decimal) else self.max_value

        return {
            "param_name": self.param_name,
            "values": serialized_values,
            "mean": self.mean,
            "std": self.std,
            "variance": self.variance,
            "cv": self.cv,
            "min_value": serialized_min,
            "max_value": serialized_max,
            "is_stable": self.is_stable,
            "stability_threshold": self.stability_threshold,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ParameterStability:
        """Deserialize from dictionary."""
        return cls(
            param_name=data["param_name"],
            values=data["values"],
            mean=data["mean"],
            std=data["std"],
            variance=data["variance"],
            cv=data["cv"],
            min_value=data["min_value"],
            max_value=data["max_value"],
            is_stable=data["is_stable"],
            stability_threshold=data.get("stability_threshold", 0.3),
        )


def _collect_parameter_values(
    window_results: list[WalkForwardWindowResult],
) -> dict[str, list[Any]]:
    param_values: dict[str, list[Any]] = {}
    for window in window_results:
        for param_name, value in window.optimization_result.best_params.items():
            param_values.setdefault(param_name, []).append(value)
    return param_values


def _is_numeric_param_value(value: Any) -> bool:
    return isinstance(value, Decimal | int | float)


def _all_values_numeric(values: list[Any]) -> bool:
    return all(_is_numeric_param_value(value) for value in values)


def _numeric_cv(mean: float, std: float) -> float:
    if abs(mean) > 1e-10:
        return std / abs(mean)
    return std if std > 0 else 0.0


def _numeric_parameter_stability(
    param_name: str,
    values: list[Any],
    stability_threshold: float,
) -> ParameterStability:
    numeric_values = [float(value) for value in values]

    if len(numeric_values) == 1:
        return ParameterStability(
            param_name=param_name,
            values=values,
            mean=numeric_values[0],
            std=0.0,
            variance=0.0,
            cv=0.0,
            min_value=values[0],
            max_value=values[0],
            is_stable=True,
            stability_threshold=stability_threshold,
        )

    mean = sum(numeric_values) / len(numeric_values)
    variance = sum((value - mean) ** 2 for value in numeric_values) / len(numeric_values)
    std = variance**0.5
    cv = _numeric_cv(mean, std)

    return ParameterStability(
        param_name=param_name,
        values=values,
        mean=mean,
        std=std,
        variance=variance,
        cv=cv,
        min_value=min(values, key=float),
        max_value=max(values, key=float),
        is_stable=cv < stability_threshold,
        stability_threshold=stability_threshold,
    )


def _categorical_parameter_stability(
    param_name: str,
    values: list[Any],
    stability_threshold: float,
) -> ParameterStability:
    unique_values = {str(value) for value in values}
    is_stable = len(unique_values) == 1

    return ParameterStability(
        param_name=param_name,
        values=values,
        mean=0.0,
        std=0.0,
        variance=0.0,
        cv=0.0 if is_stable else float("inf"),
        min_value=values[0],
        max_value=values[-1],
        is_stable=is_stable,
        stability_threshold=stability_threshold,
    )


def _parameter_stability_for_values(
    param_name: str,
    values: list[Any],
    stability_threshold: float,
) -> ParameterStability:
    if _all_values_numeric(values):
        return _numeric_parameter_stability(param_name, values, stability_threshold)
    return _categorical_parameter_stability(param_name, values, stability_threshold)


def calculate_parameter_stability(
    window_results: list[WalkForwardWindowResult],
    stability_threshold: float = 0.3,
) -> dict[str, ParameterStability]:
    """Calculate stability metrics for each optimized parameter.

    Analyzes how optimal parameters vary across walk-forward windows
    to detect instability. Uses coefficient of variation (CV) as the
    primary stability metric.

    Args:
        window_results: List of window results with optimization data
        stability_threshold: CV threshold below which a parameter is
            considered stable. Default 0.3 (30% variation).

    Returns:
        Dictionary mapping parameter names to ParameterStability objects.
        Only numeric parameters are analyzed (Decimal, int, float).

    Note:
        Categorical parameters are tracked but have CV=0 if all values
        are the same, or CV=inf if values differ (since mean is not meaningful).
    """
    if not window_results:
        return {}

    stability_results: dict[str, ParameterStability] = {}

    for param_name, values in _collect_parameter_values(window_results).items():
        try:
            stability_results[param_name] = _parameter_stability_for_values(
                param_name,
                values,
                stability_threshold,
            )
        except (TypeError, ValueError):
            # Handle unexpected value types gracefully
            logger.warning(f"Could not analyze stability for parameter: {param_name}")
            continue

    return stability_results


@dataclass
class WalkForwardResult:
    """Aggregated results from walk-forward optimization.

    Contains results from all windows plus aggregate statistics for
    evaluating parameter stability and out-of-sample performance.

    Attributes:
        windows: List of individual window results
        config: Walk-forward configuration used
        objective_metric: Metric that was optimized
        total_windows: Number of windows processed
        successful_windows: Number of windows that completed successfully
        avg_train_objective: Average training objective across windows
        avg_test_objective: Average out-of-sample objective across windows
        avg_overfitting_ratio: Average overfitting ratio across windows
        combined_test_pnl_usd: Total PnL from all test periods combined
        combined_test_return_pct: Combined return percentage across test periods
        parameter_stability: Stability analysis for each optimized parameter
    """

    windows: list[WalkForwardWindowResult]
    config: WalkForwardConfig
    objective_metric: str
    total_windows: int
    successful_windows: int
    avg_train_objective: float
    avg_test_objective: float
    avg_overfitting_ratio: float
    combined_test_pnl_usd: Decimal
    combined_test_return_pct: Decimal
    parameter_stability: dict[str, ParameterStability] = field(default_factory=dict)

    @property
    def is_overfit(self) -> bool:
        """Check if results suggest overfitting.

        Returns True if average overfitting ratio > 1.5, indicating
        training performance is 50% better than test performance.
        """
        return self.avg_overfitting_ratio > 1.5

    @property
    def avg_generalization_score(self) -> float:
        """Average generalization score across all windows."""
        if not self.windows:
            return 0.0
        scores = [w.generalization_score for w in self.windows]
        return sum(scores) / len(scores)

    @property
    def unstable_parameters(self) -> list[str]:
        """List of parameter names that show instability across windows.

        A parameter is unstable if its coefficient of variation (CV)
        exceeds the stability threshold (default 30%).
        """
        return [name for name, stability in self.parameter_stability.items() if not stability.is_stable]

    @property
    def has_parameter_instability(self) -> bool:
        """Check if any parameters show instability across windows.

        Returns True if at least one parameter has high variance
        across windows, suggesting sensitivity to training period.
        """
        return len(self.unstable_parameters) > 0

    @property
    def avg_parameter_cv(self) -> float:
        """Average coefficient of variation across all numeric parameters.

        Lower values indicate more stable parameter selection.
        Values > 0.3 (30%) suggest potential instability.
        """
        if not self.parameter_stability:
            return 0.0
        cvs = [
            s.cv
            for s in self.parameter_stability.values()
            if s.cv != float("inf")  # Exclude categorical parameters
        ]
        return sum(cvs) / len(cvs) if cvs else 0.0

    def get_optimal_params_by_window(self) -> list[dict[str, Any]]:
        """Get the optimal parameters found in each window.

        Returns:
            List of parameter dictionaries, one per window,
            in window order.
        """
        return [window.optimization_result.best_params for window in self.windows]

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "windows": [w.to_dict() for w in self.windows],
            "config": self.config.to_dict(),
            "objective_metric": self.objective_metric,
            "total_windows": self.total_windows,
            "successful_windows": self.successful_windows,
            "avg_train_objective": self.avg_train_objective,
            "avg_test_objective": self.avg_test_objective,
            "avg_overfitting_ratio": self.avg_overfitting_ratio,
            "avg_generalization_score": self.avg_generalization_score,
            "combined_test_pnl_usd": str(self.combined_test_pnl_usd),
            "combined_test_return_pct": str(self.combined_test_return_pct),
            "is_overfit": self.is_overfit,
            # Parameter stability fields
            "parameter_stability": {name: stability.to_dict() for name, stability in self.parameter_stability.items()},
            "unstable_parameters": self.unstable_parameters,
            "has_parameter_instability": self.has_parameter_instability,
            "avg_parameter_cv": self.avg_parameter_cv,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WalkForwardResult:
        """Deserialize from dictionary.

        Note: This is a partial deserialization that creates a usable
        WalkForwardResult but may not perfectly reconstruct all objects
        (especially nested results that require their own from_dict).
        """
        # Parse parameter stability
        param_stability = {}
        for name, stability_data in data.get("parameter_stability", {}).items():
            param_stability[name] = ParameterStability.from_dict(stability_data)

        return cls(
            windows=[],  # Windows require complex nested deserialization
            config=WalkForwardConfig.from_dict(data["config"]),
            objective_metric=data["objective_metric"],
            total_windows=data["total_windows"],
            successful_windows=data["successful_windows"],
            avg_train_objective=data["avg_train_objective"],
            avg_test_objective=data["avg_test_objective"],
            avg_overfitting_ratio=data["avg_overfitting_ratio"],
            combined_test_pnl_usd=Decimal(data["combined_test_pnl_usd"]),
            combined_test_return_pct=Decimal(data["combined_test_return_pct"]),
            parameter_stability=param_stability,
        )

    def summary(self) -> str:
        """Generate a human-readable summary of walk-forward results.

        Returns:
            Multi-line string with key metrics and insights.
        """
        lines = [
            "=" * 60,
            "WALK-FORWARD OPTIMIZATION RESULTS",
            "=" * 60,
            f"Windows: {self.successful_windows}/{self.total_windows} successful",
            f"Objective: {self.objective_metric}",
            "",
            "Performance Comparison:",
            f"  Avg Training {self.objective_metric}: {self.avg_train_objective:.4f}",
            f"  Avg Test {self.objective_metric}:     {self.avg_test_objective:.4f}",
            f"  Avg Overfitting Ratio:   {self.avg_overfitting_ratio:.2f}x",
            f"  Avg Generalization Score: {self.avg_generalization_score:.2%}",
            "",
            "Combined Test Period Performance:",
            f"  Total PnL:    ${self.combined_test_pnl_usd:,.2f}",
            f"  Total Return: {self.combined_test_return_pct:.2f}%",
            "",
        ]

        # Add parameter stability section
        if self.parameter_stability:
            lines.append("Parameter Stability:")
            for name, stability in self.parameter_stability.items():
                status = "stable" if stability.is_stable else "UNSTABLE"
                lines.append(
                    f"  {name}: CV={stability.cv:.2%} ({status}), range=[{stability.min_value}, {stability.max_value}]"
                )
            lines.append(f"  Average CV: {self.avg_parameter_cv:.2%}")
            lines.append("")

        if self.is_overfit:
            lines.append("⚠️  WARNING: Results suggest overfitting to training data")
        else:
            lines.append("✓ Generalization looks acceptable")

        if self.has_parameter_instability:
            lines.append(f"⚠️  WARNING: Unstable parameters: {', '.join(self.unstable_parameters)}")
        else:
            lines.append("✓ Parameter selection is stable across windows")

        lines.append("=" * 60)

        return "\n".join(lines)


# =============================================================================
# Walk-Forward Optimization Loop
# =============================================================================


_COMPUTED_CONFIG_KEYS = ("duration_seconds", "duration_days", "estimated_ticks")


def _window_config_dict(
    base_config: PnLBacktestConfig,
    start_time: datetime,
    end_time: datetime,
) -> dict[str, Any]:
    """Build a per-window config payload without computed fields."""
    config_dict = base_config.to_dict()
    for key in _COMPUTED_CONFIG_KEYS:
        config_dict.pop(key, None)
    config_dict["start_time"] = start_time.isoformat()
    config_dict["end_time"] = end_time.isoformat()
    return config_dict


def _build_window_config(
    config_cls: Any,
    base_config: PnLBacktestConfig,
    start_time: datetime,
    end_time: datetime,
    overrides: dict[str, Any] | None = None,
) -> PnLBacktestConfig:
    """Create a PnL backtest config for one walk-forward window."""
    config_dict = _window_config_dict(base_config, start_time, end_time)
    if overrides:
        config_dict.update(overrides)
    return config_cls.from_dict(config_dict)


def _partition_best_params(
    base_config: PnLBacktestConfig,
    best_params: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Split optimized params into backtest config overrides and strategy overrides."""
    valid_config_fields = {f for f in vars(base_config) if not f.startswith("_")}
    config_overrides: dict[str, Any] = {}
    strategy_overrides: dict[str, Any] = {}

    for param_name, param_value in best_params.items():
        if param_name in valid_config_fields:
            config_overrides[param_name] = param_value
        else:
            strategy_overrides[param_name] = param_value

    return config_overrides, strategy_overrides


async def _optimize_training_window(
    tuner_cls: Any,
    strategy_factory: Callable[..., Any],
    data_provider_factory: Callable[[], Any],
    backtester_factory: Callable[[Any, dict[str, Any], dict[str, Any]], Any],
    train_config: PnLBacktestConfig,
    param_ranges: OptunaParamRanges,
    objective_metric: str,
    n_trials_per_window: int,
    patience: int | None,
    fee_models: dict[str, Any],
    slippage_models: dict[str, Any],
    show_progress: bool,
    strategy_config: dict[str, Any] | None,
) -> OptimizationResult:
    """Optimize strategy/config params for one training window."""
    tuner = tuner_cls(
        objective_metric=objective_metric,
        patience=patience,
    )
    return await tuner.optimize(
        strategy_factory=strategy_factory,
        data_provider_factory=data_provider_factory,
        backtester_factory=backtester_factory,
        base_config=train_config,
        param_ranges=param_ranges,
        n_trials=n_trials_per_window,
        fee_models=fee_models,
        slippage_models=slippage_models,
        show_progress=show_progress,
        patience=patience,
        strategy_config=strategy_config,
    )


def _build_strategy_for_test(
    strategy_factory: Callable[..., Any],
    strategy_config: dict[str, Any] | None,
    strategy_overrides: dict[str, Any],
) -> Any:
    """Construct the out-of-sample strategy with the current factory arity."""
    if strategy_config is not None:
        merged_strat_config = {**strategy_config, **strategy_overrides}
        return strategy_factory(merged_strat_config)
    if strategy_overrides:
        return strategy_factory(strategy_overrides)
    return strategy_factory()


async def _run_test_window_backtest(
    config_cls: Any,
    strategy_factory: Callable[..., Any],
    data_provider_factory: Callable[[], Any],
    backtester_factory: Callable[[Any, dict[str, Any], dict[str, Any]], Any],
    base_config: PnLBacktestConfig,
    window: WalkForwardWindow,
    optimization_result: OptimizationResult,
    objective_metric: str,
    fee_models: dict[str, Any],
    slippage_models: dict[str, Any],
    strategy_config: dict[str, Any] | None,
) -> tuple[BacktestResult, float]:
    """Backtest one out-of-sample window using optimized params."""
    config_overrides, strategy_overrides = _partition_best_params(base_config, optimization_result.best_params)
    test_config = _build_window_config(
        config_cls,
        base_config,
        window.test_start,
        window.test_end,
        overrides=config_overrides,
    )
    strategy = _build_strategy_for_test(strategy_factory, strategy_config, strategy_overrides)
    data_provider = data_provider_factory()
    backtester = backtester_factory(data_provider, fee_models, slippage_models)
    test_result = await backtester.backtest(strategy, test_config)
    test_objective_value = float(getattr(test_result.metrics, objective_metric))
    return test_result, test_objective_value


async def _run_walk_forward_window(
    config_cls: Any,
    tuner_cls: Any,
    window: WalkForwardWindow,
    total_windows: int,
    strategy_factory: Callable[..., Any],
    data_provider_factory: Callable[[], Any],
    backtester_factory: Callable[[Any, dict[str, Any], dict[str, Any]], Any],
    base_config: PnLBacktestConfig,
    param_ranges: OptunaParamRanges,
    objective_metric: str,
    n_trials_per_window: int,
    patience: int | None,
    fee_models: dict[str, Any],
    slippage_models: dict[str, Any],
    show_progress: bool,
    strategy_config: dict[str, Any] | None,
) -> WalkForwardWindowResult:
    """Run training optimization and out-of-sample validation for one window."""
    logger.info(
        f"Processing window {window.window_index + 1}/{total_windows}: "
        f"Train {window.train_start.date()} to {window.train_end.date()}, "
        f"Test {window.test_start.date()} to {window.test_end.date()}"
    )
    train_config = _build_window_config(config_cls, base_config, window.train_start, window.train_end)
    optimization_result = await _optimize_training_window(
        tuner_cls=tuner_cls,
        strategy_factory=strategy_factory,
        data_provider_factory=data_provider_factory,
        backtester_factory=backtester_factory,
        train_config=train_config,
        param_ranges=param_ranges,
        objective_metric=objective_metric,
        n_trials_per_window=n_trials_per_window,
        patience=patience,
        fee_models=fee_models,
        slippage_models=slippage_models,
        show_progress=show_progress,
        strategy_config=strategy_config,
    )
    train_objective_value = optimization_result.best_value
    logger.info(
        f"Window {window.window_index + 1} training complete: best {objective_metric}={train_objective_value:.4f}"
    )

    test_result, test_objective_value = await _run_test_window_backtest(
        config_cls=config_cls,
        strategy_factory=strategy_factory,
        data_provider_factory=data_provider_factory,
        backtester_factory=backtester_factory,
        base_config=base_config,
        window=window,
        optimization_result=optimization_result,
        objective_metric=objective_metric,
        fee_models=fee_models,
        slippage_models=slippage_models,
        strategy_config=strategy_config,
    )
    logger.info(
        f"Window {window.window_index + 1} test complete: "
        f"{objective_metric}={test_objective_value:.4f} "
        f"(train was {train_objective_value:.4f})"
    )

    return WalkForwardWindowResult(
        window=window,
        optimization_result=optimization_result,
        test_result=test_result,
        train_objective_value=train_objective_value,
        test_objective_value=test_objective_value,
        objective_metric=objective_metric,
    )


def _empty_walk_forward_result(
    wf_config: WalkForwardConfig,
    objective_metric: str,
    total_windows: int,
) -> WalkForwardResult:
    """Return the existing empty result shape for no successful windows."""
    return WalkForwardResult(
        windows=[],
        config=wf_config,
        objective_metric=objective_metric,
        total_windows=total_windows,
        successful_windows=0,
        avg_train_objective=0.0,
        avg_test_objective=0.0,
        avg_overfitting_ratio=0.0,
        combined_test_pnl_usd=Decimal("0"),
        combined_test_return_pct=Decimal("0"),
    )


def _average_objective(
    window_results: list[WalkForwardWindowResult],
    attr_name: str,
    successful_windows: int,
) -> float:
    """Average a numeric objective value across successful windows."""
    return sum(getattr(window_result, attr_name) for window_result in window_results) / successful_windows


def _average_finite_overfitting_ratio(window_results: list[WalkForwardWindowResult]) -> float:
    """Average finite overfitting ratios while preserving the infinite fallback."""
    overfitting_ratios = [w.overfitting_ratio for w in window_results if w.overfitting_ratio != float("inf")]
    if overfitting_ratios:
        return sum(overfitting_ratios) / len(overfitting_ratios)
    return float("inf")


def _sum_test_metric(window_results: list[WalkForwardWindowResult], attr_name: str) -> Decimal:
    """Sum a Decimal-valued test metric across walk-forward windows."""
    return sum((getattr(w.test_result.metrics, attr_name) for w in window_results), Decimal(0))


def _aggregate_walk_forward_result(
    window_results: list[WalkForwardWindowResult],
    wf_config: WalkForwardConfig,
    objective_metric: str,
    total_windows: int,
) -> WalkForwardResult:
    """Aggregate per-window walk-forward results into the public result object."""
    successful_windows = len(window_results)

    return WalkForwardResult(
        windows=window_results,
        config=wf_config,
        objective_metric=objective_metric,
        total_windows=total_windows,
        successful_windows=successful_windows,
        avg_train_objective=_average_objective(window_results, "train_objective_value", successful_windows),
        avg_test_objective=_average_objective(window_results, "test_objective_value", successful_windows),
        avg_overfitting_ratio=_average_finite_overfitting_ratio(window_results),
        combined_test_pnl_usd=_sum_test_metric(window_results, "net_pnl_usd"),
        combined_test_return_pct=_sum_test_metric(window_results, "total_return_pct"),
        parameter_stability=calculate_parameter_stability(window_results),
    )


def _log_walk_forward_summary(result: WalkForwardResult, objective_metric: str) -> None:
    """Log final walk-forward instability warnings and completion summary."""
    if result.has_parameter_instability:
        logger.warning(
            f"Parameter instability detected: {', '.join(result.unstable_parameters)}. "
            f"Optimal parameters vary significantly across windows."
        )

    logger.info(
        f"Walk-forward optimization complete: "
        f"{result.successful_windows}/{result.total_windows} windows, "
        f"avg train {objective_metric}={result.avg_train_objective:.4f}, "
        f"avg test {objective_metric}={result.avg_test_objective:.4f}, "
        f"overfitting ratio={result.avg_overfitting_ratio:.2f}x, "
        f"avg param CV={result.avg_parameter_cv:.2%}"
    )


async def run_walk_forward_optimization(
    strategy_factory: Callable[..., Any],
    data_provider_factory: Callable[[], Any],
    backtester_factory: Callable[[Any, dict[str, Any], dict[str, Any]], Any],
    base_config: PnLBacktestConfig,
    param_ranges: OptunaParamRanges,
    wf_config: WalkForwardConfig,
    objective_metric: str = "sharpe_ratio",
    n_trials_per_window: int = 50,
    patience: int | None = 10,
    fee_models: dict[str, Any] | None = None,
    slippage_models: dict[str, Any] | None = None,
    show_progress: bool = True,
    strategy_config: dict[str, Any] | None = None,
) -> WalkForwardResult:
    """Run walk-forward optimization across multiple train/test windows.

    Walk-forward optimization addresses overfitting by:
    1. Splitting data into multiple train/test windows
    2. Optimizing parameters on each training window
    3. Testing optimized parameters on the out-of-sample test window
    4. Aggregating out-of-sample results for realistic performance estimates

    This provides a more realistic estimate of live trading performance
    than a single in-sample optimization.

    Args:
        strategy_factory: Factory function that returns a new strategy instance.
            Must be picklable (module-level function).
        data_provider_factory: Factory function that returns a new data provider.
            Must be picklable (module-level function).
        backtester_factory: Factory function that returns a new PnLBacktester.
            Takes (data_provider, fee_models, slippage_models) as arguments.
        base_config: Base backtest configuration (will be modified per window).
        param_ranges: Parameter ranges for optimization.
            See OptunaTuner for format details.
        wf_config: Walk-forward configuration (window sizes, step, etc.)
        objective_metric: Metric to optimize (default: 'sharpe_ratio')
        n_trials_per_window: Number of optimization trials per window (default: 50)
        patience: Early stopping patience (default: 10, None to disable)
        fee_models: Optional fee models dict
        slippage_models: Optional slippage models dict
        show_progress: Show progress bar during optimization (default: True)

    Returns:
        WalkForwardResult with aggregated results and per-window details

    Raises:
        ValueError: If base_config dates don't allow enough windows

    Example:
        from almanak.framework.backtesting.pnl import (
            WalkForwardConfig,
            run_walk_forward_optimization,
            continuous,
            discrete,
        )

        wf_config = WalkForwardConfig.from_days(
            train_days=90,
            test_days=30,
            step_days=30,
        )

        param_ranges = {
            "trade_size_usd": continuous(Decimal("100"), Decimal("5000")),
            "interval_seconds": discrete(3600, 14400, step=3600),
        }

        result = await run_walk_forward_optimization(
            strategy_factory=create_strategy,
            data_provider_factory=create_data_provider,
            backtester_factory=create_backtester,
            base_config=base_config,
            param_ranges=param_ranges,
            wf_config=wf_config,
            n_trials_per_window=50,
        )

        print(result.summary())
    """
    # Import here to avoid circular imports
    from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
    from almanak.framework.backtesting.pnl.optuna_tuner import OptunaTuner

    # Generate windows from base config dates
    windows = split_walk_forward(
        start_date=base_config.start_time,
        end_date=base_config.end_time,
        config=wf_config,
    )

    logger.info(
        f"Starting walk-forward optimization with {len(windows)} windows, {n_trials_per_window} trials per window"
    )

    window_results: list[WalkForwardWindowResult] = []
    total_windows = len(windows)
    fee_models = fee_models or {}
    slippage_models = slippage_models or {}

    for window in windows:
        window_result = await _run_walk_forward_window(
            config_cls=PnLBacktestConfig,
            tuner_cls=OptunaTuner,
            window=window,
            total_windows=total_windows,
            strategy_factory=strategy_factory,
            data_provider_factory=data_provider_factory,
            backtester_factory=backtester_factory,
            base_config=base_config,
            param_ranges=param_ranges,
            objective_metric=objective_metric,
            n_trials_per_window=n_trials_per_window,
            patience=patience,
            fee_models=fee_models,
            slippage_models=slippage_models,
            show_progress=show_progress,
            strategy_config=strategy_config,
        )
        window_results.append(window_result)

    if not window_results:
        return _empty_walk_forward_result(wf_config, objective_metric, total_windows)

    result = _aggregate_walk_forward_result(window_results, wf_config, objective_metric, total_windows)
    _log_walk_forward_summary(result, objective_metric)
    return result


def run_walk_forward_optimization_sync(
    strategy_factory: Callable[[], Any],
    data_provider_factory: Callable[[], Any],
    backtester_factory: Callable[[Any, dict[str, Any], dict[str, Any]], Any],
    base_config: PnLBacktestConfig,
    param_ranges: OptunaParamRanges,
    wf_config: WalkForwardConfig,
    objective_metric: str = "sharpe_ratio",
    n_trials_per_window: int = 50,
    patience: int | None = 10,
    fee_models: dict[str, Any] | None = None,
    slippage_models: dict[str, Any] | None = None,
    show_progress: bool = True,
) -> WalkForwardResult:
    """Synchronous wrapper for run_walk_forward_optimization.

    Convenience method for running walk-forward optimization from synchronous code.

    Args:
        Same as run_walk_forward_optimization()

    Returns:
        Same as run_walk_forward_optimization()
    """
    return asyncio.run(
        run_walk_forward_optimization(
            strategy_factory=strategy_factory,
            data_provider_factory=data_provider_factory,
            backtester_factory=backtester_factory,
            base_config=base_config,
            param_ranges=param_ranges,
            wf_config=wf_config,
            objective_metric=objective_metric,
            n_trials_per_window=n_trials_per_window,
            patience=patience,
            fee_models=fee_models,
            slippage_models=slippage_models,
            show_progress=show_progress,
        )
    )


__all__ = [
    "WalkForwardWindow",
    "WalkForwardConfig",
    "split_walk_forward",
    "split_walk_forward_tuples",
    # Walk-forward optimization results
    "WalkForwardWindowResult",
    "WalkForwardResult",
    # Parameter stability analysis
    "ParameterStability",
    "calculate_parameter_stability",
    # Walk-forward optimization loop
    "run_walk_forward_optimization",
    "run_walk_forward_optimization_sync",
]
