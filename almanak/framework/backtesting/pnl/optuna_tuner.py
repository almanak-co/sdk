"""Optuna-based Bayesian optimization for backtest parameter tuning.

This module provides OptunaTuner, a wrapper around Optuna's study object
for intelligent parameter optimization. It uses Bayesian optimization
(via Tree-structured Parzen Estimator) to efficiently explore parameter
spaces and find optimal configurations.

Key Components:
    - OptunaTuner: Main class wrapping Optuna study
    - ParamType: Enum for parameter types (continuous, discrete, categorical)
    - ParamRange: Typed parameter range with explicit type specification
    - OptunaTunerConfig: Configuration for the tuner
    - OptimizationResult: Result from an optimization run
    - EarlyStoppingCallback: Callback for stopping when improvement plateaus

Parameter Types:
    - Continuous: Float/Decimal ranges (min, max) with optional step and log scale
    - Discrete: Integer ranges (min, max) with optional step
    - Categorical: List of discrete choices (any type)

Early Stopping:
    The OptunaTuner supports early stopping when the optimization plateaus.
    Configure with `patience` parameter (number of trials without improvement).
    When early stopping triggers, the optimization terminates gracefully.

Example:
    from almanak.framework.backtesting.pnl.optuna_tuner import (
        OptunaTuner,
        continuous,
        discrete,
        categorical,
    )

    tuner = OptunaTuner(
        objective_metric="sharpe_ratio",
        direction="maximize",
    )

    # Define parameter ranges with explicit types
    param_ranges = {
        "trade_size_usd": continuous(Decimal("100"), Decimal("5000")),
        "interval_seconds": discrete(3600, 86400, step=3600),
        "risk_level": categorical(["low", "medium", "high"]),
    }

    # Run optimization with early stopping (patience=10 trials)
    result = await tuner.optimize(
        strategy_factory=create_strategy,
        data_provider_factory=create_data_provider,
        backtester_factory=create_backtester,
        base_config=base_config,
        param_ranges=param_ranges,
        n_trials=100,
        patience=10,  # Stop if no improvement for 10 trials
    )

    # Export optimization history to JSON
    history = tuner.export_history()
    with open("optimization_history.json", "w") as f:
        json.dump(history, f, indent=2)
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast


def _actionable_optuna_import_error(exc: ImportError) -> ImportError:
    """Map a missing-optuna ImportError to an actionable message; re-raise others."""
    if exc.name and exc.name.startswith("optuna"):
        return ImportError(
            "Backtest parameter optimization requires the optional 'optuna' "
            "dependency. Install it with: pip install 'almanak[backtest]'"
        )
    raise exc


try:
    import optuna
    from optuna.samplers import TPESampler
    from optuna.trial import FrozenTrial, Trial
except ImportError as exc:
    raise _actionable_optuna_import_error(exc) from exc

from almanak.framework.backtesting.models import BacktestResult
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig

if TYPE_CHECKING:
    from optuna.study import Study

logger = logging.getLogger(__name__)


# Valid objective metrics that can be optimized
OBJECTIVE_METRICS = {
    "sharpe_ratio",
    "sortino_ratio",
    "calmar_ratio",
    "total_return_pct",
    "annualized_return_pct",
    "max_drawdown_pct",
    "profit_factor",
    "win_rate",
    "net_pnl_usd",
}

# Direction for each metric (maximize or minimize)
METRIC_DIRECTIONS: dict[str, Literal["maximize", "minimize"]] = {
    "sharpe_ratio": "maximize",
    "sortino_ratio": "maximize",
    "calmar_ratio": "maximize",
    "total_return_pct": "maximize",
    "annualized_return_pct": "maximize",
    "max_drawdown_pct": "minimize",  # Lower drawdown is better
    "profit_factor": "maximize",
    "win_rate": "maximize",
    "net_pnl_usd": "maximize",
}


# =============================================================================
# Early Stopping Callback
# =============================================================================


class EarlyStoppingCallback:
    """Callback for early stopping when optimization improvement plateaus.

    This callback monitors the best objective value and stops the study
    if no improvement is observed for a configurable number of trials
    (patience). This prevents wasting computation when optimization
    has converged.

    Attributes:
        patience: Number of trials to wait without improvement before stopping
        min_delta: Minimum improvement required to reset patience counter
        direction: 'maximize' or 'minimize' - determines what counts as improvement
        verbose: If True, log when early stopping triggers

    Example:
        callback = EarlyStoppingCallback(patience=10, verbose=True)
        study.optimize(objective, n_trials=100, callbacks=[callback])
    """

    def __init__(
        self,
        patience: int = 10,
        min_delta: float = 0.0,
        direction: Literal["maximize", "minimize"] = "maximize",
        verbose: bool = True,
    ) -> None:
        """Initialize the early stopping callback.

        Args:
            patience: Number of trials without improvement before stopping.
                Default is 10 trials.
            min_delta: Minimum change in objective value to count as improvement.
                Default is 0.0 (any improvement counts).
            direction: Optimization direction - 'maximize' or 'minimize'.
                Determines what counts as "improvement".
            verbose: If True, log when early stopping is triggered.
        """
        if patience < 1:
            raise ValueError(f"patience must be >= 1, got {patience}")
        if min_delta < 0:
            raise ValueError(f"min_delta must be >= 0, got {min_delta}")

        self.patience = patience
        self.min_delta = min_delta
        self.direction = direction
        self.verbose = verbose

        self._best_value: float | None = None
        self._trials_without_improvement = 0
        self._stopped_early = False
        self._best_trial_number: int | None = None

    def __call__(self, study: Study, trial: optuna.trial.FrozenTrial) -> None:
        """Called after each trial completes.

        Args:
            study: The Optuna study object
            trial: The completed trial
        """
        # Skip pruned or failed trials
        if trial.state != optuna.trial.TrialState.COMPLETE:
            return

        current_value = trial.value
        if current_value is None:
            return

        # Handle inf values (failed backtests)
        if current_value in (float("inf"), float("-inf")):
            self._trials_without_improvement += 1
            self._check_early_stop(study, trial.number)
            return

        # Check if this is an improvement
        is_improvement = False

        if self._best_value is None:
            # First valid trial
            is_improvement = True
        elif self.direction == "maximize":
            is_improvement = current_value > self._best_value + self.min_delta
        else:  # minimize
            is_improvement = current_value < self._best_value - self.min_delta

        if is_improvement:
            self._best_value = current_value
            self._best_trial_number = trial.number
            self._trials_without_improvement = 0
            logger.debug(f"Early stopping: New best value {current_value:.6f} at trial {trial.number}")
        else:
            self._trials_without_improvement += 1
            logger.debug(
                f"Early stopping: No improvement for {self._trials_without_improvement}/{self.patience} trials"
            )

        self._check_early_stop(study, trial.number)

    def _check_early_stop(self, study: Study, trial_number: int) -> None:
        """Check if early stopping should trigger.

        Args:
            study: The Optuna study object
            trial_number: Current trial number
        """
        if self._trials_without_improvement >= self.patience:
            self._stopped_early = True
            if self.verbose:
                logger.info(
                    f"Early stopping triggered at trial {trial_number}. "
                    f"No improvement for {self.patience} trials. "
                    f"Best value: {self._best_value} at trial {self._best_trial_number}"
                )
            study.stop()

    @property
    def stopped_early(self) -> bool:
        """Whether the study was stopped early."""
        return self._stopped_early

    @property
    def trials_without_improvement(self) -> int:
        """Current count of trials without improvement."""
        return self._trials_without_improvement

    @property
    def best_value(self) -> float | None:
        """Best objective value seen so far."""
        return self._best_value

    def reset(self) -> None:
        """Reset the callback state for reuse."""
        self._best_value = None
        self._trials_without_improvement = 0
        self._stopped_early = False
        self._best_trial_number = None


# =============================================================================
# Trial History Export
# =============================================================================


@dataclass
class TrialHistoryEntry:
    """Single entry in optimization history.

    Attributes:
        trial_number: Sequential trial number
        state: Trial state (COMPLETE, PRUNED, FAIL)
        value: Objective value (None if not complete)
        params: Dictionary of parameter values
        datetime_start: When the trial started
        datetime_complete: When the trial completed
        duration_seconds: Trial duration in seconds
        user_attrs: User-defined attributes
        system_attrs: System attributes
    """

    trial_number: int
    state: str
    value: float | None
    params: dict[str, Any]
    datetime_start: str | None
    datetime_complete: str | None
    duration_seconds: float | None
    user_attrs: dict[str, Any] = field(default_factory=dict)
    system_attrs: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        # Convert Decimal params to strings for JSON compatibility
        serialized_params = {}
        for k, v in self.params.items():
            if isinstance(v, Decimal):
                serialized_params[k] = str(v)
            else:
                serialized_params[k] = v

        return {
            "trial_number": self.trial_number,
            "state": self.state,
            "value": self.value,
            "params": serialized_params,
            "datetime_start": self.datetime_start,
            "datetime_complete": self.datetime_complete,
            "duration_seconds": self.duration_seconds,
            "user_attrs": self.user_attrs,
            "system_attrs": self.system_attrs,
        }


@dataclass
class OptimizationHistory:
    """Complete optimization history for export.

    Attributes:
        study_name: Name of the Optuna study
        objective_metric: Metric being optimized
        direction: Optimization direction
        n_trials: Total number of trials
        n_complete: Number of completed trials
        n_pruned: Number of pruned trials
        n_failed: Number of failed trials
        best_trial_number: Trial number with best value
        best_value: Best objective value achieved
        best_params: Parameters from best trial
        param_names: List of parameter names
        trials: List of trial history entries
        stopped_early: Whether optimization stopped early
        export_timestamp: When the history was exported
    """

    study_name: str | None
    objective_metric: str
    direction: str
    n_trials: int
    n_complete: int
    n_pruned: int
    n_failed: int
    best_trial_number: int | None
    best_value: float | None
    best_params: dict[str, Any] | None
    param_names: list[str]
    trials: list[TrialHistoryEntry]
    stopped_early: bool
    export_timestamp: str

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary for JSON export."""
        # Convert Decimal params to strings
        serialized_best_params = None
        if self.best_params is not None:
            serialized_best_params = {}
            for k, v in self.best_params.items():
                if isinstance(v, Decimal):
                    serialized_best_params[k] = str(v)
                else:
                    serialized_best_params[k] = v

        return {
            "study_name": self.study_name,
            "objective_metric": self.objective_metric,
            "direction": self.direction,
            "n_trials": self.n_trials,
            "n_complete": self.n_complete,
            "n_pruned": self.n_pruned,
            "n_failed": self.n_failed,
            "best_trial_number": self.best_trial_number,
            "best_value": self.best_value,
            "best_params": serialized_best_params,
            "param_names": self.param_names,
            "trials": [t.to_dict() for t in self.trials],
            "stopped_early": self.stopped_early,
            "export_timestamp": self.export_timestamp,
        }

    def to_json(self, indent: int = 2) -> str:
        """Export to JSON string.

        Args:
            indent: JSON indentation level (default: 2)

        Returns:
            JSON string representation
        """
        return json.dumps(self.to_dict(), indent=indent)

    def save(self, path: str | Path) -> None:
        """Save history to JSON file.

        Args:
            path: File path for the JSON output
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            f.write(self.to_json())
        logger.info(f"Optimization history saved to {path}")


# =============================================================================
# Parameter Type Classes
# =============================================================================


class ParamType(Enum):
    """Parameter type enumeration for Optuna optimization.

    Defines the three main parameter types supported:
    - CONTINUOUS: Float or Decimal ranges with optional step and log scale
    - DISCRETE: Integer ranges with optional step
    - CATEGORICAL: List of discrete choices of any type
    """

    CONTINUOUS = "continuous"
    DISCRETE = "discrete"
    CATEGORICAL = "categorical"


def _numeric_bound(value: float | int | Decimal) -> float | int:
    """Return a comparable numeric bound for validation."""
    if type(value) is Decimal:
        return float(value)
    return cast(float | int, value)


def _serialize_param_range_value(value: Any) -> Any:
    """Serialize Decimal bounds without losing financial precision."""
    return str(value) if type(value) is Decimal else value


def _validate_positive_step(step: float | int) -> None:
    """Reject step values Optuna will reject later."""
    if step <= 0:
        raise ValueError("'step' must be positive")


def _validate_param_bounds(low: float | int | Decimal, high: float | int | Decimal) -> None:
    """Validate lower/upper range ordering."""
    if _numeric_bound(low) >= _numeric_bound(high):
        raise ValueError(f"'low' ({low}) must be less than 'high' ({high})")


def _validate_categorical_param_range(param: ParamRange) -> None:
    """Validate categorical-only parameter fields."""
    if _choices_empty(param):
        raise ValueError("Categorical parameters require non-empty 'choices' list")
    if param.low is not None or param.high is not None:
        raise ValueError("Categorical parameters should not have 'low' or 'high'")


def _choices_empty(param: ParamRange) -> bool:
    """Return whether a categorical range has no usable choices."""
    return param.choices is None or len(param.choices) == 0


def _validate_bounded_param_range(param: ParamRange) -> tuple[float | int | Decimal, float | int | Decimal]:
    """Validate fields shared by continuous and discrete ranges."""
    if param.low is None or param.high is None:
        raise ValueError(f"{param.param_type.value} parameters require 'low' and 'high'")
    if param.choices is not None:
        raise ValueError(f"{param.param_type.value} parameters should not have 'choices'")

    if type(param.low) is Decimal or type(param.high) is Decimal:
        object.__setattr__(param, "is_decimal", True)

    _validate_param_bounds(param.low, param.high)
    return param.low, param.high


def _validate_discrete_param_range(param: ParamRange) -> None:
    """Validate discrete integer range fields."""
    if not isinstance(param.low, int) or not isinstance(param.high, int):
        raise ValueError("Discrete parameters require integer 'low' and 'high'")
    if param.step is not None:
        if not isinstance(param.step, int):
            raise ValueError("Discrete parameter 'step' must be an integer")
        _validate_positive_step(param.step)
    if param.log:
        raise ValueError("Discrete parameters do not support 'log' scale")


def _validate_continuous_param_range(param: ParamRange) -> None:
    """Validate continuous numeric range fields."""
    if param.log and param.step is not None:
        raise ValueError("Log scale does not support 'step'")
    if param.step is not None:
        _validate_positive_step(param.step)
    if param.log:
        assert param.low is not None
        if _numeric_bound(param.low) <= 0:
            raise ValueError("Log scale requires positive 'low' value")


def _param_range_optional_fields(param: ParamRange) -> dict[str, Any]:
    """Return optional serialized fields for a ParamRange."""
    fields = {
        key: _serialize_param_range_value(value)
        for key, value in (("low", param.low), ("high", param.high))
        if value is not None
    }
    fields.update(
        {key: value for key, value in (("choices", param.choices), ("step", param.step)) if value is not None}
    )
    if param.log:
        fields["log"] = True
    return fields


@dataclass
class ParamRange:
    """Typed parameter range for Optuna optimization.

    A ParamRange explicitly defines the type and bounds for a parameter,
    making it easier to configure optimization and providing clear
    mapping to Optuna's suggest methods.

    Attributes:
        param_type: Type of parameter (continuous, discrete, categorical)
        low: Lower bound for continuous/discrete (required for ranges)
        high: Upper bound for continuous/discrete (required for ranges)
        choices: List of choices for categorical parameters
        step: Step size for discrete ranges (optional)
        log: Use log-uniform distribution for continuous params (default: False)
        is_decimal: If True, convert suggested float back to Decimal

    Example:
        # Continuous float range
        ParamRange(ParamType.CONTINUOUS, low=0.001, high=0.1)

        # Continuous with log scale (for params spanning orders of magnitude)
        ParamRange(ParamType.CONTINUOUS, low=0.0001, high=0.1, log=True)

        # Discrete integer range with step
        ParamRange(ParamType.DISCRETE, low=3600, high=86400, step=3600)

        # Categorical choices
        ParamRange(ParamType.CATEGORICAL, choices=["low", "medium", "high"])

        # Decimal range (auto-converted)
        ParamRange(ParamType.CONTINUOUS, low=Decimal("1000"), high=Decimal("10000"))
    """

    param_type: ParamType
    low: float | int | Decimal | None = None
    high: float | int | Decimal | None = None
    choices: list[Any] | None = None
    step: float | int | None = None
    log: bool = False
    is_decimal: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        """Validate the parameter range configuration."""
        if self.param_type == ParamType.CATEGORICAL:
            _validate_categorical_param_range(self)
            return

        _validate_bounded_param_range(self)
        if self.param_type == ParamType.DISCRETE:
            _validate_discrete_param_range(self)
        elif self.param_type == ParamType.CONTINUOUS:
            _validate_continuous_param_range(self)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        result: dict[str, Any] = {"param_type": self.param_type.value}
        result.update(_param_range_optional_fields(self))
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ParamRange:
        """Deserialize from dictionary."""
        param_type = ParamType(data["param_type"])
        low = data.get("low")
        high = data.get("high")
        # Convert string back to Decimal if it looks like a Decimal
        if isinstance(low, str):
            low = Decimal(low)
        if isinstance(high, str):
            high = Decimal(high)
        return cls(
            param_type=param_type,
            low=low,
            high=high,
            choices=data.get("choices"),
            step=data.get("step"),
            log=data.get("log", False),
        )


# =============================================================================
# Factory Functions for Parameter Ranges
# =============================================================================


def continuous(
    low: float | Decimal,
    high: float | Decimal,
    step: float | None = None,
    log: bool = False,
) -> ParamRange:
    """Create a continuous (float/Decimal) parameter range.

    Args:
        low: Lower bound of the range
        high: Upper bound of the range
        step: Optional step size for discrete steps within range
        log: If True, use log-uniform distribution (good for learning rates,
             or parameters spanning orders of magnitude like 0.0001 to 0.1)

    Returns:
        ParamRange configured for continuous optimization

    Examples:
        # Simple float range
        continuous(0.0, 1.0)

        # Decimal range for financial values
        continuous(Decimal("1000"), Decimal("100000"))

        # Log scale for learning rate
        continuous(0.0001, 0.1, log=True)

        # Stepped range
        continuous(0.0, 1.0, step=0.1)
    """
    return ParamRange(
        param_type=ParamType.CONTINUOUS,
        low=low,
        high=high,
        step=step,
        log=log,
    )


def discrete(
    low: int,
    high: int,
    step: int | None = None,
) -> ParamRange:
    """Create a discrete (integer) parameter range.

    Args:
        low: Lower bound of the range (inclusive)
        high: Upper bound of the range (inclusive)
        step: Optional step size (e.g., step=100 means values 0, 100, 200, ...)

    Returns:
        ParamRange configured for discrete optimization

    Examples:
        # Simple integer range
        discrete(1, 100)

        # Interval seconds (hourly steps)
        discrete(3600, 86400, step=3600)

        # Number of layers
        discrete(1, 10)
    """
    return ParamRange(
        param_type=ParamType.DISCRETE,
        low=low,
        high=high,
        step=step,
    )


def categorical(choices: list[Any]) -> ParamRange:
    """Create a categorical parameter from a list of choices.

    Args:
        choices: List of valid choices (can be any type: str, int, float, etc.)

    Returns:
        ParamRange configured for categorical optimization

    Examples:
        # String choices
        categorical(["low", "medium", "high"])

        # Integer choices
        categorical([1, 2, 4, 8, 16])

        # Mixed types (not recommended but supported)
        categorical([True, False, "auto"])
    """
    return ParamRange(
        param_type=ParamType.CATEGORICAL,
        choices=choices,
    )


def log_uniform(low: float | Decimal, high: float | Decimal) -> ParamRange:
    """Convenience function for log-uniform continuous range.

    Use for parameters that span multiple orders of magnitude,
    such as learning rates (0.0001 to 0.1) or regularization coefficients.

    Args:
        low: Lower bound (must be positive)
        high: Upper bound

    Returns:
        ParamRange configured for log-uniform distribution

    Examples:
        # Learning rate
        log_uniform(0.0001, 0.1)

        # Regularization coefficient
        log_uniform(1e-6, 1e-2)
    """
    return continuous(low, high, log=True)


# =============================================================================
# Legacy Support: Convert old-style param ranges to ParamRange
# =============================================================================


def _split_legacy_tuple(name: str, value: tuple[Any, ...]) -> tuple[Any, Any, Any | None]:
    """Split a legacy tuple into bounds plus optional step."""
    if len(value) == 2:
        min_val, max_val = value
        return min_val, max_val, None
    if len(value) == 3:
        min_val, max_val, step = value
        return min_val, max_val, step
    raise ValueError(f"Parameter '{name}' tuple must have 2 or 3 elements, got {len(value)}")


def _is_decimal_range(min_val: Any, max_val: Any) -> bool:
    """Return whether either legacy bound should preserve Decimal output."""
    return type(min_val) is Decimal or type(max_val) is Decimal


def _to_decimal_bound(value: Any) -> Decimal:
    """Convert a legacy bound to Decimal without binary-float string expansion."""
    return value if type(value) is Decimal else Decimal(str(value))


def _is_integer_range(min_val: Any, max_val: Any) -> bool:
    """Return whether both legacy bounds describe a discrete integer range."""
    return isinstance(min_val, int) and isinstance(max_val, int)


def _is_numeric_range(min_val: Any, max_val: Any) -> bool:
    """Return whether both legacy bounds describe a continuous numeric range."""
    numeric_types = (int, float)
    return isinstance(min_val, numeric_types) and isinstance(max_val, numeric_types)


def _continuous_legacy_step(step: Any | None) -> float | None:
    """Convert optional legacy continuous steps to Optuna's float step."""
    return None if step is None else float(step)


def _convert_legacy_tuple(name: str, value: tuple[Any, ...]) -> ParamRange:
    """Convert a legacy tuple range to a typed ParamRange."""
    min_val, max_val, step = _split_legacy_tuple(name, value)

    if _is_decimal_range(min_val, max_val):
        return continuous(
            _to_decimal_bound(min_val),
            _to_decimal_bound(max_val),
            step=_continuous_legacy_step(step),
        )
    if _is_integer_range(min_val, max_val):
        return discrete(min_val, max_val, step=step)
    if _is_numeric_range(min_val, max_val):
        return continuous(float(min_val), float(max_val), step=_continuous_legacy_step(step))

    raise ValueError(f"Parameter '{name}' range must be Decimal, int, or float, got {type(min_val).__name__}")


def _convert_legacy_param(
    name: str,
    value: list[Any] | tuple[Any, ...] | ParamRange,
) -> ParamRange:
    """Convert legacy parameter format to ParamRange.

    Supports backward compatibility with old format:
    - list: categorical choices
    - tuple(min, max): continuous range
    - tuple(min, max, step): discrete range with step

    Args:
        name: Parameter name (for error messages)
        value: Legacy parameter value or ParamRange

    Returns:
        ParamRange instance
    """
    # Already a ParamRange - return as-is
    if isinstance(value, ParamRange):
        return value

    # List: categorical
    if isinstance(value, list):
        return categorical(value)

    # Tuple: range
    if isinstance(value, tuple):
        return _convert_legacy_tuple(name, value)

    raise ValueError(f"Parameter '{name}' must be list, tuple, or ParamRange, got {type(value).__name__}")


# Type aliases for parameter ranges
TypedParamRanges = dict[str, ParamRange]


def _suggest_categorical_param(trial: Trial, name: str, param: ParamRange) -> Any:
    """Suggest a categorical parameter value."""
    assert param.choices is not None
    return trial.suggest_categorical(name, param.choices)


def _suggest_discrete_param(trial: Trial, name: str, param: ParamRange) -> int:
    """Suggest a discrete integer parameter value."""
    assert param.low is not None and param.high is not None
    if param.step is not None:
        return trial.suggest_int(name, int(param.low), int(param.high), step=int(param.step))
    return trial.suggest_int(name, int(param.low), int(param.high))


def _continuous_bounds(param: ParamRange) -> tuple[float, float]:
    """Return continuous bounds in Optuna's float format."""
    assert param.low is not None and param.high is not None
    return float(param.low), float(param.high)


def _suggest_continuous_float(trial: Trial, name: str, param: ParamRange) -> float:
    """Suggest a continuous float parameter value."""
    low, high = _continuous_bounds(param)
    if param.log:
        return trial.suggest_float(name, low, high, log=True)
    if param.step is not None:
        return trial.suggest_float(name, low, high, step=param.step)
    return trial.suggest_float(name, low, high)


def _suggest_continuous_param(trial: Trial, name: str, param: ParamRange) -> float | Decimal:
    """Suggest a continuous parameter value, preserving Decimal ranges."""
    suggested = _suggest_continuous_float(trial, name, param)
    if param.is_decimal:
        return Decimal(str(round(suggested, 6)))
    return suggested


@dataclass
class OptunaTunerConfig:
    """Configuration for OptunaTuner.

    Attributes:
        objective_metric: The metric to optimize (e.g., 'sharpe_ratio')
        direction: 'maximize' or 'minimize' - overrides default for metric
        study_name: Name for the Optuna study (for persistence)
        sampler_seed: Random seed for the sampler (for reproducibility)
        log_level: Optuna logging level (e.g., 'WARNING', 'INFO')
        patience: Number of trials without improvement before early stopping.
            Set to None to disable early stopping. Default is None.
        min_delta: Minimum improvement required to reset the patience counter.
            Default is 0.0 (any improvement counts).
    """

    objective_metric: str = "sharpe_ratio"
    direction: Literal["maximize", "minimize"] | None = None
    study_name: str | None = None
    sampler_seed: int | None = None
    log_level: str = "WARNING"
    patience: int | None = None
    min_delta: float = 0.0

    def __post_init__(self) -> None:
        """Validate configuration."""
        if self.objective_metric not in OBJECTIVE_METRICS:
            raise ValueError(
                f"Invalid objective_metric '{self.objective_metric}'. Valid options: {sorted(OBJECTIVE_METRICS)}"
            )

        # Set direction based on metric if not explicitly provided
        if self.direction is None:
            self.direction = METRIC_DIRECTIONS.get(self.objective_metric, "maximize")

        # Validate patience
        if self.patience is not None and self.patience < 1:
            raise ValueError(f"patience must be >= 1, got {self.patience}")

        # Validate min_delta
        if self.min_delta < 0:
            raise ValueError(f"min_delta must be >= 0, got {self.min_delta}")

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "objective_metric": self.objective_metric,
            "direction": self.direction,
            "study_name": self.study_name,
            "sampler_seed": self.sampler_seed,
            "log_level": self.log_level,
            "patience": self.patience,
            "min_delta": self.min_delta,
        }


@dataclass
class OptimizationResult:
    """Result from an optimization run.

    Attributes:
        best_params: Dictionary of best parameter values found
        best_value: Best objective value achieved
        best_trial_number: Trial number that achieved best value
        n_trials: Total number of trials run
        study_name: Name of the Optuna study
        objective_metric: Metric that was optimized
        direction: Direction of optimization
        stopped_early: Whether optimization stopped due to early stopping
        trials_without_improvement: Number of trials since last improvement (if early stopping)
    """

    best_params: dict[str, Any]
    best_value: float
    best_trial_number: int
    n_trials: int
    study_name: str | None
    objective_metric: str
    direction: str
    stopped_early: bool = False
    trials_without_improvement: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        # Convert Decimal values to strings for JSON serialization
        serialized_params = {}
        for k, v in self.best_params.items():
            if isinstance(v, Decimal):
                serialized_params[k] = str(v)
            else:
                serialized_params[k] = v

        return {
            "best_params": serialized_params,
            "best_value": self.best_value,
            "best_trial_number": self.best_trial_number,
            "n_trials": self.n_trials,
            "study_name": self.study_name,
            "objective_metric": self.objective_metric,
            "direction": self.direction,
            "stopped_early": self.stopped_early,
            "trials_without_improvement": self.trials_without_improvement,
        }


# Type alias for parameter ranges compatible with Optuna
# Values can be:
# - list: categorical/discrete choices
# - tuple of (min, max): continuous range
# - tuple of (min, max, step): discrete range with step
# - ParamRange: Explicit typed parameter range
OptunaParamRanges = dict[str, list[Any] | tuple[Any, ...] | ParamRange]


@dataclass(frozen=True)
class _TrialStateCounts:
    """Counts of Optuna trial terminal states."""

    complete: int
    pruned: int
    failed: int


@dataclass(frozen=True)
class _BestTrialSummary:
    """Best-trial fields exported in OptimizationHistory."""

    number: int | None
    value: float | None
    params: dict[str, Any] | None


def _decimal_trial_value(value: Any) -> Decimal:
    """Normalize Optuna float suggestions back to Decimal precision."""
    return Decimal(str(round(value, 6)))


def _convert_trial_param_value(name: str, value: Any, param_ranges: TypedParamRanges) -> Any:
    """Convert an exported trial param according to its original range type."""
    param_range = param_ranges.get(name)
    if param_range is not None and param_range.is_decimal:
        return _decimal_trial_value(value)
    return value


def _convert_trial_params(params: dict[str, Any], param_ranges: TypedParamRanges) -> dict[str, Any]:
    """Convert all exported trial params according to typed parameter ranges."""
    return {name: _convert_trial_param_value(name, value, param_ranges) for name, value in params.items()}


def _trial_time_fields(trial: FrozenTrial) -> tuple[str | None, str | None, float | None]:
    """Return serialized trial start, completion, and duration fields."""
    if trial.datetime_start is None:
        return None, None, None
    datetime_start = trial.datetime_start.isoformat()
    if trial.datetime_complete is None:
        return datetime_start, None, None
    datetime_complete = trial.datetime_complete.isoformat()
    duration_seconds = (trial.datetime_complete - trial.datetime_start).total_seconds()
    return datetime_start, datetime_complete, duration_seconds


def _trial_history_entry(trial: FrozenTrial, param_ranges: TypedParamRanges) -> TrialHistoryEntry:
    """Build one serializable optimization-history trial entry."""
    datetime_start, datetime_complete, duration_seconds = _trial_time_fields(trial)
    return TrialHistoryEntry(
        trial_number=trial.number,
        state=trial.state.name,
        value=trial.value,
        params=_convert_trial_params(trial.params, param_ranges),
        datetime_start=datetime_start,
        datetime_complete=datetime_complete,
        duration_seconds=duration_seconds,
        user_attrs=dict(trial.user_attrs),
        system_attrs=dict(trial.system_attrs),
    )


def _trial_state_counts(trials: list[FrozenTrial]) -> _TrialStateCounts:
    """Count completed, pruned, and failed trials."""
    return _TrialStateCounts(
        complete=sum(1 for trial in trials if trial.state == optuna.trial.TrialState.COMPLETE),
        pruned=sum(1 for trial in trials if trial.state == optuna.trial.TrialState.PRUNED),
        failed=sum(1 for trial in trials if trial.state == optuna.trial.TrialState.FAIL),
    )


def _best_trial_summary(study: Study, param_ranges: TypedParamRanges) -> _BestTrialSummary:
    """Return best-trial fields, handling studies without completed trials."""
    try:
        best_trial = study.best_trial
    except ValueError:
        return _BestTrialSummary(number=None, value=None, params=None)
    return _BestTrialSummary(
        number=best_trial.number,
        value=best_trial.value,
        params=_convert_trial_params(best_trial.params, param_ranges),
    )


def _stopped_early(callback: EarlyStoppingCallback | None) -> bool:
    """Return the exported early-stopping flag."""
    return callback.stopped_early if callback is not None else False


@dataclass(frozen=True)
class _PreparedParamRanges:
    """Typed optimizer ranges split by destination."""

    typed_ranges: TypedParamRanges
    typed_config_ranges: TypedParamRanges
    typed_strategy_ranges: TypedParamRanges


class OptunaTuner:
    """Bayesian optimization tuner using Optuna.

    Wraps Optuna's study object to provide intelligent parameter optimization
    for backtest configurations. Uses Tree-structured Parzen Estimator (TPE)
    sampler by default for efficient exploration of the parameter space.

    Attributes:
        config: OptunaTunerConfig with optimization settings
        study: Underlying Optuna study object

    Example:
        tuner = OptunaTuner(objective_metric="sharpe_ratio")

        result = await tuner.optimize(
            strategy_factory=create_strategy,
            data_provider_factory=create_provider,
            backtester_factory=create_backtester,
            base_config=base_config,
            param_ranges={
                "trade_size_usd": (Decimal("100"), Decimal("5000")),
                "interval_seconds": [3600, 7200, 14400],
            },
            n_trials=50,
        )

        print(f"Best Sharpe: {result.best_value}")
        print(f"Best params: {result.best_params}")
    """

    def __init__(
        self,
        objective_metric: str = "sharpe_ratio",
        direction: Literal["maximize", "minimize"] | None = None,
        study_name: str | None = None,
        sampler_seed: int | None = None,
        log_level: str = "WARNING",
        patience: int | None = None,
        min_delta: float = 0.0,
        config: OptunaTunerConfig | None = None,
    ) -> None:
        """Initialize the OptunaTuner.

        Args:
            objective_metric: Metric to optimize (default: 'sharpe_ratio')
            direction: 'maximize' or 'minimize' (default: based on metric)
            study_name: Name for the study (default: auto-generated)
            sampler_seed: Random seed for reproducibility
            log_level: Optuna logging level
            patience: Number of trials without improvement before early stopping.
                Set to None to disable early stopping (default: None).
            min_delta: Minimum improvement required to reset patience counter.
                Default is 0.0 (any improvement counts).
            config: Optional OptunaTunerConfig (overrides other params)
        """
        if config is not None:
            self.config = config
        else:
            self.config = OptunaTunerConfig(
                objective_metric=objective_metric,
                direction=direction,
                study_name=study_name,
                sampler_seed=sampler_seed,
                log_level=log_level,
                patience=patience,
                min_delta=min_delta,
            )

        # Set Optuna logging level
        optuna.logging.set_verbosity(getattr(optuna.logging, self.config.log_level))

        # Create sampler with optional seed
        sampler = TPESampler(seed=self.config.sampler_seed)

        # Create study
        self.study = optuna.create_study(
            study_name=self.config.study_name,
            direction=self.config.direction,
            sampler=sampler,
        )

        self._param_ranges: TypedParamRanges = {}
        self._base_config: PnLBacktestConfig | None = None
        self._early_stopping_callback: EarlyStoppingCallback | None = None

    def _suggest_from_param_range(self, trial: Trial, name: str, param: ParamRange) -> Any:
        """Suggest a parameter value using typed ParamRange.

        Maps ParamRange to the appropriate Optuna suggest_* method:
        - CATEGORICAL: suggest_categorical
        - DISCRETE: suggest_int (with optional step)
        - CONTINUOUS: suggest_float (with optional step/log)

        Args:
            trial: Optuna trial object
            name: Parameter name
            param: ParamRange with type and bounds

        Returns:
            Suggested parameter value (Decimal converted back if needed)
        """
        if param.param_type == ParamType.CATEGORICAL:
            return _suggest_categorical_param(trial, name, param)
        if param.param_type == ParamType.DISCRETE:
            return _suggest_discrete_param(trial, name, param)
        if param.param_type == ParamType.CONTINUOUS:
            return _suggest_continuous_param(trial, name, param)
        raise ValueError(f"Unknown param type: {param.param_type}")

    def _suggest_param(self, trial: Trial, name: str, values: list[Any] | tuple[Any, ...] | ParamRange) -> Any:
        """Suggest a parameter value using Optuna trial.

        Handles both legacy formats and new ParamRange objects:
        - ParamRange: Use _suggest_from_param_range for typed suggestion
        - list: Categorical/discrete choices (legacy)
        - tuple: Range (legacy, auto-converted)

        Args:
            trial: Optuna trial object
            name: Parameter name
            values: ParamRange, list (categorical), or tuple (range)

        Returns:
            Suggested parameter value
        """
        # Handle ParamRange directly
        if isinstance(values, ParamRange):
            return self._suggest_from_param_range(trial, name, values)

        # Legacy support: convert to ParamRange first
        param = _convert_legacy_param(name, values)
        return self._suggest_from_param_range(trial, name, param)

    def _create_config_from_trial(self, trial: Trial) -> tuple[PnLBacktestConfig, dict[str, Any]]:
        """Create a PnLBacktestConfig and strategy param overrides from trial.

        Args:
            trial: Optuna trial object

        Returns:
            Tuple of (PnLBacktestConfig with suggested config params,
                      dict of suggested strategy param overrides)
        """
        if self._base_config is None:
            raise RuntimeError("Base config not set. Call optimize() first.")

        # Start with base config dict
        config_dict = self._base_config.to_dict()

        # Remove computed properties
        for key in ["duration_seconds", "duration_days", "estimated_ticks"]:
            config_dict.pop(key, None)

        # Suggest and update config params (PnLBacktestConfig fields only)
        for name, values in self._config_param_ranges.items():
            suggested = self._suggest_param(trial, name, values)
            config_dict[name] = suggested

        # Suggest strategy params (passed to strategy factory, not PnLBacktestConfig)
        strategy_overrides: dict[str, Any] = {}
        for name, values in self._strategy_param_ranges.items():
            suggested = self._suggest_param(trial, name, values)
            strategy_overrides[name] = suggested

        return PnLBacktestConfig.from_dict(config_dict), strategy_overrides

    def _create_objective(  # noqa: C901
        self,
        strategy_factory: Callable[..., Any],
        data_provider_factory: Callable[[], Any],
        backtester_factory: Callable[[Any, dict[str, Any], dict[str, Any]], Any],
        fee_models: dict[str, Any],
        slippage_models: dict[str, Any],
        extra_configs: list[PnLBacktestConfig] | None = None,
    ) -> Callable[[Trial], float]:
        """Create the objective function for Optuna.

        Args:
            strategy_factory: Factory function for strategy.
                Called with no args or a config dict depending on param ranges.
            data_provider_factory: Factory function for data provider
            backtester_factory: Factory function for backtester
            fee_models: Fee models dict
            slippage_models: Slippage models dict
            extra_configs: Additional PnLBacktestConfig objects for multi-period scoring.
                When provided, each trial runs backtests across all configs (base + extras)
                and the objective metric is averaged across all periods.

        Returns:
            Objective function that takes a Trial and returns float
        """

        def _create_strategy_with_overrides(overrides: dict[str, Any]) -> Any:
            """Create a strategy, merging overrides into strategy config."""
            if overrides and self._strategy_config is not None:
                merged_config = {**self._strategy_config, **overrides}
                return strategy_factory(merged_config)
            elif overrides:
                # No base strategy config -- pass overrides directly
                return strategy_factory(overrides)
            else:
                return strategy_factory()

        def objective(trial: Trial) -> float:
            """Objective function for Optuna optimization."""
            # Create config from trial suggestions (uses base_config as template)
            config, strategy_overrides = self._create_config_from_trial(trial)

            if extra_configs:
                # Multi-period: run backtest for each period, average the metric
                all_configs = [config]
                for extra in extra_configs:
                    # Apply trial's suggested params to each extra config's time window
                    extra_dict = extra.to_dict()
                    config_dict = config.to_dict()
                    # Keep the extra config's time window but use trial's suggested params
                    for key in config_dict:
                        if key not in (
                            "start_time",
                            "end_time",
                            "duration_seconds",
                            "duration_days",
                            "estimated_ticks",
                        ):
                            extra_dict[key] = config_dict[key]
                    for key in ("duration_seconds", "duration_days", "estimated_ticks"):
                        extra_dict.pop(key, None)
                    all_configs.append(PnLBacktestConfig.from_dict(extra_dict))

                metric_values = []
                for period_config in all_configs:
                    try:
                        strategy = _create_strategy_with_overrides(strategy_overrides)
                        data_provider = data_provider_factory()
                        backtester = backtester_factory(data_provider, fee_models, slippage_models)
                        result: BacktestResult = asyncio.run(backtester.backtest(strategy, period_config))
                        metric_value = getattr(result.metrics, self.config.objective_metric)
                        if isinstance(metric_value, Decimal):
                            metric_value = float(metric_value)
                        metric_values.append(metric_value)
                    except Exception as e:
                        logger.warning(f"Trial {trial.number} failed for a period: {e}")
                        if self.config.direction == "maximize":
                            metric_values.append(float("-inf"))
                        else:
                            metric_values.append(float("inf"))

                avg_metric = sum(metric_values) / len(metric_values)
                logger.debug(
                    f"Trial {trial.number}: avg {self.config.objective_metric} = {avg_metric:.4f} "
                    f"(across {len(all_configs)} periods)"
                )
                return avg_metric
            else:
                # Single-period: original behavior
                strategy = _create_strategy_with_overrides(strategy_overrides)
                data_provider = data_provider_factory()
                backtester = backtester_factory(data_provider, fee_models, slippage_models)

                try:
                    result = asyncio.run(backtester.backtest(strategy, config))
                    metric_value = getattr(result.metrics, self.config.objective_metric)
                    if isinstance(metric_value, Decimal):
                        metric_value = float(metric_value)
                    logger.debug(f"Trial {trial.number}: {self.config.objective_metric} = {metric_value}")
                    return metric_value

                except Exception as e:
                    logger.warning(f"Trial {trial.number} failed: {e}")
                    if self.config.direction == "maximize":
                        return float("-inf")
                    else:
                        return float("inf")

        return objective

    def _partition_param_ranges(
        self,
        base_config: PnLBacktestConfig,
        param_ranges: OptunaParamRanges,
    ) -> tuple[OptunaParamRanges, OptunaParamRanges]:
        """Split tunable ranges into PnLBacktestConfig and strategy params."""
        valid_config_fields = {f for f in vars(base_config) if not f.startswith("_")}
        config_param_ranges: OptunaParamRanges = {}
        strategy_param_ranges: OptunaParamRanges = {}

        for field_name, field_range in param_ranges.items():
            if field_name in valid_config_fields:
                config_param_ranges[field_name] = field_range
            else:
                strategy_param_ranges[field_name] = field_range

        return config_param_ranges, strategy_param_ranges

    def _log_param_range_groups(
        self,
        config_param_ranges: OptunaParamRanges,
        strategy_param_ranges: OptunaParamRanges,
    ) -> None:
        """Log which optimizer ranges target config vs strategy construction."""
        if strategy_param_ranges:
            logger.info(
                f"Strategy params to optimize: {sorted(strategy_param_ranges.keys())} "
                f"(will be passed to strategy factory)"
            )
        if config_param_ranges:
            logger.info(f"Backtest config params to optimize: {sorted(config_param_ranges.keys())}")

    def _convert_param_ranges(self, param_ranges: OptunaParamRanges) -> TypedParamRanges:
        """Convert legacy optimizer range declarations to typed ParamRange objects."""
        return {name: _convert_legacy_param(name, value) for name, value in param_ranges.items()}

    def _prepare_param_ranges(
        self,
        base_config: PnLBacktestConfig,
        param_ranges: OptunaParamRanges,
    ) -> _PreparedParamRanges:
        """Validate, partition, log, and type optimizer parameter ranges."""
        if not param_ranges:
            raise ValueError("param_ranges cannot be empty")

        config_param_ranges, strategy_param_ranges = self._partition_param_ranges(base_config, param_ranges)
        self._log_param_range_groups(config_param_ranges, strategy_param_ranges)

        return _PreparedParamRanges(
            typed_ranges=self._convert_param_ranges(param_ranges),
            typed_config_ranges=self._convert_param_ranges(config_param_ranges),
            typed_strategy_ranges=self._convert_param_ranges(strategy_param_ranges),
        )

    def _store_objective_state(
        self,
        prepared_ranges: _PreparedParamRanges,
        base_config: PnLBacktestConfig,
        strategy_config: dict[str, Any] | None,
    ) -> None:
        """Store optimizer state consumed by the per-trial objective."""
        self._param_ranges = prepared_ranges.typed_ranges
        self._config_param_ranges = prepared_ranges.typed_config_ranges
        self._strategy_param_ranges = prepared_ranges.typed_strategy_ranges
        self._base_config = base_config
        self._strategy_config = strategy_config

    def _build_early_stopping_callbacks(
        self,
        patience: int | None,
        min_delta: float | None,
    ) -> list[Callable[[Study, optuna.trial.FrozenTrial], None]]:
        """Build Optuna callbacks, preserving method-arg override precedence."""
        effective_patience = patience if patience is not None else self.config.patience
        effective_min_delta = min_delta if min_delta is not None else self.config.min_delta
        callbacks: list[Callable[[Study, optuna.trial.FrozenTrial], None]] = []

        if effective_patience is not None:
            self._early_stopping_callback = EarlyStoppingCallback(
                patience=effective_patience,
                min_delta=effective_min_delta,
                direction=self.config.direction or "maximize",
                verbose=True,
            )
            callbacks.append(self._early_stopping_callback)
            logger.info(f"Early stopping enabled: patience={effective_patience}, min_delta={effective_min_delta}")

        return callbacks

    def _log_optimization_start(self, n_trials: int) -> None:
        logger.info(
            f"Starting Optuna optimization: {n_trials} trials, "
            f"metric={self.config.objective_metric}, direction={self.config.direction}"
        )

    async def _run_study_optimization(
        self,
        objective: Callable[[Trial], float],
        n_trials: int,
        timeout: float | None,
        show_progress: bool,
        callbacks: list[Callable[[Study, optuna.trial.FrozenTrial], None]],
    ) -> None:
        """Run Optuna's synchronous study loop without blocking the event loop."""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: self.study.optimize(
                objective,
                n_trials=n_trials,
                timeout=timeout,
                show_progress_bar=show_progress,
                callbacks=callbacks if callbacks else None,
            ),
        )

    def _read_early_stopping_state(self) -> tuple[bool, int]:
        """Return whether early stopping fired and the final stale-trial count."""
        if self._early_stopping_callback is None:
            return False, 0
        return (
            self._early_stopping_callback.stopped_early,
            self._early_stopping_callback.trials_without_improvement,
        )

    def _best_params_with_decimal_restored(self, typed_ranges: TypedParamRanges) -> dict[str, Any]:
        """Restore Decimal-valued best params from Optuna's float representation."""
        best_params: dict[str, Any] = {}
        for name, value in self.study.best_params.items():
            param = typed_ranges.get(name)
            if param is not None and param.is_decimal:
                best_params[name] = Decimal(str(round(value, 6)))
            else:
                best_params[name] = value
        return best_params

    def _log_optimization_complete(self, stopped_early: bool) -> None:
        completion_reason = "early stopping" if stopped_early else "completed all trials"
        logger.info(
            f"Optimization {completion_reason}. Best {self.config.objective_metric}: "
            f"{self.study.best_value:.4f} (trial {self.study.best_trial.number})"
        )

    def _build_optimization_result(
        self,
        best_params: dict[str, Any],
        stopped_early: bool,
        trials_without_improvement: int,
    ) -> OptimizationResult:
        """Assemble the public optimization result from the Optuna study."""
        return OptimizationResult(
            best_params=best_params,
            best_value=self.study.best_value,
            best_trial_number=self.study.best_trial.number,
            n_trials=len(self.study.trials),
            study_name=self.study.study_name,
            objective_metric=self.config.objective_metric,
            direction=self.config.direction or "maximize",
            stopped_early=stopped_early,
            trials_without_improvement=trials_without_improvement,
        )

    async def optimize(
        self,
        strategy_factory: Callable[..., Any],
        data_provider_factory: Callable[[], Any],
        backtester_factory: Callable[[Any, dict[str, Any], dict[str, Any]], Any],
        base_config: PnLBacktestConfig,
        param_ranges: OptunaParamRanges,
        n_trials: int = 50,
        fee_models: dict[str, Any] | None = None,
        slippage_models: dict[str, Any] | None = None,
        timeout: float | None = None,
        show_progress: bool = True,
        patience: int | None = None,
        min_delta: float | None = None,
        extra_configs: list[PnLBacktestConfig] | None = None,
        strategy_config: dict[str, Any] | None = None,
    ) -> OptimizationResult:
        """Run Bayesian optimization to find best parameters.

        Uses Optuna's TPE sampler to efficiently explore the parameter space
        and find the configuration that optimizes the objective metric.

        Args:
            strategy_factory: Factory function that returns a new strategy instance.
                Must be picklable (module-level function).
                Called as ``strategy_factory()`` when no strategy param ranges are defined.
                Called as ``strategy_factory(config_dict)`` with a merged dict when
                strategy param ranges are present (optionally merged with ``strategy_config``).
            data_provider_factory: Factory function that returns a new data provider.
                Must be picklable (module-level function).
            backtester_factory: Factory function that returns a new PnLBacktester.
                Takes (data_provider, fee_models, slippage_models) as arguments.
            base_config: Base configuration to use as template
            param_ranges: Dictionary mapping parameter names to ranges.
                - list: categorical choices
                - tuple(min, max): continuous range
                - tuple(min, max, step): discrete range with step
            n_trials: Number of optimization trials (default: 50)
            fee_models: Optional fee models dict
            slippage_models: Optional slippage models dict
            timeout: Optional timeout in seconds for entire optimization
            show_progress: Show Optuna progress bar (default: True)
            patience: Number of trials without improvement before early stopping.
                Overrides config value if provided. Set to None to disable.
            min_delta: Minimum improvement required to reset patience counter.
                Overrides config value if provided.
            extra_configs: Additional PnLBacktestConfig objects for multi-period optimization.
                When provided, each trial evaluates across all configs (base + extras) and
                the objective metric is averaged. Useful for testing robustness across
                multiple time periods.

        Returns:
            OptimizationResult with best parameters and value

        Example:
            result = await tuner.optimize(
                strategy_factory=create_strategy,
                data_provider_factory=create_provider,
            backtester_factory=create_backtester,
            base_config=base_config,
            param_ranges={
                    "trade_size_usd": (Decimal("100"), Decimal("5000")),
                    "interval_seconds": [3600, 7200, 14400],
            },
                n_trials=100,
                patience=10,  # Stop if no improvement for 10 trials
            )
        """
        prepared_ranges = self._prepare_param_ranges(base_config, param_ranges)
        self._store_objective_state(prepared_ranges, base_config, strategy_config)
        objective = self._create_objective(
            strategy_factory=strategy_factory,
            data_provider_factory=data_provider_factory,
            backtester_factory=backtester_factory,
            fee_models=fee_models or {},
            slippage_models=slippage_models or {},
            extra_configs=extra_configs,
        )
        callbacks = self._build_early_stopping_callbacks(patience, min_delta)

        self._log_optimization_start(n_trials)
        await self._run_study_optimization(
            objective=objective,
            n_trials=n_trials,
            timeout=timeout,
            show_progress=show_progress,
            callbacks=callbacks,
        )
        stopped_early, trials_without_improvement = self._read_early_stopping_state()
        best_params = self._best_params_with_decimal_restored(prepared_ranges.typed_ranges)
        self._log_optimization_complete(stopped_early)
        return self._build_optimization_result(best_params, stopped_early, trials_without_improvement)

    def optimize_sync(
        self,
        strategy_factory: Callable[[], Any],
        data_provider_factory: Callable[[], Any],
        backtester_factory: Callable[[Any, dict[str, Any], dict[str, Any]], Any],
        base_config: PnLBacktestConfig,
        param_ranges: OptunaParamRanges,
        n_trials: int = 50,
        fee_models: dict[str, Any] | None = None,
        slippage_models: dict[str, Any] | None = None,
        timeout: float | None = None,
        show_progress: bool = True,
        patience: int | None = None,
        min_delta: float | None = None,
        extra_configs: list[PnLBacktestConfig] | None = None,
    ) -> OptimizationResult:
        """Synchronous wrapper for optimize().

        Convenience method for running optimization from synchronous code.

        Args:
            Same as optimize()

        Returns:
            Same as optimize()
        """
        return asyncio.run(
            self.optimize(
                strategy_factory=strategy_factory,
                data_provider_factory=data_provider_factory,
                backtester_factory=backtester_factory,
                base_config=base_config,
                param_ranges=param_ranges,
                n_trials=n_trials,
                fee_models=fee_models,
                slippage_models=slippage_models,
                timeout=timeout,
                show_progress=show_progress,
                patience=patience,
                min_delta=min_delta,
                extra_configs=extra_configs,
            )
        )

    def get_trials_dataframe(self) -> Any:
        """Get optimization history as a pandas DataFrame.

        Returns:
            DataFrame with columns for trial parameters and values
        """
        return self.study.trials_dataframe()

    def get_best_trial(self) -> FrozenTrial:
        """Get the best trial from the study.

        Returns:
            Optuna FrozenTrial object with best parameters
        """
        return self.study.best_trial

    def get_param_importances(self) -> dict[str, float]:
        """Get parameter importance scores.

        Uses Optuna's importance evaluator to estimate how much each
        parameter contributes to the objective variance.

        Returns:
            Dictionary mapping parameter names to importance scores
        """
        try:
            return optuna.importance.get_param_importances(self.study)
        except Exception as e:
            logger.warning(f"Could not compute param importances: {e}")
            return {}

    def export_history(self) -> OptimizationHistory:
        """Export complete optimization history.

        Returns an OptimizationHistory object containing all trial data,
        which can be serialized to JSON for analysis or persistence.

        Returns:
            OptimizationHistory with all trial data and metadata

        Example:
            history = tuner.export_history()

            # Save to file
            history.save("optimization_history.json")

            # Or get as JSON string
            json_str = history.to_json()

            # Or get as dict
            data = history.to_dict()
        """
        study_trials = list(self.study.trials)
        trials = [_trial_history_entry(trial, self._param_ranges) for trial in study_trials]
        counts = _trial_state_counts(study_trials)
        best = _best_trial_summary(self.study, self._param_ranges)

        return OptimizationHistory(
            study_name=self.study.study_name,
            objective_metric=self.config.objective_metric,
            direction=self.config.direction or "maximize",
            n_trials=len(study_trials),
            n_complete=counts.complete,
            n_pruned=counts.pruned,
            n_failed=counts.failed,
            best_trial_number=best.number,
            best_value=best.value,
            best_params=best.params,
            param_names=list(self._param_ranges.keys()),
            trials=trials,
            stopped_early=_stopped_early(self._early_stopping_callback),
            export_timestamp=datetime.now().isoformat(),
        )

    def save_history(self, path: str | Path) -> None:
        """Save optimization history to JSON file.

        Convenience method that exports history and saves to file.

        Args:
            path: File path for the JSON output

        Example:
            tuner.save_history("results/optimization_history.json")
        """
        history = self.export_history()
        history.save(path)


__all__ = [
    # Core classes
    "OptunaTuner",
    "OptunaTunerConfig",
    "OptimizationResult",
    # Early stopping
    "EarlyStoppingCallback",
    # History export
    "OptimizationHistory",
    "TrialHistoryEntry",
    # Parameter types
    "ParamType",
    "ParamRange",
    "TypedParamRanges",
    # Factory functions for creating parameter ranges
    "continuous",
    "discrete",
    "categorical",
    "log_uniform",
    # Legacy type alias (backward compatible)
    "OptunaParamRanges",
    # Metrics
    "OBJECTIVE_METRICS",
    "METRIC_DIRECTIONS",
]
