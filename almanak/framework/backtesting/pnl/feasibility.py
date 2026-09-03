"""Pure, pre-materialization feasibility gate for historical PnL backtests.

Exact-pool state is served in serial ``_MAX_POINTS_PER_REQUEST``-point gateway
pages, so a long window spends most of a bounded job budget loading data and
then dies with a generic timeout. This module estimates that cost from the
declared window alone -- no gateway calls, no network -- so an infeasible run
is rejected in seconds with a concrete, shorter window to retry with.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import TYPE_CHECKING

from almanak.config.backtest import backtest_feasibility_knob
from almanak.framework.backtesting.pnl.error_handling import PreflightValidationError
from almanak.framework.backtesting.pnl.providers.snapshot_pool_state import _MAX_POINTS_PER_REQUEST

if TYPE_CHECKING:
    from almanak.framework.backtesting.pnl.config import PnLBacktestConfig

logger = logging.getLogger(__name__)

FEASIBILITY_CHECK = "backtest_window_feasibility"
WINDOW_TOO_LONG = "WINDOW_TOO_LONG"

ENV_PAGE_LATENCY_SECONDS = "ALMANAK_BACKTEST_PAGE_LATENCY_ESTIMATE_SECONDS"
ENV_TICKS_PER_SECOND = "ALMANAK_BACKTEST_TICKS_PER_SECOND_ESTIMATE"
ENV_BUDGET_SECONDS = "ALMANAK_BACKTEST_BUDGET_SECONDS"
ENV_SAFETY_MARGIN = "ALMANAK_BACKTEST_FEASIBILITY_SAFETY_MARGIN"

# Measured on staging: one 128-point GetDexPoolStateSeries page takes ~12-23s
# against an archive gateway with no cache in front of it.
DEFAULT_PAGE_LATENCY_SECONDS = 20.0
DEFAULT_TICKS_PER_SECOND = 3.5
# Fallback only: the platform runner job injects ALMANAK_BACKTEST_BUDGET_SECONDS
# from the same value it passes to `gcloud run jobs ... --task-timeout`, so the
# gate follows the real task timeout wherever that is raised or lowered. The
# fallback mirrors the runner's value, which is sized for one year at a
# one-hour tick with the safety margin below applied.
DEFAULT_BUDGET_SECONDS = 7200.0
# Data + simulation are not the whole run: price-grid iteration, metrics and
# artifact upload also draw on the same budget.
DEFAULT_SAFETY_MARGIN = 0.8


@dataclass(frozen=True, slots=True)
class FeasibilityKnobs:
    """Env-overridable cost model inputs."""

    page_latency_seconds: float = DEFAULT_PAGE_LATENCY_SECONDS
    ticks_per_second: float = DEFAULT_TICKS_PER_SECOND
    budget_seconds: float = DEFAULT_BUDGET_SECONDS
    safety_margin: float = DEFAULT_SAFETY_MARGIN

    def __post_init__(self) -> None:
        for name in ("page_latency_seconds", "ticks_per_second", "budget_seconds", "safety_margin"):
            value = getattr(self, name)
            if value <= 0 or not math.isfinite(value):
                raise ValueError(f"FeasibilityKnobs.{name} must be positive and finite, got {value!r}")

    @classmethod
    def from_env(cls) -> FeasibilityKnobs:
        # Env reads live behind the config-service boundary; only the knob
        # names and defaults are owned here.
        return cls(
            page_latency_seconds=backtest_feasibility_knob(ENV_PAGE_LATENCY_SECONDS, DEFAULT_PAGE_LATENCY_SECONDS),
            ticks_per_second=backtest_feasibility_knob(ENV_TICKS_PER_SECOND, DEFAULT_TICKS_PER_SECOND),
            budget_seconds=backtest_feasibility_knob(ENV_BUDGET_SECONDS, DEFAULT_BUDGET_SECONDS),
            safety_margin=backtest_feasibility_knob(ENV_SAFETY_MARGIN, DEFAULT_SAFETY_MARGIN),
        )

    @property
    def usable_budget_seconds(self) -> float:
        return self.budget_seconds * self.safety_margin


@dataclass(frozen=True, slots=True)
class FeasibilityEstimate:
    """Estimated wall-clock cost of one backtest window, before any fetch."""

    duration_seconds: int
    interval_seconds: int
    target_count: int
    points_per_target: int
    pages_per_target: int
    ticks: int
    data_seconds: float
    simulation_seconds: float
    knobs: FeasibilityKnobs
    # The strategy's own decision cadence, when declared. A coarser tick than
    # this starves the strategy's indicators, so it is never recommended.
    strategy_cadence_seconds: int | None = None

    @property
    def total_seconds(self) -> float:
        return self.data_seconds + self.simulation_seconds

    @property
    def budget_seconds(self) -> float:
        return self.knobs.budget_seconds

    @property
    def usable_budget_seconds(self) -> float:
        return self.knobs.usable_budget_seconds

    @property
    def duration_days(self) -> float:
        return self.duration_seconds / 86400

    @property
    def feasible(self) -> bool:
        return self.total_seconds <= self.usable_budget_seconds


def expected_points(duration_seconds: int, interval_seconds: int) -> int:
    """Points one pool-state target materializes for the window.

    Mirrors ``range(start_ts, end_ts + 1, interval)`` in
    ``SnapshotPoolStateSource.materialize_history``.
    """
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    if duration_seconds < 0:
        return 0
    return duration_seconds // interval_seconds + 1


def expected_pages(points: int) -> int:
    """Serial ``GetDexPoolStateSeries`` pages needed for ``points`` samples."""
    if points <= 0:
        return 0
    return math.ceil(points / _MAX_POINTS_PER_REQUEST)


def estimate_cost(
    *,
    duration_seconds: int,
    interval_seconds: int,
    target_count: int,
    knobs: FeasibilityKnobs | None = None,
    strategy_cadence_seconds: int | None = None,
) -> FeasibilityEstimate:
    """Estimate data + simulation seconds for one window. Pure computation."""
    resolved = knobs if knobs is not None else FeasibilityKnobs.from_env()
    if target_count < 0:
        raise ValueError("target_count cannot be negative")
    if strategy_cadence_seconds is not None and strategy_cadence_seconds <= 0:
        raise ValueError("strategy_cadence_seconds must be positive when given")
    points = expected_points(duration_seconds, interval_seconds)
    pages = expected_pages(points)
    ticks = max(duration_seconds, 0) // interval_seconds
    return FeasibilityEstimate(
        duration_seconds=duration_seconds,
        interval_seconds=interval_seconds,
        target_count=target_count,
        points_per_target=points,
        pages_per_target=pages,
        ticks=ticks,
        data_seconds=pages * resolved.page_latency_seconds * target_count,
        simulation_seconds=ticks / resolved.ticks_per_second,
        knobs=resolved,
        strategy_cadence_seconds=strategy_cadence_seconds,
    )


def estimate_config_cost(
    config: PnLBacktestConfig,
    *,
    target_count: int,
    knobs: FeasibilityKnobs | None = None,
    strategy_cadence_seconds: int | None = None,
) -> FeasibilityEstimate:
    return estimate_cost(
        duration_seconds=config.duration_seconds,
        interval_seconds=config.interval_seconds,
        target_count=target_count,
        knobs=knobs,
        strategy_cadence_seconds=strategy_cadence_seconds,
    )


def max_feasible_ticks(*, target_count: int, knobs: FeasibilityKnobs) -> int:
    """Largest tick count whose estimated cost still fits the usable budget.

    Cost is monotone non-decreasing in tick count, so a bisection over ticks
    is exact under this model.
    """

    def cost(ticks: int) -> float:
        pages = expected_pages(ticks + 1)
        return pages * knobs.page_latency_seconds * target_count + ticks / knobs.ticks_per_second

    if cost(0) > knobs.usable_budget_seconds:
        return 0
    low, high = 0, 1
    while cost(high) <= knobs.usable_budget_seconds:
        low, high = high, high * 2
    while high - low > 1:
        middle = (low + high) // 2
        if cost(middle) <= knobs.usable_budget_seconds:
            low = middle
        else:
            high = middle
    return low


class BacktestWindowTooLongError(PreflightValidationError):
    """Raised when the declared window cannot finish inside the job budget.

    Carries the duck-typed ``preflight_code`` / ``preflight_blockers`` surface
    platform runners map onto structured API failures.
    """

    preflight_code = WINDOW_TOO_LONG

    def __init__(self, estimate: FeasibilityEstimate) -> None:
        message = _window_too_long_message(estimate)
        recommendations = _window_too_long_recommendations(estimate)
        super().__init__(
            message=message,
            failed_checks=[FEASIBILITY_CHECK],
            recommendations=recommendations,
            error_count=1,
            warning_count=0,
            code=WINDOW_TOO_LONG,
            details={
                "code": WINDOW_TOO_LONG,
                "estimated_seconds": round(estimate.total_seconds, 1),
                "data_seconds": round(estimate.data_seconds, 1),
                "simulation_seconds": round(estimate.simulation_seconds, 1),
                "budget_seconds": estimate.budget_seconds,
                "usable_budget_seconds": round(estimate.usable_budget_seconds, 1),
                "pool_state_pages": estimate.pages_per_target * estimate.target_count,
                "pool_state_targets": estimate.target_count,
                "requested_days": round(estimate.duration_days, 2),
                "feasible_days": round(_feasible_days(estimate), 2),
                "strategy_cadence_seconds": estimate.strategy_cadence_seconds,
                # Operator-only escape hatches: deliberately kept out of the
                # user-facing recommendations, which end users cannot act on.
                "knob_env_vars": {
                    ENV_BUDGET_SECONDS: estimate.knobs.budget_seconds,
                    ENV_SAFETY_MARGIN: estimate.knobs.safety_margin,
                    ENV_PAGE_LATENCY_SECONDS: estimate.knobs.page_latency_seconds,
                    ENV_TICKS_PER_SECOND: estimate.knobs.ticks_per_second,
                },
            },
        )
        self.estimate = estimate
        self.preflight_code = WINDOW_TOO_LONG
        self.preflight_blockers = [
            {
                "code": WINDOW_TOO_LONG,
                "message": message,
                "recommendations": recommendations,
            }
        ]


def _feasible_days(estimate: FeasibilityEstimate) -> float:
    ticks = max_feasible_ticks(target_count=estimate.target_count, knobs=estimate.knobs)
    return ticks * estimate.interval_seconds / 86400


def _coarser_interval_hours(estimate: FeasibilityEstimate) -> float | None:
    """Interval that keeps the requested window inside budget, if one exists.

    ``None`` when no such interval exists or when it would be coarser than the
    strategy's own decision cadence: a tick the indicators cannot fill is not
    a remedy the user can act on.
    """
    ticks = max_feasible_ticks(target_count=estimate.target_count, knobs=estimate.knobs)
    if ticks <= 0:
        return None
    coarser_seconds = math.ceil(estimate.duration_seconds / ticks)
    if estimate.strategy_cadence_seconds is not None and coarser_seconds > estimate.strategy_cadence_seconds:
        return None
    return coarser_seconds / 3600


def _window_too_long_message(estimate: FeasibilityEstimate) -> str:
    return (
        f"Requested {estimate.duration_days:.1f}-day window needs ~{estimate.total_seconds:.0f}s of data+sim "
        f"against a {estimate.usable_budget_seconds:.0f}s budget "
        f"({estimate.budget_seconds:.0f}s job budget x {estimate.knobs.safety_margin:.2f} safety margin): "
        f"{estimate.pages_per_target} pool-state pages x {estimate.target_count} target(s) at "
        f"~{estimate.knobs.page_latency_seconds:.0f}s/page = {estimate.data_seconds:.0f}s, plus "
        f"{estimate.ticks} ticks at ~{estimate.knobs.ticks_per_second:g} ticks/s = {estimate.simulation_seconds:.0f}s"
    )


def _window_too_long_recommendations(estimate: FeasibilityEstimate) -> list[str]:
    """User-facing remediation lines.

    Rendered verbatim in the platform UI and by the Almanak Code agent, so every
    line must be something the person running the backtest can actually do --
    no operator-only knobs (those live in ``details["knob_env_vars"]``).
    """
    feasible_days = _feasible_days(estimate)
    recommendations = [
        f"shorten the window to ~{feasible_days:.1f} days at the current {estimate.interval_seconds}s interval"
    ]
    coarser_hours = _coarser_interval_hours(estimate)
    if coarser_hours is not None and coarser_hours > estimate.interval_seconds / 3600:
        recommendations.append(f"use a coarser interval of ~{coarser_hours:.1f}h to keep the full window")
    recommendations.append(
        f"hosted backtest runs are currently limited to ~{estimate.budget_seconds / 60:.0f} min of run time"
    )
    return recommendations


def enforce_window_feasibility(
    config: PnLBacktestConfig,
    *,
    target_count: int,
    knobs: FeasibilityKnobs | None = None,
    strategy_cadence_seconds: int | None = None,
) -> FeasibilityEstimate | None:
    """Reject infeasible windows before any pool-state page is fetched.

    Returns ``None`` when no historical pool-state target is declared -- those
    runs never pay the paging cost this gate models.
    """
    if target_count <= 0:
        return None
    estimate = estimate_config_cost(
        config,
        target_count=target_count,
        knobs=knobs,
        strategy_cadence_seconds=strategy_cadence_seconds,
    )
    if not estimate.feasible:
        raise BacktestWindowTooLongError(estimate)
    logger.info(
        "Pool-state feasibility: ~%.0fs estimated (%d pages x %d target(s), %d ticks) within %.0fs budget",
        estimate.total_seconds,
        estimate.pages_per_target,
        estimate.target_count,
        estimate.ticks,
        estimate.usable_budget_seconds,
    )
    return estimate


__all__ = [
    "BacktestWindowTooLongError",
    "FEASIBILITY_CHECK",
    "FeasibilityEstimate",
    "FeasibilityKnobs",
    "WINDOW_TOO_LONG",
    "enforce_window_feasibility",
    "estimate_config_cost",
    "estimate_cost",
    "expected_pages",
    "expected_points",
    "max_feasible_ticks",
]
