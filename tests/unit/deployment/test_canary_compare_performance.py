"""Characterization tests for ``CanaryDeployment.compare_performance``.

These tests lock in the current decision-gate semantics before the function
is refactored (Phase 8.5). Each test exercises a pure-function slice: build
a ``CanaryDeployment`` with controlled metrics, call ``compare_performance``,
and assert on ``decision`` + ``*_ratio`` fields.

Covers:
- Insufficient metrics (either side ``None`` -> CONTINUE).
- Canary wins on all three "critical" levers (pnl, drawdown) plus the
  "soft" levers (sharpe, win rate).
- Mixed verdicts (pnl below threshold, sharpe below threshold, etc.).
- Baseline wins unambiguously (all criteria fail -> ROLLBACK).
- Tied on all metrics (exact equality -> PROMOTE under default criteria).
- Insufficient trades -> CONTINUE regardless of other metrics.
- Division-by-zero edges (zero-baseline pnl, zero-baseline drawdown).
- NaN/inf passthrough in Decimal metric values.
- ``require_positive_pnl`` branch.
- Error-rate branch.

No network, no I/O, no timeline events required (``emit_events=False``).
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.framework.deployment.canary import (
    CanaryComparison,
    CanaryConfig,
    CanaryDecision,
    CanaryDeployment,
    CanaryMetrics,
    PromotionCriteria,
)
from almanak.framework.models.strategy_version import PerformanceMetrics


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _build_metrics(
    *,
    net_pnl_usd: str = "0",
    max_drawdown: str = "0",
    sharpe_ratio: str | None = None,
    win_rate: str | None = None,
    total_trades: int = 0,
) -> PerformanceMetrics:
    """Construct a ``PerformanceMetrics`` with Decimal-friendly defaults."""
    return PerformanceMetrics(
        net_pnl_usd=Decimal(net_pnl_usd),
        max_drawdown=Decimal(max_drawdown),
        sharpe_ratio=Decimal(sharpe_ratio) if sharpe_ratio is not None else None,
        win_rate=Decimal(win_rate) if win_rate is not None else None,
        total_trades=total_trades,
    )


def _make_deployment(
    *,
    canary_perf: PerformanceMetrics | None,
    stable_perf: PerformanceMetrics | None,
    canary_trade_count: int = 10,
    stable_trade_count: int = 10,
    canary_error_count: int = 0,
    stable_error_count: int = 0,
    criteria: PromotionCriteria | None = None,
) -> CanaryDeployment:
    """Build a ``CanaryDeployment`` preloaded with ``CanaryMetrics``.

    ``canary_perf``/``stable_perf`` set to ``None`` skip that side so the
    "insufficient metrics" branch can be exercised.
    """
    config = CanaryConfig(
        canary_percent=10,
        observation_period_minutes=60,
        check_interval_seconds=60,
        emit_events=False,
        promotion_criteria=criteria or PromotionCriteria(),
    )
    deployment = CanaryDeployment(
        strategy_id="s1",
        stable_version_id="v_stable",
        canary_version_id="v_canary",
        config=config,
    )

    if canary_perf is not None:
        deployment.state.canary_metrics = CanaryMetrics(
            version_id="v_canary",
            capital_allocated_usd=Decimal("10000"),
            metrics=canary_perf,
            error_count=canary_error_count,
            trade_count=canary_trade_count,
            is_canary=True,
        )
    if stable_perf is not None:
        deployment.state.stable_metrics = CanaryMetrics(
            version_id="v_stable",
            capital_allocated_usd=Decimal("90000"),
            metrics=stable_perf,
            error_count=stable_error_count,
            trade_count=stable_trade_count,
            is_canary=False,
        )

    return deployment


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------


def test_missing_canary_metrics_returns_continue_with_placeholder() -> None:
    """If canary metrics are not initialised, decision is CONTINUE and reasons
    report insufficient data; returned comparison still carries both sides."""
    deployment = _make_deployment(
        canary_perf=None,
        stable_perf=_build_metrics(net_pnl_usd="100", total_trades=10),
    )

    result = deployment.compare_performance()

    assert isinstance(result, CanaryComparison)
    assert result.decision == CanaryDecision.CONTINUE
    assert result.decision_reasons == ["Insufficient metrics data"]
    # Placeholder canary side is the synthesized empty ``CanaryMetrics``.
    assert result.canary_metrics.version_id == "v_canary"
    assert result.stable_metrics.version_id == "v_stable"


def test_missing_stable_metrics_returns_continue() -> None:
    """Symmetric: stable missing also short-circuits to CONTINUE."""
    deployment = _make_deployment(
        canary_perf=_build_metrics(net_pnl_usd="100", total_trades=10),
        stable_perf=None,
    )

    result = deployment.compare_performance()

    assert result.decision == CanaryDecision.CONTINUE
    assert result.decision_reasons == ["Insufficient metrics data"]


def test_canary_wins_on_all_metrics_promotes() -> None:
    """Canary wins on pnl, drawdown, sharpe, win_rate -> PROMOTE."""
    canary = _build_metrics(
        net_pnl_usd="200",
        max_drawdown="0.05",
        sharpe_ratio="2.0",
        win_rate="0.8",
        total_trades=10,
    )
    stable = _build_metrics(
        net_pnl_usd="100",
        max_drawdown="0.1",
        sharpe_ratio="1.0",
        win_rate="0.5",
        total_trades=10,
    )
    deployment = _make_deployment(canary_perf=canary, stable_perf=stable)

    result = deployment.compare_performance()

    assert result.decision == CanaryDecision.PROMOTE
    assert result.decision_reasons == ["All promotion criteria met"]
    assert result.pnl_ratio == Decimal("2")
    assert result.drawdown_ratio == Decimal("0.5")
    assert result.sharpe_ratio == Decimal("2")
    assert result.win_rate_ratio == Decimal("1.6")


def test_canary_wins_two_of_three_ties_on_one_promotes() -> None:
    """Canary ties pnl (1.0) but beats drawdown and sharpe -> PROMOTE
    since all ratios still satisfy the default thresholds."""
    canary = _build_metrics(
        net_pnl_usd="100",
        max_drawdown="0.05",
        sharpe_ratio="1.5",
        win_rate="0.6",
        total_trades=10,
    )
    stable = _build_metrics(
        net_pnl_usd="100",
        max_drawdown="0.1",
        sharpe_ratio="1.0",
        win_rate="0.5",
        total_trades=10,
    )
    deployment = _make_deployment(canary_perf=canary, stable_perf=stable)

    result = deployment.compare_performance()

    assert result.decision == CanaryDecision.PROMOTE
    assert result.pnl_ratio == Decimal("1")


def test_mixed_verdict_pnl_below_threshold_rolls_back() -> None:
    """Canary beats drawdown + sharpe but pnl ratio 0.5 < 0.9 -> ROLLBACK.
    PnL is the hard gate: it short-circuits before soft metrics are
    inspected."""
    canary = _build_metrics(
        net_pnl_usd="50",
        max_drawdown="0.05",
        sharpe_ratio="2.0",
        win_rate="0.7",
        total_trades=10,
    )
    stable = _build_metrics(
        net_pnl_usd="100",
        max_drawdown="0.1",
        sharpe_ratio="1.0",
        win_rate="0.5",
        total_trades=10,
    )
    deployment = _make_deployment(canary_perf=canary, stable_perf=stable)

    result = deployment.compare_performance()

    assert result.decision == CanaryDecision.ROLLBACK
    assert any("PnL ratio too low" in r for r in result.decision_reasons)


def test_sharpe_below_threshold_requests_manual_review() -> None:
    """Canary matches pnl + drawdown but sharpe < min -> MANUAL_REVIEW,
    not ROLLBACK (sharpe is a soft criterion in the cascade)."""
    canary = _build_metrics(
        net_pnl_usd="100",
        max_drawdown="0.05",
        sharpe_ratio="0.5",  # ratio 0.5 < 0.8
        win_rate="0.6",
        total_trades=10,
    )
    stable = _build_metrics(
        net_pnl_usd="100",
        max_drawdown="0.1",
        sharpe_ratio="1.0",
        win_rate="0.5",
        total_trades=10,
    )
    deployment = _make_deployment(canary_perf=canary, stable_perf=stable)

    result = deployment.compare_performance()

    assert result.decision == CanaryDecision.MANUAL_REVIEW
    assert any("Sharpe ratio too low" in r for r in result.decision_reasons)


def test_win_rate_below_threshold_requests_manual_review() -> None:
    """Canary passes pnl + drawdown + sharpe but win_rate ratio below
    threshold -> MANUAL_REVIEW."""
    canary = _build_metrics(
        net_pnl_usd="100",
        max_drawdown="0.05",
        sharpe_ratio="1.0",
        win_rate="0.3",  # ratio 0.5 < 0.8
        total_trades=10,
    )
    stable = _build_metrics(
        net_pnl_usd="100",
        max_drawdown="0.1",
        sharpe_ratio="1.0",
        win_rate="0.6",
        total_trades=10,
    )
    deployment = _make_deployment(canary_perf=canary, stable_perf=stable)

    result = deployment.compare_performance()

    assert result.decision == CanaryDecision.MANUAL_REVIEW
    assert any("Win rate ratio too low" in r for r in result.decision_reasons)


def test_baseline_wins_unambiguously_rolls_back() -> None:
    """Canary loses on pnl and drawdown -> ROLLBACK."""
    canary = _build_metrics(
        net_pnl_usd="20",
        max_drawdown="0.3",
        sharpe_ratio="0.5",
        win_rate="0.3",
        total_trades=10,
    )
    stable = _build_metrics(
        net_pnl_usd="100",
        max_drawdown="0.05",
        sharpe_ratio="1.5",
        win_rate="0.7",
        total_trades=10,
    )
    deployment = _make_deployment(canary_perf=canary, stable_perf=stable)

    result = deployment.compare_performance()

    assert result.decision == CanaryDecision.ROLLBACK
    # pnl ratio 0.2 gate fires first per cascade.
    assert any("PnL ratio too low" in r for r in result.decision_reasons)


def test_tied_on_all_metrics_promotes_under_default_criteria() -> None:
    """Exact equality across all metrics: every ratio is 1.0, which clears
    the default thresholds (pnl>=0.9, drawdown<=1.2, sharpe>=0.8,
    win_rate>=0.8) so the canary is PROMOTED. Locked as characterization."""
    canary = _build_metrics(
        net_pnl_usd="100",
        max_drawdown="0.1",
        sharpe_ratio="1.0",
        win_rate="0.5",
        total_trades=10,
    )
    stable = _build_metrics(
        net_pnl_usd="100",
        max_drawdown="0.1",
        sharpe_ratio="1.0",
        win_rate="0.5",
        total_trades=10,
    )
    deployment = _make_deployment(canary_perf=canary, stable_perf=stable)

    result = deployment.compare_performance()

    assert result.decision == CanaryDecision.PROMOTE
    assert result.pnl_ratio == Decimal("1")
    assert result.drawdown_ratio == Decimal("1")
    assert result.sharpe_ratio == Decimal("1")
    assert result.win_rate_ratio == Decimal("1")


def test_insufficient_trades_returns_continue() -> None:
    """Trade count below ``min_trades`` -> CONTINUE, regardless of how good
    the metrics look. PROMOTE is blocked until more samples arrive."""
    canary = _build_metrics(
        net_pnl_usd="1000",
        max_drawdown="0.01",
        sharpe_ratio="5.0",
        win_rate="0.99",
        total_trades=2,
    )
    stable = _build_metrics(
        net_pnl_usd="100",
        max_drawdown="0.1",
        sharpe_ratio="1.0",
        win_rate="0.5",
        total_trades=10,
    )
    deployment = _make_deployment(
        canary_perf=canary,
        stable_perf=stable,
        canary_trade_count=2,
    )

    result = deployment.compare_performance()

    assert result.decision == CanaryDecision.CONTINUE
    assert any("Insufficient trades" in r for r in result.decision_reasons)


def test_zero_baseline_pnl_with_positive_canary_sets_sentinel_and_promotes() -> None:
    """When stable pnl == 0 and canary pnl > 0, ``pnl_ratio`` uses the
    ``+999`` sentinel (canary dominates). Decision -> PROMOTE."""
    canary = _build_metrics(
        net_pnl_usd="100",
        max_drawdown="0.05",
        sharpe_ratio="1.0",
        win_rate="0.5",
        total_trades=10,
    )
    stable = _build_metrics(
        net_pnl_usd="0",
        max_drawdown="0.1",
        sharpe_ratio="1.0",
        win_rate="0.5",
        total_trades=10,
    )
    deployment = _make_deployment(canary_perf=canary, stable_perf=stable)

    result = deployment.compare_performance()

    assert result.pnl_ratio == Decimal("999")
    assert result.decision == CanaryDecision.PROMOTE


def test_zero_baseline_pnl_with_negative_canary_sets_sentinel_and_rolls_back() -> None:
    """Stable pnl == 0, canary pnl < 0 -> ``pnl_ratio`` = ``-999``,
    decision = ROLLBACK (pnl ratio gate fires)."""
    canary = _build_metrics(
        net_pnl_usd="-50",
        max_drawdown="0.05",
        sharpe_ratio="1.0",
        win_rate="0.5",
        total_trades=10,
    )
    stable = _build_metrics(
        net_pnl_usd="0",
        max_drawdown="0.1",
        sharpe_ratio="1.0",
        win_rate="0.5",
        total_trades=10,
    )
    deployment = _make_deployment(canary_perf=canary, stable_perf=stable)

    result = deployment.compare_performance()

    assert result.pnl_ratio == Decimal("-999")
    assert result.decision == CanaryDecision.ROLLBACK


def test_zero_baseline_pnl_with_zero_canary_leaves_ratio_none() -> None:
    """Both pnl values zero -> ``pnl_ratio`` stays ``None``, no gate fires
    on it, decision falls through to PROMOTE (all ratios None or 1.0)."""
    canary = _build_metrics(
        net_pnl_usd="0",
        max_drawdown="0.05",
        sharpe_ratio="1.0",
        win_rate="0.5",
        total_trades=10,
    )
    stable = _build_metrics(
        net_pnl_usd="0",
        max_drawdown="0.1",
        sharpe_ratio="1.0",
        win_rate="0.5",
        total_trades=10,
    )
    deployment = _make_deployment(canary_perf=canary, stable_perf=stable)

    result = deployment.compare_performance()

    assert result.pnl_ratio is None
    assert result.decision == CanaryDecision.PROMOTE


def test_zero_baseline_drawdown_with_positive_canary_sets_sentinel() -> None:
    """Stable drawdown == 0, canary drawdown > 0 -> ``drawdown_ratio`` =
    ``999``. Gate fires (999 > 1.2) -> ROLLBACK."""
    canary = _build_metrics(
        net_pnl_usd="100",
        max_drawdown="0.05",
        sharpe_ratio="1.0",
        win_rate="0.5",
        total_trades=10,
    )
    stable = _build_metrics(
        net_pnl_usd="100",
        max_drawdown="0",
        sharpe_ratio="1.0",
        win_rate="0.5",
        total_trades=10,
    )
    deployment = _make_deployment(canary_perf=canary, stable_perf=stable)

    result = deployment.compare_performance()

    assert result.drawdown_ratio == Decimal("999")
    assert result.decision == CanaryDecision.ROLLBACK
    assert any("Drawdown ratio too high" in r for r in result.decision_reasons)


def test_zero_baseline_sharpe_skips_sharpe_ratio() -> None:
    """When stable sharpe is zero-or-None, ``sharpe_ratio`` is left as
    ``None`` and the sharpe gate is skipped entirely."""
    canary = _build_metrics(
        net_pnl_usd="100",
        max_drawdown="0.05",
        sharpe_ratio="0.2",  # would fail if a ratio were computed
        win_rate="0.5",
        total_trades=10,
    )
    stable = _build_metrics(
        net_pnl_usd="100",
        max_drawdown="0.1",
        sharpe_ratio=None,
        win_rate="0.5",
        total_trades=10,
    )
    deployment = _make_deployment(canary_perf=canary, stable_perf=stable)

    result = deployment.compare_performance()

    assert result.sharpe_ratio is None
    assert result.decision == CanaryDecision.PROMOTE


def test_require_positive_pnl_blocks_non_positive_canary() -> None:
    """Even if all ratios are nominally fine, ``require_positive_pnl``
    forces ROLLBACK for canary pnl <= 0."""
    canary = _build_metrics(
        net_pnl_usd="0",
        max_drawdown="0.05",
        sharpe_ratio="1.0",
        win_rate="0.5",
        total_trades=10,
    )
    stable = _build_metrics(
        net_pnl_usd="-100",  # so pnl_ratio = 0 / -100 = 0, but require_positive_pnl gate fires first
        max_drawdown="0.1",
        sharpe_ratio="1.0",
        win_rate="0.5",
        total_trades=10,
    )
    criteria = PromotionCriteria(require_positive_pnl=True)
    deployment = _make_deployment(
        canary_perf=canary, stable_perf=stable, criteria=criteria
    )

    result = deployment.compare_performance()

    assert result.decision == CanaryDecision.ROLLBACK
    assert any("Canary PnL not positive" in r for r in result.decision_reasons)


def test_high_error_rate_rolls_back() -> None:
    """Error rate above ``max_error_rate`` short-circuits to ROLLBACK
    before any ratio gate is consulted."""
    canary = _build_metrics(
        net_pnl_usd="500",
        max_drawdown="0.01",
        sharpe_ratio="3.0",
        win_rate="0.9",
        total_trades=10,
    )
    stable = _build_metrics(
        net_pnl_usd="100",
        max_drawdown="0.1",
        sharpe_ratio="1.0",
        win_rate="0.5",
        total_trades=10,
    )
    deployment = _make_deployment(
        canary_perf=canary,
        stable_perf=stable,
        canary_error_count=3,  # error_rate = 0.3 > 0.1
        canary_trade_count=10,
    )

    result = deployment.compare_performance()

    assert result.decision == CanaryDecision.ROLLBACK
    assert any("Error rate too high" in r for r in result.decision_reasons)


@pytest.mark.parametrize(
    ("canary_pnl", "stable_pnl"),
    [
        ("Infinity", "100"),
        ("-Infinity", "100"),
        ("100", "Infinity"),
    ],
)
def test_inf_metric_values_do_not_raise(
    canary_pnl: str, stable_pnl: str
) -> None:
    """``Decimal("Infinity")`` is a legal finite-to-infinity value; the
    function must not raise on it. The exact decision is an implementation
    artifact (ratio becomes +/-Infinity or 0), so we only lock in the
    non-raising contract and a valid enum decision."""
    canary = _build_metrics(
        net_pnl_usd=canary_pnl,
        max_drawdown="0.05",
        sharpe_ratio="1.0",
        win_rate="0.5",
        total_trades=10,
    )
    stable = _build_metrics(
        net_pnl_usd=stable_pnl,
        max_drawdown="0.1",
        sharpe_ratio="1.0",
        win_rate="0.5",
        total_trades=10,
    )
    deployment = _make_deployment(canary_perf=canary, stable_perf=stable)

    result = deployment.compare_performance()

    assert isinstance(result.decision, CanaryDecision)


def test_nan_metric_values_short_circuit_to_manual_review() -> None:
    """``Decimal("NaN")`` is non-orderable (``<`` / ``>`` raise
    ``decimal.InvalidOperation`` under the default context). The decision
    cascade must detect any NaN ratio before ordering gates fire and
    short-circuit to MANUAL_REVIEW with a specific reason, rather than
    crashing on the first ordering comparison."""
    canary = _build_metrics(
        net_pnl_usd="NaN",
        max_drawdown="0.05",
        sharpe_ratio="1.0",
        win_rate="0.5",
        total_trades=10,
    )
    stable = _build_metrics(
        net_pnl_usd="100",
        max_drawdown="0.1",
        sharpe_ratio="1.0",
        win_rate="0.5",
        total_trades=10,
    )
    deployment = _make_deployment(canary_perf=canary, stable_perf=stable)

    result = deployment.compare_performance()

    assert isinstance(result, CanaryComparison)
    assert result.decision == CanaryDecision.MANUAL_REVIEW
    assert result.decision_reasons == ["Non-comparable metric ratio (NaN)"]
    # pnl_ratio is the NaN carrier - assert it was propagated intact.
    assert result.pnl_ratio is not None
    assert result.pnl_ratio.is_nan()
