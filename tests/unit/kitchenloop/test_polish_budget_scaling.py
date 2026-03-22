"""Tests for polish budget dynamic scaling logic.

Validates that the Kitchen Loop correctly scales the polish budget
and max PRs based on open PR count (backpressure).

VIB-1708: Polish budget was fixed at 7200s regardless of PR count,
causing budget exhaustion when open PRs exceeded 10.

Kitchen Loop iteration 119, VIB-1708.
"""

from __future__ import annotations

import pytest


def _compute_polish_params(
    open_pr_count: int,
    base_timeout: int = 7200,
    base_max_prs: int = 4,
    aggressive_max_prs: int = 6,
    min_per_pr_timeout: int = 1200,
) -> dict:
    """Pure-Python reimplementation of the kitchenloop.sh polish budget logic.

    This mirrors the bash logic so we can unit-test the arithmetic without
    running shell scripts.
    """
    polish_timeout = base_timeout
    polish_max_prs = base_max_prs

    # Backpressure scaling (mirrors kitchenloop.sh lines ~1404-1425)
    if open_pr_count > 12:
        if polish_max_prs < aggressive_max_prs:
            polish_max_prs = aggressive_max_prs
        if polish_timeout < 14400:
            polish_timeout = 14400
    elif open_pr_count > 8:
        if polish_max_prs < 5:
            polish_max_prs = 5
        if polish_timeout < 10800:
            polish_timeout = 10800

    # Budget arithmetic (mirrors kitchenloop.sh lines ~1425-1433)
    max_prs_for_budget = (polish_timeout - 600) // min_per_pr_timeout
    if max_prs_for_budget < 1:
        max_prs_for_budget = 1
    if polish_max_prs > max_prs_for_budget:
        polish_max_prs = max_prs_for_budget

    per_pr_timeout = polish_timeout // (polish_max_prs + 1)
    if per_pr_timeout < min_per_pr_timeout:
        per_pr_timeout = min_per_pr_timeout

    return {
        "polish_timeout": polish_timeout,
        "polish_max_prs": polish_max_prs,
        "per_pr_timeout": per_pr_timeout,
        "max_prs_for_budget": max_prs_for_budget,
    }


class TestPolishBudgetScaling:
    """Test dynamic polish budget scaling based on open PR count."""

    def test_low_backpressure_keeps_defaults(self):
        """5 open PRs: no scaling, keep 7200s budget and 4 max PRs."""
        result = _compute_polish_params(open_pr_count=5)
        assert result["polish_timeout"] == 7200
        assert result["polish_max_prs"] == 4

    def test_moderate_backpressure_scales_to_3h(self):
        """10 open PRs: scale to 10800s (3h) and 5 max PRs."""
        result = _compute_polish_params(open_pr_count=10)
        assert result["polish_timeout"] == 10800
        assert result["polish_max_prs"] == 5

    def test_high_backpressure_scales_to_4h(self):
        """15 open PRs: scale to 14400s (4h) and 6 max PRs."""
        result = _compute_polish_params(open_pr_count=15)
        assert result["polish_timeout"] == 14400
        assert result["polish_max_prs"] == 6

    def test_high_backpressure_per_pr_timeout_adequate(self):
        """15 open PRs: per-PR timeout should be >= 1200s (20 min)."""
        result = _compute_polish_params(open_pr_count=15)
        assert result["per_pr_timeout"] >= 1200

    def test_high_backpressure_fits_6_prs(self):
        """15 open PRs: budget should fit max_prs_for_budget >= 6."""
        result = _compute_polish_params(open_pr_count=15)
        assert result["max_prs_for_budget"] >= 6

    def test_boundary_8_prs_no_scaling(self):
        """Exactly 8 open PRs: no scaling (threshold is > 8)."""
        result = _compute_polish_params(open_pr_count=8)
        assert result["polish_timeout"] == 7200
        assert result["polish_max_prs"] == 4

    def test_boundary_9_prs_moderate_scaling(self):
        """9 open PRs: moderate scaling triggers (> 8)."""
        result = _compute_polish_params(open_pr_count=9)
        assert result["polish_timeout"] == 10800
        assert result["polish_max_prs"] == 5

    def test_boundary_12_prs_moderate_scaling(self):
        """Exactly 12 open PRs: still moderate (threshold is > 12 for high)."""
        result = _compute_polish_params(open_pr_count=12)
        assert result["polish_timeout"] == 10800
        assert result["polish_max_prs"] == 5

    def test_boundary_13_prs_high_scaling(self):
        """13 open PRs: high scaling triggers (> 12)."""
        result = _compute_polish_params(open_pr_count=13)
        assert result["polish_timeout"] == 14400
        assert result["polish_max_prs"] == 6

    def test_zero_prs_no_scaling(self):
        """0 open PRs: no scaling."""
        result = _compute_polish_params(open_pr_count=0)
        assert result["polish_timeout"] == 7200
        assert result["polish_max_prs"] == 4

    def test_open_pr_count_unavailable_no_scaling(self):
        """-1 open PRs (query failure sentinel): keep defaults."""
        result = _compute_polish_params(open_pr_count=-1)
        assert result["polish_timeout"] == 7200
        assert result["polish_max_prs"] == 4

    def test_vib1708_scenario_15_prs(self):
        """Reproduce VIB-1708: 15 open PRs with old 7200s budget would only fit 5 PRs.

        With the fix, 14400s budget fits 11 max PRs, 6 attempted (aggressive cap).
        Per-PR timeout is ~2057s (34 min), enough for prep + audit + CI.
        """
        result = _compute_polish_params(open_pr_count=15)
        # Budget doubled
        assert result["polish_timeout"] == 14400
        # Max PRs capped at aggressive limit
        assert result["polish_max_prs"] == 6
        # Per-PR timeout > 1200s minimum
        assert result["per_pr_timeout"] >= 1200
        # Budget can theoretically handle more than 6
        assert result["max_prs_for_budget"] >= 6

    def test_already_high_base_timeout_not_reduced(self):
        """If base timeout is already >= 14400, don't reduce it."""
        result = _compute_polish_params(open_pr_count=15, base_timeout=18000)
        assert result["polish_timeout"] == 18000  # kept higher base


class TestPolishPerPrTimeout:
    """Test per-PR timeout calculation."""

    def test_per_pr_minimum_enforced(self):
        """Per-PR timeout never drops below 1200s."""
        result = _compute_polish_params(open_pr_count=5, base_timeout=2000)
        assert result["per_pr_timeout"] >= 1200

    def test_per_pr_scales_with_budget(self):
        """Higher budget gives more time per PR."""
        low = _compute_polish_params(open_pr_count=5, base_timeout=7200)
        high = _compute_polish_params(open_pr_count=15)  # 14400s
        assert high["per_pr_timeout"] >= low["per_pr_timeout"]
