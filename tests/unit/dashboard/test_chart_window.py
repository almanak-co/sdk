"""Unit tests for the pure chart-window/decimation policy (VIB-5059 Phase 2)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.dashboard.chart_window import (
    DEFAULT_CANDLE_BUDGET,
    MAX_POINTS_CEILING,
    MAX_POINTS_FLOOR,
    TIMEFRAME_SECONDS,
    NavPoint,
    clamp_max_points,
    decimate_nav,
    granularity_for_range,
    validate_window,
)

_BASE = datetime(2026, 1, 1, tzinfo=UTC)


def _series(values: list[float]) -> list[NavPoint]:
    return [NavPoint(_BASE + timedelta(minutes=5 * i), Decimal(str(v))) for i, v in enumerate(values)]


# ---------------------------------------------------------------------------
# validate_window
# ---------------------------------------------------------------------------


def test_validate_window_inverted_raises() -> None:
    with pytest.raises(ValueError, match="inverted window"):
        validate_window(from_ts=2000, to_ts=1000)


def test_validate_window_equal_bounds_raises() -> None:
    with pytest.raises(ValueError):
        validate_window(from_ts=1000, to_ts=1000)


@pytest.mark.parametrize(
    "from_ts,to_ts",
    [(0, 0), (0, 1000), (1000, 0), (1000, 2000)],
)
def test_validate_window_open_or_ordered_ok(from_ts: int, to_ts: int) -> None:
    # Open bounds (0) and a properly-ordered closed window are all valid.
    validate_window(from_ts=from_ts, to_ts=to_ts)


# ---------------------------------------------------------------------------
# clamp_max_points
# ---------------------------------------------------------------------------


def test_clamp_below_floor() -> None:
    assert clamp_max_points(1) == MAX_POINTS_FLOOR
    assert clamp_max_points(0) == MAX_POINTS_FLOOR


def test_clamp_above_ceiling() -> None:
    assert clamp_max_points(10_000_000) == MAX_POINTS_CEILING


def test_clamp_in_range_passthrough() -> None:
    assert clamp_max_points(1500) == 1500


# ---------------------------------------------------------------------------
# decimate_nav
# ---------------------------------------------------------------------------


def test_decimate_empty() -> None:
    assert decimate_nav([], 1500) == []


@pytest.mark.parametrize("n", [1, 2, 5, 100])
def test_decimate_returns_verbatim_when_under_budget(n: int) -> None:
    pts = _series([float(i) for i in range(n)])
    out = decimate_nav(pts, max_points=1500)
    assert out == pts  # verbatim, no thinning


@pytest.mark.parametrize("budget", [2, 3, 4, 5, 10, 100, 1500])
def test_decimate_never_exceeds_budget(budget: int) -> None:
    pts = _series([float(i % 37) for i in range(20_000)])
    out = decimate_nav(pts, max_points=budget)
    assert len(out) <= clamp_max_points(budget)


def test_decimate_keeps_anchors_verbatim() -> None:
    pts = _series([float(i % 11) for i in range(10_000)])
    out = decimate_nav(pts, max_points=600)
    assert out[0] == pts[0]
    assert out[-1] == pts[-1]


def test_decimate_output_strictly_ascending_unique() -> None:
    pts = _series([float(i % 13) for i in range(10_000)])
    out = decimate_nav(pts, max_points=600)
    ts = [p.timestamp for p in out]
    assert ts == sorted(ts)
    assert len(set(ts)) == len(ts)  # no duplicate timestamps


def test_decimate_preserves_singleton_spike_pointwise() -> None:
    # A flat series with one deep singleton drawdown spike buried in the middle.
    values = [100.0] * 10_000
    spike_idx = 4321
    values[spike_idx] = 1.0  # the drawdown trough
    pts = _series(values)
    out = decimate_nav(pts, max_points=1500)

    assert len(out) < len(pts)  # actually decimated
    spike = pts[spike_idx]
    # The exact spike sample (timestamp AND value) must survive — pointwise.
    assert spike in out, "spike sample was averaged/smoothed away"
    # And a flat neighbour from a different bucket is dropped (proves thinning).
    assert pts[0 + 1] not in out or pts[len(pts) // 2 + 7] not in out


def test_decimate_preserves_bucket_max_too() -> None:
    # Spike up (peak) must also survive, not only troughs.
    values = [100.0] * 6_000
    peak_idx = 2222
    values[peak_idx] = 9_999.0
    pts = _series(values)
    out = decimate_nav(pts, max_points=900)
    assert pts[peak_idx] in out


# ---------------------------------------------------------------------------
# granularity_for_range
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "range_seconds",
    [3600, 86_400, 7 * 86_400, 30 * 86_400, 365 * 86_400],
)
def test_granularity_bounded_for_range(range_seconds: int) -> None:
    tf = granularity_for_range(range_seconds, candle_budget=DEFAULT_CANDLE_BUDGET)
    assert tf in TIMEFRAME_SECONDS
    candle_count = range_seconds / TIMEFRAME_SECONDS[tf]
    assert candle_count <= DEFAULT_CANDLE_BUDGET


def test_granularity_picks_finest_that_fits() -> None:
    # 1h range fits 1m candles (60 <= 720) -> finest.
    assert granularity_for_range(3600) == "1m"
    # 1 year: only 1d fits the budget.
    assert granularity_for_range(365 * 86_400) == "1d"


def test_granularity_degenerate_range_safe() -> None:
    assert granularity_for_range(0) == "1m"
    assert granularity_for_range(-5) == "1m"


def test_legacy_ohlcv_window_values_unchanged() -> None:
    # Delegation/addition must not perturb the legacy recent-window count table.
    from almanak.framework.dashboard.templates import _ohlcv_window as ow

    assert ow.ohlcv_limit_for_timeframe("1m") == 720
    assert ow.ohlcv_limit_for_timeframe("5m") == 720
    assert ow.ohlcv_limit_for_timeframe("15m") == 720
    assert ow.ohlcv_limit_for_timeframe("1h") == 168
    assert ow.ohlcv_limit_for_timeframe("4h") == 180
    assert ow.ohlcv_limit_for_timeframe("1d") == 120
    assert ow.ohlcv_limit_for_timeframe("unknown") == ow.DEFAULT_CANDLE_LIMIT
