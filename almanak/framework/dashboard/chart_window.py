"""Shared windowing + decimation policy for dashboard chart data (VIB-5059 Phase 2).

This module is the single source of truth for the *time-travel* chart contract:
given a requested window and a constant point budget, return a bounded series
that still shows the truth (drawdown spikes survive). It is a **pure** module —
no I/O, no gateway, no Streamlit, no heavy imports — so both the gateway
(``dashboard_service`` builds the NAV series server-side) and the dashboard UI
(OHLCV granularity selection) can import it without a layering inversion. This
mirrors the existing ``quant_aggregations`` shared-compute pattern that the
gateway already imports.

Three concerns live here:

* **Window validation** (:func:`validate_window`) — reject an inverted window
  loudly so the gateway maps it to ``INVALID_ARGUMENT`` instead of silently
  returning an empty-but-complete-looking series.
* **Point-budget decimation** (:func:`decimate_nav`) — thin an arbitrarily long
  NAV series down to ``max_points`` while preserving, per time bucket, the
  **min**, **max**, and **last** point. Plain averaging hides drawdown spikes;
  keeping the extremes does not. The window's first and last raw points are
  always retained verbatim so the chart's endpoints equal the money tiles.
* **Granularity ladder** (:func:`granularity_for_range`) — generalize the
  ``_ohlcv_window`` recent-window ladder to *any* range: pick the finest OHLCV
  timeframe whose candle count stays within a budget, so a one-year window asks
  for ~365 daily candles, never ~105 000 five-minute candles.

Mode contract (proto3-safe with plain scalars): on the request,
``max_points > 0`` selects windowed mode; ``max_points <= 0`` is the legacy
recent-window default and never reaches this module. Within windowed mode a
``from_ts``/``to_ts`` of ``0`` is an open bound (inception / now).
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

# ---------------------------------------------------------------------------
# Point budget constants
# ---------------------------------------------------------------------------

DEFAULT_MAX_POINTS = 1500
"""Budget applied when windowed mode is requested without an explicit, larger value."""

MAX_POINTS_CEILING = 5000
"""Hard upper bound on returned points. A client cannot request the O(lifetime)
over-fetch this ticket removes back in via a giant ``max_points``."""

MAX_POINTS_FLOOR = 2
"""Lower bound on the effective budget. Two points is the minimum that can carry
both window anchors (first + last raw point); a request for ``1`` is clamped up
to this floor rather than dropping an anchor."""


def clamp_max_points(max_points: int) -> int:
    """Clamp a windowed-mode ``max_points`` into ``[FLOOR, CEILING]``.

    Only meaningful in windowed mode (``max_points > 0`` at the wire). A value
    below the floor (e.g. ``1``) is raised to :data:`MAX_POINTS_FLOOR` so both
    window anchors fit; a value above the ceiling is lowered to
    :data:`MAX_POINTS_CEILING` so the response is never unbounded.
    """
    if max_points < MAX_POINTS_FLOOR:
        return MAX_POINTS_FLOOR
    if max_points > MAX_POINTS_CEILING:
        return MAX_POINTS_CEILING
    return max_points


# ---------------------------------------------------------------------------
# Window validation
# ---------------------------------------------------------------------------


def validate_window(from_ts: int, to_ts: int) -> None:
    """Raise :class:`ValueError` on an inverted window; otherwise return ``None``.

    ``from_ts``/``to_ts`` are Unix seconds where ``0`` is an **open bound**
    (``from_ts=0`` → from inception, ``to_ts=0`` → until now). An open bound is
    always valid. The only rejected shape is a fully-closed *inverted* window —
    both bounds non-zero with ``from_ts >= to_ts`` — which can never contain a
    point and almost always signals a client bug. Rejecting it loudly stops the
    gateway from returning a silent empty series the operator would read as "no
    history".
    """
    if from_ts > 0 and to_ts > 0 and from_ts >= to_ts:
        raise ValueError(f"inverted window: from_ts ({from_ts}) must be strictly before to_ts ({to_ts})")


# ---------------------------------------------------------------------------
# NAV decimation
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class NavPoint:
    """One NAV sample on the chart: a timestamp and a measured USD value.

    ``value`` is a :class:`~decimal.Decimal` — a *measured* value. Unmeasured
    samples (``""`` / ``None`` on the wire — the Empty≠Zero case) are excluded
    by the caller before decimation and never reach this type, so a missing
    measurement can never masquerade as a ``$0`` trough that corrupts min/max
    bucketing.
    """

    timestamp: datetime
    value: Decimal


def decimate_nav(points: list[NavPoint], max_points: int) -> list[NavPoint]:
    """Thin ``points`` to at most ``max_points`` while preserving spikes.

    ``points`` MUST be sorted ascending by timestamp (the store's
    ``ORDER BY timestamp ASC``) and carry unique timestamps (guaranteed by the
    ``(deployment_id, timestamp)`` uniqueness of ``portfolio_snapshots``).

    Policy:

    * ``len(points) <= max_points`` → returned **verbatim** (no information loss
      when the series already fits the budget; a 1- or 2-point window is returned
      as-is, so a non-empty window never decimates to empty).
    * otherwise the series is partitioned into contiguous, roughly-equal-count
      buckets and, per bucket, the **min-value**, **max-value**, and **last**
      points are kept (so a V-shaped drawdown keeps both shoulders and the trough;
      a monotonic bucket keeps its endpoints). The window's first and last raw
      points are always added, so the chart endpoints equal the money tiles. The
      result is de-duplicated by timestamp and returned ascending, with length
      ``<= max_points``.

    The min/max retention is what makes a downsample faithful: an averaging /
    LTTB-on-mean implementation would smooth a single-sample drawdown spike away;
    this keeps the exact spike sample. Returned points are a subset of the input
    (verbatim values, never interpolated).
    """
    n = len(points)
    if n == 0:
        return []
    budget = clamp_max_points(max_points)
    if n <= budget:
        return list(points)

    # Buckets each contribute up to 3 indices (min, max, last); reserve 2 slots
    # for the global first/last anchors so the worst case 3*num_buckets + 2 stays
    # within budget. When even one full bucket would not fit (budget < 5), return
    # just the two anchors rather than overshooting the budget.
    num_buckets = (budget - 2) // 3
    if num_buckets < 1:
        return [points[0], points[-1]]
    keep: set[int] = {0, n - 1}  # anchors: first + last raw point, verbatim

    for b in range(num_buckets):
        lo = (b * n) // num_buckets
        hi = ((b + 1) * n) // num_buckets
        if lo >= hi:
            continue
        min_i = lo
        max_i = lo
        for i in range(lo + 1, hi):
            if points[i].value < points[min_i].value:
                min_i = i
            elif points[i].value > points[max_i].value:
                max_i = i
        last_i = hi - 1
        keep.add(min_i)
        keep.add(max_i)
        keep.add(last_i)

    return [points[i] for i in sorted(keep)]


# ---------------------------------------------------------------------------
# OHLCV granularity ladder (generalizes _ohlcv_window for arbitrary ranges)
# ---------------------------------------------------------------------------

TIMEFRAME_SECONDS: dict[str, int] = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}
"""Canonical OHLCV timeframe → seconds ladder, finest → coarsest. Single source
of truth for the range→granularity mapping (``_ohlcv_window`` keeps its legacy
recent-window *count* table; this is the time-axis the windowed feature adds)."""

DEFAULT_CANDLE_BUDGET = 720
"""Max candles to request for a windowed price chart. Matches the bounded
recent-window caps in ``_ohlcv_window`` (1m/5m/15m → 720) so the windowed path
never asks the OHLCV provider for an unbounded candle count."""


def granularity_for_range(range_seconds: float, candle_budget: int = DEFAULT_CANDLE_BUDGET) -> str:
    """Pick the finest OHLCV timeframe whose candle count fits ``candle_budget``.

    Walks the ladder finest → coarsest and returns the first timeframe ``tf``
    where ``range_seconds / TIMEFRAME_SECONDS[tf] <= candle_budget``. A 1-year
    window therefore resolves to ``1d`` (~365 candles), a 1-hour window to ``1m``
    (60 candles) — bounded at every scale, never the ~105 000 five-minute candles
    a naïve fixed-granularity window would request. Falls back to the coarsest
    timeframe (``1d``) for ranges so large even daily candles exceed the budget,
    rather than an unbounded request.

    Shipped here alongside the decimation engine it pairs with; the OHLCV
    price-chart consumer (TA/LP templates request candles at this granularity for
    the selected range) is wired in VIB-5114.
    """
    if range_seconds <= 0 or candle_budget <= 0:
        # Degenerate request: hand back the finest timeframe and let the bounded
        # OHLCV layer cap the count — never raise, never unbounded.
        return "1m"
    for tf, secs in TIMEFRAME_SECONDS.items():
        if range_seconds / secs <= candle_budget:
            return tf
    return "1d"


def candles_for_range(range_seconds: float, timeframe: str, candle_budget: int = DEFAULT_CANDLE_BUDGET) -> int:
    """Candle count to request for ``range_seconds`` at ``timeframe`` — bounded.

    Pairs with :func:`granularity_for_range` so a windowed price-chart fetch
    (VIB-5114) asks for exactly the candles spanning the selected window and no
    more: ``ceil(range_seconds / timeframe_seconds)``, clamped to ``[1,
    candle_budget]``. Because :func:`granularity_for_range` chose ``timeframe``
    precisely so the span fits the budget, the clamp is a defensive ceiling, not
    the normal path — a 7-day window at ``15m`` resolves to ~672 candles, a
    1-year window at ``1d`` to ~365, never an unbounded request. An unknown
    timeframe (not in :data:`TIMEFRAME_SECONDS`) falls back to the full budget
    rather than guessing a span — fail-safe, never unbounded.
    """
    secs = TIMEFRAME_SECONDS.get(timeframe)
    if not secs or range_seconds <= 0:
        return max(1, candle_budget)
    needed = math.ceil(range_seconds / secs)  # ceil — correct for int and float range_seconds
    return max(1, min(needed, candle_budget))
