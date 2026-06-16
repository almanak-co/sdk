"""Shared OHLCV recent-window policy for dashboard templates (VIB-4969).

Dashboard templates fetch a *recent* slice of OHLCV candles to draw the price
chart and to recompute the indicator series client-side. The slice length must
trade off two competing pressures:

* **Long enough** to (a) warm up the configured indicator (RSI/MACD/Bollinger/…
  need tens-to-hundreds of candles) and (b) give visual context for the
  buy/sell markers.
* **Bounded** so a fine granularity (``5m``/``1m``) does not balloon into
  thousands of candles fetched + plotted on every Streamlit rerender.

The cap is a candle **count**, not a fixed wall-clock window. A uniform "7-day
window" would be 2016 candles at ``5m`` / 10080 at ``1m`` (far too many) yet
only 7 candles at ``1d`` (too few to warm up a 14-period indicator). Instead we
anchor on ``168`` — the legacy ``1h`` value (= 1 week), preserved EXACTLY so
existing callers see no behaviour change — and pick neighbouring caps that keep
each request bounded while still spanning a useful recent window:

==========  =================  ===================
 timeframe   candle count       ≈ wall-clock span
==========  =================  ===================
 ``1m``      720                0.5 day
 ``5m``      720                ~2.5 days
 ``15m``     720                ~7.5 days
 ``1h``      168 (legacy)       7 days
 ``4h``      180                ~30 days
 ``1d``      120                ~120 days
==========  =================  ===================

Unknown / future timeframes fall back to the legacy ``168`` rather than an
unbounded or guessed request — fail-safe, never silently unbounded.

This policy lives in one module so the TA and LP templates (and any future
template that fetches OHLCV for a chart) cannot drift apart.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta

_TIMEFRAME_CANDLE_LIMITS: dict[str, int] = {
    "1m": 720,
    "5m": 720,
    "15m": 720,
    "1h": 168,
    "4h": 180,
    "1d": 120,
}

DEFAULT_CANDLE_LIMIT = 168
"""Legacy fallback (1 week at 1h). Used for unknown timeframes."""

DEFAULT_TIMEFRAME = "1h"
"""Fallback granularity when a caller passes a falsy/unset timeframe."""

BACKFILL_CANDLE_BUFFER = 12
"""Extra candles fetched beyond the earliest plotted signal (VIB-5156) so the
oldest buy/sell marker lands *inside* the price window — with a few candles of
lead-in for visual context and indicator warm-up — rather than flush against
the left edge where ``_clip_signals_to_price_window`` might shave it off."""

MAX_BACKFILL_CANDLE_LIMIT = 2000
"""Hard ceiling on the signal-aware recent-window candle count (VIB-5156).

The recent-window caps (above) top out at 720. After a long-running deployment
the earliest still-displayed marker can predate that window, so the limit is
grown just enough to cover it — but never unbounded. 2000 candles is ~7 days at
``5m`` / ~83 days at ``1h`` / ~5.5 years at ``1d`` — ample for any realistic
earliest marker while keeping a single bounded request, mirroring the
"never silently unbounded" policy of the recent-window table above."""


def normalize_timeframe(timeframe: str | None) -> str:
    """Coerce a falsy / empty timeframe to :data:`DEFAULT_TIMEFRAME`.

    A strategy may carry ``data_granularity: null`` (or omit it), and callers
    may pass ``None`` / ``""`` / whitespace. Handing that straight to
    ``api_client.get_ohlcv(timeframe=...)`` errors at the data layer, so
    normalize at the boundary. Non-empty values pass through unchanged (the
    OHLCV layer owns case/alias canonicalization).
    """
    if timeframe is None:
        return DEFAULT_TIMEFRAME
    tf = str(timeframe).strip()
    return tf or DEFAULT_TIMEFRAME


def ohlcv_limit_for_timeframe(timeframe: str) -> int:
    """Return the recent-window candle count to request for ``timeframe``.

    See the module docstring for the policy. Unknown timeframes fall back to
    :data:`DEFAULT_CANDLE_LIMIT` (168) rather than an unbounded request.
    """
    return _TIMEFRAME_CANDLE_LIMITS.get(str(timeframe).lower().strip(), DEFAULT_CANDLE_LIMIT)


@dataclass(frozen=True)
class ChartWindow:
    """Resolved OHLCV-fetch window for a dashboard price chart (VIB-5114).

    ``timeframe`` / ``limit`` drive ``api_client.get_ohlcv``; ``from_ts`` bounds
    the trade-tape marker fetch to the same window (``None`` ⇒ legacy newest-N
    markers). A window built from the strategy's configured timeframe (no
    operator range selected) carries ``from_ts=None`` so the marker fetch — and
    therefore the rendered chart — is byte-for-byte the pre-VIB-5114 behaviour.
    """

    timeframe: str
    limit: int
    from_ts: datetime | None


def build_chart_window(config_timeframe: str | None, range_seconds: int | None) -> ChartWindow:
    """Build the price-chart :class:`ChartWindow` for a (possibly unset) range.

    Pure (no Streamlit, no I/O) so both templates and unit tests share one
    decision. ``range_seconds`` is the operator's selected NAV range translated
    to trailing-window seconds (``almanak.framework.dashboard.sections.
    selected_nav_range_seconds``):

    - ``None`` (no range selected / unknown preset) or ``0`` (``"All"`` = open
      bound / full lifetime) → the **legacy** window: the strategy's configured
      timeframe, that timeframe's recent-window candle cap, and ``from_ts=None``
      (newest-N markers). Byte-for-byte unchanged from before VIB-5114.
    - a positive value → a **windowed** fetch following the range: the candle
      granularity becomes :func:`~almanak.framework.dashboard.chart_window.
      granularity_for_range` for that span, the candle count
      :func:`~almanak.framework.dashboard.chart_window.candles_for_range`
      (bounded), and ``from_ts`` the window start (``now - range_seconds``).
    """
    timeframe = normalize_timeframe(config_timeframe)
    if not range_seconds or range_seconds <= 0:
        return ChartWindow(timeframe=timeframe, limit=ohlcv_limit_for_timeframe(timeframe), from_ts=None)

    # Imported here (not at module top) so this lightweight, lean-import module
    # does not eagerly pull the chart_window compute module on every import.
    from almanak.framework.dashboard.chart_window import candles_for_range, granularity_for_range

    windowed_tf = granularity_for_range(range_seconds)
    return ChartWindow(
        timeframe=windowed_tf,
        limit=candles_for_range(range_seconds, windowed_tf),
        from_ts=datetime.now(UTC) - timedelta(seconds=range_seconds),
    )


def _timeframe_seconds(timeframe: str) -> int | None:
    """Seconds per candle for ``timeframe`` (single source of truth shared with
    the windowed path), or ``None`` for an unknown timeframe.

    Imported lazily so this lean-import module does not eagerly pull the heavier
    ``chart_window`` compute module on every import (mirrors
    :func:`build_chart_window`)."""
    from almanak.framework.dashboard.chart_window import TIMEFRAME_SECONDS

    return TIMEFRAME_SECONDS.get(normalize_timeframe(timeframe).lower())


def extend_window_to_cover_signal(
    window: ChartWindow,
    earliest_signal_ts: datetime | None,
    now: datetime | None = None,
) -> ChartWindow:
    """Grow a *legacy recent* window so the price line reaches ``earliest_signal_ts``.

    The recent-window candle count (:func:`ohlcv_limit_for_timeframe`) is chosen
    from the timeframe alone — it never consults the trade tape. After a redeploy
    a strategy's earliest still-displayed buy/sell marker can predate that fixed
    window, so the marker renders with no price/indicator line beneath it (and,
    post-VIB-5058, is clipped away entirely). This grows ``limit`` to
    ``ceil((now - earliest_signal_ts) / timeframe_seconds) + BACKFILL_CANDLE_BUFFER``
    and threads ``from_ts = earliest_signal_ts`` so the marker fetch covers the
    same span — clamped to :data:`MAX_BACKFILL_CANDLE_LIMIT`, never unbounded.

    Back-compat (byte-for-byte identical to the pre-VIB-5156 window) when **any**
    of the following hold — the common case — so the heavily-asserted legacy
    contract stays green:

    * ``window.from_ts is not None`` — an operator already selected a bounded NAV
      range (VIB-5114); that window is authoritative and is never widened here.
    * ``earliest_signal_ts is None`` — no signals to cover.
    * the earliest signal is **not older** than the current ``limit`` already
      reaches back (``earliest_signal_ts >= now - limit * timeframe_seconds``).
    * the timeframe is unknown (no seconds mapping) — fail-safe, leave it alone.
    """
    if window.from_ts is not None or earliest_signal_ts is None:
        return window

    secs = _timeframe_seconds(window.timeframe)
    if not secs:
        return window

    # Defensive: the sole production caller (``_earliest_signal_ts``) always
    # returns UTC-aware, but this is a public pure helper — normalize a naive
    # input rather than raise on the ``now - earliest_signal_ts`` subtraction.
    if earliest_signal_ts.tzinfo is None:
        earliest_signal_ts = earliest_signal_ts.replace(tzinfo=UTC)
    now = now or datetime.now(UTC)
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    # Coverage of the current limit, in seconds back from now.
    covered_seconds = window.limit * secs
    needed_seconds = (now - earliest_signal_ts).total_seconds()
    if needed_seconds <= covered_seconds:
        # The earliest signal is already inside the recent window — unchanged.
        return window

    needed_candles = math.ceil(needed_seconds / secs) + BACKFILL_CANDLE_BUFFER
    extended_limit = min(max(window.limit, needed_candles), MAX_BACKFILL_CANDLE_LIMIT)
    if extended_limit == window.limit:
        # Already at/above what we'd ask for (e.g. clamped by the ceiling and the
        # base limit is the ceiling) — leave the window object identical.
        return window
    return replace(window, limit=extended_limit, from_ts=earliest_signal_ts)
