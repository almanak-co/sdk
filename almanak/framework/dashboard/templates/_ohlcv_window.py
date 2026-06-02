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
