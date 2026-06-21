"""VIB-5345 — the default *display* window is decoupled from the *fetch* window.

The TA price/RSI charts used to plot the whole fetched candle span (multi-day:
~7 days for a 1h strategy), crowding the recent action of a freshly-deployed
strategy into the right edge. The fix bounds only the *visible* x-axis to a
configurable default (~1 day) while still FETCHING the wide span the indicator
warmup + signal-marker coverage need, and computing the indicator over that full
series.

These tests pin:
1. the pure :func:`display_window_bounds` derivation (floor safety-net, strategy
   anchoring, disable, tz-normalisation);
2. the FETCH span is untouched — the indicator is still computed over the full
   fetched series, and the full series is still plotted as *data*;
3. the rendered plotly x-axis *range* spans <= the display default and never
   starts before the strategy start, across every TA render path.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd

from almanak.framework.dashboard.templates._ohlcv_window import (
    DEFAULT_DISPLAY_WINDOW_SECONDS,
    display_window_bounds,
    ohlcv_limit_for_timeframe,
)
from almanak.framework.dashboard.templates.ta_dashboard import (
    TADashboardConfig,
    _RSI_DECISION_BUFFER,
    _apply_display_window,
    _naive_utc,
    _render_charts_section,
    get_macd_config,
    get_rsi_config,
    multi_ta_config,
    prepare_ta_session_state,
)

_NOW = datetime(2026, 6, 21, 12, 0, 0, tzinfo=UTC)
_DAY = 86400


# ----------------------------------------------------------------------
# display_window_bounds — pure derivation
# ----------------------------------------------------------------------


def test_no_data_end_returns_none() -> None:
    assert display_window_bounds(None, None) is None


def test_disabled_cap_returns_none() -> None:
    # display_window_seconds <= 0 is the explicit "plot the full fetched span" opt-out.
    assert display_window_bounds(_NOW, None, 0) is None
    assert display_window_bounds(_NOW, _NOW - timedelta(hours=1), -5) is None


def test_floor_is_the_safety_net_when_strategy_start_is_too_early() -> None:
    # VIB-5343: _strategy_start_time can report a prior run's start a day early.
    # The floor (now - window) must win so we never show pre-strategy data.
    too_early = _NOW - timedelta(days=5)
    start, end = display_window_bounds(_NOW, too_early, _DAY)
    assert end == _NOW
    assert start == _NOW - timedelta(seconds=_DAY)
    assert (end - start) == timedelta(seconds=_DAY)


def test_recent_strategy_start_tightens_the_window() -> None:
    # A 3h-old deployment shows ~its 3h of life, not a blank day.
    start_3h = _NOW - timedelta(hours=3)
    start, end = display_window_bounds(_NOW, start_3h, _DAY)
    assert start == start_3h
    assert end == _NOW
    assert (end - start) < timedelta(seconds=_DAY)


def test_degenerate_future_start_falls_back_to_floor() -> None:
    # A start at/after data_end is degenerate (clock skew / bad timeline) — never
    # let the window collapse or invert; fall back to the floor.
    start, end = display_window_bounds(_NOW, _NOW + timedelta(hours=1), _DAY)
    assert start == _NOW - timedelta(seconds=_DAY)
    assert end == _NOW


def test_naive_timestamps_are_treated_as_utc() -> None:
    naive_end = datetime(2026, 6, 21, 12, 0, 0)  # noqa: DTZ001 — intentionally naive
    naive_start = datetime(2026, 6, 21, 9, 0, 0)  # noqa: DTZ001
    start, end = display_window_bounds(naive_end, naive_start, _DAY)
    assert end == _NOW
    assert start == _NOW - timedelta(hours=3)


def test_window_never_exceeds_the_display_default() -> None:
    for sst in (None, _NOW - timedelta(days=30), _NOW - timedelta(hours=1)):
        start, end = display_window_bounds(_NOW, sst, _DAY)
        assert (end - start) <= timedelta(seconds=_DAY)
        assert start >= _NOW - timedelta(seconds=_DAY)


# ----------------------------------------------------------------------
# Render integration — fetch vs display decoupling
# ----------------------------------------------------------------------

_FETCH_DAYS = 7
_FETCH_N = _FETCH_DAYS * 24  # 168 hourly candles ≈ 7 days
_FETCH_START = pd.Timestamp("2026-06-14", tz="UTC")


def _ohlcv_payload(n: int = _FETCH_N) -> list[dict[str, Any]]:
    """``n`` hourly candles starting at ``_FETCH_START`` (the wide FETCH span)."""
    times = pd.date_range(_FETCH_START, periods=n, freq="1h")
    return [
        {
            "timestamp": t.isoformat(),
            "open": str(2000.0 + i),
            "high": str(2005.0 + i),
            "low": str(1995.0 + i),
            "close": str(2000.0 + (i % 23) * 3.0),  # oscillate so RSI is well-defined
            "volume": "1",
        }
        for i, t in enumerate(times)
    ]


class _Client:
    def __init__(self, ohlcv: list[dict[str, Any]]) -> None:
        self._ohlcv = ohlcv

    def get_ohlcv(self, **_: Any) -> list[dict[str, Any]]:
        return self._ohlcv

    def get_trade_tape(self) -> dict[str, Any]:
        return {"rows": [], "has_more": False}

    def get_timeline(self, **_: Any) -> list[dict[str, Any]]:
        return []


def _capture_streamlit(monkeypatch) -> list[Any]:
    import almanak.framework.dashboard.templates.ta_dashboard as tad

    figs: list[Any] = []
    monkeypatch.setattr(tad.st, "plotly_chart", lambda fig=None, *a, **k: figs.append(fig))
    monkeypatch.setattr(tad.st, "caption", lambda *a, **k: None)
    return figs


def _xaxis_range(fig: Any) -> tuple[pd.Timestamp, pd.Timestamp]:
    rng = fig.layout.xaxis.range
    assert rng is not None, "x-axis display range must be set"
    return pd.to_datetime(rng[0], utc=True), pd.to_datetime(rng[1], utc=True)


def _price_trace_span(fig: Any) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Min/max x of the (largest) line trace — the price series carries the data."""
    best: list[pd.Timestamp] = []
    for trace in fig.data:
        if getattr(trace, "mode", None) == "lines" and trace.x is not None:
            xs = pd.to_datetime(list(trace.x), utc=True)
            if len(xs) > len(best):
                best = list(xs)
    assert best, "expected at least one line trace carrying the series"
    return min(best), max(best)


def test_fetch_span_unchanged_and_covers_indicator_warmup() -> None:
    # The display cap must NOT shrink the fetch policy. A 1h dashboard still
    # requests its full recent-window candle count (168 = 7 days), comfortably
    # more than the RSI warmup needs (period + decision buffer).
    period = 14
    fetch_limit = ohlcv_limit_for_timeframe("1h")
    assert fetch_limit == 168  # legacy fetch policy, untouched by VIB-5345
    assert fetch_limit >= period + _RSI_DECISION_BUFFER


def test_indicator_computed_over_full_fetched_series(monkeypatch) -> None:
    # Decoupling proof #1: the RSI series spans the WHOLE fetched window, not just
    # the displayed day — the cap is display-only.
    _capture_streamlit(monkeypatch)
    state = prepare_ta_session_state(
        _Client(_ohlcv_payload()),
        session_state={},
        config=get_rsi_config(period=14),
    )
    rsi = state["rsi_history"]
    span = rsi.index.max() - rsi.index.min()
    assert span >= timedelta(days=_FETCH_DAYS - 1), "RSI must be computed across the full fetch"


def test_rsi_path_caps_display_but_keeps_full_data(monkeypatch) -> None:
    figs = _capture_streamlit(monkeypatch)
    state = prepare_ta_session_state(
        _Client(_ohlcv_payload()), session_state={}, config=get_rsi_config(period=14)
    )
    _render_charts_section(state, {}, get_rsi_config(period=14), period=14)

    assert figs, "RSI render path must emit a figure"
    fig = figs[0]
    start, end = _xaxis_range(fig)
    # (a) visible window <= the display default (~1 day) ...
    assert (end - start) <= timedelta(seconds=DEFAULT_DISPLAY_WINDOW_SECONDS) + timedelta(minutes=1)
    # (b) ... but the full ~7 days of data is still plotted (decoupling proof #2).
    data_min, data_max = _price_trace_span(fig)
    assert data_min <= _FETCH_START + timedelta(hours=1)
    assert (data_max - data_min) >= timedelta(days=_FETCH_DAYS - 1)


def test_display_window_never_starts_before_strategy_start(monkeypatch) -> None:
    figs = _capture_streamlit(monkeypatch)
    state = prepare_ta_session_state(
        _Client(_ohlcv_payload()), session_state={}, config=get_rsi_config(period=14)
    )
    # Strategy started only 3h before the last candle — window must anchor there,
    # not show a full day of pre-strategy candles.
    data_end = pd.to_datetime(state["price_history"]["time"], utc=True).max()
    strat_start = data_end - timedelta(hours=3)
    state["strategy_start_time"] = strat_start
    _render_charts_section(state, {}, get_rsi_config(period=14), period=14)

    start, end = _xaxis_range(figs[0])
    assert start == pd.Timestamp(strat_start)
    assert (end - start) <= timedelta(hours=3) + timedelta(minutes=1)


def test_disabled_cap_leaves_axis_unbounded(monkeypatch) -> None:
    figs = _capture_streamlit(monkeypatch)
    config = TADashboardConfig(
        indicator_name="RSI", indicator_period=14, upper_threshold=70, lower_threshold=30, display_window_seconds=0
    )
    state = prepare_ta_session_state(_Client(_ohlcv_payload()), session_state={}, config=config)
    _render_charts_section(state, {}, config, period=14)
    # No explicit range set → plotly auto-ranges over the full fetched span.
    assert figs[0].layout.xaxis.range is None


def test_dedicated_path_caps_display(monkeypatch) -> None:
    figs = _capture_streamlit(monkeypatch)
    config = get_macd_config()
    state = prepare_ta_session_state(_Client(_ohlcv_payload()), session_state={}, config=config)
    _render_charts_section(state, {}, config, period=12)
    # The dedicated MACD path renders price + indicator panel — both capped.
    assert figs, "dedicated render path must emit figures"
    for fig in figs:
        if fig.layout.xaxis.range is not None:
            start, end = _xaxis_range(fig)
            assert (end - start) <= timedelta(seconds=DEFAULT_DISPLAY_WINDOW_SECONDS) + timedelta(minutes=1)


def test_multi_indicator_path_caps_display(monkeypatch) -> None:
    figs = _capture_streamlit(monkeypatch)
    config = multi_ta_config(get_rsi_config(period=14), get_macd_config())
    state = prepare_ta_session_state(_Client(_ohlcv_payload()), session_state={}, config=config)
    _render_charts_section(state, {}, config, period=14)

    assert figs, "multi-indicator render path must emit a figure"
    start, end = _xaxis_range(figs[0])
    assert (end - start) <= timedelta(seconds=DEFAULT_DISPLAY_WINDOW_SECONDS) + timedelta(minutes=1)


# ----------------------------------------------------------------------
# tz-mismatch regression (Gemini review on PR #2958)
# ----------------------------------------------------------------------


def test_apply_display_window_emits_tz_naive_range() -> None:
    # Bounds come in tz-AWARE (display_window_bounds stamps UTC); the applied
    # plotly range must be tz-NAIVE so it matches plotly's tz-naive trace data.
    import plotly.graph_objects as go

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=[_NOW - timedelta(hours=1), _NOW], y=[1, 2]))
    bounds = display_window_bounds(_NOW, None, _DAY)
    assert bounds[0].tzinfo is not None and bounds[1].tzinfo is not None  # input is aware
    _apply_display_window(fig, bounds)
    lo, hi = fig.layout.xaxis.range
    assert pd.Timestamp(lo).tzinfo is None, "applied range lower bound must be tz-naive"
    assert pd.Timestamp(hi).tzinfo is None, "applied range upper bound must be tz-naive"
    # Same wall-clock instant, just tz dropped — no accidental shift.
    assert pd.Timestamp(hi) == pd.Timestamp(_NOW).tz_localize(None)


def test_naive_utc_converts_aware_to_utc_wallclock_and_passes_naive_through() -> None:
    aware = pd.Timestamp("2026-06-21T05:00:00", tz="America/New_York")  # 09:00 UTC
    assert _naive_utc(aware) == pd.Timestamp("2026-06-21T09:00:00")
    naive = pd.Timestamp("2026-06-21T09:00:00")
    assert _naive_utc(naive) == naive


def _strip_tz_state(state: dict[str, Any]) -> dict[str, Any]:
    """Make the prepared price/indicator series tz-NAIVE — the real failure path.

    Live ``price_history`` can arrive tz-naive (a custom dashboard / provider that
    yields naive datetime64). Plotly then plots a tz-naive axis; before the fix
    the display-window range was tz-aware and silently mismatched it → blank chart.
    """
    pdf = state["price_history"].copy()
    pdf["time"] = pd.to_datetime(pdf["time"], utc=True).dt.tz_localize(None)
    state["price_history"] = pdf
    rsi = state["rsi_history"].copy()
    rsi.index = pd.DatetimeIndex(rsi.index).tz_localize(None)
    state["rsi_history"] = rsi
    return state


def test_tz_naive_price_history_range_matches_axis_data(monkeypatch) -> None:
    """Regression: a tz-naive price axis must get a tz-naive (matching) range.

    Reproduces the Gemini-flagged blank-chart bug: with a tz-naive incoming
    price frame, the plotly trace data serialises offset-free; the display-window
    range must serialise offset-free too (same basis), else the chart blanks on a
    non-UTC browser. Asserts at the JSON layer that BOTH are offset-free and that
    the window sits within the plotted data span.
    """
    figs = _capture_streamlit(monkeypatch)
    state = prepare_ta_session_state(
        _Client(_ohlcv_payload()), session_state={}, config=get_rsi_config(period=14)
    )
    _strip_tz_state(state)
    _render_charts_section(state, {}, get_rsi_config(period=14), period=14)

    assert figs, "RSI render path must emit a figure"
    fig = figs[0]

    # 1) The stored range is tz-naive (matches the tz-naive axis data).
    lo, hi = _xaxis_range_naive(fig)
    assert lo.tzinfo is None and hi.tzinfo is None

    # 2) JSON round-trip: data.x and xaxis.range are on the SAME (offset-free)
    #    basis — the exact mismatch that caused the blank chart.
    payload = json.loads(fig.to_json())
    data_x0 = payload["data"][0]["x"][0]
    rng = payload["layout"]["xaxis"]["range"]
    assert "+00:00" not in data_x0, "trace data is offset-free (plotly tz-naive)"
    assert all("+00:00" not in str(b) and "Z" not in str(b) for b in rng), (
        f"range must match the offset-free trace data, got {rng}"
    )

    # 3) The window is non-degenerate and lands within the plotted data span.
    data_min, data_max = _price_trace_span_naive(fig)
    assert lo < hi
    assert lo >= data_min - timedelta(minutes=1)
    assert hi <= data_max + timedelta(minutes=1)
    assert (hi - lo) <= timedelta(seconds=DEFAULT_DISPLAY_WINDOW_SECONDS) + timedelta(minutes=1)


def _xaxis_range_naive(fig: Any) -> tuple[pd.Timestamp, pd.Timestamp]:
    rng = fig.layout.xaxis.range
    assert rng is not None, "x-axis display range must be set"
    return pd.Timestamp(rng[0]), pd.Timestamp(rng[1])


def _price_trace_span_naive(fig: Any) -> tuple[pd.Timestamp, pd.Timestamp]:
    best: list[pd.Timestamp] = []
    for trace in fig.data:
        if getattr(trace, "mode", None) == "lines" and trace.x is not None:
            xs = [pd.Timestamp(x) for x in trace.x]
            if len(xs) > len(best):
                best = xs
    assert best, "expected at least one line trace carrying the series"
    return min(best), max(best)
