"""VIB-5058 — buy/sell markers must not plot outside the OHLCV price window.

The marker series comes from the trade tape (newest N intents, reaching
arbitrarily far back in time) while the price/indicator series is capped to a
recent candle window (VIB-4969, ``_ohlcv_window.py``). Before the fix,
``_render_charts_section`` plotted every signal unconditionally, so any trade
older than the first plotted candle rendered as a triangle floating in empty
space with no price line under it (observed live on a 5m RSI strategy whose
tape spanned ~3.5 days against a ~2.5-day candle window).

These tests pin the contract: signals older than the plotted price window are
clipped before any render path; in-window signals are untouched.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from almanak.framework.dashboard.templates.ta_dashboard import (
    _clip_signals_to_price_window,
    _render_charts_section,
    get_rsi_config,
    prepare_ta_session_state,
)

_N = 80
_WINDOW_START = pd.Timestamp("2026-05-12", tz="UTC")


def _ohlcv_payload() -> list[dict[str, Any]]:
    """80 hourly candles starting at ``_WINDOW_START`` (the plotted window)."""
    times = pd.date_range(_WINDOW_START, periods=_N, freq="1h")
    return [
        {
            "timestamp": t.isoformat(),
            "open": str(2000.0 + i),
            "high": str(2005.0 + i),
            "low": str(1995.0 + i),
            "close": str(2000.0 + i),
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


def _capture_streamlit(monkeypatch) -> dict[str, Any]:
    """Record the figures handed to ``st.plotly_chart`` (no Streamlit ctx)."""
    import almanak.framework.dashboard.templates.ta_dashboard as tad

    calls: dict[str, Any] = {"figs": []}
    monkeypatch.setattr(
        tad.st,
        "plotly_chart",
        lambda fig=None, *a, **k: calls["figs"].append(fig),
    )
    monkeypatch.setattr(tad.st, "caption", lambda *a, **k: None)
    return calls


def _marker_times(fig: Any) -> list[pd.Timestamp]:
    """All x-coordinates of buy/sell triangle marker traces in a figure."""
    times: list[pd.Timestamp] = []
    for trace in fig.data:
        symbol = getattr(getattr(trace, "marker", None), "symbol", None)
        # Bar traces (e.g. the MACD histogram) carry no ``mode`` attribute.
        if getattr(trace, "mode", None) == "markers" and symbol in ("triangle-up", "triangle-down"):
            times.extend(pd.to_datetime(list(trace.x), utc=True))
    return times


def _session_with_signals(buy: list, sell: list) -> dict[str, Any]:
    state = prepare_ta_session_state(
        _Client(_ohlcv_payload()),
        session_state={"buy_signals": buy, "sell_signals": sell, "total_trades": len(buy) + len(sell)},
        config=get_rsi_config(period=14),
    )
    return state


# ----------------------------------------------------------------------
# Render-path contract (the live repro)
# ----------------------------------------------------------------------


def test_signals_older_than_price_window_are_not_plotted(monkeypatch):
    """Markers that predate the first plotted candle must be clipped (VIB-5058)."""
    calls = _capture_streamlit(monkeypatch)
    stale = _WINDOW_START - pd.Timedelta(days=2)
    fresh = _WINDOW_START + pd.Timedelta(hours=10)
    state = _session_with_signals(
        buy=[[stale, 1990.0], [fresh, 2010.0]],
        sell=[[stale + pd.Timedelta(hours=1), 1991.0]],
    )

    _render_charts_section(state, {}, get_rsi_config(period=14), period=14)

    assert calls["figs"], "RSI render path must emit a figure"
    times = _marker_times(calls["figs"][0])
    assert times, "in-window signals must still be plotted"
    floating = [t for t in times if t < _WINDOW_START]
    assert not floating, f"markers plotted before the price window start: {floating}"
    assert fresh in times, "the in-window buy marker must survive clipping"


def test_in_window_signals_are_untouched(monkeypatch):
    """A tape fully inside the candle window renders every marker (no over-clip)."""
    calls = _capture_streamlit(monkeypatch)
    t1 = _WINDOW_START + pd.Timedelta(hours=5)
    t2 = _WINDOW_START + pd.Timedelta(hours=20)
    state = _session_with_signals(buy=[[t1, 2005.0]], sell=[[t2, 2020.0]])

    _render_charts_section(state, {}, get_rsi_config(period=14), period=14)

    times = _marker_times(calls["figs"][0])
    assert sorted(times) == [t1, t2]


# ----------------------------------------------------------------------
# Helper unit contract
# ----------------------------------------------------------------------


def _price_df() -> pd.DataFrame:
    times = pd.date_range(_WINDOW_START, periods=10, freq="1h")
    return pd.DataFrame({"time": times, "price": [2000.0 + i for i in range(10)]})


def test_clip_drops_only_pre_window_rows() -> None:
    signals = pd.DataFrame(
        {
            "time": [
                _WINDOW_START - pd.Timedelta(days=1),
                _WINDOW_START,  # boundary: first candle is in-window
                _WINDOW_START + pd.Timedelta(hours=3),
            ],
            "price": [1.0, 2.0, 3.0],
        }
    )
    clipped = _clip_signals_to_price_window(signals, _price_df())
    assert clipped is not None
    assert list(clipped["price"]) == [2.0, 3.0]


def test_clip_returns_none_when_everything_is_stale() -> None:
    signals = pd.DataFrame(
        {"time": [_WINDOW_START - pd.Timedelta(days=3)], "price": [1.0]}
    )
    assert _clip_signals_to_price_window(signals, _price_df()) is None


def test_clip_handles_naive_signal_times_as_utc() -> None:
    """Caller-supplied frames may carry tz-naive datetimes — treated as UTC."""
    signals = pd.DataFrame(
        {
            "time": [
                (_WINDOW_START - pd.Timedelta(days=1)).tz_localize(None),
                (_WINDOW_START + pd.Timedelta(hours=2)).tz_localize(None),
            ],
            "price": [1.0, 2.0],
        }
    )
    clipped = _clip_signals_to_price_window(signals, _price_df())
    assert clipped is not None
    assert list(clipped["price"]) == [2.0]


def test_clip_handles_naive_price_axis() -> None:
    """A caller-supplied close-only price frame may be tz-naive too."""
    price = _price_df()
    price["time"] = price["time"].dt.tz_localize(None)
    signals = pd.DataFrame(
        {
            "time": [_WINDOW_START - pd.Timedelta(days=1), _WINDOW_START + pd.Timedelta(hours=2)],
            "price": [1.0, 2.0],
        }
    )
    clipped = _clip_signals_to_price_window(signals, price)
    assert clipped is not None
    assert list(clipped["price"]) == [2.0]


def test_clip_passthrough_on_none_and_unusable_window() -> None:
    assert _clip_signals_to_price_window(None, _price_df()) is None
    # Unusable price window (all-NaT) must not silently eat every marker.
    signals = pd.DataFrame({"time": [_WINDOW_START], "price": [1.0]})
    bad_price = pd.DataFrame({"time": [pd.NaT, pd.NaT], "price": [1.0, 2.0]})
    out = _clip_signals_to_price_window(signals, bad_price)
    assert out is not None and len(out) == 1


def test_signal_newer_than_last_candle_stays_visible(monkeypatch):
    """Right edge is intentionally NOT clipped (asymmetric window contract).

    A trade can legitimately post seconds after the newest fetched candle
    (the tape is fresher than the OHLCV snapshot, and the tape cannot contain
    future trades — the overhang is bounded by one candle interval plus data
    lag). Hiding it would suppress the user's most recent action; unlike the
    pre-window case there is a price line ending immediately adjacent to it.
    """
    calls = _capture_streamlit(monkeypatch)
    last_candle = _WINDOW_START + pd.Timedelta(hours=_N - 1)
    just_after = last_candle + pd.Timedelta(minutes=30)
    state = _session_with_signals(buy=[[just_after, 2080.0]], sell=[])

    _render_charts_section(state, {}, get_rsi_config(period=14), period=14)

    times = _marker_times(calls["figs"][0])
    assert times == [just_after], "a marker just after the newest candle must stay visible"


def test_signal_far_beyond_last_candle_is_clipped(monkeypatch):
    """Right-edge NEGATIVE case: beyond one candle interval past the newest
    candle (a stalled OHLCV feed while trading continues), markers are
    dropped — they would float with no price line, same as the pre-window
    case. The bound is the plotted axis's own candle spacing, so it is
    deterministic and operationally verifiable.
    """
    calls = _capture_streamlit(monkeypatch)
    last_candle = _WINDOW_START + pd.Timedelta(hours=_N - 1)
    within_bound = last_candle + pd.Timedelta(minutes=30)   # <= 1h interval: kept
    beyond_bound = last_candle + pd.Timedelta(hours=6)      # > 1h interval: clipped
    state = _session_with_signals(
        buy=[[within_bound, 2080.0], [beyond_bound, 2090.0]], sell=[]
    )

    _render_charts_section(state, {}, get_rsi_config(period=14), period=14)

    times = _marker_times(calls["figs"][0])
    assert times == [within_bound], (
        f"only the within-bound marker may render, got {times}"
    )


def test_single_candle_axis_skips_right_edge_clip() -> None:
    """One candle = no measurable spacing — the right-edge bound cannot be
    computed, so only the left-edge clip applies (no silent over-clipping)."""
    price = pd.DataFrame({"time": [_WINDOW_START], "price": [2000.0]})
    signals = pd.DataFrame(
        {"time": [_WINDOW_START + pd.Timedelta(days=2)], "price": [1.0]}
    )
    clipped = _clip_signals_to_price_window(signals, price)
    assert clipped is not None and len(clipped) == 1


def _assert_clipped_across_figs(calls, stale, fresh):
    all_times = []
    for fig in calls["figs"]:
        all_times.extend(_marker_times(fig))
    assert fresh in all_times, "the in-window marker must render on this path"
    assert stale not in all_times, "the stale marker leaked through this render path"


def test_multi_indicator_path_clips_signals(monkeypatch):
    """VIB-4897 stacked multi-indicator layout gets the same clip (observed,
    not assumed: the composite figure is inspected directly)."""
    from almanak.framework.dashboard.templates.ta_dashboard import (
        get_macd_config,
        multi_ta_config,
    )

    calls = _capture_streamlit(monkeypatch)
    stale = _WINDOW_START - pd.Timedelta(days=2)
    fresh = _WINDOW_START + pd.Timedelta(hours=10)
    config = multi_ta_config(get_rsi_config(period=14), get_macd_config())
    state = prepare_ta_session_state(
        _Client(_ohlcv_payload()),
        session_state={"buy_signals": [[stale, 1990.0], [fresh, 2010.0]], "sell_signals": [], "total_trades": 2},
        config=config,
    )

    _render_charts_section(state, {}, config, period=14)

    assert calls["figs"], "multi-indicator path must emit a figure"
    _assert_clipped_across_figs(calls, stale, fresh)


def test_dedicated_renderer_path_clips_signals(monkeypatch):
    """MACD (a _DEDICATED_RENDERERS entry) draws price+signals via
    plot_price_with_signals — its figure must carry only clipped markers."""
    from almanak.framework.dashboard.templates.ta_dashboard import get_macd_config

    calls = _capture_streamlit(monkeypatch)
    stale = _WINDOW_START - pd.Timedelta(days=2)
    fresh = _WINDOW_START + pd.Timedelta(hours=10)
    config = get_macd_config()
    state = prepare_ta_session_state(
        _Client(_ohlcv_payload()),
        session_state={"buy_signals": [[stale, 1990.0], [fresh, 2010.0]], "sell_signals": [], "total_trades": 2},
        config=config,
    )

    _render_charts_section(state, {}, config, period=14)

    assert calls["figs"], "dedicated-renderer path must emit figures"
    _assert_clipped_across_figs(calls, stale, fresh)


def test_generic_indicator_path_clips_signals(monkeypatch):
    """An indicator with no dedicated renderer falls to the generic
    plot_price_with_signals path — same clip contract, observed on its figure."""
    from almanak.framework.dashboard.templates.ta_dashboard import TADashboardConfig

    calls = _capture_streamlit(monkeypatch)
    stale = _WINDOW_START - pd.Timedelta(days=2)
    fresh = _WINDOW_START + pd.Timedelta(hours=10)
    config = TADashboardConfig(indicator_name="CUSTOM", timeframe="1h")
    state = prepare_ta_session_state(
        _Client(_ohlcv_payload()),
        session_state={"buy_signals": [[stale, 1990.0], [fresh, 2010.0]], "sell_signals": [], "total_trades": 2},
        config=config,
    )

    _render_charts_section(state, {}, config, period=14)

    assert calls["figs"], "generic path must emit a figure"
    _assert_clipped_across_figs(calls, stale, fresh)


def test_clip_compares_aware_timestamps_across_zones_by_instant() -> None:
    """Aware/aware cross-zone: a US/Eastern trade timestamp against a UTC
    price axis must compare by INSTANT (tz_convert), never by stripped or
    re-localized wall-clock time. A wall-clock comparison would shift
    markers by the UTC offset — silently dropping in-window trades or
    keeping stale ones.
    """
    eastern = "US/Eastern"
    signals = pd.DataFrame(
        {
            "time": [
                # Same INSTANT as the window start, expressed in Eastern —
                # wall-clock reads 5h earlier; must be kept (boundary).
                _WINDOW_START.tz_convert(eastern),
                # 2h inside the window, expressed in Eastern — must be kept.
                (_WINDOW_START + pd.Timedelta(hours=2)).tz_convert(eastern),
                # 1 day BEFORE the window, expressed in Eastern — must drop.
                (_WINDOW_START - pd.Timedelta(days=1)).tz_convert(eastern),
            ],
            "price": [1.0, 2.0, 3.0],
        }
    )
    clipped = _clip_signals_to_price_window(signals, _price_df())
    assert clipped is not None
    assert list(clipped["price"]) == [1.0, 2.0], (
        "instant-equality across zones broken: expected the boundary and "
        f"in-window Eastern timestamps to survive, got {list(clipped['price'])}"
    )


def test_rsi_subplot_path_clips_signals(monkeypatch):
    """RSI combined price+RSI subplot layout, named alongside the other three
    render-path tests: the figure is verified to BE the 2-row combined layout
    (price row + RSI row share one figure) and its markers are inspected
    directly — the stale marker must not appear, the fresh one must.
    """
    import re

    calls = _capture_streamlit(monkeypatch)
    stale = _WINDOW_START - pd.Timedelta(days=2)
    fresh = _WINDOW_START + pd.Timedelta(hours=10)
    state = _session_with_signals(buy=[[stale, 1990.0], [fresh, 2010.0]], sell=[])

    _render_charts_section(state, {}, get_rsi_config(period=14), period=14)

    assert len(calls["figs"]) == 1, "RSI path renders ONE combined figure"
    fig = calls["figs"][0]
    subplot_rows = sum(1 for key in fig.layout if re.fullmatch(r"yaxis\d*", key))
    assert subplot_rows == 2, f"expected the 2-row price+RSI combined subplot, got {subplot_rows} rows"
    _assert_clipped_across_figs(calls, stale, fresh)


def test_clip_handles_mixed_offset_string_timestamps() -> None:
    """Mixed-offset inputs (ISO rows spanning DST, or Z mixed with -05:00)
    must parse to UTC and clip — without ``utc=True`` in the parse they come
    back object-dtype and the ``.dt`` accessor raises, crashing the render
    (Codex audit P2 on PR #2731).
    """
    signals = pd.DataFrame(
        {
            "time": [
                "2026-05-10T00:00:00Z",        # pre-window (UTC offset form)
                "2026-05-11T21:00:00-05:00",   # = 2026-05-12T02:00Z, in-window
                "2026-05-12T03:00:00",         # NAIVE mixed in: treated as UTC,
                                               # must NOT collapse to NaT and drop
            ],
            "price": [1.0, 2.0, 3.0],
        }
    )
    clipped = _clip_signals_to_price_window(signals, _price_df())
    assert clipped is not None
    assert list(clipped["price"]) == [2.0, 3.0]
