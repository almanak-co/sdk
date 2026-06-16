"""VIB-5156 — OHLCV recent window backfills to cover the earliest signal.

The legacy recent-window candle count (``_ohlcv_window.ohlcv_limit_for_timeframe``)
is chosen from the timeframe alone — it never consults the trade tape. After a
redeploy a strategy's earliest still-displayed buy/sell marker can predate that
fixed window, so the marker renders with no price/indicator line beneath it (and,
post-VIB-5058, is clipped away). ``extend_window_to_cover_signal`` grows the
window's candle ``limit`` (and threads ``from_ts``) just enough to cover the
earliest signal — clamped to a ceiling, never unbounded — and is a strict no-op
when there is nothing older than the current window (back-compat).

These tests pin: (1) the pure derivation across older / no-older / ceiling cases,
(2) the operator-windowed path is never widened, and (3) the integration through
``prepare_ta_session_state`` lifts the requested OHLCV ``limit`` when the tape
reaches back before the recent window — and leaves it byte-for-byte otherwise.
"""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd

from almanak.framework.dashboard.chart_window import TIMEFRAME_SECONDS
from almanak.framework.dashboard.templates import get_rsi_config, prepare_ta_session_state
from almanak.framework.dashboard.templates._ohlcv_window import (
    BACKFILL_CANDLE_BUFFER,
    MAX_BACKFILL_CANDLE_LIMIT,
    ChartWindow,
    extend_window_to_cover_signal,
    ohlcv_limit_for_timeframe,
)
from almanak.framework.dashboard.templates.ta_dashboard import _earliest_signal_ts

_NOW = datetime(2026, 6, 16, 12, 0, 0, tzinfo=UTC)


def _legacy_window(timeframe: str = "5m") -> ChartWindow:
    """The legacy recent window the resolver builds when no range is selected."""
    return ChartWindow(timeframe=timeframe, limit=ohlcv_limit_for_timeframe(timeframe), from_ts=None)


# ----------------------------------------------------------------------
# extend_window_to_cover_signal — pure derivation
# ----------------------------------------------------------------------


def test_no_signal_leaves_window_identical() -> None:
    window = _legacy_window("5m")
    out = extend_window_to_cover_signal(window, None, now=_NOW)
    assert out is window  # identity: byte-for-byte back-compat


def test_signal_inside_current_window_is_unchanged() -> None:
    # 5m window of 720 candles reaches back 720 * 300s = 2.5 days.
    window = _legacy_window("5m")
    inside = _NOW - timedelta(hours=12)  # well within 2.5 days
    out = extend_window_to_cover_signal(window, inside, now=_NOW)
    assert out == window
    assert out.from_ts is None  # marker fetch stays the legacy newest-N


def test_older_signal_extends_limit_and_sets_from_ts() -> None:
    # Earliest signal 5 days back: 5m window (2.5 days) is too short.
    window = _legacy_window("5m")
    earliest = _NOW - timedelta(days=5)
    out = extend_window_to_cover_signal(window, earliest, now=_NOW)

    secs = TIMEFRAME_SECONDS["5m"]
    needed = math.ceil((5 * 86400) / secs) + BACKFILL_CANDLE_BUFFER
    assert out.limit == needed
    assert out.limit > window.limit
    assert out.timeframe == "5m"
    # from_ts threads the earliest signal so the marker fetch covers the span too.
    assert out.from_ts == earliest


def test_extension_is_clamped_to_ceiling() -> None:
    # An ancient signal would need far more than the ceiling at 1m granularity.
    window = ChartWindow(timeframe="1m", limit=ohlcv_limit_for_timeframe("1m"), from_ts=None)
    earliest = _NOW - timedelta(days=365)  # ~525k candles at 1m, way over ceiling
    out = extend_window_to_cover_signal(window, earliest, now=_NOW)
    assert out.limit == MAX_BACKFILL_CANDLE_LIMIT
    assert out.from_ts == earliest


def test_operator_windowed_path_is_never_widened() -> None:
    # from_ts already set (operator selected a bounded NAV range, VIB-5114) =>
    # that window is authoritative; an older signal does NOT widen it.
    windowed = ChartWindow(timeframe="15m", limit=200, from_ts=_NOW - timedelta(days=7))
    out = extend_window_to_cover_signal(windowed, _NOW - timedelta(days=30), now=_NOW)
    assert out is windowed


def test_unknown_timeframe_is_left_alone() -> None:
    # No seconds mapping => fail-safe, leave the window untouched (never guess).
    window = ChartWindow(timeframe="3h", limit=168, from_ts=None)
    out = extend_window_to_cover_signal(window, _NOW - timedelta(days=30), now=_NOW)
    assert out is window


def test_buffer_keeps_oldest_marker_off_the_left_edge() -> None:
    window = _legacy_window("1h")  # 168 candles = 7 days back
    earliest = _NOW - timedelta(days=10)
    out = extend_window_to_cover_signal(window, earliest, now=_NOW)
    secs = TIMEFRAME_SECONDS["1h"]
    bare = math.ceil((10 * 86400) / secs)
    assert out.limit == bare + BACKFILL_CANDLE_BUFFER  # buffer applied


def test_tz_naive_earliest_signal_is_normalized_not_raised() -> None:
    # Public pure helper: a naive earliest_signal_ts must be treated as UTC
    # rather than raising on the (now - earliest) subtraction.
    window = _legacy_window("5m")
    naive = (_NOW - timedelta(days=5)).replace(tzinfo=None)
    out = extend_window_to_cover_signal(window, naive, now=_NOW)
    # Equivalent to the tz-aware 5-day case: window grows, naive input is
    # normalized to UTC-aware on the threaded from_ts (no raise on subtraction).
    assert out.limit > window.limit
    assert out.from_ts == naive.replace(tzinfo=UTC)


# ----------------------------------------------------------------------
# _earliest_signal_ts
# ----------------------------------------------------------------------


def _sig(times: list[str]) -> pd.DataFrame:
    return pd.DataFrame({"time": pd.to_datetime(times, utc=True), "price": [1.0] * len(times)})


def test_earliest_signal_ts_picks_min_across_buy_and_sell() -> None:
    buys = _sig(["2026-06-10T00:00:00Z", "2026-06-12T00:00:00Z"])
    sells = _sig(["2026-06-09T00:00:00Z"])  # earliest overall
    out = _earliest_signal_ts(buys, sells)
    assert out == datetime(2026, 6, 9, tzinfo=UTC)
    assert isinstance(out, datetime) and not isinstance(out, pd.Timestamp)


def test_earliest_signal_ts_empty_frames_returns_none() -> None:
    empty = pd.DataFrame(columns=["time", "price"])
    assert _earliest_signal_ts(empty, empty) is None


def test_earliest_signal_ts_handles_only_one_side() -> None:
    empty = pd.DataFrame(columns=["time", "price"])
    sells = _sig(["2026-06-09T00:00:00Z"])
    assert _earliest_signal_ts(empty, sells) == datetime(2026, 6, 9, tzinfo=UTC)


def test_earliest_signal_ts_handles_mixed_timezones() -> None:
    # One frame tz-naive, the other tz-aware: the cross-frame min must not raise
    # (each frame's minimum is normalized to UTC before comparison). This is the
    # realistic mixed-tz shape — a single column stays internally uniform.
    buys = pd.DataFrame(  # tz-naive column
        {"time": pd.to_datetime(["2026-06-10T00:00:00", "2026-06-12T00:00:00"]), "price": [1.0, 1.0]}
    )
    sells = _sig(["2026-06-09T00:00:00Z"])  # earliest overall, tz-aware
    out = _earliest_signal_ts(buys, sells)
    assert out == datetime(2026, 6, 9, tzinfo=UTC)


# ----------------------------------------------------------------------
# Integration through prepare_ta_session_state
# ----------------------------------------------------------------------


def _ohlcv_payload(prices: list[float], start: str = "2026-05-12T00:00:00Z") -> list[dict[str, Any]]:
    times = pd.date_range(start, periods=len(prices), freq="5min", tz="UTC")
    return [
        {"timestamp": t.isoformat(), "open": str(p), "high": str(p), "low": str(p), "close": str(p), "volume": "1"}
        for t, p in zip(times, prices, strict=True)
    ]


class _RecordingClient:
    """Records the get_ohlcv kwargs; serves a configurable trade tape."""

    def __init__(self, tape_rows: list[dict[str, Any]] | None = None) -> None:
        self.ohlcv_kwargs: dict[str, Any] | None = None
        self.tape_call_count = 0
        self.tape_from_ts: Any = None
        self._tape_rows = tape_rows or []
        self._ohlcv = _ohlcv_payload([2300.0 + i for i in range(60)])

    def get_ohlcv(self, **kwargs: Any) -> list[dict[str, Any]]:
        self.ohlcv_kwargs = kwargs
        return self._ohlcv

    def get_trade_tape(self, from_ts: Any = None, **_: Any) -> dict[str, Any]:
        self.tape_call_count += 1
        self.tape_from_ts = from_ts
        return {"rows": self._tape_rows, "has_more": False}

    def get_timeline(self, **_: Any) -> list[dict[str, Any]]:
        return []


def _config():
    cfg = get_rsi_config(period=14, timeframe="5m")
    cfg.base_token, cfg.quote_token, cfg.chain = "WETH", "USDC", "arbitrum"
    return cfg


def _swap_row(ts: str) -> dict[str, Any]:
    return {
        "timestamp": ts,
        "intent_type": "SWAP",
        "token_in": "USDC",
        "amount_in": "3",
        "token_out": "WETH",
        "amount_out": "0.0015",
        "effective_price": "0.0005",
    }


def test_prepare_extends_limit_when_tape_predates_window() -> None:
    # A buy ~5 days old vs a 5m recent window (~2.5 days) => limit must grow.
    old_ts = (datetime.now(UTC) - timedelta(days=5)).isoformat()
    client = _RecordingClient(tape_rows=[_swap_row(old_ts)])

    prepare_ta_session_state(client, session_state={}, config=_config())

    assert client.ohlcv_kwargs is not None
    base = ohlcv_limit_for_timeframe("5m")
    assert client.ohlcv_kwargs["limit"] > base, "old marker must extend the candle window"
    # Tape fetched exactly once (the backfill fetch is reused for markers).
    assert client.tape_call_count == 1
    # Legacy newest-N marker contract (VIB-5114): the backfill fetch is the
    # page-capped newest-N page (from_ts=None), not a windowed fetch.
    assert client.tape_from_ts is None


def test_prepare_keeps_legacy_limit_when_no_older_signal() -> None:
    # A recent buy (1h ago) sits inside the 5m window => byte-for-byte legacy.
    recent_ts = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
    client = _RecordingClient(tape_rows=[_swap_row(recent_ts)])

    prepare_ta_session_state(client, session_state={}, config=_config())

    assert client.ohlcv_kwargs is not None
    assert client.ohlcv_kwargs["limit"] == ohlcv_limit_for_timeframe("5m") == 720


def test_prepare_keeps_legacy_limit_when_tape_empty() -> None:
    client = _RecordingClient(tape_rows=[])
    prepare_ta_session_state(client, session_state={}, config=_config())
    assert client.ohlcv_kwargs is not None
    assert client.ohlcv_kwargs["limit"] == 720


def test_prepare_does_not_extend_for_caller_supplied_signals() -> None:
    # A custom dashboard that already supplied markers must not trigger a tape
    # fetch or any window growth — the legacy window is used verbatim.
    old_ts = (datetime.now(UTC) - timedelta(days=5)).isoformat()
    client = _RecordingClient(tape_rows=[_swap_row(old_ts)])
    state = {
        "buy_signals": pd.DataFrame([{"time": pd.Timestamp("2020-01-01", tz="UTC"), "price": 1.0}]),
        "sell_signals": pd.DataFrame(columns=["time", "price"]),
        "total_trades": 1,
    }

    prepare_ta_session_state(client, session_state=state, config=_config())

    assert client.ohlcv_kwargs is not None
    assert client.ohlcv_kwargs["limit"] == 720
    assert client.tape_call_count == 0, "no tape fetch when signals are caller-supplied"
