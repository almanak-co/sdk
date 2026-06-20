"""The dashboard "Start" line must never plot to the right of the first trade.

A buy/sell marker rendering BEFORE the dashed "Start" line is impossible — a
strategy cannot trade before it deployed. It was observed live (20260618 TA
report): the green buy triangle sat just left of "Start". Root cause:
``_strategy_start_time`` reads ``STRATEGY_STARTED`` via ``get_timeline(limit=200)``,
which (a) truncates so the original start ages out of the window on long runs,
and (b) is re-emitted on a dashboard relaunch — both making "Start" land AFTER
real trades. ``_anchored_start_time`` clamps the line to the earliest plotted
marker; these tests pin that contract.
"""

from __future__ import annotations

import pandas as pd
import pytest

from almanak.framework.dashboard.templates.ta_dashboard import _anchored_start_time


def _signals(*times: str) -> pd.DataFrame:
    return pd.DataFrame(
        {"time": pd.to_datetime(list(times), utc=True), "price": [1700.0] * len(times)}
    )


def test_clamps_start_back_to_first_trade_when_reported_start_is_too_late():
    # The bug: reported start is AFTER the buy (truncated timeline / relaunch).
    buy = _signals("2026-06-18T04:45:43Z")
    sell = pd.DataFrame()
    late_start = "2026-06-18T08:00:00Z"  # 3h after the buy — impossible
    out = _anchored_start_time(late_start, buy, sell)
    assert out == pd.Timestamp("2026-06-18T04:45:43Z")  # clamped to the trade


def test_correct_earlier_start_is_preserved():
    # When the reported start is already before the first trade, keep it exactly.
    buy = _signals("2026-06-18T04:45:43Z")
    good_start = "2026-06-18T04:45:10Z"  # 33s before the buy — correct
    out = _anchored_start_time(good_start, buy, pd.DataFrame())
    assert out == pd.Timestamp("2026-06-18T04:45:10Z")


def test_no_signals_leaves_start_unchanged():
    out = _anchored_start_time("2026-06-18T08:00:00Z", pd.DataFrame(), pd.DataFrame())
    assert out == pd.Timestamp("2026-06-18T08:00:00Z")


def test_sell_marker_also_clamps():
    sell = _signals("2026-06-18T05:00:00Z")
    out = _anchored_start_time("2026-06-18T09:00:00Z", pd.DataFrame(), sell)
    assert out == pd.Timestamp("2026-06-18T05:00:00Z")


def test_earliest_across_both_frames_wins():
    buy = _signals("2026-06-18T06:00:00Z")
    sell = _signals("2026-06-18T04:30:00Z")  # earlier than the buy
    out = _anchored_start_time("2026-06-18T10:00:00Z", buy, sell)
    assert out == pd.Timestamp("2026-06-18T04:30:00Z")


def test_unparseable_start_falls_back_to_first_signal():
    buy = _signals("2026-06-18T04:45:43Z")
    out = _anchored_start_time("not-a-date", buy, pd.DataFrame())
    assert out == pd.Timestamp("2026-06-18T04:45:43Z")


def test_none_start_and_no_signals_returns_none():
    assert _anchored_start_time(None, pd.DataFrame(), pd.DataFrame()) is None


def test_none_start_with_valid_trade_anchors_at_trade():
    # Contract: the line is absent ONLY when there is neither a start time nor a
    # trade. A missing/None reported start WITH a real trade must still draw the
    # line — anchored at that first trade — never return None and drop the line.
    buy = _signals("2026-06-18T03:48:00Z")
    assert _anchored_start_time(None, buy, pd.DataFrame()) == pd.Timestamp("2026-06-18T03:48:00Z")
    sell = _signals("2026-06-18T05:00:00Z")
    assert _anchored_start_time(None, pd.DataFrame(), sell) == pd.Timestamp("2026-06-18T05:00:00Z")


def test_tz_naive_reported_start_is_handled():
    # tz-naive start strings must not raise and must still clamp correctly.
    buy = _signals("2026-06-18T04:45:43Z")
    out = _anchored_start_time("2026-06-18 08:00:00", buy, pd.DataFrame())
    assert out == pd.Timestamp("2026-06-18T04:45:43Z")


# ----------------------------------------------------------------------
# Malformed SIGNAL frames (the clamping dimension). A junk/missing/empty signal
# time must never (a) raise and take down the render, nor (b) be misparsed into a
# bogus marker time that drags "Start" away from the real first trade. These pin
# the contract Codex's Phase-1 critique flagged as untested (VIB-5287).
# ----------------------------------------------------------------------


def test_unparseable_signal_time_does_not_raise_and_falls_back():
    # A non-datetime "time" value (custom dashboard / future caller) must coerce
    # to NaT and be dropped — not raise DateParseError mid-render. With no usable
    # marker to clamp to, the reported start is kept as-is.
    bad = pd.DataFrame({"time": ["not-a-date"], "price": [1700.0]})
    out = _anchored_start_time("2026-06-18T08:00:00Z", bad, pd.DataFrame())
    assert out == pd.Timestamp("2026-06-18T08:00:00Z")


def test_missing_time_column_in_signal_frame_falls_back():
    # No "time" column => no plottable marker => nothing to clamp to.
    no_time = pd.DataFrame({"price": [1700.0]})
    out = _anchored_start_time("2026-06-18T08:00:00Z", no_time, pd.DataFrame())
    assert out == pd.Timestamp("2026-06-18T08:00:00Z")


def test_all_nat_signal_times_fall_back():
    # All-NaT "time" column: every value drops, no marker time exists.
    all_nat = pd.DataFrame({"time": [pd.NaT, pd.NaT], "price": [1700.0, 1700.0]})
    out = _anchored_start_time("2026-06-18T08:00:00Z", all_nat, pd.DataFrame())
    assert out == pd.Timestamp("2026-06-18T08:00:00Z")


@pytest.mark.filterwarnings("ignore:Could not infer format")
def test_partially_malformed_signal_times_clamp_to_valid_minimum():
    # A frame mixing a junk value and a real trade time must still clamp to the
    # real one (junk coerced to NaT and dropped, the valid 03:48 survives).
    # The mixed object-dtype column makes pandas parse element-wise (benign
    # "Could not infer format" warning) — that element-wise coerce is exactly the
    # behaviour under test, so the warning is expected and filtered.
    mixed = pd.DataFrame(
        {"time": ["not-a-date", "2026-06-18T03:48:00Z"], "price": [1700.0, 1700.0]}
    )
    out = _anchored_start_time("2026-06-18T14:51:00Z", mixed, pd.DataFrame())
    assert out == pd.Timestamp("2026-06-18T03:48:00Z")


def test_tz_naive_signal_frame_clamps_without_raising():
    # tz-naive marker column vs tz-aware reported start: must normalize and clamp,
    # never raise on the tz-aware/naive comparison.
    naive = pd.DataFrame(
        {"time": pd.to_datetime(["2026-06-18T03:48:00"]), "price": [1700.0]}
    )
    out = _anchored_start_time("2026-06-18T14:51:00Z", naive, pd.DataFrame())
    assert out == pd.Timestamp("2026-06-18T03:48:00Z")


def test_tz_mixed_across_buy_and_sell_frames_clamps_to_earliest():
    # buy frame tz-naive, sell frame tz-aware: earliest across both wins, no raise.
    naive_buy = pd.DataFrame(
        {"time": pd.to_datetime(["2026-06-18T05:00:00"]), "price": [1700.0]}
    )
    aware_sell = _signals("2026-06-18T03:48:00Z")
    out = _anchored_start_time("2026-06-18T14:51:00Z", naive_buy, aware_sell)
    assert out == pd.Timestamp("2026-06-18T03:48:00Z")


# ----------------------------------------------------------------------
# Call-site contract: the render path must consult _anchored_start_time
# UNCONDITIONALLY. A correct helper is useless if the caller guards it out — this
# is the exact gap (an `if strategy_start_time is not None:` wrapper) that would
# drop the Start line whenever the timeline read came back empty but trades exist.
# ----------------------------------------------------------------------


class _StStub:
    """Minimal streamlit stub: every call is a no-op except plotly_chart, whose
    figure we capture. The single-indicator RSI render path only uses
    subheader/info/warning/plotly_chart (no context managers / column unpacking),
    so this is sufficient to drive it."""

    def __init__(self) -> None:
        self.figs: list = []

    def plotly_chart(self, fig, *args, **kwargs):  # noqa: ANN001
        self.figs.append(fig)

    def __getattr__(self, _name):
        return lambda *a, **k: None


def _drive_charts(monkeypatch, *, reported_start, buy_time="2026-06-18T03:48:00Z"):
    import almanak.framework.dashboard.templates.ta_dashboard as tad

    stub = _StStub()
    monkeypatch.setattr(tad, "st", stub)

    times = pd.date_range("2026-06-18T00:00:00Z", periods=12, freq="h")
    session_state = {
        "price_history": [[t, 1700.0 + i] for i, t in enumerate(times)],
        "buy_signals": [[buy_time, 1703.0]],
        "sell_signals": None,
        "rsi_data": [(t, 50.0) for t in times],
        "strategy_start_time": reported_start,
    }
    config = tad.TADashboardConfig(indicator_name="RSI")
    tad._render_charts_section(session_state, {}, config, period=14)

    assert stub.figs, "no figure was rendered"
    fig = stub.figs[-1]
    start = [tr for tr in fig.data if getattr(tr, "name", None) == "Start"]
    return start


def test_call_site_draws_start_line_when_reported_start_is_none(monkeypatch):
    # VIB-5287 core regression: reported start is None (empty timeline read) but a
    # trade exists -> the Start line must STILL be drawn, anchored at the trade.
    start = _drive_charts(monkeypatch, reported_start=None)
    assert start, "Start line was dropped when reported start was None but a trade exists"
    assert pd.Timestamp(start[0].x[0]) == pd.Timestamp("2026-06-18T03:48:00Z")


def test_call_site_clamps_start_line_when_reported_start_is_too_late(monkeypatch):
    # A late reported start + an earlier trade -> the rendered Start trace sits at
    # the trade, proving the call site routes through the clamp (not the raw start).
    start = _drive_charts(monkeypatch, reported_start="2026-06-18T14:51:00Z")
    assert start, "Start line missing"
    assert pd.Timestamp(start[0].x[0]) == pd.Timestamp("2026-06-18T03:48:00Z")


# ----------------------------------------------------------------------
# Numeric (out-of-contract) signal times. pandas reads a raw int as an epoch-ns
# offset, so a direct helper call with an int "time" yields a 1970 timestamp
# (GIGO). The production RENDER path is nonetheless safe — these pin both facts so
# the card's "never a 1970 mis-clamp" claim is testable, not aspirational.
# ----------------------------------------------------------------------


def test_numeric_signal_time_direct_call_is_out_of_contract_gigo():
    # Documented out-of-contract behaviour: an int in "time" is interpreted by
    # pandas as epoch-ns (-> 1970). This is GIGO and is NEVER produced by
    # _trade_rows_to_signals (which emits datetimes); the render-path test below
    # proves it cannot reach the screen. Pinned so the behaviour is intentional,
    # not an accident a future change could silently alter.
    buy = pd.DataFrame({"time": [0], "price": [1703.0]})
    out = _anchored_start_time("2026-06-18T14:51:00Z", buy, pd.DataFrame())
    assert out == pd.Timestamp("1970-01-01T00:00:00Z")


def test_numeric_signal_time_does_not_leak_1970_through_render(monkeypatch):
    # PRODUCTION-REACHABLE path: an int signal time coerces to a 1970 epoch, which
    # is then clipped out by _clip_signals_to_price_window (1970 is outside the
    # 2026 price window) -> no marker survives -> the Start line falls back to the
    # reported start. The "1970 mis-clamp" the card warns about cannot reach a
    # rendered figure.
    start = _drive_charts(monkeypatch, reported_start="2026-06-18T14:51:00Z", buy_time=0)
    assert start, "Start line missing"
    rendered = pd.Timestamp(start[0].x[0])
    assert rendered == pd.Timestamp("2026-06-18T14:51:00Z"), f"1970 leaked into the render: {rendered}"
    assert rendered.year != 1970
