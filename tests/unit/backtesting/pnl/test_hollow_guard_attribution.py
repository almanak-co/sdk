"""Hollow-guard attribution accuracy.

Three misattributions fixed and pinned (each observed on real staging runs):
1. Warm-up-only input gaps (indicator windows filling) no longer read as
   "the strategy held because inputs were missing" — one run had RSI missing
   on 14/2161 ticks and the guard blamed a data outage.
2. A rejections-only run is hollow too: rejected TradeRecords in
   ``portfolio.trades`` used to suppress the guard (one run had 130
   rejections and 0 executed fills).
3. A run that traded but persistently starved one input gets its own
   PARTIALLY STARVED warning — a dead strategy leg can hide behind a busy
   one (a delta-neutral run traded its lending leg while its LP leg starved).
"""

from almanak.framework.backtesting.pnl._engine_helpers import _failure_pattern


def _entry(ticks: int, first_tick: int, last_tick: int) -> dict:
    return {"ticks": ticks, "detail": "x", "first_tick": first_tick, "last_tick": last_tick}


class TestFailurePattern:
    def test_warm_up_shape(self):
        # 14 failing ticks at the start of a 2161-tick run (indicator warm-up).
        assert _failure_pattern(_entry(14, 1, 14), 2161) == "warm_up"

    def test_persistent_shape(self):
        # Every tick failed (an unserved input).
        assert _failure_pattern(_entry(2161, 1, 2161), 2161) == "persistent"

    def test_persistent_wins_over_warm_up_in_short_runs(self):
        # 6/6 ticks failed: the whole run — persistent even though the run is
        # shorter than the warm-up horizon.
        assert _failure_pattern(_entry(6, 1, 6), 6) == "persistent"

    def test_intermittent_shape(self):
        # Failures across the run but nowhere near every tick.
        assert _failure_pattern(_entry(200, 1, 2000), 2161) == "intermittent"

    def test_late_burst_is_not_warm_up(self):
        # A 14-tick failure burst at the END of the run is not warm-up.
        assert _failure_pattern(_entry(14, 2100, 2113), 2161) == "intermittent"

    def test_warm_up_horizon_floor_is_60_ticks(self):
        # Short warm-ups in long runs: 60-tick floor covers e.g. RSI(14) on
        # 4h candles under hourly ticks (~56 ticks).
        assert _failure_pattern(_entry(56, 1, 56), 2161) == "warm_up"

    def test_no_tick_info_is_intermittent(self):
        assert _failure_pattern({"ticks": 5, "detail": "x"}, 2161) == "intermittent"
