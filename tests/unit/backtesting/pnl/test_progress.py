import asyncio
from unittest.mock import Mock

import pytest

from almanak.framework.backtesting.pnl.progress import progress_scope, report_progress


def test_observation_is_optional_and_exceptions_do_not_escape():
    report_progress("simulating", 1, 10)
    observer = Mock(side_effect=RuntimeError("offline"))
    with progress_scope(observer):
        report_progress("simulating", 1, 10)
    observer.assert_called_once()


def test_simulation_clock_excludes_loading_and_scope_is_restored(monkeypatch):
    now = [10.0]
    monkeypatch.setattr("almanak.framework.backtesting.pnl.progress.time.monotonic", lambda: now[0])
    events = []
    nested = []
    with progress_scope(events.append):
        report_progress("loading_data")
        now[0] = 100.0
        report_progress("simulating", 0, 10)
        now[0] = 102.0
        with progress_scope(nested.append):
            report_progress("preparing")
        report_progress("simulating", 1, 10)
    report_progress("saving_results")
    assert [e.simulation_elapsed_ms for e in events] == [0, 0, 2000]
    assert len(nested) == 1
    assert events[-1].completed_ticks == 1


def test_concurrent_scopes_do_not_mix_runs():
    async def run(phase):
        events = []
        with progress_scope(events.append):
            await asyncio.sleep(0)
            report_progress(phase)
        return events

    async def both():
        return await asyncio.gather(run("preparing"), run("loading_data"))

    a, b = asyncio.run(both())
    assert [e.phase for e in a] == ["preparing"]
    assert [e.phase for e in b] == ["loading_data"]


def test_final_simulation_metrics_survive_calculation_and_saving(monkeypatch):
    now = [10.0]
    monkeypatch.setattr("almanak.framework.backtesting.pnl.progress.time.monotonic", lambda: now[0])
    events = []
    with progress_scope(events.append):
        report_progress("loading_data")
        now[0] = 100.0
        report_progress("simulating", 0, 4)
        now[0] = 102.0
        report_progress("simulating", 4, 4)
        # Final queued intents still belong to simulation.
        now[0] = 103.0
        report_progress("calculating_results")
        now[0] = 150.0
        report_progress("calculating_results")
        now[0] = 200.0
        report_progress("saving_results")
    assert [event.phase for event in events[-3:]] == ["calculating_results", "calculating_results", "saving_results"]
    assert [(event.completed_ticks, event.total_ticks, event.simulation_elapsed_ms) for event in events[-3:]] == [
        (4, 4, 3000),
        (4, 4, 3000),
        (4, 4, 3000),
    ]
    assert events[2].simulation_elapsed_ms == 2000
    # A fresh run cannot inherit the prior run's completed counters or clock.
    with progress_scope(events.append):
        report_progress("saving_results")
    assert (events[-1].completed_ticks, events[-1].total_ticks, events[-1].simulation_elapsed_ms) == (0, 0, 0)


def test_simulation_updates_preserve_known_total_when_omitted():
    events = []
    with progress_scope(events.append):
        report_progress("simulating", total_ticks=4)
        report_progress("simulating", completed_ticks=1)
        report_progress("simulating", completed_ticks=5)
    assert [(event.completed_ticks, event.total_ticks) for event in events] == [(0, 4), (1, 4), (5, 5)]


def test_loading_batches_reset_timing_and_clear_after_failure(monkeypatch):
    from almanak.framework.backtesting.pnl.progress import loading_batches

    clock = [10.0]
    monkeypatch.setattr("almanak.framework.backtesting.pnl.progress.time.monotonic", lambda: clock[0])
    seen = []
    with progress_scope(seen.append):
        report_progress("loading_data")
        with loading_batches("pool_state", 3) as advance:
            clock[0] = 32.0
            advance()
            assert seen[-1].loading.completed_batches == 1
            assert seen[-1].loading.elapsed_ms == 22000
        assert seen[-1].loading is None
        with pytest.raises(ValueError), loading_batches("prices", 2):
            assert seen[-1].loading.elapsed_ms == 0
            raise ValueError("provider failure")
        assert seen[-1].loading is None
        report_progress("simulating", 1, 10)
        count = len(seen)
        with loading_batches("pool_state", 3) as advance:
            advance()
        assert len(seen) == count


@pytest.mark.asyncio
async def test_loading_observer_crosses_gateway_executor_and_isolates_runs():
    import asyncio

    from almanak.framework.backtesting.pnl.progress import loading_batches
    from almanak.framework.backtesting.pnl.providers.perp._gateway_history import run_sync_gateway_call

    async def observe(total):
        seen = []

        def fetch():
            with loading_batches("pool_state", total) as advance:
                advance()

        with progress_scope(seen.append):
            report_progress("loading_data")
            await run_sync_gateway_call(fetch)
        return [p.loading.total_batches for p in seen if p.loading]

    assert await asyncio.gather(observe(2), observe(7)) == [[2, 2], [7, 7]]


def test_loading_observer_failure_does_not_break_fetch():
    from almanak.framework.backtesting.pnl.progress import loading_batches

    calls = 0

    def broken(_):
        nonlocal calls
        calls += 1
        raise RuntimeError("observer")

    with progress_scope(broken):
        report_progress("loading_data")
        with loading_batches("prices", 2) as advance:
            advance()
    assert calls == 4
