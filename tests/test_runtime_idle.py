"""Idle coordinator cadence and worker timeout retries in the run loop."""

import json
from datetime import timedelta

import pytest

from almanak import runtime as runtime_module
from almanak.lending import Decision, Memory
from almanak.runtime import (
    IDLE_POLL_SECONDS,
    TIMEOUT_BACKOFF_CAP_SECONDS,
    WORKER_TIMEOUT_HORIZON_SECONDS,
    next_delay,
    wait,
)
from almanak.triggers import TeardownEvent
from almanak.worker_process import WorkerStartupTimeout, WorkerWallTimeout
from tests.test_triggers import START, scheduled  # noqa: F401


class Stop(Exception):
    pass


@pytest.fixture
def loop(scheduled, monkeypatch):  # noqa: F811
    """The scheduled coordinator with a sleep that advances its clock and a load counter."""
    s = scheduled
    s.gateway.close = lambda: None
    s.sleeps = []
    s.loads = 0
    s.ticks = []

    def sleep(seconds):
        s.sleeps.append(seconds)
        s.clock.advance(seconds)

    original_load = s.journal.load

    def load():
        s.loads += 1
        return original_load()

    original_tick = s.runner.tick

    def tick():
        s.ticks.append(((s.clock() - START).total_seconds(), s.loads))
        return original_tick()

    monkeypatch.setattr(runtime_module.time, "sleep", sleep)
    # The timeout horizon reads the monotonic clock; here it is the coordinator's clock.
    monkeypatch.setattr(
        runtime_module.time, "monotonic", lambda: (s.clock() - START).total_seconds()
    )
    monkeypatch.setattr(s.journal, "load", load)
    monkeypatch.setattr(s.runner, "tick", tick)
    return s


def stop_at(s, seconds):
    def interrupt():
        if s.clock() - START >= timedelta(seconds=seconds):
            raise Stop

    return interrupt


def test_idle_coordinator_reads_once_per_wait_and_wakes_when_due(loop, tmp_path):
    s = loop
    s.runner.export = tmp_path / "dashboard"
    s.runner.tick()
    assert s.journal.load().schedule.next_due_at == START + timedelta(seconds=5)
    s.ticks.clear()
    with pytest.raises(Stop):
        s.runner.run(interrupt=stop_at(s, 12))
    # Idle ticks wait for the deadline instead of polling every second, and an evaluation
    # is followed by the ordinary one-second interval.
    assert [at for at, _ in s.ticks] == [0, 5, 6, 10, 11]
    assert all(seconds <= 1 for seconds in s.sleeps)
    assert len(s.calls.decisions) == 3
    loads = [after - before for (_, before), (_, after) in zip(s.ticks, s.ticks[1:], strict=False)]
    # An idle iteration reads the checkpoint once: its dashboard export reuses that read.
    assert loads[0] == loads[2] == 1
    # Five iterations export; only the three that recorded a decision change the file.
    assert json.loads((s.runner.export / "input.json").read_text())["revision"] == 3


def test_idle_wait_is_bounded_by_the_poll_interval(loop):
    s = loop
    s.runner.tick()
    s.runner.tick()
    state, due = s.runner.idle
    assert state == s.journal.load() and due == START + timedelta(seconds=5)
    s.runner.idle = (state, s.clock() + timedelta(seconds=300))
    assert next_delay(s.runner, 1, 0) == IDLE_POLL_SECONDS
    s.runner.idle = (state, s.clock() + timedelta(seconds=2.5))
    assert next_delay(s.runner, 1, 0) == 2.5
    s.runner.idle = (state, s.clock() - timedelta(seconds=1))
    assert next_delay(s.runner, 1, 0) == 0
    s.runner.idle = None
    assert next_delay(s.runner, 1, 0) == 1
    assert next_delay(s.runner, 1, 2) == 2 * IDLE_POLL_SECONDS


def test_interrupt_is_honored_between_one_second_slices(monkeypatch):
    sleeps = []
    monkeypatch.setattr(runtime_module.time, "sleep", sleeps.append)

    def interrupt():
        if len(sleeps) == 2:
            raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        wait(IDLE_POLL_SECONDS, interrupt)
    assert sleeps == [1, 1]
    sleeps.clear()
    wait(2.5, lambda: None)
    assert sleeps == [1, 1, 0.5]


def timing_out(s, monkeypatch, failures, error=WorkerWallTimeout, before=None):
    """Time out the next `failures` evaluations, calling `before(attempt)` ahead of each."""
    attempts = []

    def decide(_definition, context):
        attempts.append(context)
        if before is not None:
            before(len(attempts))
        if len(attempts) <= failures:
            raise error(f"strategy worker exceeded {error.__name__}")
        s.calls.decisions.append(context)
        return Decision(memory=Memory(values={"evaluations": len(s.calls.decisions)}))

    monkeypatch.setattr("almanak.runtime.strategy_decision", decide)
    return attempts


def events(capsys, name="strategy_worker_timeout"):
    lines = capsys.readouterr().err.splitlines()
    return [json.loads(line) for line in lines if f'"event": "{name}"' in line]


def test_timed_out_decision_is_evaluated_again_for_the_same_deadline(loop, monkeypatch, capsys):
    s = loop
    s.runner.tick()
    s.clock.advance(5)
    timing_out(s, monkeypatch, 2)
    with pytest.raises(Stop):
        s.runner.run(interrupt=stop_at(s, 21))
    evaluation = s.journal.load().schedule.last_evaluation
    # Two timeouts back off for one and then two poll intervals; the retry keeps the
    # original deadline and records when it actually started.
    assert evaluation.due_at == START + timedelta(seconds=5)
    assert evaluation.started_at == START + timedelta(seconds=5 + 5 + 10)
    assert s.journal.load().memory.values == {"evaluations": 2}
    err = capsys.readouterr().err.splitlines()
    timeouts = [json.loads(line) for line in err if "strategy_worker_timeout" in line]
    assert timeouts == [
        {
            "event": "strategy_worker_timeout",
            "status": "degraded",
            "kind": "wall",
            "mode": "running",
            "attempt": n,
            "elapsed_seconds": elapsed,
            "retry_in_seconds": retry,
            "stops_after_seconds": WORKER_TIMEOUT_HORIZON_SECONDS,
        }
        for n, elapsed, retry in ((1, 0, 5), (2, 5, 10))
    ]
    recovered = [json.loads(line) for line in err if "strategy_worker_recovered" in line]
    assert recovered == [
        {"event": "strategy_worker_recovered", "attempts": 2, "elapsed_seconds": 15}
    ]


def test_back_off_doubles_to_its_cap():
    assert [next_delay(None, 1, n) for n in range(1, 8)] == [5, 10, 20, 40, 60, 60, 60]
    assert TIMEOUT_BACKOFF_CAP_SECONDS == 60


def test_startup_timeouts_are_retried_like_wall_timeouts(loop, monkeypatch, capsys):
    s = loop
    s.runner.tick()
    s.clock.advance(5)
    timing_out(s, monkeypatch, 1, error=WorkerStartupTimeout)
    with pytest.raises(Stop):
        s.runner.run(interrupt=stop_at(s, 11))
    assert s.journal.load().memory.values == {"evaluations": 2}
    [event] = events(capsys)
    assert event["kind"] == "startup" and event["status"] == "degraded"


def test_timeouts_stop_a_running_deployment_only_after_the_horizon(loop, monkeypatch, capsys):
    s = loop
    s.runner.tick()
    s.clock.advance(5)
    before = s.journal.load()
    attempts = timing_out(s, monkeypatch, 10_000)
    with pytest.raises(WorkerWallTimeout, match="wall-time limit|timed out") as failure:
        s.runner.run()
    assert "seconds" in str(failure.value) and isinstance(
        failure.value.__cause__, WorkerWallTimeout
    )
    assert s.journal.load() == before
    assert s.runner.closed
    logged = events(capsys)
    *degraded, stopped = logged
    assert len(logged) == len(attempts)
    assert all(e["status"] == "degraded" for e in degraded)
    assert degraded[-1]["elapsed_seconds"] < WORKER_TIMEOUT_HORIZON_SECONDS
    assert stopped["status"] == "stopped" and stopped["retry_in_seconds"] is None
    assert stopped["elapsed_seconds"] >= WORKER_TIMEOUT_HORIZON_SECONDS
    # Retries never wait longer than the cap, so the coordinator keeps evaluating.
    assert max(e["retry_in_seconds"] for e in degraded) == TIMEOUT_BACKOFF_CAP_SECONDS


def test_ticks_that_evaluate_nothing_do_not_end_a_streak(loop, monkeypatch, capsys):
    """A multi-chain tick can skip a chain that is not due without starting a worker; such
    a tick must neither reset the no-success horizon nor log a recovery."""
    s = loop
    s.runner.tick()
    s.clock.advance(5)
    attempts = timing_out(s, monkeypatch, 10_000)
    evaluating = s.runner.tick
    calls = []

    def alternating():
        calls.append(None)
        return False if len(calls) % 2 == 0 else evaluating()

    monkeypatch.setattr(s.runner, "tick", alternating)
    with pytest.raises(WorkerWallTimeout, match="timed out"):
        s.runner.run()
    err = capsys.readouterr().err
    assert "strategy_worker_recovered" not in err
    logged = [json.loads(line) for line in err.splitlines() if "strategy_worker_timeout" in line]
    assert len(calls) == 2 * len(attempts) - 1
    assert [e["attempt"] for e in logged] == list(range(1, len(attempts) + 1))
    assert logged[-1]["status"] == "stopped"
    assert logged[-1]["elapsed_seconds"] >= WORKER_TIMEOUT_HORIZON_SECONDS


def test_market_refusal_replans_neither_end_nor_hide_a_timeout_streak(loop, monkeypatch, capsys):
    """A re-plan after a market refusal (#146) starts no worker, so it leaves the streak;
    a timeout in the same cycle still counts, and its back-off wins over the re-plan's."""
    from almanak.gateway.ethereum import DataError

    s = loop
    s.runner.tick()
    s.clock.advance(5)
    timing_out(s, monkeypatch, 2)
    evaluating = s.runner.tick
    calls = []

    def refusing_then_evaluating():
        calls.append(None)
        if len(calls) == 2:
            s.runner.replanning().delay = 0.0
            s.runner.count_replan(DataError("simulation_reverted", "pool moved"), "prepare")
            return False
        return evaluating()

    monkeypatch.setattr(s.runner, "tick", refusing_then_evaluating)
    with pytest.raises(Stop):
        s.runner.run(interrupt=stop_at(s, 60))
    err = capsys.readouterr().err
    timeouts = [json.loads(line) for line in err.splitlines() if "strategy_worker_timeout" in line]
    recovered = [json.loads(line) for line in err.splitlines() if "worker_recovered" in line]
    assert [e["attempt"] for e in timeouts] == [1, 2]
    # The second timeout follows the refusal tick and still backs off as attempt 2.
    assert timeouts[1]["retry_in_seconds"] == 10
    assert [r["attempts"] for r in recovered] == [2]


def test_teardown_requested_while_backing_off_is_evaluated(loop, monkeypatch, capsys):
    s = loop
    s.runner.tick()
    s.clock.advance(5)

    def request_teardown(attempt):
        if attempt == 3:
            s.journal.teardown()

    attempts = timing_out(s, monkeypatch, 3, before=request_teardown)
    final = s.runner.run()
    assert final.mode == "closed"
    assert isinstance(attempts[-1].trigger, TeardownEvent)
    assert [e["mode"] for e in events(capsys)] == ["running", "running", "teardown"]


def test_timeouts_never_stop_a_deployment_in_teardown(loop, monkeypatch, capsys):
    s = loop
    s.runner.tick()
    s.journal.teardown()
    # Twice the horizon of consecutive timeouts, then the host recovers and teardown closes.
    failures = 2 * WORKER_TIMEOUT_HORIZON_SECONDS // TIMEOUT_BACKOFF_CAP_SECONDS
    timing_out(s, monkeypatch, failures)
    assert s.runner.run().mode == "closed"
    logged = events(capsys)
    assert len(logged) == failures
    assert logged[-1]["elapsed_seconds"] > WORKER_TIMEOUT_HORIZON_SECONDS
    assert {(e["mode"], e["status"], e["stops_after_seconds"]) for e in logged} == {
        ("teardown", "degraded", None)
    }


def test_other_worker_failures_stop_the_coordinator_at_once(loop, monkeypatch):
    s = loop
    s.runner.tick()
    s.clock.advance(5)

    def fail(_definition, _context):
        raise RuntimeError("strategy worker failed: ")

    monkeypatch.setattr("almanak.runtime.strategy_decision", fail)
    with pytest.raises(RuntimeError, match="strategy worker failed"):
        s.runner.run()
    assert s.sleeps == []
