"""External events use the shared journal and cannot be skipped by pending work."""

import subprocess
import sys

import pytest

from almanak.backtest.engine import Journal, Simulator, Transition
from almanak.backtest.types import UnixTime
from almanak.effects import AssetDelta, Effects
from almanak.strategy_api import Memory, StrategyDecision
from tests.test_backtest_core import OUTPUT, Empty, Receipt, State, Swap, SwapModel, initial


class Events(SwapModel):
    times = (100, 101, 102, 105, 110, 129, 130)

    def __init__(self):
        super().__init__()
        self.trace = []

    def next_event(self, state, until):
        cursor = state.event_cursor if state.event_cursor is not None else state.time - 1
        return next((UnixTime(t) for t in self.times if cursor < t <= until), None)

    def external_event(self, state, time):
        self.trace.append(("event", int(time)))
        return Transition[Empty, Receipt](
            effects=Effects[Empty](assets=(AssetDelta(key=OUTPUT, amount=1),)),
            result=Receipt(acquired=1),
        )

    def deliver_event(self, state, result):
        self.trace.append(("delivery", int(state.time)))
        return Memory(
            values={
                **state.memory.values,
                "events": state.memory.values.get("events", 0) + 1,
            }
        )

    def deliver(self, state, result):
        return Memory(values={**state.memory.values, "bought": result.acquired})

    def decide(self, state, observations, trigger):
        self.trace.append(("decision", int(state.time)))
        return super().decide(state, observations, trigger)

    def execute(self, state, pending):
        self.trace.append(("execute", int(pending.eligible_at)))
        return super().execute(state, pending)


def run(path, *, fault=None, model=None, state=None):
    journal = Journal(path, State)
    if not path.exists():
        journal.create(state or initial())
    return Simulator(journal, model or Events(), fault=fault).run()


def test_market_events_precede_execution_and_do_not_change_decision_cadence(tmp_path):
    model = Events()
    result = run(tmp_path / "state.sqlite", model=model)
    assert result.memory.values == {"bought": 6, "events": 7}
    assert result.portfolio.balance(OUTPUT) == 13
    assert len(result.completed) == result.delivered == 1
    assert len(result.events) == result.events_delivered == 7
    assert result.event_cursor == 130
    assert model.trace[:10] == [
        ("event", 100),
        ("delivery", 100),
        ("decision", 100),
        ("event", 101),
        ("delivery", 101),
        ("event", 102),
        ("delivery", 102),
        ("execute", 102),
        ("event", 105),
        ("delivery", 105),
    ]
    assert [time for stage, time in model.trace if stage == "decision"] == [100, 110, 120]
    assert [event.time for event in result.events] == list(model.times)


@pytest.mark.parametrize(
    "stage",
    [
        "event_effects",
        "event_commit",
        "event_delivery",
        "event_delivery_commit",
        "pending_commit",
        "effects_commit",
    ],
)
def test_event_interruption_reproduces_exact_state(tmp_path, stage):
    expected = run(tmp_path / "expected.sqlite")

    def interrupt(current):
        if current == stage:
            raise SystemExit("interrupted")

    with pytest.raises(SystemExit):
        run(tmp_path / "interrupted.sqlite", fault=interrupt)
    assert run(tmp_path / "interrupted.sqlite") == expected


@pytest.mark.parametrize("stage", ["event_effects", "event_commit", "event_delivery_commit"])
def test_real_process_death_does_not_duplicate_events(tmp_path, stage):
    path = tmp_path / "state.sqlite"
    code = """
import os, sys
from pathlib import Path
from tests.test_backtest_events import run

def interrupt(current):
    if current == sys.argv[2]:
        os._exit(74)
run(Path(sys.argv[1]), fault=interrupt)
"""
    process = subprocess.run(
        [sys.executable, "-c", code, str(path), stage],
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert process.returncode == 74, process.stderr
    expected = run(tmp_path / "expected.sqlite")
    assert run(path) == expected


def test_input_only_event_advances_cursor_without_a_notification(tmp_path):
    class Inputs(Events):
        def external_event(self, state, time):
            return None

    result = run(tmp_path / "state.sqlite", model=Inputs())
    assert result.events == () and result.event_cursor == 130
    assert result.portfolio.balance(OUTPUT) == 6


def test_pending_end_mark_processes_events_but_never_fills_after_end(tmp_path):
    state = initial(end=UnixTime(101))
    result = run(tmp_path / "state.sqlite", state=state)
    assert result.pending is not None and not result.completed
    assert result.portfolio.balance(OUTPUT) == 2
    assert result.events_delivered == 2 and result.time == 101


class Cleanup(Events):
    times = (132, 145)

    def decide(self, state, observations, trigger):
        if not trigger.kind == "teardown":
            return StrategyDecision[Swap](memory=state.memory)
        calls = state.memory.values.get("cleanup_calls", [])
        return StrategyDecision[Swap](
            memory=Memory(
                values={
                    **state.memory.values,
                    "cleanup_calls": [*calls, int(state.time)],
                }
            )
        )

    def closed(self, state, observations):
        return state.time >= 150


def test_cleanup_no_action_wait_and_risk_events_survive_interruption(tmp_path):
    state = initial(end_policy="teardown", cleanup_seconds=30)
    expected = run(tmp_path / "expected.sqlite", model=Cleanup(), state=state)
    path = tmp_path / "interrupted.sqlite"

    def interrupt(stage):
        retained = Journal(path, State).load()
        if stage == "decision_commit" and retained.wake_at is not None:
            raise SystemExit("lost acknowledgement after handler memory commit")

    with pytest.raises(SystemExit):
        run(path, model=Cleanup(), state=state, fault=interrupt)
    result = run(path, model=Cleanup())
    assert result == expected
    assert result.memory.values["cleanup_calls"] == [130, 140, 150]
    assert [event.time for event in result.events] == [132, 145]


def test_connector_cannot_return_out_of_window_or_nonadvancing_events(tmp_path):
    class Invalid(Events):
        def next_event(self, state, until):
            return UnixTime(100)

    with pytest.raises(ValueError, match="durable cursor"):
        run(tmp_path / "state.sqlite", model=Invalid())
    journal = Journal(tmp_path / "state.sqlite", State)
    assert len(journal.load().events) == 1


def test_invalid_event_effects_do_not_consume_the_input_cursor(tmp_path):
    class Unfunded(Events):
        def external_event(self, state, time):
            return Transition[Empty, Receipt](
                effects=Effects[Empty](assets=(AssetDelta(key=OUTPUT, amount=-1),)),
                result=Receipt(acquired=0),
            )

    with pytest.raises(ValueError, match="insufficient"):
        run(tmp_path / "state.sqlite", model=Unfunded())
    assert Journal(tmp_path / "state.sqlite", State).load() == initial()


def test_external_event_cannot_leave_an_unhandled_continuation(tmp_path):
    class Incomplete(Events):
        def external_event(self, state, time):
            return super().external_event(state, time).model_copy(update={"complete": False})

    with pytest.raises(ValueError, match="intent continuation"):
        run(tmp_path / "state.sqlite", model=Incomplete())
    assert Journal(tmp_path / "state.sqlite", State).load() == initial()
