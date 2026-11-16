"""Deterministic event progression over typed connector effects and one journal."""

import fcntl
import os
import sqlite3
from collections.abc import Callable
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Protocol, get_args, runtime_checkable

from pydantic import model_validator

from almanak.backtest import progress
from almanak.backtest.types import Capability, Name, UnixTime
from almanak.effects import Effects, Portfolio
from almanak.models import Model, UInt
from almanak.strategy_api import Memory, StrategyDecision
from almanak.triggers import DecisionSchedule, IntervalSelection, PositiveSeconds, TriggerEvent


class Schedule(Model):
    start: UnixTime
    end: UnixTime
    decision_interval_override_seconds: PositiveSeconds | None = None
    latency_seconds: UInt
    end_policy: Literal["mark_open", "teardown"]
    cleanup_seconds: UInt

    @model_validator(mode="after")
    def interval(self):
        if self.end <= self.start:
            raise ValueError("backtest requires an increasing window")
        if self.end_policy == "teardown" and self.cleanup_seconds == 0:
            raise ValueError("teardown requires an explicit positive cleanup deadline")
        return self

    @property
    def deadline(self) -> UnixTime:
        return UnixTime(self.end + self.cleanup_seconds)

    def opportunities(self, seconds: int) -> int:
        return (self.end - self.start + seconds - 1) // seconds


class Pending[Intent](Model):
    identity: str
    decision_identity: str
    trigger: TriggerEvent
    intent: Intent
    decision_time: UnixTime
    eligible_at: UnixTime


class Transition[Payload: Model, Result](Model):
    effects: Effects[Payload]
    result: Result
    complete: bool = True


class Completed[Payload: Model, Intent, Result](Model):
    pending: Pending[Intent]
    transition: Transition[Payload, Result]


class RecordedEvent[Payload: Model, Result](Model):
    identity: str
    time: UnixTime
    transition: Transition[Payload, Result]


class RunState[Payload: Model, Intent, Result](Model):
    schema_version: Literal[3] = 3
    run_id: Name
    revision: UInt = 0
    time: UnixTime
    schedule: Schedule
    interval: IntervalSelection
    decision_schedule: DecisionSchedule = DecisionSchedule()
    evaluations: UInt = 0
    mode: Literal["running", "teardown", "finished"] = "running"
    portfolio: Portfolio[Payload]
    memory: Memory = Memory()
    pending: Pending[Intent] | None = None
    continuation: Pending[Intent] | None = None
    completed: tuple[Completed[Payload, Intent, Result], ...] = ()
    cancelled: tuple[Pending[Intent], ...] = ()
    delivered: UInt = 0
    events: tuple[RecordedEvent[Payload, Result], ...] = ()
    events_delivered: UInt = 0
    event_cursor: UnixTime | None = None
    wake_at: UnixTime | None = None

    @model_validator(mode="after")
    def progress(self):
        self.decision_schedule.validate_trigger(self.interval.effective)
        if self.interval.override_seconds != self.schedule.decision_interval_override_seconds:
            raise ValueError("effective interval differs from the selected override")
        if self.pending is not None and self.continuation is not None:
            raise ValueError("a backtest cannot prepare two concurrent steps")
        if self.delivered > len(self.completed):
            raise ValueError("outcome delivery cannot precede recorded effects")
        if self.events_delivered > len(self.events):
            raise ValueError("event delivery cannot precede recorded effects")
        if self.event_cursor is not None and self.event_cursor > self.time:
            raise ValueError("event cursor cannot exceed simulation time")
        if self.wake_at is not None:
            if self.mode != "teardown" or not self.time <= self.wake_at <= self.schedule.deadline:
                raise ValueError("cleanup wake time must remain within its unfinished deadline")
            if self.pending is not None or self.continuation is not None:
                raise ValueError("cleanup cannot wait while an intent is admitted")
        if self.time < self.schedule.start:
            raise ValueError("simulation cannot precede its initial state")
        return self


class Driver[Payload: Model, Intent, Result, Observation](Protocol):
    def observe(self, state: RunState[Payload, Intent, Result]) -> Observation: ...

    def decide(
        self, state, observations: Observation, trigger: TriggerEvent
    ) -> StrategyDecision[Intent]: ...

    def route(self, intent: Intent) -> tuple[Capability, str]: ...

    def eligible_at(self, intent: Intent, time: UnixTime, latency: int) -> UnixTime: ...

    def execute(self, state, pending: Pending[Intent]) -> Transition[Payload, Result]: ...

    def deliver(self, state, result: Result) -> Memory: ...

    def closed(self, state, observations: Observation) -> bool: ...


@runtime_checkable
class ExternalEvents(Protocol):
    def next_event(self, state, until: UnixTime) -> UnixTime | None: ...

    def external_event(self, state, time: UnixTime) -> Transition | None: ...

    def deliver_event(self, state, result) -> Memory: ...


@runtime_checkable
class EndBoundary(Protocol):
    def cancel_at_end(self, intent) -> bool: ...


@runtime_checkable
class ContinuationTiming(Protocol):
    def continuation_at(self, state, pending) -> UnixTime: ...


class Journal[State: Model]:
    def __init__(self, path: Path, state_type: type[State]):
        self.path = path
        self.state_type = state_type
        self._connection: sqlite3.Connection | None = None

    def open_connection(self):
        db = sqlite3.connect(self.path, timeout=30)
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        return db

    @contextmanager
    def connect(self):
        db = self._connection if self._connection is not None else self.open_connection()
        try:
            with db:
                yield db
        finally:
            if self._connection is None:
                db.close()

    def create(self, initial: State):
        if self.path.exists():
            raise ValueError("backtest journal already exists; resume the recorded run")
        with self.connect() as db:
            db.execute("CREATE TABLE checkpoint (id INTEGER PRIMARY KEY, body TEXT NOT NULL)")
            db.execute("INSERT INTO checkpoint VALUES (1, ?)", (initial.model_dump_json(),))
        sync_directory(self.path.parent)

    def load(self) -> State:
        if not self.path.is_file():
            raise ValueError("backtest journal is missing")
        with self.connect() as db:
            row = db.execute("SELECT body FROM checkpoint WHERE id=1").fetchone()
        if row is None:
            raise ValueError("backtest checkpoint is missing")
        return self.state_type.model_validate_json(row[0])

    def save(self, before: State, after: State) -> State:
        # Revalidate copies; model_copy does not validate updated economic fields.
        after = self.state_type.model_validate_json(after.model_dump_json())
        with self.connect() as db:
            changed = db.execute(
                "UPDATE checkpoint SET body=? WHERE id=1 AND body=?",
                (after.model_dump_json(), before.model_dump_json()),
            ).rowcount
            if changed != 1:
                raise RuntimeError("backtest checkpoint changed; reload before recording")
        return after

    @contextmanager
    def coordinator(self):
        with self.path.with_suffix(".lock").open("a") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RuntimeError("backtest already has a coordinator") from None
            try:
                self._connection = self.open_connection()
                yield
            finally:
                if self._connection is not None:
                    self._connection.close()
                    self._connection = None
                fcntl.flock(lock, fcntl.LOCK_UN)


class Simulator[Payload: Model, Intent, Result, Observation]:
    def __init__(
        self,
        journal: Journal[RunState[Payload, Intent, Result]],
        driver: Driver[Payload, Intent, Result, Observation],
        *,
        fault: Callable[[str], None] | None = None,
    ):
        self.journal = journal
        self.driver = driver
        self.fault = fault or (lambda stage: None)
        self.completed_type = get_args(journal.state_type.model_fields["completed"].annotation)[0]
        self.pending_type = get_args(journal.state_type.model_fields["pending"].annotation)[0]
        self.event_type = get_args(journal.state_type.model_fields["events"].annotation)[0]

    def save(self, state, **changes):
        return self.journal.save(
            state, state.model_copy(update={"revision": state.revision + 1, **changes})
        )

    def tick(self) -> bool:
        state = self.journal.load()
        progress.simulation(
            state.schedule.start,
            state.schedule.end,
            state.schedule.deadline,
            state.time,
            state.mode,
            state.evaluations,
        )
        if state.mode == "finished":
            return True
        if state.delivered < len(state.completed):
            self.deliver(state)
        elif state.events_delivered < len(state.events):
            self.deliver_event(state)
        elif state.wake_at is not None:
            self.advance_to(state, state.wake_at, wake_at=None)
        elif state.pending is not None:
            self.execute(state)
        elif state.time >= state.schedule.end and state.mode == "running":
            if self.advance_events(state, state.time):
                return False
            mode = "finished" if state.schedule.end_policy == "mark_open" else "teardown"
            self.save(state, mode=mode, continuation=None)
        elif state.continuation is not None:
            self.continue_intent(state)
        else:
            self.decide(state)
        return False

    def advance_events(self, state, until) -> bool:
        if not isinstance(self.driver, ExternalEvents):
            return False
        timestamp = self.driver.next_event(state, UnixTime(until))
        if timestamp is None:
            return False
        if not state.time <= timestamp <= until:
            raise ValueError("connector event lies outside its eligible window")
        if state.event_cursor is not None and timestamp <= state.event_cursor:
            raise ValueError("connector event did not advance its durable cursor")
        transition = self.driver.external_event(state, timestamp)
        changes: dict[str, Any] = {"time": timestamp, "event_cursor": timestamp}
        if transition is not None:
            if not transition.complete:
                raise ValueError("external events cannot require an intent continuation")
            event = self.event_type(
                identity=f"{state.run_id}:event:{len(state.events)}",
                time=timestamp,
                transition=transition,
            )
            changes.update(
                portfolio=state.portfolio.apply(transition.effects),
                events=(*state.events, event),
            )
        self.fault("event_effects")
        self.save(state, **changes)
        self.fault("event_commit")
        return True

    def deliver_event(self, state):
        if not isinstance(self.driver, ExternalEvents):
            raise ValueError("recorded event requires its connector delivery contract")
        result = state.events[state.events_delivered].transition.result
        memory = self.driver.deliver_event(state, result)
        self.fault("event_delivery")
        self.save(state, memory=memory, events_delivered=state.events_delivered + 1)
        self.fault("event_delivery_commit")

    def deliver(self, state):
        result = state.completed[state.delivered].transition.result
        memory = self.driver.deliver(state, result)
        self.fault("outcome")
        self.save(state, memory=memory, delivered=state.delivered + 1)
        self.fault("delivery_commit")

    def execute(self, state):
        pending = state.pending
        limit = state.schedule.deadline if state.mode == "teardown" else state.schedule.end
        if self.advance_events(state, min(pending.eligible_at, limit)):
            return
        cancels = (
            state.mode == "running"
            and pending.eligible_at == limit
            and isinstance(self.driver, EndBoundary)
            and self.driver.cancel_at_end(pending.intent)
        )
        if pending.eligible_at > limit or cancels:
            # A final historical mark cannot fill an action that is not yet eligible.
            self.expire(state, limit)
            return
        capability, action = self.driver.route(pending.intent)
        capability.require(pending.eligible_at, action)
        transition = self.driver.execute(state, pending)
        portfolio = state.portfolio.apply(transition.effects)
        self.fault("effects")
        completed = self.completed_type(pending=pending, transition=transition)
        self.save(
            state,
            time=pending.eligible_at,
            portfolio=portfolio,
            pending=None,
            completed=(*state.completed, completed),
            continuation=None if transition.complete else pending,
        )
        self.fault("effects_commit")

    def expire(self, state, limit):
        if state.mode == "teardown":
            raise ValueError("cleanup cannot execute before its deadline")
        if state.schedule.end_policy == "teardown":
            self.save(
                state,
                mode="teardown",
                time=UnixTime(limit),
                pending=None,
                cancelled=(*state.cancelled, state.pending),
            )
        else:
            self.save(state, mode="finished", time=UnixTime(limit))

    def decide(self, state):
        if self.advance_events(state, state.time):
            return
        event = state.decision_schedule.event(
            datetime.fromtimestamp(state.time, UTC), teardown=state.mode == "teardown"
        )
        if event is None:
            next_time = UnixTime(
                min(UnixTime(state.decision_schedule.next_due_at), state.schedule.end)
            )
            self.advance_to(state, next_time)
            return
        observations = self.driver.observe(state)
        decision = self.driver.decide(state, observations, event)
        self.fault("decision")
        progress = {
            "memory": decision.memory,
            "decision_schedule": state.decision_schedule.after(state.interval.effective, event),
            "evaluations": state.evaluations + 1,
        }
        if decision.intent is None:
            self.hold(state, observations, progress)
            return
        pending = self.prepare(state, decision.intent, event)
        self.save(state, pending=pending, **progress)
        self.fault("pending_commit")

    def prepare(self, state, intent, event):
        eligible = self.eligibility(state, intent)
        return self.pending_type(
            identity=f"{state.run_id}:{len(state.completed) + len(state.cancelled)}",
            decision_identity=f"{state.run_id}:decision:{state.evaluations}",
            trigger=event,
            intent=intent,
            decision_time=state.time,
            eligible_at=eligible,
        )

    def eligibility(self, state, intent, *, continuation=None):
        capability, action = self.driver.route(intent)
        capability.require(state.time, action)
        eligible = (
            self.driver.continuation_at(state, continuation)
            if continuation is not None and isinstance(self.driver, ContinuationTiming)
            else self.driver.eligible_at(intent, state.time, state.schedule.latency_seconds)
        )
        if eligible <= state.time:
            raise ValueError("simulated execution must follow its decision")
        return eligible

    def continue_intent(self, state):
        previous = state.continuation
        eligible = self.eligibility(state, previous.intent, continuation=previous)
        pending = previous.model_copy(
            update={
                "identity": f"{state.run_id}:{len(state.completed) + len(state.cancelled)}",
                "eligible_at": eligible,
            }
        )
        self.save(state, pending=pending, continuation=None)
        self.fault("pending_commit")

    def hold(self, state, observations, progress):
        if state.mode == "teardown" and self.driver.closed(state, observations):
            self.save(state, mode="finished", **progress)
            return
        if state.mode == "running":
            self.save(state, **progress)
            self.fault("decision_commit")
            return
        limit = state.schedule.deadline if state.mode == "teardown" else state.schedule.end
        if state.time >= limit:
            raise ValueError("cleanup deadline reached with unresolved obligations")
        next_time = UnixTime(min(state.time + state.interval.effective.seconds, limit))
        # The next wake survives a crash after a no-action teardown handler.
        # Re-evaluating at the old time could repeat a strategy memory transition.
        self.save(state, wake_at=next_time, **progress)
        self.fault("decision_commit")

    def advance_to(self, state, next_time, **changes):
        if not self.advance_events(state, next_time):
            self.save(state, time=next_time, **changes)

    def run(self):
        with self.journal.coordinator():
            while not self.tick():
                pass
        return self.journal.load()


def sync_directory(path: Path):
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
