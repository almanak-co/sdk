"""Shared ETA reporting for trusted backtest orchestration and SDK observers."""

from __future__ import annotations

import fcntl
import math
import secrets
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from statistics import median
from time import monotonic
from typing import TextIO

from almanak.backtest.progress_history import History, atomic_snapshot
from almanak.backtest.progress_types import (
    Basis,
    Eta,
    PhaseProgress,
    PhaseTiming,
    ProgressSnapshot,
    Status,
    TimingEntry,
)

_CURRENT: ContextVar[ProgressJob | None] = ContextVar("backtest_progress", default=None)
_PHASE_SCOPE: ContextVar[str] = ContextVar("backtest_progress_phase_scope", default="")


def estimate(seconds: float | None, basis: Basis, now: datetime) -> Eta:
    if seconds is None:
        return Eta(basis=basis)
    if not math.isfinite(seconds) or seconds < 0:
        return Eta()
    try:
        finish = now + timedelta(seconds=seconds)
    except OverflowError:
        return Eta()
    return Eta(remaining_seconds=seconds, estimated_finish_at=finish, basis=basis)


@dataclass
class Phase:
    phase_id: str
    label: str
    unit: str = "items"
    total: float | None = None
    total_is_estimate: bool = False
    completed: float = 0.0
    baseline: float = 0.0
    elapsed: float = 0.0
    started: float | None = None
    finished: bool = False
    prior: float | None = None
    prior_units: float | None = None

    def duration(self, now: float) -> float:
        return self.elapsed + (now - self.started if self.started is not None else 0)

    def remaining(self, now: float) -> tuple[float | None, Basis]:
        if self.finished:
            return 0.0, "complete"
        if self.total == 0 and self.started is None:
            return 0.0, "complete"
        if self.total is not None and self.completed >= self.total:
            return None, "calibrating"  # Publication/commit may still be in progress.
        fresh = self.completed - self.baseline
        if self.started is not None and fresh > 0 and self.total is not None:
            return self.duration(now) / fresh * (self.total - self.completed), "observed"
        return self.previous_remaining(now)

    def previous_remaining(self, now: float) -> tuple[float | None, Basis]:
        if self.prior is None:
            return None, "calibrating"
        remaining = self.prior
        if self.total is not None and self.prior_units:
            remaining *= (self.total - self.completed) / self.prior_units
        remaining -= self.duration(now)
        if remaining <= 0:
            return None, "calibrating"
        return remaining, "previous_runs"

    def snapshot(self, monotonic: float, utc: datetime) -> PhaseProgress:
        seconds, basis = self.remaining(monotonic)
        return PhaseProgress(
            phase_id=self.phase_id,
            label=self.label,
            unit=self.unit,
            completed_units=self.completed,
            total_units=self.total,
            total_is_estimate=self.total_is_estimate,
            finished=self.finished,
            elapsed_seconds=self.duration(monotonic),
            eta=estimate(seconds, basis, utc),
        )


def total_eta(phases: tuple[PhaseProgress, ...], status: Status, now: datetime) -> Eta:
    if status == "completed":
        return estimate(0.0, "complete", now)
    if status != "running":
        return Eta(basis="unavailable")
    remaining = [p.eta.remaining_seconds for p in phases if not p.finished]
    if not remaining or any(value is None for value in remaining):
        return Eta()
    bases = {p.eta.basis for p in phases if not p.finished}
    basis: Basis = "mixed" if len(bases) > 1 else next(iter(bases))
    return estimate(sum(value for value in remaining if value is not None), basis, now)


class ProgressJob:
    def __init__(
        self,
        path: Path,
        timing_key: str,
        *,
        history: Path | None = None,
        callback: Callable[[ProgressSnapshot], None] | None = None,
        stream: TextIO | None = None,
        json_events: bool = False,
        resume: bool = False,
        interval: float = 1.0,
    ):
        if not math.isfinite(interval) or interval <= 0:
            raise ValueError("progress interval must be positive")
        self.path, self.timing_key = path, timing_key
        self.history = History(history) if history is not None else None
        self.callback, self.stream, self.json_events = callback, stream, json_events
        self.resume, self.interval = resume, interval
        self.job_id, self.attempt_id = secrets.token_hex(16), secrets.token_hex(16)
        self.sequence = 0
        self.status: Status = "running"
        self.phases: dict[str, Phase] = {}
        self.active: str | None = None
        self.evaluations: int | None = None
        self.observer_errors = 0
        self.started = monotonic()
        self.last_emit = float("-inf")
        self.lock = threading.RLock()
        self.stop = threading.Event()
        self.priors: tuple[TimingEntry, ...] = ()

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.file_lock = self.path.with_name(self.path.name + ".lock").open("a")
        try:
            fcntl.flock(self.file_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.restore()
            self.token = _CURRENT.set(self)
            self.thread = threading.Thread(target=self.heartbeat, daemon=True)
            self.thread.start()
            return self
        except BaseException:
            self.file_lock.close()
            raise

    def restore(self):
        if self.path.exists():
            previous = ProgressSnapshot.model_validate_json(self.path.read_text())
            if self.resume:
                self.job_id, self.sequence = previous.job_id, previous.sequence
        if self.history is not None:
            self.observe(self.load_priors)
        if self.priors:
            for row in self.priors[-1].phases:
                self.plan(row.phase_id, row.label, row.total_units, row.unit, estimated=True)

    def load_priors(self):
        assert self.history is not None
        self.priors = self.history.matching(self.timing_key)

    def plan(self, phase_id, label, total=None, unit="items", *, estimated=False):
        with self.lock:
            if phase_id not in self.phases or self.phases[phase_id].unit != unit:
                self.phases[phase_id] = self.new_phase(phase_id, label, unit)
            phase = self.phases[phase_id]
            phase.label = label
            phase.total, phase.total_is_estimate = total, estimated
            return phase

    def new_phase(self, phase_id, label, unit):
        samples = [
            p
            for row in self.priors
            for p in row.phases
            if p.phase_id == phase_id and p.unit == unit
        ]
        phase = Phase(phase_id, label, unit)
        if samples:
            phase.prior = median(p.elapsed_seconds for p in samples)
            counts = [p.total_units for p in samples if p.total_units is not None]
            phase.prior_units = median(counts) if counts else None
        return phase

    def discard(self, prefix):
        with self.lock:
            for key in tuple(self.phases):
                if key.startswith(prefix) and key != self.active:
                    del self.phases[key]

    def begin(self, phase_id, label, total=None, unit="items", *, completed=0.0, estimated=False):
        with self.lock:
            if self.active in ("startup", "simulation_setup") and phase_id != self.active:
                self.finish(self.active)
            phase = self.plan(phase_id, label, total, unit, estimated=estimated)
            phase.completed = phase.baseline = float(completed)
            phase.started = monotonic()
            self.active = phase_id
            self.publish(force=True)

    def update(self, completed=None, *, total=None, estimated=None):
        with self.lock:
            if self.active is None:
                return
            phase = self.phases[self.active]
            if completed is not None:
                phase.completed = float(completed)
            if total is not None:
                phase.total = float(total)
            if estimated is not None:
                phase.total_is_estimate = estimated
            self.publish()

    def advance(self, units=1.0):
        with self.lock:
            if self.active is not None:
                self.update(self.phases[self.active].completed + units)

    def finish(self, phase_id):
        with self.lock:
            phase = self.phases[phase_id]
            phase.elapsed = phase.duration(monotonic())
            phase.started, phase.finished = None, True
            if phase.unit == "phases":
                phase.completed = 1.0
            phase.total = phase.completed
            phase.total_is_estimate = False
            if self.active == phase_id:
                self.active = None
            self.publish(force=True)

    def snapshot(self):
        now, utc = monotonic(), datetime.now(UTC)
        phases = tuple(p.snapshot(now, utc) for p in self.phases.values())
        if self.status in ("failed", "interrupted"):
            phases = tuple(p.model_copy(update={"eta": Eta(basis="unavailable")}) for p in phases)
        return ProgressSnapshot(
            job_id=self.job_id,
            attempt_id=self.attempt_id,
            sequence=self.sequence,
            status=self.status,
            updated_at=utc,
            stale_after_seconds=max(15.0, self.interval * 3),
            elapsed_seconds=now - self.started,
            phase=self.active,
            phases=phases,
            eta=total_eta(phases, self.status, utc),
            timing_key=self.timing_key,
            observer_errors=self.observer_errors,
            decision_evaluations=self.evaluations,
        )

    def publish(self, *, force=False):
        with self.lock:
            now = monotonic()
            if not force and now - self.last_emit < self.interval:
                return
            self.last_emit, self.sequence = now, self.sequence + 1
            snapshot = self.snapshot()
            self.observe(lambda: atomic_snapshot(self.path, snapshot))
            if self.callback is not None:
                self.observe(lambda: self.callback(snapshot))
            if self.stream is not None:
                self.observe(lambda: self.render(snapshot))

    def observe(self, operation):
        try:
            operation()
        except Exception:
            self.observer_errors += 1

    def render(self, snapshot):
        assert self.stream is not None
        if self.json_events:
            self.stream.write(snapshot.model_dump_json() + "\n")
        else:
            line = terminal_line(snapshot)
            if self.stream.isatty():
                suffix = "\n" if snapshot.status != "running" else ""
                self.stream.write("\r\x1b[2K" + line + suffix)
            else:
                self.stream.write(line + "\n")
        self.stream.flush()

    def clear_terminal(self):
        if self.stream is not None and not self.json_events and self.stream.isatty():
            self.stream.write("\r\x1b[2K")
            self.stream.flush()

    def heartbeat(self):
        while not self.stop.wait(self.interval):
            self.publish()

    def record_timing(self):
        if self.history is not None:
            phases = tuple(
                PhaseTiming(
                    phase_id=p.phase_id,
                    label=p.label,
                    unit=p.unit,
                    total_units=max(0.0, p.completed - p.baseline),
                    total_is_estimate=p.total_is_estimate,
                    elapsed_seconds=p.elapsed,
                )
                for p in self.phases.values()
                if p.finished
            )
            self.history.record(TimingEntry(key=self.timing_key, phases=phases))

    def __exit__(self, kind, error, traceback):
        self.stop.set()
        self.thread.join()
        try:
            self.status = outcome_status(kind)
            if self.status == "completed":
                self.observe(self.record_timing)
            self.publish(force=True)
        finally:
            _CURRENT.reset(self.token)
            self.file_lock.close()


def outcome_status(kind) -> Status:
    if kind is None:
        return "completed"
    if issubclass(kind, (KeyboardInterrupt, SystemExit)):
        return "interrupted"
    return "failed"


def terminal_line(snapshot: ProgressSnapshot) -> str:
    phase = next((p for p in snapshot.phases if p.phase_id == snapshot.phase), None)
    label = phase.label if phase else snapshot.status
    remaining = snapshot.eta.remaining_seconds
    eta = "calculating" if remaining is None else f"~{remaining:.0f}s"
    if snapshot.status in ("failed", "interrupted"):
        eta = "unavailable"
    detail = ""
    if phase is not None:
        total = "?" if phase.total_units is None else f"{phase.total_units:g}"
        detail = f" | {phase.completed_units:g}/{total} {phase.unit}"
        if remaining is None and phase.eta.remaining_seconds is not None:
            detail += f" | phase ETA ~{phase.eta.remaining_seconds:.0f}s"
    return (
        f"Backtest {snapshot.status}: {label}{detail}"
        f" | elapsed {snapshot.elapsed_seconds:.0f}s | total ETA {eta}"
    )


def current() -> ProgressJob | None:
    return _CURRENT.get()


def phase_key(name: str) -> str:
    return _PHASE_SCOPE.get() + name


@contextmanager
def phase_scope(name: str) -> Iterator[None]:
    """Keep component phase counts distinct inside one acquisition job."""
    token = _PHASE_SCOPE.set(_PHASE_SCOPE.get() + name + ".")
    try:
        yield
    finally:
        _PHASE_SCOPE.reset(token)


@contextmanager
def phase(
    phase_id, label, total=None, unit="items", *, completed=0.0, estimated=False
) -> Iterator[None]:
    job = current()
    phase_id = phase_key(phase_id)
    if job is not None:
        job.begin(phase_id, label, total, unit, completed=completed, estimated=estimated)
    yield
    if job is not None:
        job.finish(phase_id)


def advance(units=1.0):
    if (job := current()) is not None:
        job.advance(units)


@contextmanager
def activity(phase_id) -> Iterator[None]:
    job = current()
    if job is None:
        yield
        return
    planned = job.phases[phase_key(phase_id)]
    with phase(
        phase_id,
        planned.label,
        planned.total,
        planned.unit,
        completed=planned.completed,
        estimated=planned.total_is_estimate,
    ):
        yield


def simulation(start, end, deadline, at, mode, evaluations):
    job = current()
    if job is None:
        return
    job.evaluations = evaluations
    if mode == "finished":
        for name in ("simulation", "teardown"):
            if name in job.phases and not job.phases[name].finished:
                completed = (end - start) if name == "simulation" else max(0, at - end)
                job.phases[name].completed = float(completed)
                job.finish(name)
        return
    name = "teardown" if mode == "teardown" else "simulation"
    left, right = (end, deadline) if name == "teardown" else (start, end)
    completed = float(max(0, min(at, right) - left))
    if job.active != name:
        if name == "teardown" and "simulation" in job.phases:
            job.finish("simulation")
        job.begin(
            name,
            "Closing remaining obligations" if name == "teardown" else "Simulating strategy",
            None if name == "teardown" else float(right - left),
            "simulation_seconds",
            completed=completed,
            estimated=name == "teardown",
        )
    job.update(completed)


def simulation_setup():
    if (job := current()) is not None and job.active != "simulation_setup":
        job.begin("simulation_setup", "Opening frozen simulation inputs", 1.0, "phases")
