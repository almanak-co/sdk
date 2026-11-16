"""Versioned operational progress, independent of financial checkpoints."""

from datetime import datetime
from typing import Annotated, Literal

from pydantic import AwareDatetime, Field, model_validator

from almanak.models import Model, UInt

Seconds = Annotated[float, Field(ge=0, allow_inf_nan=False)]
Status = Literal["running", "completed", "failed", "interrupted"]
Basis = Literal["calibrating", "previous_runs", "observed", "mixed", "complete", "unavailable"]


class Eta(Model):
    remaining_seconds: Seconds | None = None
    estimated_finish_at: AwareDatetime | None = None
    basis: Basis = "calibrating"

    @model_validator(mode="after")
    def paired(self):
        if (self.remaining_seconds is None) != (self.estimated_finish_at is None):
            raise ValueError("ETA duration and finish time must both be present or absent")
        return self


class PhaseProgress(Model):
    phase_id: str
    label: str
    unit: str
    completed_units: Seconds
    total_units: Seconds | None
    total_is_estimate: bool
    finished: bool
    elapsed_seconds: Seconds
    eta: Eta


class ProgressSnapshot(Model):
    schema_version: Literal[1] = 1
    kind: Literal["backtest_progress"] = "backtest_progress"
    job_id: str
    attempt_id: str
    sequence: UInt
    status: Status
    updated_at: AwareDatetime
    stale_after_seconds: Seconds = 15.0
    elapsed_seconds: Seconds
    phase: str | None
    phases: tuple[PhaseProgress, ...]
    eta: Eta
    timing_key: str
    observer_errors: UInt = 0
    decision_evaluations: UInt | None = None

    def visible_eta(self, now: datetime) -> Eta | None:
        """Consumers must stop countdowns when the worker's heartbeat expires."""
        if self.status != "running":
            return None
        if (now - self.updated_at).total_seconds() > self.stale_after_seconds:
            return None
        return self.eta


class PhaseTiming(Model):
    phase_id: str
    label: str
    unit: str
    total_units: Seconds | None
    total_is_estimate: bool = False
    elapsed_seconds: Seconds


class TimingEntry(Model):
    key: str
    phases: tuple[PhaseTiming, ...]


class TimingHistory(Model):
    schema_version: Literal[1] = 1
    entries: tuple[TimingEntry, ...] = ()
