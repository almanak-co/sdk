"""Decision eligibility shared by wall-clock and simulated-time coordinators."""

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated, Literal

from pydantic import AfterValidator, Field, model_validator

from almanak.models import Model

PositiveSeconds = Annotated[int, Field(strict=True, gt=0)]
HOLD_SECONDS_DESCRIPTION = (
    "Optional. Read only by strategy code for its own timed exit; connectors never act on it."
    " Omit to hold until teardown."
)


def utc_instant(value: datetime) -> datetime:
    if value.utcoffset() is None:
        raise ValueError("decision time requires an explicit timezone")
    return value.astimezone(UTC)


UtcInstant = Annotated[datetime, AfterValidator(utc_instant)]


def utc_now() -> datetime:
    return datetime.now(UTC)


def offset_clock(path: Path) -> Callable[[], datetime]:
    """Wall time plus the whole seconds a fork test has advanced its fork.

    `almanak test run` advances an identified fork's block time and this offset together,
    so a decision interval elapses without editing the author's interval. Lease and
    receipt timing keep wall time.
    """

    def clock() -> datetime:
        return utc_now() + timedelta(seconds=read_offset(path))

    return clock


def read_offset(path: Path) -> int:
    return int(path.read_text())


class IntervalTrigger(Model):
    kind: Literal["interval"]
    seconds: PositiveSeconds


class IntervalEvent(Model):
    kind: Literal["interval"] = "interval"
    started_at: UtcInstant
    due_at: UtcInstant

    @model_validator(mode="after")
    def eligible(self):
        if self.started_at < self.due_at:
            raise ValueError("interval evaluation cannot precede its deadline")
        return self


class TeardownEvent(Model):
    kind: Literal["teardown"] = "teardown"
    started_at: UtcInstant


class OperatorEvent(Model):
    """One operator request on a deployment that retains no strategy.

    `request` names the operator's request, separately from the decision identity the
    runtime assigns, so a repeated command is recognized instead of executed twice.
    """

    kind: Literal["operator"] = "operator"
    started_at: UtcInstant
    request: Annotated[str, Field(pattern=r"^[0-9a-f]{32}$")]


TriggerEvent = Annotated[IntervalEvent | TeardownEvent | OperatorEvent, Field(discriminator="kind")]


class DecisionSchedule(Model):
    last_evaluation: IntervalEvent | None = None
    next_due_at: UtcInstant | None = None

    @model_validator(mode="after")
    def paired(self):
        if (self.last_evaluation is None) != (self.next_due_at is None):
            raise ValueError("decision schedule requires both evaluation and deadline")
        return self

    def validate_trigger(self, trigger: IntervalTrigger) -> None:
        expected = (
            self.last_evaluation.started_at + timedelta(seconds=trigger.seconds)
            if self.last_evaluation is not None
            else None
        )
        if self.next_due_at != expected:
            raise ValueError("decision deadline differs from the configured interval")

    def due(self, now: datetime) -> bool:
        now = utc_instant(now)
        return self.next_due_at is None or now >= self.next_due_at

    def event(self, now: datetime, *, teardown: bool = False) -> TriggerEvent | None:
        now = utc_instant(now)
        if teardown:
            return TeardownEvent(started_at=now)
        if not self.due(now):
            return None
        return IntervalEvent(started_at=now, due_at=self.next_due_at or now)

    def after(self, trigger: IntervalTrigger, event: TriggerEvent) -> "DecisionSchedule":
        if isinstance(event, (TeardownEvent, OperatorEvent)):
            return self
        if self.event(event.started_at) != event:
            raise ValueError("interval event does not match the retained deadline")
        return DecisionSchedule(
            last_evaluation=event,
            next_due_at=event.started_at + timedelta(seconds=trigger.seconds),
        )


class IntervalSelection(Model):
    """Resolve a backtest override before constructing its effective parameters."""

    configured: IntervalTrigger
    override_seconds: PositiveSeconds | None = None

    @model_validator(mode="after")
    def coarser(self):
        if self.override_seconds is not None and self.override_seconds <= self.configured.seconds:
            raise ValueError("backtest interval override must exceed the configured interval")
        return self

    @property
    def effective(self) -> IntervalTrigger:
        if self.override_seconds is None:
            return self.configured
        return IntervalTrigger(kind="interval", seconds=self.override_seconds)

    def report(self) -> dict:
        return {
            "configured_seconds": self.configured.seconds,
            "effective_seconds": self.effective.seconds,
            "decision_interval_override_seconds": self.override_seconds,
            "cadence_fidelity": "configured" if self.override_seconds is None else "reduced",
        }
