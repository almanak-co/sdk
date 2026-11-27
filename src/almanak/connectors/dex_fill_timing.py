"""How old the pool state of a modeled exchange step was, and whether latency moved it.

A modeled step executes on the latest prepared sample at or before its eligible time,
which is the decision time plus the configured latency. When the sampling interval is
longer than the latency, that sample is usually the one the strategy decided on, so the
latency changes the step's timestamp and not the state it executes against. These
fields report that; they never change a fill.
"""

from typing import Annotated

from pydantic import Field

from almanak.models import UInt

# Seconds from the source time of the sample a step executed on to its eligible time.
StateAge = Annotated[UInt | None, Field(exclude_if=lambda v: v is None)]
# True when that sample is the one the decision observed.
OnDecisionSample = Annotated[bool | None, Field(exclude_if=lambda v: v is None)]


def measured(book, target, pending):
    """The timing fields of a step that executed on the sample in force at `eligible_at`."""
    record = book.at(target, pending.eligible_at)
    decided = book.at(target, pending.decision_time)
    return {
        "fill_state_age_seconds": int(pending.eligible_at) - int(record.source_time),
        "fill_on_decision_sample": record.reference == decided.reference,
    }


def summary(state):
    """Report block over every completed step that recorded its state age."""
    timed = [
        result
        for result in (c.transition.result for c in state.completed)
        if getattr(result, "fill_state_age_seconds", None) is not None
    ]
    ages = [result.fill_state_age_seconds for result in timed]
    return {
        "latency_seconds": state.schedule.latency_seconds,
        "steps": len(timed),
        "steps_on_decision_sample": sum(bool(r.fill_on_decision_sample) for r in timed),
        "max_fill_state_age_seconds": max(ages) if ages else None,
        "interpretation": (
            "a step on its decision sample executed against the pool state the strategy "
            "decided on, so the configured latency did not change its price"
        ),
    }
