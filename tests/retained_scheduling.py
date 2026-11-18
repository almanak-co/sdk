"""Add synthetic scheduling inputs to old component evidence without rewriting captures."""

from copy import deepcopy
from datetime import UTC, datetime


def config_input(raw):
    return {**raw, "decision_trigger": {"kind": "interval", "seconds": 5}}


def synthetic_trigger(timestamp=1787184000, *, teardown=False):
    time = datetime.fromtimestamp(timestamp, UTC).isoformat()
    return (
        {"kind": "teardown", "started_at": time}
        if teardown
        else {
            "kind": "interval",
            "started_at": time,
            "due_at": time,
        }
    )


def checkpoint_input(raw):
    value = deepcopy(raw)
    uniswap = value["config"].get("connector") == "uniswap_v4"
    value.update(
        schema_version=2 if uniswap else 5, schedule={}, config=config_input(value["config"])
    )
    for outcome in value["outcomes"]:
        # These receipts predate the scheduler. This is component input, not timing proof.
        before = outcome["before"]
        block = before["pool"]["block"] if uniswap else before["position"]["reserve"]["block"]
        outcome["decision"]["trigger"] = synthetic_trigger(block["timestamp"])
    return value


def synthetic_event(timestamp=1787184000, *, teardown=False):
    from almanak.triggers import IntervalEvent, TeardownEvent

    time = datetime.fromtimestamp(timestamp, UTC)
    return (
        TeardownEvent(started_at=time) if teardown else IntervalEvent(started_at=time, due_at=time)
    )
