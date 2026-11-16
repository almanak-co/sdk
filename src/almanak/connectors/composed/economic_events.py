"""Atomic timestamp batches retain each connector's native event and recovery history."""

from datetime import UTC, datetime

from almanak.backtest.engine import Transition
from almanak.connectors.composed.economic_inventory import shared_transition
from almanak.connectors.composed.economic_model import component_state
from almanak.connectors.composed.economic_state import ComponentEvent, Event, Position
from almanak.effects import Effects, PositionChange


def next_event(model, state, until):
    eligible = []
    for identity, driver in model.components.items():
        method = getattr(driver, "next_event", None)
        if method is not None:
            time = method(component_state(state, identity), until)
            if time is not None:
                eligible.append(time)
    return min(eligible) if eligible else None


def component_events(model, state, time):
    """Apply each connector's native event in order, each seeing the earlier ones."""
    working = state
    components = []
    assets = []
    for identity, driver in model.components.items():
        method = getattr(driver, "external_event", None)
        native = None if method is None else method(component_state(working, identity), time)
        if native is None:
            continue
        components.append(ComponentEvent(connector=identity, transition=native))
        shared = shared_transition(working, native)
        assets.extend(shared.effects.assets)
        working = working.model_copy(update={"portfolio": working.portfolio.apply(shared.effects)})
    return working, components, assets


def external_event(model, state, time):
    working, components, assets = component_events(model, state, time)
    if not components:
        return None
    before = {p.key: p for p in state.portfolio.positions}
    return Transition(
        effects=Effects[Position](
            assets=tuple(assets),
            positions=tuple(
                PositionChange[Position](before=before[p.key], after=p)
                for p in working.portfolio.positions
                if before[p.key] != p
            ),
        ),
        result=Event(effective_at=datetime.fromtimestamp(time, UTC), components=tuple(components)),
    )
