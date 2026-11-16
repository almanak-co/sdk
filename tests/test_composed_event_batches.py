"""Synthetic missing-input events verify atomic connector batches and scoped delivery."""

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from almanak.backtest.dataset import DataKey, InputUnavailable
from almanak.backtest.engine import Transition
from almanak.connectors.composed.economic_binding import ComposedEconomic
from almanak.connectors.composed.economic_events import external_event, next_event
from almanak.connectors.composed.economic_state import ComponentEvent, Event, Position
from almanak.connectors.compound_v3.economic_state import Event as CompoundEvent
from almanak.connectors.compound_v3.economic_state import FundInputGap
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.morpho_blue.economic_data import key
from almanak.connectors.morpho_blue.economic_state import Event as MorphoEvent
from almanak.connectors.morpho_blue.economic_state import MarketInputGap
from almanak.effects import Effects
from almanak.strategy_api import SimulationEvidence
from tests.test_composed_lending_prepared import prepared as prepared


def missing_events(target, time):
    morpho_gap = InputUnavailable(key(target.reserve.market, "risk"), time, "synthetic gap").gap
    compound_gap = InputUnavailable(
        DataKey(
            connector=ConnectorId("compound_v3"),
            chain=target.fund.chain,
            source="synthetic-comet",
            product="risk",
        ),
        time,
        "synthetic gap",
    ).gap
    evidence = SimulationEvidence(
        execution_id="synthetic-event-batch", model="synthetic", inputs=()
    )
    return (
        MorphoEvent(
            kind="input_unavailable",
            effective_at=datetime.fromtimestamp(time, UTC),
            market_changes=(),
            liquidations=(),
            unavailable=(morpho_gap,),
            affected_markets=(
                MarketInputGap(source=target.reserve.market, gap=morpho_gap, fund=target.fund),
            ),
            simulation=evidence,
        ),
        CompoundEvent(
            kind="input_unavailable",
            effective_at=datetime.fromtimestamp(time, UTC),
            absorptions=(),
            unavailable=(compound_gap,),
            affected_funds=(FundInputGap(fund=target.fund, gap=compound_gap),),
            simulation=evidence,
        ),
    )


@pytest.mark.parametrize("prepared", ["morpho"], indirect=True)
def test_two_native_events_share_one_atomic_timestamp_without_cross_typed_records(prepared):
    p = prepared
    state = ComposedEconomic.initial(p.settings, p.config, p.interval)
    time = p.settings.schedule.start + 1
    events = missing_events(p.config.target("loan"), time)
    identities = (ConnectorId("morpho_blue"), ConnectorId("compound_v3"))
    transitions = tuple(Transition(effects=Effects[Position](), result=event) for event in events)
    calls = []

    def driver(identity, transition):
        def emit(view, at):
            calls.append(identity)
            assert at == time
            assert all(held.key.connector == identity for held in view.portfolio.positions)
            return transition

        return SimpleNamespace(next_event=lambda view, until: time, external_event=emit)

    model = SimpleNamespace(
        components={
            identity: driver(identity, transition)
            for identity, transition in zip(identities, transitions, strict=True)
        }
    )
    assert next_event(model, state, time) == time
    batch = external_event(model, state, time)
    assert calls == list(identities)
    assert state.portfolio.apply(batch.effects) == state.portfolio
    assert tuple(c.transition.result for c in batch.result.components) == events
    restored = Event.model_validate_json(batch.result.model_dump_json())
    assert restored == batch.result
    with pytest.raises(ValueError, match="connector identity"):
        ComponentEvent(connector=identities[1], transition=transitions[0])
    with pytest.raises(ValueError, match="only once"):
        Event(
            effective_at=restored.effective_at,
            components=(restored.components[0], restored.components[0]),
        )
    with pytest.raises(ValueError, match="logical timestamp"):
        Event(effective_at=datetime.fromtimestamp(time + 1, UTC), components=restored.components)
