"""A composed GMX liquidation batch is accounting only: no author hook, unchanged memory."""

from datetime import UTC, datetime
from types import SimpleNamespace

from almanak.backtest.engine import Transition
from almanak.connectors.composed import economic_model
from almanak.connectors.composed.economic_model import ACCOUNTING_EVENTS, EconomicDriver
from almanak.connectors.composed.economic_state import ComponentEvent, Event, Position
from almanak.connectors.gmx_v2.economic_parameters import position_key
from almanak.connectors.gmx_v2.economic_perpetual_state import (
    EconomicLiquidationBatch,
    EconomicLiquidationResult,
    SimulatedPosition,
)
from almanak.connectors.identifiers import ConnectorId
from almanak.effects import Effects, PositionChange
from almanak.effects import Position as HeldPosition
from almanak.strategy_api import Memory, SimulationEvidence
from tests.test_gmx_economic_perpetual_state import position as position


def liquidation_event(position):
    held = SimulatedPosition(
        identity="hypothetical-held",
        size_usd_e30=100,
        index_amount=position.escrow.intent.collateral,
        collateral=position.escrow.intent.collateral,
        pending_impact_amount=0,
        borrowing_factor=0,
        funding_fee_amount_per_size=0,
        long_claimable_funding_amount_per_size=0,
        short_claimable_funding_amount_per_size=0,
        increased_at=1,
        decreased_at=0,
    )
    before = position.model_copy(
        update={"owned": position.owned.model_copy(update={"position": held})}
    )
    result = EconomicLiquidationResult(
        allocation=position.target.allocation,
        effective_at=datetime.fromtimestamp(2, UTC),
        simulation=SimulationEvidence(execution_id="event-1", model="component-test", inputs=()),
        before=before,
        after=position,
        assets=(),
        reason="min collateral",
        unpaid_usd_e30=7,
        fees_collected=False,
    )
    key = position_key(position.target)
    transition = Transition(
        effects=Effects[Position](
            positions=(
                PositionChange[Position](
                    before=HeldPosition[Position](key=key, payload=before),
                    after=HeldPosition[Position](key=key, payload=position),
                ),
            )
        ),
        result=EconomicLiquidationBatch(results=(result,)),
    )
    component = ComponentEvent(connector=ConnectorId("gmx_v2"), transition=transition)
    return Event(effective_at=result.effective_at, components=(component,))


def test_composed_liquidation_batch_leaves_memory_untouched_without_an_author_hook(
    position, monkeypatch
):
    def forbidden(*args, **kwargs):
        raise AssertionError("composed liquidation delivery must not invoke the author hook")

    monkeypatch.setattr(economic_model, "strategy_worker", forbidden)
    memory = Memory(values={"stage": 3})
    state = SimpleNamespace(memory=memory)
    driver = SimpleNamespace(config=None, strategy=None)
    event = liquidation_event(position)
    assert Event.model_validate_json(event.model_dump_json()) == event
    assert event.components[0].transition.result.results[0].unpaid_usd_e30 == 7
    assert EconomicDriver.deliver_event(driver, state, event) == memory
    assert "perpetual_liquidations" in ACCOUNTING_EVENTS


def test_composed_delivery_still_routes_other_event_kinds_to_the_hook(position, monkeypatch):
    calls = []

    def hook(request):
        calls.append(request)
        return Memory(values={"seen": True}).model_dump_json()

    monkeypatch.setattr(economic_model, "strategy_worker", hook)
    monkeypatch.setattr(economic_model, "EventRequest", lambda **kwargs: kwargs)
    monkeypatch.setattr(economic_model, "EventInput", lambda **kwargs: kwargs)
    event = liquidation_event(position)
    foreign = SimpleNamespace(
        components=(
            SimpleNamespace(transition=SimpleNamespace(result=SimpleNamespace(kind="liquidation"))),
        )
    )
    state = SimpleNamespace(memory=Memory())
    driver = SimpleNamespace(config=None, strategy=None)
    assert EconomicDriver.deliver_event(driver, state, foreign) == Memory(values={"seen": True})
    assert len(calls) == 1
    assert EconomicDriver.deliver_event(driver, state, event) == Memory()
    assert len(calls) == 1
