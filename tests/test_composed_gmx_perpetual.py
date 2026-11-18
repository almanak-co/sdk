"""Shared cash refresh preserves hypothetical perpetual obligations and request identity."""

from almanak.connectors.composed.economic_model import ownership
from almanak.connectors.composed.economic_state import Position
from almanak.connectors.composed.models import wallet_key
from almanak.connectors.gmx_v2.economic_parameters import position_key
from almanak.effects import AssetDelta, Balance, Effects, Portfolio
from almanak.effects import Position as HeldPosition
from almanak.models import AssetAmount
from tests.test_composed_economic_inventory import apply
from tests.test_gmx_economic_perpetual_state import position as position


def test_composed_liquidation_event_retains_connector_scope_and_pending_request(position):
    from datetime import UTC, datetime

    import pytest

    from almanak.backtest.engine import Transition
    from almanak.connectors.composed.economic_state import ComponentEvent, Event
    from almanak.connectors.gmx_v2.economic_perpetual_state import (
        EconomicLiquidationBatch,
        EconomicLiquidationResult,
        SimulatedPosition,
    )
    from almanak.connectors.identifiers import ConnectorId
    from almanak.effects import PositionChange
    from almanak.strategy_api import SimulationEvidence

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
    event = Event(effective_at=result.effective_at, components=(component,))
    assert Event.model_validate_json(event.model_dump_json()) == event
    assert event.components[0].transition.result.results[0].after.escrow == before.escrow
    with pytest.raises(ValueError, match="connector identity"):
        ComponentEvent(connector=ConnectorId("aave_v3"), transition=transition)


def test_composed_initialization_uses_perpetual_connector_binding():
    from types import SimpleNamespace

    from almanak.connectors.composed.economic_inventory import held_position
    from almanak.connectors.gmx_v2.economic_perpetual_state import EconomicPerpetualPosition
    from tests.gmx_evidence import checkpoint

    captured = checkpoint()
    native = captured.baseline[0].owned
    held = held_position(native, SimpleNamespace(component=lambda _: captured.config))
    assert isinstance(held.payload, EconomicPerpetualPosition)
    assert held.payload.owned.inventory == native.inventory
    assert held.payload.owned.position is None
    assert held.payload.escrow is None
    assert HeldPosition[Position].model_validate_json(held.model_dump_json()) == held


def test_shared_cash_refresh_keeps_perpetual_escrow_claims_and_authority(position):
    from types import SimpleNamespace

    inventory = tuple(AssetAmount(asset=a, amount=50) for a in position.target.assets)
    claim = position.owned.claimable_funding[0].model_copy(update={"amount": 7})
    deferred = position.owned.deferred_collateral[1].model_copy(update={"amount": 9})
    owned = position.owned.model_copy(
        update={
            "inventory": inventory,
            "claimable_funding": (claim, position.owned.claimable_funding[1]),
            "deferred_collateral": (position.owned.deferred_collateral[0], deferred),
        }
    )
    payload = position.model_copy(update={"owned": owned})
    portfolio = Portfolio[Position](
        balances=tuple(
            Balance(key=wallet_key(position.target, a.asset), amount=a.amount) for a in inventory
        ),
        positions=(HeldPosition[Position](key=position_key(position.target), payload=payload),),
    )
    asset = position.target.collateral_asset
    changed = apply(
        portfolio,
        Effects[Position](assets=(AssetDelta(key=wallet_key(position.target, asset), amount=11),)),
    )
    restored = changed.positions[0].payload
    assert next(a.amount for a in restored.owned.inventory if a.asset == asset) == 61
    assert restored.escrow == payload.escrow
    assert (restored.spent, restored.spent_notional_usd_e30, restored.allowances) == (
        payload.spent,
        payload.spent_notional_usd_e30,
        payload.allowances,
    )
    assert restored.owned.claimable_funding == owned.claimable_funding
    assert restored.owned.deferred_collateral == owned.deferred_collateral
    assert ownership(SimpleNamespace(portfolio=changed)) == (restored.owned,)
