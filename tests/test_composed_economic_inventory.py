"""Synthetic economic ledger checks; these are not historical or fork evidence."""

from types import SimpleNamespace

import pytest

from almanak.connectors.aave_v3.economic_state import Position as LendingPosition
from almanak.connectors.composed.economic_inventory import (
    held_position,
    shared_transition,
    validate_views,
)
from almanak.connectors.composed.economic_state import Position
from almanak.connectors.composed.inventory import initial_owned
from almanak.connectors.composed.models import wallet_key
from almanak.effects import AssetDelta, Effects, Portfolio, PositionChange
from almanak.effects import Position as HeldPosition
from tests.test_composed_inventory import config as config


def portfolio(config):
    return Portfolio[Position](
        balances=config.inventory,
        positions=tuple(held_position(p) for p in initial_owned(config)),
    )


def transition(effects):
    from almanak.backtest.engine import Transition

    return Transition[Position, str](effects=effects, result="component-only")


def apply(state, effects):
    changed = shared_transition(SimpleNamespace(portfolio=state), transition(effects))
    result = state.apply(changed.effects)
    validate_views(result)
    return Portfolio[Position].model_validate_json(result.model_dump_json())


def test_native_borrow_effect_updates_shared_cash_and_exchange_view(config):
    state = portfolio(config)
    before = state.positions[1]
    target = before.payload.target
    # Native generic effects must compare equal against the composed journal's position.
    native_before = HeldPosition[LendingPosition](key=before.key, payload=before.payload)
    native_after = native_before.model_copy(
        update={"payload": before.payload.model_copy(update={"debt_scaled": 100})}
    )
    effects = Effects[LendingPosition](
        assets=(AssetDelta(key=wallet_key(target, target.asset), amount=100),),
        positions=(PositionChange[LendingPosition](before=native_before, after=native_after),),
    )
    state = apply(state, effects)
    assert state.positions[1].payload.debt_scaled == 100
    view = state.positions[-1].payload.owned
    assert (
        next(a.amount for a in (view.inventory0, view.inventory1) if a.asset == target.asset) == 100
    )
    assert state.balance(wallet_key(target, target.asset)) == 100

    trade = config.target("position")
    spent = Effects[Position](
        assets=(
            AssetDelta(key=wallet_key(target, target.asset), amount=-100),
            AssetDelta(key=wallet_key(trade, trade.counter_currency), amount=2),
        )
    )
    state = apply(state, spent)
    assert state.balance(wallet_key(target, target.asset)) == 0
    with pytest.raises(ValueError, match="insufficient"):
        apply(state, spent)


def test_economic_views_reject_invented_cash(config):
    state = portfolio(config)
    held = state.positions[-1]
    owned = held.payload.owned
    forged = owned.model_copy(
        update={"inventory0": owned.inventory0.model_copy(update={"amount": 1})}
    )
    state = state.model_copy(
        update={
            "positions": (
                *state.positions[:-1],
                held.model_copy(
                    update={"payload": held.payload.model_copy(update={"owned": forged})}
                ),
            )
        }
    )
    with pytest.raises(ValueError, match="shared fund cash"):
        validate_views(state)


def test_native_effect_cannot_remove_configured_ownership(config):
    state = portfolio(config)
    effects = Effects[Position](
        positions=(PositionChange[Position](before=state.positions[0], after=None),)
    )
    with pytest.raises(ValueError, match="configured position identities"):
        apply(state, effects)
