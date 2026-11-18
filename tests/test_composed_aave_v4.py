"""Aave V4 binding and cash conservation components, separate from fork evidence."""

from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.connectors.aave_v4.asset_effects import effects
from almanak.connectors.aave_v4.models import DebtAllocation, Ownership
from almanak.connectors.aave_v4.operations import BorrowOperation, RepayOperation
from almanak.connectors.composed.authority import declared, selected
from almanak.connectors.composed.economic_binding import ComposedEconomic
from almanak.connectors.composed.economic_inventory import shared_transition
from almanak.connectors.composed.economic_model import component_state, ownership
from almanak.connectors.composed.economic_state import State
from almanak.connectors.composed.models import OwnedParameters, wallet_key
from almanak.connectors.identifiers import ConnectorId
from almanak.effects import AssetDelta, Effects, PositionChange
from almanak.effects import Position as HeldPosition
from almanak.models import AssetAmount
from almanak.runtime import strategy_capabilities, validate_strategy_config
from almanak.strategy import StrategyDefinition
from tests.composed_public import economic_inputs
from tests.test_composed_economic_inventory import transition

AAVE = ConnectorId("aave_v4")


@pytest.fixture(params=["ethereum", "avalanche"])
def inputs(request):
    reference = (
        Path(__file__).parents[1] / "strategies" / f"{request.param}_aave_v4_borrow_and_trade"
    )
    _, config, settings, interval = economic_inputs(reference.name)
    state = ComposedEconomic.initial(settings, config, interval)
    return config, settings, state, StrategyDefinition.read(reference / "strategy.py")


def test_native_supply_and_debt_survive_wire_and_sandbox(inputs):
    config, settings, state, strategy = inputs
    assert OwnedParameters.model_validate_json(config.model_dump_json()) == config
    native = config.component(AAVE)
    assert native.allocations == (config.allocation("collateral"),)
    assert native.debts == (config.allocation("loan"),)
    assert native.debts[0] is config.allocation("loan")
    assert isinstance(native.debts[0], DebtAllocation)
    validate_strategy_config(strategy, config)
    caps = strategy_capabilities(strategy, config)
    assert caps == declared(config)
    assert {r.kind for r in selected(caps, AAVE).requirements} == {"aave_supply", "aave_borrow"}
    assert State.model_validate_json(state.model_dump_json()) == state
    assert len(component_state(state, AAVE).portfolio.positions) == 2
    assert sum(isinstance(o, Ownership) for o in ownership(state)) == 2
    assert settings.component(AAVE, native).data.source == "aave_v4_archive"


@pytest.mark.parametrize("fault", [None, "schedule", "interval", "targets", "key"])
def test_component_state_keeps_frozen_schedule_and_position_identity(inputs, fault):
    from almanak.connectors.composed.aave_v4_economic import Driver

    config, settings, state, strategy = inputs
    native = config.component(AAVE)
    driver = Driver(
        native,
        settings.component(AAVE, native),
        strategy,
        SimpleNamespace(),
        authority=selected(declared(config), AAVE),
    )
    state = component_state(state, AAVE)
    changes = {
        "schedule": {"schedule": state.schedule.model_copy(update={"end": state.schedule.end + 1})},
        "interval": {"interval": state.interval.model_copy(update={"override_seconds": 10})},
        "targets": {"portfolio": state.portfolio.model_copy(update={"positions": ()})},
        "key": {
            "portfolio": state.portfolio.model_copy(
                update={
                    "positions": (
                        state.portfolio.positions[0].model_copy(
                            update={"key": state.portfolio.positions[1].key}
                        ),
                        state.portfolio.positions[1],
                    )
                }
            )
        },
    }
    if fault is None:
        driver.validate_state(state)
    else:
        with pytest.raises(ValueError, match="economic"):
            driver.validate_state(state.model_copy(update=changes[fault]))


def test_borrow_is_one_cash_change_and_preserves_debt_payload(inputs):
    from almanak.backtest.economic_state import EconomicPosition

    config, _, state, _ = inputs
    before = next(
        position
        for position in state.portfolio.positions
        if isinstance(position.payload, EconomicPosition)
        and position.payload.target.allocation == "loan"
    )
    native = HeldPosition[EconomicPosition](key=before.key, payload=before.payload)
    after = native.model_copy(
        update={"payload": native.payload.model_copy(update={"drawn_shares": 5})}
    )
    key = wallet_key(config.target("loan"), config.target("loan").asset)
    effect = Effects[EconomicPosition](
        assets=(AssetDelta(key=key, amount=5),),
        positions=(PositionChange[EconomicPosition](before=native, after=after),),
    )
    changed = shared_transition(state, transition(effect))
    portfolio = state.portfolio.apply(changed.effects)
    assert portfolio.balance(key) == state.portfolio.balance(key) + 5
    assert next(p.payload.drawn_shares for p in portfolio.positions if p.key == before.key) == 5
    cash = next(p.payload.owned for p in portfolio.positions if hasattr(p.payload, "owned"))
    assert next(
        a.amount for a in (cash.inventory0, cash.inventory1) if a.asset == key.asset
    ) == portfolio.balance(key)
    assert type(portfolio).model_validate_json(portfolio.model_dump_json()) == portfolio


@pytest.mark.parametrize("operation,sign", [(BorrowOperation, 1), (RepayOperation, -1)])
def test_native_cash_uses_reconciled_effect_and_excludes_external_wallet_delta(
    inputs, operation, sign
):
    config, _, _, _ = inputs
    target = config.target("loan")
    outcome = SimpleNamespace(
        success=True,
        operation=operation(target=target, amount=AssetAmount(asset=target.asset, amount=10)),
        assets=9,
        before=SimpleNamespace(wallet_balance=100),
        after=SimpleNamespace(wallet_balance=9876),
    )
    assert effects(outcome) == (AssetDelta(key=wallet_key(target, target.asset), amount=sign * 9),)
    outcome.success = False
    assert effects(outcome) == ()


def test_missing_reverse_input_does_not_erase_debt_or_cash(inputs):
    from almanak.backtest.economic_state import EconomicPosition

    _, _, state, _ = inputs
    loan = next(
        position
        for position in state.portfolio.positions
        if isinstance(position.payload, EconomicPosition)
        and position.payload.target.allocation == "loan"
    )
    debt = loan.model_copy(update={"payload": loan.payload.model_copy(update={"drawn_shares": 17})})
    state = state.model_copy(
        update={
            "portfolio": state.portfolio.model_copy(
                update={
                    "positions": tuple(
                        debt if position.key == loan.key else position
                        for position in state.portfolio.positions
                    )
                }
            )
        }
    )
    failed = shared_transition(state, transition(Effects[EconomicPosition]()))
    assert state.portfolio.apply(failed.effects) == state.portfolio
    assert next(o.drawn_shares for o in ownership(state) if o.target.allocation == "loan") == 17
