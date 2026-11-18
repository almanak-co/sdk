"""Spark identity and shared inventory survive composition without Aave coercion."""

from pathlib import Path

import pytest

from almanak.connectors.composed.authority import Capabilities, declared, selected
from almanak.connectors.composed.economic_binding import ComposedEconomic
from almanak.connectors.composed.economic_inventory import shared_transition
from almanak.connectors.composed.economic_model import component_state, ownership
from almanak.connectors.composed.economic_state import State
from almanak.connectors.composed.models import OwnedParameters, wallet_key
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.sparklend import economic_state as spark
from almanak.effects import AssetDelta, Effects, PositionChange
from almanak.effects import Position as HeldPosition
from tests.composed_public import economic_inputs
from tests.test_composed_economic_inventory import transition

REFERENCE = Path(__file__).parents[1] / "strategies/ethereum_spark_curve_borrow_and_trade"
SPARK = ConnectorId("sparklend")


@pytest.fixture
def inputs():
    _, config, settings, interval = economic_inputs(REFERENCE.name)
    return config, settings, ComposedEconomic.initial(settings, config, interval)


def test_spark_wire_and_capability_views_keep_native_identity(inputs):
    config, settings, state = inputs
    restored = OwnedParameters.model_validate_json(config.model_dump_json())
    assert restored == config
    assert config.component(SPARK).allocations[0] is config.allocations[0]
    assert config.target("loan").fund == config.target("trade").fund
    caps = declared(config)
    assert Capabilities.model_validate_json(caps.model_dump_json()) == caps
    assert all(r.target.reserve.kind == "sparklend" for r in selected(caps, SPARK).requirements)
    assert type(settings.component(SPARK, config.component(SPARK))).__module__.endswith(
        "sparklend.economic_config"
    )
    assert State.model_validate_json(state.model_dump_json()) == state
    assert isinstance(component_state(state, SPARK), spark.State)
    assert all(type(p).__module__.endswith("sparklend.observations") for p in ownership(state)[:2])


def test_native_spark_borrow_changes_one_cash_balance_and_curve_view(inputs):
    config, _, state = inputs
    before = state.portfolio.positions[1]
    key = wallet_key(before.payload.target, before.payload.target.asset)
    previous = state.portfolio.balance(key)
    native = HeldPosition[spark.Position](key=before.key, payload=before.payload)
    after = native.model_copy(
        update={"payload": native.payload.model_copy(update={"debt_scaled": 5})}
    )
    effects = Effects[spark.Position](
        assets=(AssetDelta(key=key, amount=5),),
        positions=(PositionChange[spark.Position](before=native, after=after),),
    )
    changed = shared_transition(state, transition(effects))
    portfolio = state.portfolio.apply(changed.effects)
    assert portfolio.balance(key) == previous + 5
    assert portfolio.positions[1].payload.debt_scaled == 5
    assert (
        next(
            v.amount
            for v in portfolio.positions[-1].payload.owned.inventory.root
            if v.asset == key.asset
        )
        == previous + 5
    )
    reconstructed = type(state.portfolio).model_validate_json(portfolio.model_dump_json())
    assert reconstructed == portfolio
    assert isinstance(reconstructed.positions[1].payload, spark.Position)


def test_spark_policy_cannot_be_relabelled_as_aave(inputs):
    _, settings, _ = inputs
    raw = settings.model_dump(mode="json")
    raw["models"]["aave_v3"] = raw["models"].pop("sparklend")
    with pytest.raises(ValueError):
        type(settings).model_validate(raw)
