"""Composition boundary checks use configured markets, without claiming provider proofs."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.connectors.composed.authority import Capabilities, declared
from almanak.connectors.composed.economic_binding import ComposedEconomic
from almanak.connectors.composed.economic_inventory import held_position, shared_transition
from almanak.connectors.composed.economic_state import Position
from almanak.connectors.composed.inventory import initial_owned
from almanak.connectors.composed.models import OwnedParameters, wallet_key
from almanak.effects import AssetDelta, Effects, Portfolio
from almanak.triggers import IntervalSelection
from scripts.legacy_configuration.author import read_author
from scripts.legacy_configuration.backtest import read_backtest
from tests.composed_public import economic_inputs
from tests.legacy_references import harness_directory
from tests.test_composed_economic_inventory import transition

ROOT = Path(__file__).parents[1] / "strategies"


@pytest.fixture(
    params=[
        "base_aave_pancake_borrow_and_trade",
        "aerodrome_base_volatile_trading",
        "aerodrome_slipstream_base_trading",
    ]
)
def config(request):
    directory = ROOT / "base_aave_pancake_borrow_and_trade"
    _, value, _, _ = economic_inputs(directory.name)
    if request.param != directory.name:
        native = read_author(harness_directory(request.param) / "config.yaml").allocations[0]
        native = native.model_copy(
            update={"name": "trade", "account": value.allocation("trade").account}
        )
        value = value.model_copy(update={"allocations": (*value.allocations[:2], native)})
    return OwnedParameters.model_validate_json(value.model_dump_json())


def test_concrete_ownership_capabilities_and_shared_cash_survive_wire(config):
    caps = declared(config)
    assert Capabilities.model_validate_json(caps.model_dump_json()) == caps
    portfolio = Portfolio[Position](
        balances=config.inventory,
        positions=tuple(held_position(p) for p in initial_owned(config)),
    )
    assert Portfolio[Position].model_validate_json(portfolio.model_dump_json()) == portfolio
    target = config.target("loan")
    key = wallet_key(target, target.asset)
    change = shared_transition(
        SimpleNamespace(portfolio=portfolio),
        transition(Effects[Position](assets=(AssetDelta(key=key, amount=123),))),
    )
    changed = portfolio.apply(change.effects)
    assert changed.balance(key) == portfolio.balance(key) + 123
    owned = changed.positions[-1].payload.owned
    assert next(
        v.amount for v in (owned.inventory0, owned.inventory1) if v.asset == target.asset
    ) == changed.balance(key)
    assert type(owned.target) is type(config.target("trade"))


def test_shared_inventory_cannot_duplicate_fund_cash(config):
    raw = config.model_dump(mode="json")
    raw["inventory"].append(raw["inventory"][0])
    with pytest.raises(ValueError, match="duplicate"):
        OwnedParameters.model_validate_json(json.dumps(raw))


def test_native_prepared_policy_and_journal_keep_connector_identity(config):
    from almanak.connectors.composed.economic_config import EconomicConfig
    from almanak.connectors.composed.economic_model import component_state
    from almanak.connectors.composed.economic_prepare import native_preparation
    from almanak.connectors.composed.economic_state import State
    from almanak.connectors.identifiers import ConnectorId
    from almanak.connectors.target_routing import target_connector

    directory = ROOT / "base_aave_pancake_borrow_and_trade"
    _, _, settings, _ = economic_inputs(directory.name)
    identity = target_connector(config.target("trade"))
    if identity != ConnectorId("pancakeswap_v3"):
        reference = harness_directory(
            {
                ConnectorId("aerodrome"): "aerodrome_base_volatile_trading",
                ConnectorId("aerodrome_slipstream"): "aerodrome_slipstream_base_trading",
            }[identity]
        )
        native = read_backtest(
            reference / "backtest-economic.yaml", read_author(reference / "config.yaml")
        )
        raw = settings.model_dump(mode="json")
        raw["models"].pop("pancakeswap_v3")
        raw["models"][identity.value] = {
            name: native.model_dump(mode="json")[name]
            for name in ("data", "costs", "swap_model", "liquidity_model")
        }
        settings = EconomicConfig.model_validate_json(json.dumps(raw))
    selected = settings.component(identity, config.component(identity))
    assert selected.data.source == f"{identity.value}_archive"
    assert native_preparation(identity).__name__.endswith(f"{identity.value}.economic_prepare")
    interval = IntervalSelection(configured=config.decision_trigger)
    state = ComposedEconomic.initial(settings, config, interval)
    assert State.model_validate_json(state.model_dump_json()) == state
    view = component_state(state, identity)
    assert len(view.portfolio.positions) == 1
    assert type(view.portfolio.positions[0].payload.target) is type(config.target("trade"))


def test_swap_reserves_shared_fund_input(config):
    from almanak.connectors.aerodrome.operations import Swap as AerodromeSwap
    from almanak.connectors.aerodrome_slipstream.operations import Swap as SlipstreamSwap
    from almanak.connectors.composed.execution import spending
    from almanak.connectors.pancakeswap_v3.operations import Swap as PancakeSwap
    from almanak.connectors.target_routing import target_connector
    from almanak.models import AssetAmount

    target = config.target("trade")
    cls = {
        "pancakeswap_v3": PancakeSwap,
        "aerodrome": AerodromeSwap,
        "aerodrome_slipstream": SlipstreamSwap,
    }[target_connector(target).value]
    amount = AssetAmount(asset=target.funding_currency, amount=10)
    operation = cls(
        target=target,
        amount=amount,
        minimum=AssetAmount(asset=target.counter_currency, amount=1),
        deadline=1,
    )
    assert spending(operation) == (amount,)
