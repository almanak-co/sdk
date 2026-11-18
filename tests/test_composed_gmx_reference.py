"""The mixed reference keeps GMX escrow separate from its one shared cash ledger."""

from pathlib import Path

import pytest

from almanak.connectors.composed.authority import Capabilities, declared
from almanak.connectors.composed.economic_binding import ComposedEconomic
from almanak.connectors.composed.economic_inventory import shared_transition, validate_views
from almanak.connectors.composed.economic_state import Position, State
from almanak.connectors.composed.models import Config, wallet_key
from almanak.connectors.gmx_v2.economic_state import Escrow
from almanak.connectors.gmx_v2.exchange_domain import ExchangeTarget
from almanak.connectors.identifiers import ConnectorId
from almanak.dex_intents import SwapExactInput
from almanak.effects import AssetDelta, Effects, PositionChange
from almanak.runtime import validate_strategy_config
from almanak.strategy import StrategyDefinition
from tests.composed_public import economic_inputs
from tests.test_composed_economic_inventory import transition

ROOT = Path(__file__).parents[1] / "strategies"


@pytest.fixture(params=("arbitrum", "avalanche"))
def reference(request):
    return ROOT / f"{request.param}_aave_borrow_and_trade_gmx"


@pytest.fixture
def setup(reference):
    author, config, settings, interval = economic_inputs(reference.name)
    return author, settings, config, ComposedEconomic.initial(settings, config, interval)


def test_actual_reference_validates_and_retains_explicit_policies(setup, reference):
    author, settings, parameters, state = setup
    validate_strategy_config(StrategyDefinition.read(reference / "strategy.py"), author)
    assert type(settings).model_validate_json(settings.model_dump_json()) == settings
    assert State.model_validate_json(state.model_dump_json()) == state
    config = Config.model_construct(
        **{n: getattr(parameters, n) for n in type(parameters).model_fields}
    )
    caps = declared(config)
    assert Capabilities.model_validate_json(caps.model_dump_json()) == caps
    assert config.target("loan").fund == config.target("trade").fund
    assert set(config.execution_components) == {ConnectorId("aave_v3"), ConnectorId("gmx_v2")}


def test_borrowed_cash_refreshes_gmx_view_without_expanding_protocol_limits(setup):
    _, _, config, state = setup
    target = config.target("loan")
    key = wallet_key(target, target.asset)
    before = state.portfolio.positions[-1].payload
    change = shared_transition(
        state, transition(Effects[Position](assets=(AssetDelta(key=key, amount=100),)))
    )
    portfolio = state.portfolio.apply(change.effects)
    after = portfolio.positions[-1].payload
    assert portfolio.balance(key) == state.portfolio.balance(key) + 100
    assert next(
        v.amount for v in after.owned.inventory if v.asset == key.asset
    ) == portfolio.balance(key)
    assert (after.spent, after.allowances, after.escrow) == (
        before.spent,
        before.allowances,
        before.escrow,
    )
    validate_views(portfolio)


def test_admitted_escrow_survives_shared_cash_refresh_and_restart(setup):
    _, _, config, state = setup
    held = state.portfolio.positions[-1]
    allocation = config.allocation("trade")
    intent = SwapExactInput[ExchangeTarget](
        target=held.payload.target,
        amount=allocation.entry_amount,
        minimum_output=allocation.entry_minimum_output,
        deadline=state.time + 300,
    )
    escrow = Escrow(
        intent=intent,
        identity="retained-admission",
        accepted_at=state.time,
        inputs=(allocation.entry_amount,),
        execution_fee=allocation.execution_fee,
    )
    payload = held.payload.model_copy(update={"escrow": escrow})
    changed = held.model_copy(update={"payload": payload})
    amount = allocation.entry_amount.amount + allocation.execution_fee.amount
    effects = Effects[Position](
        assets=(
            AssetDelta(
                key=wallet_key(held.payload.target, allocation.entry_amount.asset), amount=-amount
            ),
        ),
        positions=(PositionChange[Position](before=held, after=changed),),
    )
    change = shared_transition(state, transition(effects))
    resumed = state.model_copy(update={"portfolio": state.portfolio.apply(change.effects)})
    restored = State.model_validate_json(resumed.model_dump_json())
    assert restored.portfolio.positions[-1].payload.escrow == escrow
    assert (
        restored.portfolio.balance(wallet_key(held.payload.target, allocation.entry_amount.asset))
        == state.portfolio.balance(wallet_key(held.payload.target, allocation.entry_amount.asset))
        - amount
    )
    validate_views(restored.portfolio)


def test_keeper_continuation_uses_connector_timing_and_only_its_positions(setup):
    from types import SimpleNamespace

    from almanak.connectors.composed.economic_model import EconomicDriver

    _, _, config, state = setup
    target = config.target("trade")
    pending = SimpleNamespace(intent=SimpleNamespace(target=target))
    seen = []

    class KeeperDriver:
        def continuation_at(self, scoped, retained):
            seen.append((scoped, retained))
            return state.time + 7

    driver = EconomicDriver.__new__(EconomicDriver)
    driver.config = config
    driver.components = {ConnectorId("gmx_v2"): KeeperDriver()}
    assert driver.continuation_at(state, pending) == state.time + 7
    scoped, retained = seen[0]
    assert retained is pending
    assert len(scoped.portfolio.positions) == 1
    assert scoped.portfolio.positions[0].payload.target == target
    assert scoped.portfolio.balances == state.portfolio.balances


def test_composed_dashboard_keeps_gmx_protocol_facts_distinct_from_shared_cash(setup):
    from almanak.connectors.composed.dashboard import gmx_datasets
    from almanak.connectors.composed.inventory import initial_owned
    from almanak.connectors.composed.state import Checkpoint

    _, _, parameters, _ = setup
    config = Config.model_construct(
        **{n: getattr(parameters, n) for n in type(parameters).model_fields}
    )
    state = Checkpoint.model_construct(
        config=config, capabilities=declared(config), owned=initial_owned(config), delivered=0
    )
    datasets = gmx_datasets(state)
    assert set(datasets) == {"gmx_positions", "gmx_requests", "gmx_shares"}
    assert datasets["gmx_positions"].rows == datasets["gmx_requests"].rows == ()
    assert len(datasets["gmx_shares"].rows) == 1
    assert datasets["gmx_shares"].rows[0]["asset"] == str(config.target("trade").market.share_asset)
    assert "same holding" in datasets["gmx_shares"].coverage
