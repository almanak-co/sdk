"""Lending decisions read the fund's owned cash from the shared inventory."""

from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from almanak.backtest.engine import Simulator
from almanak.configuration.composed import Parameters
from almanak.configuration.yaml import load
from almanak.connectors.composed.economic_model import EconomicDriver
from almanak.connectors.composed.execution import validate_inventory
from almanak.connectors.composed.inventory import ownership
from almanak.connectors.composed.lifecycle import ComposedLifecycle
from almanak.connectors.composed.state import Checkpoint, DecisionInput
from almanak.connectors.composition import connector_for
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.morpho_blue.operations import SupplyOperation
from almanak.gateway.ethereum import DataError
from almanak.models import Account, AccountBinding, AssetAmount, Eoa, StrategyAccounts
from almanak.runtime import owned_for_decision
from almanak.strategy import StrategyDefinition
from almanak.strategy_api import Memory
from almanak.triggers import IntervalEvent
from almanak.worker_session import preloaded_workers
from scripts.legacy_configuration.author import normalize
from tests.aave_v3_helpers import recorded_v3 as recorded_v3
from tests.legacy_references import harness_directory
from tests.test_composed_economic_runner import FrozenLendingBook
from tests.test_composed_economic_runner import runner as runner
from tests.test_composed_inventory import checkpoint, forked

NOW = datetime(2026, 9, 7, 10, tzinfo=UTC)
# What the Aave withdrawal returned: the supplied principal plus its accrued interest.
WITHDRAWN = 1_003_412


def rotation_wire():
    """Aave V3 and Morpho Blue USDC supply allocations on one Base fund."""
    aave = load(harness_directory("aave_v3_base_lending") / "config.yaml").value
    morpho = load(harness_directory("morpho_blue_base_lending") / "config.yaml").value
    aave.pop("execution")
    aave["strategy"].pop("connector")
    supply = aave["strategy"]["allocations"]["loan"]
    supply.update(market="aave", supply_limit="1000", borrow_limit={"base_units": 0})
    earn = morpho["strategy"]["allocations"]["earn"]
    earn.update(market="morpho", account="fund")
    aave["strategy"]["allocations"] = {"aave": supply, "morpho": earn}
    aave["assets"].update(morpho["assets"])
    aave["markets"] = {
        "aave": {"connector": "aave_v3", "reserve": aave["markets"]["loan"]},
        "morpho": {"connector": "morpho_blue", "reserve": morpho["markets"]["earn"]},
    }
    return aave


@pytest.fixture
def config():
    author = normalize(rotation_wire())
    fund = Account(chain=author.author_accounts["fund"].chain, address="0x" + "12" * 20)
    return forked(
        Parameters(
            decision_trigger=author.decision_trigger,
            asset_metadata=author.asset_metadata,
            allocations=author.allocations,
            accounts=StrategyAccounts(
                bindings=(AccountBinding(name="fund", profile=Eoa(kind="eoa", fund=fund)),)
            ),
        )
    )


def withdrawn(config):
    """The checkpoint after a receipt-proven full withdrawal from Aave."""
    state = checkpoint(config)
    aave = config.target("aave")
    supplied = state.owned[0].model_copy(update={"supply_scaled": 950_000})
    state = state.model_copy(update={"owned": (supplied, *state.owned[1:])})
    outcome = SimpleNamespace(
        success=True,
        operation=SimpleNamespace(target=aave, kind="withdraw"),
        assets=WITHDRAWN,
        after=SimpleNamespace(
            position=SimpleNamespace(supplied_scaled=0, debt_scaled=0, collateral_enabled=False)
        ),
    )
    return Checkpoint.model_validate_json(
        state.model_copy(update=ownership(state, outcome)).model_dump_json()
    )


def rotate(ctx):
    cash = ctx.position("morpho").inventory
    return ctx.decision(intent=ctx.actions.supply("morpho", cash.amount))


def test_rotation_supplies_the_cash_the_withdrawal_returned(config):
    state = withdrawn(config)
    assert all(position.inventory is None for position in state.owned)
    assert "inventory" not in state.model_dump()["owned"][0]
    context = DecisionInput(
        config=state.config,
        memory=Memory(),
        owned=owned_for_decision(connector_for(state.config), state),
        observations=(),
        trigger=IntervalEvent(started_at=NOW, due_at=NOW),
    )
    usdc = config.target("aave").asset
    assert config.target("morpho").asset == usdc
    owned = AssetAmount(asset=usdc, amount=WITHDRAWN)
    assert context.position("aave").inventory == context.position("morpho").inventory == owned
    restored = DecisionInput.model_validate_json(context.model_dump_json())
    assert restored.position("morpho").inventory == owned
    intent = rotate(restored).intent
    assert intent.target == config.target("morpho")
    assert intent.amount == owned
    supply = SupplyOperation(target=intent.target, amount=intent.amount)
    validate_inventory(state, supply)
    more = supply.model_copy(update={"amount": AssetAmount(asset=usdc, amount=WITHDRAWN + 1)})
    with pytest.raises(DataError, match="retained fund inventory"):
        validate_inventory(state, more)


def test_checkpoint_refuses_a_retained_lending_record_with_cash(config):
    state = withdrawn(config)
    projected = ComposedLifecycle.decision_owned(state)
    with pytest.raises(ValueError, match="projected fund cash"):
        Checkpoint.model_validate_json(
            state.model_copy(update={"owned": projected}).model_dump_json()
        )


class AccruingBook(FrozenLendingBook):
    """Supply interest of one part per million each second from the first read."""

    def __init__(self):
        super().__init__()
        self.start = None

    def at(self, source, time):
        market = super().at(source, time)
        self.start = time if self.start is None else self.start
        growth = market.supply_index * (time - self.start) // 10**6
        return replace(market, supply_index=market.supply_index + growth)


RESUPPLY = """
from typing import cast

from almanak.api import StrategyState, declared


class State(StrategyState):
    phase: int = 0


def capabilities(config):
    return declared(config)


def decide(context):
    phase = cast(State, context.memory).phase
    if context.teardown:
        held = context.require_observation("collateral")
        if held.position.supplied_scaled or held.position.collateral_enabled or held.allowance:
            return context.decision(context.actions.withdraw("collateral"))
        return context.decision()
    if phase > 2:
        return context.decision()
    if phase == 1:
        return context.decision(context.actions.withdraw("collateral"))
    amount = context.config.allocation("collateral").capital
    if phase == 2:
        amount = context.position("collateral").inventory.amount
    return context.decision(context.actions.supply("collateral", amount))


def on_outcome(context):
    memory = cast(State, context.memory)
    if context.result.status != "completed":
        return memory.updated(phase=3)
    return memory.updated(phase=memory.phase + 1)
"""


def test_backtest_resupplies_the_principal_and_interest_a_withdrawal_returned(runner, tmp_path):
    journal, model = runner(tmp_path / "state.sqlite")
    model.book.components[ConnectorId("aave_v3")] = AccruingBook()
    strategy = StrategyDefinition(filename="strategy.py", source=RESUPPLY)
    model = EconomicDriver(model.config, model.settings, strategy, model.book)
    with preloaded_workers():
        result = Simulator(journal, model).run()
    supplied, withdrawal, resupplied = (c.transition.result for c in result.completed[:3])
    assert [r.intent.kind for r in (supplied, withdrawal, resupplied)] == [
        "supply",
        "withdraw",
        "supply",
    ]
    assert withdrawal.assets.amount > supplied.intent.amount.amount
    assert resupplied.intent.amount == withdrawal.assets


def test_single_connector_decisions_keep_their_retained_records(recorded_v3):
    connector = connector_for(recorded_v3.config)
    owned = owned_for_decision(connector, recorded_v3)
    assert owned is recorded_v3.owned
    context = connector.codec.decision_input(
        config=recorded_v3.config,
        memory=recorded_v3.memory,
        owned=owned,
        observations=recorded_v3.baseline,
        trigger=IntervalEvent(started_at=NOW, due_at=NOW),
    )
    assert all(position.inventory is None for position in context.owned)
