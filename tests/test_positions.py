"""Position identity, shared funding and strategy transitions without a provider."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from almanak.connectors.aave_v4.fork import ForkConfig
from almanak.connectors.aave_v4.models import DecisionRecord, LendingConfig, Memory, Ownership
from almanak.connectors.aave_v4.reader import plan_operation
from almanak.connectors.composition import connector_for
from almanak.gateway.ethereum import DataError
from almanak.gateway.fork_execution import ForkGateway
from almanak.gateway.fork_fixture import Fork
from almanak.runtime import validate_initial_balances
from almanak.state import Journal, Lease
from almanak.strategy import StrategyDefinition
from tests.reference_parameters import authored_values
from tests.test_runtime_recovery import CapturedGateway, coordinator, tick_until

ROOT = Path(__file__).parents[1]


@pytest.fixture
def multiple(recorded):
    first = recorded.config.allocations[0]
    second = first.model_copy(
        update={
            "name": "bluechip_weth",
            "reserve": first.reserve.model_copy(
                update={
                    "spoke": "0x973a023a77420ba610f06b3858ad991df6d85a08",
                    "hub": "0x943827dca022d0f354a8a8c332da1e5eb9f9f931",
                }
            ),
        }
    )
    return LendingConfig.model_validate(
        {
            **recorded.config.model_dump(),
            "allocations": (first, second),
        }
    )


def test_two_allocations_in_one_wallet_retain_distinct_protocol_scope(multiple):
    first, second = (multiple.target(a.name) for a in multiple.allocations)
    assert first.fund == second.fund
    assert first.identity != second.identity
    assert first.reserve.asset == second.reserve.asset
    assert multiple.withdraw(second.allocation).target == second


@pytest.mark.parametrize("change", ["name", "position", "conflicting_hub", "conflicting_asset"])
def test_aliases_cannot_invent_independent_supply_balances(multiple, change):
    first, second = multiple.allocations
    if change == "name":
        second = second.model_copy(update={"name": first.name})
    else:
        reserve = first.reserve
        if change == "conflicting_hub":
            reserve = reserve.model_copy(update={"hub": second.reserve.hub})
        if change == "conflicting_asset":
            from almanak.caip import AssetType

            reserve = reserve.model_copy(
                update={"asset": AssetType("eip155:31337/erc20:0x" + "11" * 20)}
            )
        second = second.model_copy(update={"reserve": reserve})
    with pytest.raises(ValidationError, match="unique|alias"):
        LendingConfig.model_validate({**multiple.model_dump(), "allocations": (first, second)})


def test_wallet_funding_covers_sum_of_its_allocations(recorded, multiple):
    owned = tuple(Ownership(target=multiple.target(a.name), shares=0) for a in multiple.allocations)
    state = recorded.model_copy(update={"config": multiple, "mode": "running", "owned": owned})
    baseline = recorded.baseline[0]
    observed = tuple(snapshot_for(baseline, item.target) for item in owned)
    validate_initial_balances(state, observed)
    insufficient = tuple(
        item.model_copy(update={"wallet_balance": multiple.allocations[0].capital})
        for item in observed
    )
    with pytest.raises(RuntimeError, match="combined allocation capital"):
        validate_initial_balances(state, insufficient)


def snapshot_for(snapshot, target):
    reserve = snapshot.position.reserve.model_copy(update={"source": target.reserve})
    position = snapshot.position.model_copy(update={"account": target.fund, "reserve": reserve})
    return snapshot.model_copy(
        update={"allocation": target.allocation, "account": target.account, "position": position}
    )


def test_fork_funds_shared_wallet_once(tmp_path, monkeypatch, reference_strategy):
    from tests.fork.eoa import configuration

    config = ForkConfig.model_validate_json(json.dumps(configuration()))
    pinned = ForkConfig.model_validate_json(
        (ROOT / "tests/fixtures/aave-v4-interval-config.json").read_text()
    )
    config = config.resolved(pinned.origin)
    calls = []
    node = object.__new__(Fork)
    node.origin = config.origin
    node.metadata = {"instanceId": "0x" + "12" * 32}
    node.rpc = SimpleNamespace(endpoint="http://127.0.0.1:1234")

    def fund(*args, **kwargs):
        assert kwargs == {"plain_account": True}
        calls.append(args)
        return SimpleNamespace(address="0x" + "34" * 20, key=bytes.fromhex("56" * 32))

    node.funded_signer = fund
    deployment = node.prepare(config, tmp_path / "deployment")
    # The legacy fixture deployment predates strategy.parameters; the knobs are the reference's.
    deployment = deployment.model_copy(
        update={"parameters": authored_values("aave_v4_weth_supply")}
    )
    assert len(calls) == len(deployment.accounts.bindings) == 1
    assert len(deployment.allocations) == 2
    assert calls[0][1] == config.funds[0].token_balance
    keys = json.loads((tmp_path / "deployment/fork-keys.json").read_text())
    assert set(keys) == {config.funds[0].account}
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    journal = Journal(tmp_path / "state.sqlite")
    journal.create(deployment, reference_strategy)
    lease = Lease(journal, journal.load())
    assert len(lease.files) == 3
    lease.close()
    Lease(journal, journal.load()).close()


def test_missing_memory_admits_a_repeated_supply_and_keeps_ownership(recorded):
    allocation = recorded.config.allocations[0]
    intent = recorded.config.supply(allocation.name, allocation.capital)
    before = recorded.closure[0]
    decision = DecisionRecord(
        identity="ab" * 16,
        trigger=recorded.outcomes[0].decision.trigger,
        intent=intent,
        observations=(before,),
        memory=Memory(),
    )
    state = recorded.model_copy(
        update={"mode": "running", "memory": Memory(), "decision": decision}
    )
    backend = SimpleNamespace(
        config=recorded.config,
        connector=connector_for(recorded.config),
        reader=SimpleNamespace(chain=before.target.fund.chain),
        guard=lambda: None,
    )
    # Scope-only authority (blueprint 27): a repeated supply needs no budget, so lost
    # memory neither blocks nor unlocks it, and retained ownership is unchanged.
    ForkGateway.validate_operation(backend, state, plan_operation(intent, before), before)
    assert state.owned == recorded.owned


def test_no_action_memory_changes_survive_restart(tmp_path, recorded, reference_strategy):
    definition = StrategyDefinition(
        filename="strategy.py",
        source=reference_strategy.source
        + """
from almanak.connectors.aave_v4.models import Decision, Memory

class State(State):
    ticks: int = 0

def decide(context):
    return Decision(memory=context.memory.updated(ticks=context.memory.ticks + 1))
""",
    )
    journal = Journal(tmp_path / "state.sqlite")
    journal.create(recorded.config, definition)
    gateway = CapturedGateway(journal, recorded)
    runner = coordinator(journal, gateway)
    runner.tick()
    runner.tick()
    assert journal.load().memory.values["ticks"] == 1
    runner = coordinator(Journal(journal.path), gateway)
    runner.tick()
    assert journal.load().memory.values["ticks"] == 1
    runner.clock = lambda: journal.load().schedule.next_due_at
    runner.tick()
    assert journal.load().memory.values["ticks"] == 2
    assert gateway.broadcasts == []
    assert all(item.shares == 0 for item in journal.load().owned)


@pytest.mark.parametrize("stage", ["outcome", "transition"])
def test_outcome_memory_commit_is_applied_once(tmp_path, recorded, reference_strategy, stage):
    definition = reference_strategy.model_copy(
        update={
            "source": reference_strategy.source
            + """
class State(State):
    deliveries: int = 0

_original_outcome = on_outcome

def on_outcome(context):
    memory = _original_outcome(context)
    return memory.updated(deliveries=memory.deliveries + 1)
"""
        }
    )
    journal = Journal(tmp_path / "state.sqlite")
    journal.create(recorded.config, definition)
    gateway = CapturedGateway(journal, recorded)

    def crash(actual, outcome):
        if actual == stage and outcome.operation.kind == "supply":
            raise InterruptedError(stage)

    with pytest.raises(InterruptedError):
        tick_until(coordinator(journal, gateway, crash), lambda state: False)
    resumed = coordinator(Journal(journal.path), gateway)
    state = tick_until(resumed, lambda s: bool(s.memory.values.get("supplied_at")))
    assert state.memory.values["deliveries"] == state.delivered == len(state.outcomes) == 2
    assert len(gateway.broadcasts) == 2


@pytest.mark.parametrize("amount", [0, -1, 2**256])
def test_supply_intent_rejects_invalid_amounts(recorded, amount):
    with pytest.raises(ValidationError):
        recorded.config.supply(recorded.config.allocations[0].name, amount)


def test_amount_and_position_scope_cannot_be_relabelled(recorded, multiple):
    from almanak.connectors.aave_v4.models import SupplyTarget
    from almanak.lending_intents import SupplyIntent, WithdrawIntent
    from almanak.models import AssetAmount

    first, second = multiple.allocations
    intent = multiple.supply(second.name, second.capital)
    with pytest.raises(ValueError, match="position scope"):
        plan_operation(intent, recorded.baseline[0])
    with pytest.raises(ValidationError, match="reserve asset"):
        SupplyIntent[SupplyTarget](
            target=multiple.target(first.name),
            amount=AssetAmount(asset=first.native_asset, amount=1),
        )
    with pytest.raises(ValidationError, match="None"):
        WithdrawIntent[SupplyTarget](target=multiple.target(first.name), amount=intent.amount)


def test_share_movement_changes_only_its_position(recorded, multiple):
    from almanak.connectors.aave_v4.lifecycle import apply_share_movement

    first, second = (multiple.target(a.name) for a in multiple.allocations)
    owned = (Ownership(target=first, shares=7), Ownership(target=second, shares=11))
    before = snapshot_for(recorded.outcomes[1].before, second)
    operation = plan_operation(multiple.supply(second.allocation, 10**16), before)
    changed = apply_share_movement(owned, operation, 5)
    assert changed == (owned[0], Ownership(target=second, shares=16))


@pytest.mark.parametrize("fixture", ["aave-v4-caip-lifecycle", "aave-v4-position-lifecycle"])
def test_prior_schema_stays_auditable_without_rewriting_evidence(tmp_path, fixture):
    import sqlite3

    body = (ROOT / f"tests/fixtures/{fixture}.json").read_text()
    journal = Journal(tmp_path / "prior-schema.sqlite")
    with sqlite3.connect(journal.path) as connection:
        connection.execute("CREATE TABLE checkpoint (id INTEGER PRIMARY KEY, body TEXT)")
        connection.execute("INSERT INTO checkpoint VALUES (1, ?)", (body,))
    assert journal.audit()["legacy_checkpoint"] == json.loads(body)
    assert journal.audit()["caip_compliance"] is True
    with pytest.raises(ValueError, match="audit-only"):
        journal.load()
    with pytest.raises(ValueError):
        journal.teardown()
    with journal.connect() as connection:
        assert connection.execute("SELECT body FROM checkpoint").fetchone()[0] == body


def test_revert_stops_after_delivery_without_repeating_the_trade(
    tmp_path, recorded, reference_strategy
):
    journal = Journal(tmp_path / "state.sqlite")
    journal.create(recorded.config, reference_strategy)
    outcome = recorded.outcomes[1].model_copy(update={"success": False, "assets": 0, "shares": 0})
    journal.update(lambda state: state.model_copy(update={"outcomes": (outcome,)}))
    gateway = CapturedGateway(journal, recorded)
    runner = coordinator(journal, gateway)
    with pytest.raises(RuntimeError, match="reverted; outcome delivered"):
        runner.tick()
    assert journal.load().delivered == 1
    assert gateway.broadcasts == []
    with pytest.raises(DataError, match="recorded execution failure"):
        runner.tick()
