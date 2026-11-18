import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from almanak.connectors.aave_v4.fork import ForkConfig
from almanak.connectors.aave_v4.models import DecisionInput, Memory, Ownership
from almanak.runtime import Runtime, report, strategy_decision
from almanak.state import Journal
from almanak.strategy import StrategyDefinition
from almanak.triggers import TeardownEvent

ROOT = Path(__file__).parents[1]


@pytest.fixture
def initial(recorded):
    return DecisionInput(
        config=recorded.config,
        memory=Memory(),
        owned=recorded.owned,
        observations=recorded.baseline,
        trigger=recorded.outcomes[0].decision.trigger,
    )


@pytest.mark.parametrize("stage", ["approved", "holding", "withdraw", "revoke", "closed"])
def test_reference_decisions_follow_recorded_obligations(
    recorded, initial, reference_strategy, stage
):
    name = recorded.config.allocations[0].name
    approval = next(item for item in recorded.outcomes if item.operation.kind == "approve")
    supplied = next(item for item in recorded.outcomes if item.operation.kind == "supply")
    if stage == "approved":
        context = initial.model_copy(update={"observations": (approval.after,)})
        expected = recorded.config.supply(name, recorded.config.allocation(name).capital)
    elif stage in ("holding", "withdraw"):
        context = initial.model_copy(
            update={
                "memory": recorded.memory,
                "owned": (Ownership(target=recorded.config.target(name), shares=supplied.shares),),
                "observations": (supplied.after,),
                "trigger": TeardownEvent(started_at=initial.trigger.started_at)
                if stage == "withdraw"
                else initial.trigger,
            }
        )
        expected = recorded.config.withdraw(name) if stage == "withdraw" else None
    else:
        context = initial.model_copy(
            update={
                "trigger": TeardownEvent(started_at=initial.trigger.started_at),
                "observations": (approval.after,) if stage == "revoke" else recorded.closure,
            }
        )
        expected = recorded.config.withdraw(name) if stage == "revoke" else None
    assert strategy_decision(reference_strategy, context).intent == expected


def test_decision_snapshot_survives_changed_and_deleted_source(
    tmp_path, initial, reference_strategy
):
    path = tmp_path / "strategy.py"
    path.write_text(reference_strategy.source)
    definition = StrategyDefinition.read(path)
    journal = Journal(tmp_path / "state.sqlite")
    journal.create(initial.config, definition)
    path.write_text('raise RuntimeError("modified file must not execute")')
    path.unlink()
    state = journal.load()
    assert state.strategy == definition
    assert report(state)["strategy"]["sha256"] == definition.sha256
    allocation = initial.config.allocations[0]
    assert strategy_decision(state.strategy, initial).intent == initial.config.supply(
        allocation.name, allocation.capital
    )


@pytest.mark.parametrize(
    "source",
    [
        "def decide(context):\n    raise RuntimeError('broken')",
        "def decide(context):\n    return False",
    ],
)
def test_strategy_errors_cannot_become_healthy_hold(initial, source):
    definition = StrategyDefinition(filename="broken.py", source=source)
    with pytest.raises(RuntimeError, match="strategy worker failed"):
        strategy_decision(definition, initial)


def test_missing_observation_cannot_trigger_an_entry(initial, reference_strategy):
    decision = strategy_decision(
        reference_strategy, initial.model_copy(update={"observations": ()})
    )
    assert decision.intent is None
    assert decision.memory.schema_id is not None
    assert decision.memory.values == {"supplied_at": {}, "borrowed_at": {}, "repaid_at": {}}


def test_reading_a_definition_does_not_execute_it_in_the_coordinator(tmp_path):
    marker = tmp_path / "executed"
    source = tmp_path / "strategy.py"
    source.write_text(f"from pathlib import Path\nPath({str(marker)!r}).touch()\n")
    StrategyDefinition.read(source)
    assert not marker.exists()


def test_start_refuses_a_key_file_inside_strategy_readable_code(
    tmp_path, initial, reference_strategy
):
    with pytest.raises(ValueError, match="strategy-readable"):
        Runtime(
            "unused",
            ROOT / "src/fork-keys.json",
            create=initial.config,
            strategy=reference_strategy,
        )
    assert not (tmp_path / "state.sqlite").exists()


def test_legacy_audit_remains_available_without_implicitly_selecting_code(
    tmp_path, initial, reference_strategy, monkeypatch
):
    journal = Journal(tmp_path / "state.sqlite")
    journal.create(initial.config, reference_strategy)
    journal.update(lambda state: state.model_copy(update={"strategy": None}))
    assert report(journal.load())["strategy"] is None
    monkeypatch.setattr("almanak.runtime.GatewayClient", lambda *_: journal)
    monkeypatch.setattr(journal, "close", lambda: None, raising=False)
    with pytest.raises(ValueError, match="recorded strategy"):
        Runtime("unused", tmp_path / "no-key")


@pytest.mark.parametrize("change", ["asset", "chain", "duplicate", "token_funding", "gas_funding"])
def test_fork_setup_refuses_unsupported_or_inconsistent_inputs(change):
    from tests.fork.eoa import configuration

    config = ForkConfig.model_validate_json(json.dumps(configuration()))
    raw = config.model_dump()
    fund = raw["funds"][0]
    if change == "asset":
        raw["allocations"][0]["reserve"]["asset"] = "0x" + "11" * 20
    elif change == "chain":
        raw["allocations"][0]["reserve"]["chain"]["chain_id"] = 10
    elif change == "duplicate":
        raw["funds"] = (fund, fund)
    elif change == "token_funding":
        fund["token_balance"] = sum(a["capital"] for a in raw["allocations"]) - 1
    else:
        fund["native_balance"] = fund["token_balance"]
    with pytest.raises(ValidationError):
        ForkConfig.model_validate(raw)
