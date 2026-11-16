import copy
import json
import sqlite3
from uuid import uuid4

import pytest
from pydantic import ValidationError

from almanak.connectors.aave_v4.models import DecisionInput, LendingConfig, Memory
from almanak.connectors.aave_v4.reader import receipt_movement
from almanak.gateway.ethereum import DataError
from almanak.gateway.fork_execution import ForkGateway
from almanak.runtime import Runtime, strategy_decision
from almanak.state import Journal, Lease, release_claims


@pytest.fixture
def config(recorded):
    raw = recorded.config.model_dump()
    instance = "0x" + uuid4().hex * 2
    raw["fork_instance"] = instance
    for binding in raw["accounts"]["bindings"]:
        binding["profile"]["fund"]["chain"]["environment"] = "anvil:" + instance
    for allocation in raw["allocations"]:
        allocation["reserve"]["chain"]["environment"] = "anvil:" + instance
    return LendingConfig.model_validate(raw)


def test_resume_cannot_create_an_empty_deployment(tmp_path):
    path = tmp_path / "missing.sqlite"
    with pytest.raises(sqlite3.OperationalError):
        Journal(path).load()
    assert not path.exists()


def test_create_does_not_replace_durable_identity(tmp_path, config, reference_strategy):
    journal = Journal(tmp_path / "state.sqlite")
    journal.create(config, reference_strategy)
    initial = journal.load()
    with pytest.raises(FileExistsError):
        journal.create(config, reference_strategy)
    assert journal.load() == initial


@pytest.mark.parametrize("field", ["capital", "interval"])
def test_incompatible_resume_preserves_state_and_teardown(
    tmp_path, config, reference_strategy, monkeypatch, field
):
    journal = Journal(tmp_path / "state.sqlite")
    journal.create(config, reference_strategy)
    raw = config.model_dump()
    if field == "capital":
        raw["allocations"][0]["capital"] *= 2
    else:
        raw["decision_trigger"]["seconds"] *= 2
    monkeypatch.setattr("almanak.runtime.GatewayClient", lambda *_: journal)
    monkeypatch.setattr(journal, "close", lambda: None, raising=False)
    with pytest.raises(ValueError, match="configuration changed"):
        Runtime(
            "not-an-endpoint",
            tmp_path / "no-key",
            expected=LendingConfig.model_validate(raw),
        )
    assert journal.load().config == config
    assert journal.teardown().mode == "teardown"
    assert journal.load().config == config


def test_invalid_checkpoint_update_rolls_back(tmp_path, config, reference_strategy):
    journal = Journal(tmp_path / "state.sqlite")
    journal.create(config, reference_strategy)
    original = journal.load()
    with pytest.raises(ValidationError):
        journal.update(lambda current: current.model_copy(update={"schema_version": 6}))
    assert journal.load() == original


def test_persistent_fund_claim_survives_coordinator_exit(tmp_path, config, reference_strategy):
    first = Journal(tmp_path / "first.sqlite")
    second = Journal(tmp_path / "second.sqlite")
    first.create(config, reference_strategy)
    second.create(config, reference_strategy)
    lease = Lease(first, first.load())
    with pytest.raises(BlockingIOError):
        Lease(first, first.load())
    lease.close()
    with pytest.raises(RuntimeError, match="already claimed"):
        Lease(second, second.load())
    resumed = Lease(first, first.load())
    resumed.close()


def test_recorded_receipt_preserves_actual_assets_and_shares(recorded):
    allocation = recorded.config.allocations[0]
    for outcome in recorded.outcomes:
        assert receipt_movement(
            outcome.receipt, allocation, outcome.before.position.account, outcome.operation
        ) == (outcome.assets, outcome.shares)
    supply = next(item for item in recorded.outcomes if item.operation.kind == "supply")
    assert supply.assets == allocation.capital
    assert supply.shares == supply.after.position.supplied_shares


@pytest.mark.parametrize(
    "change", ["missing", "wrong_fund", "wrong_reserve", "duplicate", "empty_topics"]
)
def test_receipt_cannot_invent_or_misattribute_a_position(recorded, change):
    outcome = next(item for item in recorded.outcomes if item.operation.kind == "supply")
    receipt = copy.deepcopy(outcome.receipt)
    log = next(
        item
        for item in receipt["logs"]
        if item["address"].lower() == recorded.config.allocations[0].reserve.spoke
    )
    if change == "missing":
        receipt["logs"].remove(log)
    elif change == "duplicate":
        receipt["logs"].append(copy.deepcopy(log))
    elif change == "wrong_fund":
        log["topics"][3] = "0x" + "11" * 32
    elif change == "empty_topics":
        log["topics"] = []
    else:
        log["topics"][1] = "0x" + "11" * 32
    with pytest.raises(DataError):
        receipt_movement(
            receipt,
            recorded.config.allocations[0],
            outcome.before.position.account,
            outcome.operation,
        )


@pytest.mark.parametrize(
    "endpoint",
    [
        "https://127.0.0.1:8545",
        "http://example.com:8545",
        "http://127.0.0.1",
        "http://127.0.0.1:8545/rpc",
        "http://:hidden@127.0.0.1:8545",
    ],
)
def test_executor_refuses_other_transports_before_reading_keys(config, tmp_path, endpoint):
    with pytest.raises(ValueError, match="loopback"):
        ForkGateway(config, endpoint, tmp_path / "missing-key")


def test_gateway_admits_a_second_supply_without_a_budget(recorded, monkeypatch):
    gateway = object.__new__(ForkGateway)
    gateway.config = recorded.config
    monkeypatch.setattr(gateway, "guard", lambda: None)
    supply = next(item for item in recorded.outcomes if item.operation.kind == "supply")
    from types import SimpleNamespace

    gateway.reader = SimpleNamespace(chain=supply.before.block.chain)
    state = recorded.model_copy(update={"mode": "running", "decision": supply.decision})
    # Scope-only authority (blueprint 27): no cumulative supply budget refuses it.
    gateway.validate_operation(state, supply.operation, supply.before)


@pytest.mark.parametrize(
    "metadata", [None, {}, {"forkedNetwork": None}, {"forkedNetwork": {}}, {"forkedNetwork": []}]
)
def test_gateway_refuses_missing_or_malformed_fork_identity(config, metadata):
    class Reader:
        chain = config.accounts.bindings[0].profile.fund.chain

        def _rpc(self, method, params):
            assert method == "anvil_metadata"
            return metadata

    gateway = object.__new__(ForkGateway)
    gateway.config = config
    gateway.reader = Reader()
    with pytest.raises(DataError, match="fork instance or origin changed"):
        gateway.guard()


def test_strategy_process_proposes_supply_without_approval_plumbing(recorded, reference_strategy):
    context = DecisionInput(
        config=recorded.config,
        memory=Memory(),
        owned=recorded.owned,
        observations=recorded.baseline,
        trigger=recorded.outcomes[0].decision.trigger,
    )
    allocation = recorded.config.allocations[0]
    assert strategy_decision(reference_strategy, context).intent == recorded.config.supply(
        allocation.name, allocation.capital
    )


def test_release_claims_removes_only_the_stopped_journals_claims(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    root = tmp_path / ".local/state/almanak-v3/claims"
    root.mkdir(parents=True)
    mine = tmp_path / "run" / "state.sqlite"
    (root / "a.claim").write_text(json.dumps({"database": str(mine), "deployment_id": "1"}))
    (root / "b.claim").write_text(json.dumps({"database": "/other", "deployment_id": "2"}))
    assert release_claims(mine) == 1
    assert [p.name for p in root.iterdir()] == ["b.claim"]


def test_release_claims_skips_unreadable_claims(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    root = tmp_path / ".local/state/almanak-v3/claims"
    root.mkdir(parents=True)
    mine = tmp_path / "run" / "state.sqlite"
    (root / "list.claim").write_text("[1, 2]")
    (root / "binary.claim").write_bytes(b"\xff\xfe")
    (root / "mine.claim").write_text(json.dumps({"database": str(mine)}))
    assert release_claims(mine) == 1
    assert sorted(p.name for p in root.iterdir()) == ["binary.claim", "list.claim"]
