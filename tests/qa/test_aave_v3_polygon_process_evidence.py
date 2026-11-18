"""Retained fork checkpoints must preserve identities across actual process loss."""

import json

import pytest

from almanak.connectors.aave_v3.state import Checkpoint
from almanak.strategy import StrategyDefinition
from tests.legacy_references import current_imports_if_legacy
from tests.qa.catalog import ROOT
from tests.qa.test_aave_v3 import artifact
from tests.scope_only_sources import reviewed_json, reviewed_record
from tests.strategy_fork.pool_lending_lifecycle import assert_successful_outcomes, assert_zero_owned

PACKAGE = ROOT / "docs/internal/reports/evidence/2026-09-09-rollout/aave-v3-polygon-process"
CASES = json.loads((PACKAGE / "manifest.json").read_text())["cases"]


def retained(case):
    prefix = case["directory"] + "/"
    state = Checkpoint.model_validate_json(
        reviewed_json(artifact(PACKAGE, prefix + "checkpoint.json.gz"))
    )
    record = json.loads(artifact(PACKAGE, prefix + "result.json.gz"))
    assert record["status"] == "passed"
    assert current_imports_if_legacy(state.strategy) == StrategyDefinition.read(
        ROOT / "strategies" / case["reference"] / "strategy.py"
    )
    assert (state.mode, state.pending, state.plan, state.decision) == ("closed", None, None, None)
    assert state.delivered == len(state.outcomes) == case["outcomes"]
    assert {b.profile.kind for b in state.config.accounts.bindings} == {case["profile"]}
    assert_successful_outcomes(state)
    assert_zero_owned(state)
    return state, record


@pytest.mark.parametrize("case", [c for c in CASES if c["kind"] == "cleanup"])
def test_approval_only_interruption_closes_without_economic_exposure(case):
    state, record = retained(case)
    before = Checkpoint.model_validate_json(
        json.dumps(reviewed_record(record["interrupted_checkpoint"]))
    )
    assert before.delivered < len(before.outcomes) == 1
    assert before.outcomes[0].operation.kind == "approve"
    assert [outcome.operation.kind for outcome in state.outcomes] == ["approve", "revoke"]
    assert state.outcomes[0] == before.outcomes[0]
    assert all(outcome.debt_delta == outcome.supply_delta == 0 for outcome in state.outcomes)


@pytest.mark.parametrize("case", [c for c in CASES if c["kind"] == "recovery"])
def test_lost_broadcast_response_keeps_one_borrow_through_delivery_and_close(case):
    state, record = retained(case)
    reserved = Checkpoint.model_validate_json(json.dumps(reviewed_record(record["reserved"])))
    lost = Checkpoint.model_validate_json(json.dumps(reviewed_record(record["lost_response"])))
    committed = Checkpoint.model_validate_json(json.dumps(reviewed_record(record["committed"])))
    assert reserved.pending is None and reserved.plan.operation.kind == "borrow"
    assert reserved.plan.before.position.debt_scaled == 0
    pending = lost.pending
    assert pending.operation.kind == "borrow"
    assert pending.attempt_id == reserved.plan.attempt_id
    assert pending.tx_hash == record["broadcast_transaction"]["hash"]
    assert committed.pending is None and committed.delivered < len(committed.outcomes)
    (borrow,) = [outcome for outcome in state.outcomes if outcome.operation.kind == "borrow"]
    assert borrow == committed.outcomes[-1]
    assert borrow.tx_hash == pending.tx_hash and borrow.operation == pending.operation
