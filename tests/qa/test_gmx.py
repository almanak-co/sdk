"""Recorded request acceptance, liquidation and wrong scopes cannot satisfy native actions."""

import gzip
import json
from dataclasses import replace

import pytest

from almanak.caip import ChainId
from almanak.connectors.gmx_v2.state import Checkpoint
from almanak.connectors.support import EconomicAction
from tests.gmx_retired import current
from tests.qa.catalog import ROOT, catalog, validate
from tests.qa.gmx import actions
from tests.qa.intent_coverage import assert_native_direct
from tests.qa.worker import assert_fork_actions

RECORDS = json.loads((ROOT / "tests/fixtures/gmx-qa-native-references.json").read_text())
CASES = tuple(c for c in catalog() if str(c.connector) == "gmx_v2")


def retained(row):
    # The checkpoints predate scope-only authority (blueprint 27): their allocations carry
    # the retired GMX maxima and their permission partitions the quotas. Every fact the
    # action checks read (outcomes, requests, delivery, targets) stays as captured.
    return current(json.loads(gzip.decompress((ROOT / row["checkpoint"]).read_bytes())))


def case_for(row):
    return next(
        c
        for c in CASES
        if c.reference == row["reference"]
        and c.profile == row["profile"]
        and c.input_policy == "pinned"
        and c.operator_mode == "configured"
        and not c.scenario
    )


@pytest.mark.parametrize("row", RECORDS, ids=lambda r: r["reference"] + "/" + r["profile"])
def test_retained_reference_has_completed_economic_actions_and_cleanup(tmp_path, row):
    case = case_for(row)
    raw = retained(row)
    source = ROOT / "strategies" / case.reference / "config.yaml"
    config = tmp_path / "config.yaml"
    config.write_bytes(source.read_bytes())
    assert_fork_actions(case, raw, config)
    state = Checkpoint.model_validate_json(json.dumps(raw))
    assert state.mode == "closed" and state.delivered == len(state.outcomes)
    assert not any(r.unresolved for r in state.requests)
    assert not any(o.needs_cleanup for o in state.closure)
    assert_native_direct(case, state)


@pytest.mark.parametrize("row", RECORDS, ids=lambda r: r["reference"] + "/" + r["profile"])
def test_creation_receipts_alone_do_not_prove_keeper_execution(row):
    raw = retained(row)
    raw["outcomes"] = [o for o in raw["outcomes"] if o["kind"] != "gmx_request_outcome"]
    raw["delivered"] = len(raw["outcomes"])
    names = {a["name"] for a in raw["config"]["allocations"]}
    assert actions(raw, names, case_for(row).chains[0]) <= {"revoke_position", "revoke_exchange"}


def test_wrong_chain_allocation_and_accounting_failure_cannot_prove_actions():
    row = next(r for r in RECORDS if r["reference"] == "gmx_v2_arbitrum_position_path")
    raw = retained(row)
    names = {a["name"] for a in raw["config"]["allocations"]}
    assert not actions(raw, names, ChainId("eip155:43114"))
    assert not actions(raw, {"foreign"}, ChainId("eip155:42161"))
    for outcome in raw["outcomes"]:
        outcome["accounting_issues"] = ["synthetic missing transfer attribution"]
    assert not actions(raw, names, ChainId("eip155:42161"))


def test_external_liquidation_is_not_a_completed_author_close_intent():
    path = (
        ROOT
        / "docs/internal/reports/evidence/2026-09-09-gmx-liquidation"
        / "complete-arbitrum-eoa-external_commit/checkpoint.json.gz"
    )
    raw = current(json.loads(gzip.decompress(path.read_bytes())))
    state = Checkpoint.model_validate_json(json.dumps(raw))
    names = {a.name for a in state.config.allocations}
    assert "close_position" not in actions(raw, names, ChainId("eip155:42161"))
    case = next(c for c in CASES if c.reference == "gmx_v2_arbitrum_position")
    with pytest.raises(AssertionError, match="missing direct completed"):
        assert_native_direct(replace(case, actions=("close_position",)), state)


def test_gmx_declarations_require_native_reference_mapping_without_invented_models():
    assert len(CASES) == 93  # plus the six fork cases of gmx_v2_arbitrum_price_stop
    cleanup = [c for c in CASES if c.scenario == "gmx_cleanup"]
    assert len(cleanup) == 24
    assert {(c.reference, c.actions) for c in cleanup} == {
        (f"gmx_v2_{chain}_{path}", (EconomicAction(revocation),))
        for chain in ("arbitrum", "avalanche")
        for path, revocation in (("position", "revoke_position"), ("swap", "revoke_exchange"))
    }
    fork_cases = [c for c in CASES if c.lane == "fork"]
    assert len(fork_cases) == 90
    assert all(
        EconomicAction("revoke_exchange") not in c.actions for c in fork_cases if not c.scenario
    )
    assert all(not c.model for c in fork_cases)
    backtest_cases = [c for c in CASES if c.lane == "backtest"]
    assert [c.id for c in backtest_cases] == [
        f"backtest/{policy}/gmx_v2_arbitrum_liquidity/economic"
        for policy in ("pinned", "fresh", "native")
    ]
    assert all(c.model == "gmx-v2-published-opportunity-integer-gm-v1" for c in backtest_cases)
    cases = catalog()
    validate(cases)
    with pytest.raises(ValueError, match="QA coverage gaps"):
        validate([c for c in cases if str(c.connector) != "gmx_v2"])


CLEANUP_RECORDS = json.loads((ROOT / "tests/fixtures/gmx-qa-cleanup-references.json").read_text())


@pytest.mark.parametrize("row", CLEANUP_RECORDS, ids=lambda row: row["case"])
def test_interrupted_positive_approval_requires_a_direct_completed_revocation(row):
    case = next(c for c in CASES if c.id == row["case"])
    raw = retained(row)
    state = Checkpoint.model_validate_json(json.dumps(raw))
    assert state.mode == "closed" and not state.requests
    assert state.delivered == len(state.outcomes) == 2
    approved, revoked = state.outcomes
    assert approved.operation.kind == revoked.operation.kind == "gmx_approval"
    assert approved.operation.amount.amount > 0 and approved.after.needs_cleanup
    assert approved.strategy_result().status == "intermediate"
    assert revoked.operation.amount.amount == 0 and not revoked.after.needs_cleanup
    assert revoked.strategy_result().intent.kind == "revoke_position"
    assert_native_direct(case, state)
    assert actions(raw, {owned.target.allocation for owned in state.owned}, case.chains[0]) == {
        "revoke_position"
    }
    without_delivery = {**raw, "delivered": 1}
    assert not actions(without_delivery, {state.owned[0].target.allocation}, case.chains[0])
