"""Retained Robinhood references prove closure, authority profiles and offline recovery."""

import gzip
import hashlib
import json
from pathlib import Path

import pytest

from almanak.caip import ChainId
from almanak.connectors.composed.state import Checkpoint as Composed
from almanak.connectors.morpho_blue.state import Checkpoint as Native
from tests.scope_only_sources import reviewed_json

ROOT = Path(__file__).parents[2]
EVIDENCE = ROOT / "docs/internal/reports/evidence/2026-09-09-lending-rollout/robinhood-lending"


def raw(name):
    return gzip.decompress((EVIDENCE / (name + ".gz")).read_bytes())


def record(name):
    return json.loads(raw(name))


def summary():
    return record("audit/summary.json")


def checkpoint(prefix, path, cls):
    candidates = list((EVIDENCE / prefix / path).rglob("checkpoint.json.gz"))
    assert len(candidates) == 1
    return cls.model_validate_json(reviewed_json(gzip.decompress(candidates[0].read_bytes())))


def assert_closed(state):
    assert state.mode == "closed" and state.pending is None and state.plan is None
    assert state.delivered == len(state.outcomes)
    assert all(outcome.success for outcome in state.outcomes)


def test_manifest_and_complete_matrix():
    manifest = json.loads((EVIDENCE / "manifest.json").read_text())
    for name, digest in manifest["files"].items():
        assert hashlib.sha256((EVIDENCE / name).read_bytes()).hexdigest() == digest
    assert summary()["counts"] == {
        "native": 6,
        "prepared": 3,
        "recovery": 2,
        "composed_native": 3,
        "composed_prepared": 1,
    }


@pytest.mark.parametrize("index", range(6))
def test_native_reference_closed_with_required_profile(index):
    row = summary()["native"][index]
    state = checkpoint("original", row["path"], Native)
    assert_closed(state)
    assert all(position.empty for position in state.owned)
    assert all(observation.allowance == 0 for observation in state.closure)
    assert all(binding.profile.kind == row["profile"] for binding in state.config.accounts.bindings)
    if row["operator"] == "distinct":
        assert len({b.profile.submitter for b in state.config.accounts.bindings}) == 2
    assert len(row["intents"]) == 9
    assert {a.chain.chain_id for a in state.config.positions} == {ChainId("eip155:4663")}


@pytest.mark.parametrize("index", range(3))
def test_prepared_lanes_reproduce_under_os_network_isolation(index):
    row = summary()["prepared"][index]
    path = "original/" + row["path"]
    report = record(path + "/run/report.json")
    assert report["closed"] and report["completed_intents"] == 9
    assert report["provider_requests_during_execution"] == 0
    assert record(path + "/run/reproduction.json")["reproduced"]
    for phase in ("cached-prepare", "execute", "resume", "reproduce"):
        result = record(path + "/" + phase + ".json")
        assert result["status"] == "passed" and result["network"] == "isolated"


@pytest.mark.parametrize("index", range(2))
def test_native_process_recovery_keeps_one_borrow_attempt(index):
    row = summary()["recovery"][index]
    path = "original/" + row["path"]
    proof = record(path + "/result.json")
    state = checkpoint("original", row["path"], Native)
    assert_closed(state)
    assert all(position.empty for position in state.owned)
    pending = proof["lost_response"]["pending"]
    assert proof["reserved"]["plan"]["attempt_id"] == pending["attempt_id"]
    assert proof["broadcast_transaction"]["hash"] == pending["tx_hash"]
    assert proof["committed"]["outcomes"][-1]["tx_hash"] == pending["tx_hash"]
    assert sum(outcome.operation.kind == "borrow" for outcome in state.outcomes) == 1


@pytest.mark.parametrize("index", range(3))
def test_composition_reverses_and_repays_before_collateral_release(index):
    row = summary()["composed_native"][index]
    state = checkpoint("composition", row["path"], Composed)
    assert_closed(state)
    assert all(position.empty for position in state.owned[:2])
    completed = [o.decision.intent.kind for o in state.outcomes if o.strategy_result().complete]
    assert completed == [
        "supply",
        "borrow",
        "swap_exact_input",
        "exit_exchange",
        "repay",
        "withdraw",
    ]
    assert not state.memory.values["traded"]
    assert all(o.allowance == 0 for o in state.closure[:2])
    assert not state.closure[-1].needs_cleanup


def test_composed_prepared_run_is_closed_and_reproducible():
    path = "prepared/" + summary()["composed_prepared"][0]["path"]
    report = record(path + "/run/report.json")
    assert report["closed"] and report["completed_intents"] == 6
    assert set(report["components"]) == {"morpho_blue", "curve"}
    assert report["provider_requests_during_execution"] == 0
    assert record(path + "/run/reproduction.json")["reproduced"]
    for phase in ("cached-prepare", "execute", "resume", "reproduce"):
        result = record(path + "/" + phase + ".json")
        assert result["status"] == "passed" and result["network"] == "isolated"


def test_original_oversized_loan_is_a_retained_financial_refusal():
    proof = record("original/original-composition-refusal.json")
    assert proof["offline_capacity_replay"] == "refused"
    assert proof["requested_base_units"] > proof["supply_assets"] - proof["borrow_assets"]
    assert proof["loan_asset"].startswith("eip155:4663/erc20:")
    assert proof["checkpoint_sha256"]
