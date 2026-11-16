"""Retained composed runs must prove both connectors and preserve borrowed obligations."""

import gzip
import hashlib
import json
from dataclasses import replace

import pytest

from almanak.connectors.composition import checkpoint_from_json, connector_for
from almanak.connectors.support import EconomicAction
from tests.lending_captures import current_manifest
from tests.qa.catalog import ROOT, catalog
from tests.qa.worker import assert_backtest, assert_fork_actions
from tests.scope_only_sources import reviewed_json, reviewed_record

EVIDENCE = ROOT / "docs/internal/reports/evidence/2026-09-09-rollout"
CHAINS = ("arbitrum", "optimism", "bnb", "polygon", "avalanche")
CASES = {case.id: case for case in catalog()}
PACKAGES = ("aave-composition-qa-native", "aave-avalanche-composition-qa-native")
NATIVE = tuple(
    (package, row)
    for package in PACKAGES
    for row in json.loads((EVIDENCE / package / "manifest.json").read_text())["cases"]
)


def manifest(package):
    return json.loads((EVIDENCE / package / "manifest.json").read_text())


def artifact(package, name):
    files = {row["path"]: row["sha256"] for row in manifest(package)["files"]}
    raw = (EVIDENCE / package / name).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == files[name]
    return gzip.decompress(raw)


def unique_name(package, prefix, suffix):
    names = [
        row["path"]
        for row in manifest(package)["files"]
        if row["path"].startswith(prefix) and row["path"].endswith(suffix)
    ]
    assert len(names) == 1
    return names[0]


@pytest.mark.parametrize("package,row", NATIVE, ids=[r[1]["case"] for r in NATIVE])
def test_native_compositions_have_delivered_actions_for_both_connectors(package, row):
    case = CASES[row["case"]]
    name = unique_name(package, row["evidence"] + "/", "/checkpoint.json.gz")
    state = checkpoint_from_json(reviewed_json(artifact(package, name).decode()))
    assert row["returncode"] == 0
    assert (state.mode, state.plan, state.pending, state.decision) == ("closed", None, None, None)
    assert state.delivered == len(state.outcomes)
    assert {binding.profile.kind for binding in state.config.accounts.bindings} == {case.profile}
    assert all(outcome.success and not outcome.accounting_issues for outcome in state.outcomes)
    connector_for(state.config).lifecycle.validate_closure(state, state.closure)
    author = ROOT / "strategies" / case.reference / "config.yaml"
    assert_fork_actions(case, state.model_dump(mode="json"), author)
    impossible = replace(case.scopes[0], actions=(EconomicAction.REFRESH,))
    with pytest.raises(AssertionError):
        assert_fork_actions(
            replace(case, scopes=(impossible, *case.scopes[1:])),
            state.model_dump(mode="json"),
            author,
        )


@pytest.mark.parametrize("chain", CHAINS)
def test_prepared_composition_delivered_economic_actions(tmp_path, chain):
    package = (
        "aave-avalanche-composition-prepared"
        if chain == "avalanche"
        else "aave-composition-prepared"
    )
    for name in ("manifest.json", "report.json", "state.sqlite"):
        (tmp_path / name).write_bytes(artifact(package, f"isolated/{chain}/run/{name}.gz"))
    case = CASES[f"backtest/pinned/{chain}_aave_borrow_and_trade/economic"]
    current_manifest(tmp_path)
    assert_backtest(case, tmp_path)
    impossible = replace(case.scopes[0], actions=(EconomicAction.REFRESH,))
    with pytest.raises(AssertionError):
        assert_backtest(replace(case, scopes=(impossible, *case.scopes[1:])), tmp_path)


def test_all_five_references_keep_both_profiles_and_input_lanes():
    assert len(NATIVE) == 30
    for chain in CHAINS:
        reference = chain + "_aave_borrow_and_trade"
        cases = [case for case in CASES.values() if case.reference == reference]
        assert len(cases) == 9
        assert len([case for case in cases if case.lane == "fork"]) == 6
        assert all(len(case.scopes) == 2 for case in cases)


def process_cases():
    for package in ("aave-composition-borrow-process", "aave-avalanche-composition-process"):
        for row in manifest(package)["files"]:
            name = row["path"]
            if name.endswith("/result.json.gz"):
                record = json.loads(artifact(package, name))
                if record["status"] == "passed" and "committed" in record:
                    yield package, name


PROCESS = tuple(process_cases())


@pytest.mark.parametrize("package,name", PROCESS)
def test_borrow_process_recovery_commits_each_transaction_once(package, name):
    record = json.loads(artifact(package, name))
    reserved = checkpoint_from_json(json.dumps(reviewed_record(record["reserved"])))
    unknown = checkpoint_from_json(json.dumps(reviewed_record(record["lost_response"])))
    committed = checkpoint_from_json(json.dumps(reviewed_record(record["committed"])))
    final = checkpoint_from_json(
        reviewed_json(
            artifact(package, name.replace("/result.json.gz", "/checkpoint.json.gz")).decode()
        )
    )
    assert reserved.pending is None and reserved.plan.operation.kind == "borrow"
    assert unknown.pending.attempt_id == reserved.plan.attempt_id
    assert record["broadcast_transaction"]["hash"] == unknown.pending.tx_hash
    assert committed.pending is None and committed.delivered < len(committed.outcomes)
    assert committed.outcomes[-1].tx_hash == unknown.pending.tx_hash
    assert final.mode == "closed" and final.delivered == len(final.outcomes)
    assert sum(outcome.tx_hash == unknown.pending.tx_hash for outcome in final.outcomes) == 1
    connector_for(final.config).lifecycle.validate_closure(final, final.closure)
    assert len(PROCESS) == 10


def refusal_cases():
    for package in ("aave-composition-reversal-process", "aave-avalanche-composition-process"):
        for row in manifest(package)["files"]:
            name = row["path"]
            if name.endswith("/reversal-proof.json.gz"):
                yield package, name


REFUSALS = tuple(refusal_cases())


@pytest.mark.parametrize("package,name", REFUSALS)
def test_failed_reverse_trade_preserves_debt_until_recovery(package, name):
    proof = json.loads(artifact(package, name))
    failed = checkpoint_from_json(json.dumps(reviewed_record(proof["failed_checkpoint"])))
    assert failed.mode == "teardown" and failed.pending is None and failed.plan is None
    assert failed.memory.values["traded"]
    assert failed.owned[0].supply_scaled > 0 and failed.owned[0].collateral_enabled
    assert failed.owned[1].debt_scaled > 0
    assert proof["restored_router_completed_reference"]
    prefix = name.removesuffix("reversal-proof.json.gz")
    final_name = unique_name(package, prefix, "/checkpoint.json.gz")
    final = checkpoint_from_json(reviewed_json(artifact(package, final_name).decode()))
    assert final.mode == "closed" and final.delivered == len(final.outcomes)
    connector_for(final.config).lifecycle.validate_closure(final, final.closure)
    assert len(REFUSALS) == 5
