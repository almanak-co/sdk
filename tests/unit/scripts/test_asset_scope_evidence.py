"""Scoped projections replay real immutable ledger records, never index labels."""

from __future__ import annotations

import copy
import hashlib
import json

import pytest

from qa_lab import qa_history
from qa_lab.asset_scenarios import build_scope
from qa_lab.asset_scope_evidence import _identity_observation, admit_scoped_rows, project_scoped_history
from tests.unit.scripts import test_qa_asset_scenarios as scenario_examples

contract = scenario_examples.contract

SDK = {
    "commit": "a" * 40,
    "branch": "test/scoped-history",
    "dirty": False,
    "sdk_version": "9.9.9-test",
    "source": "executing-worktree",
}


def _scope(contract, scenario_id="equity"):
    scenario = copy.deepcopy(contract)
    scenario["scenario_id"] = scenario_id
    return build_scope(scenario, scenario["parent_bindings"][0], "test.py::test_" + scenario_id)


def _append(store, scope, *, run_id, verdict, hour, summary_verdict=None, raw_receipt=None):
    run_dir = store / "intents" / run_id
    run_dir.mkdir(parents=True)
    (run_dir / "manifest.json").write_text(json.dumps({"run_id": run_id, "sdk": SDK}))
    summary = {
        "cells": [
            {
                "intent_cell_id": scope["claim"]["parent_cell_id"],
                "asset_scope": scope,
                "status": summary_verdict or verdict,
                "nodeids": [scope["proof_node"]],
            }
        ]
    }
    (run_dir / "summary.json").write_text(json.dumps(summary))
    if raw_receipt is not None:
        (run_dir / "receipt.json").write_text(json.dumps(raw_receipt))
    return qa_history.append_experiment(
        store=store,
        surface="intent",
        run_id=run_id,
        run_dir=run_dir,
        manifest_path=run_dir / "manifest.json",
        sdk=SDK,
        cell_verdicts={scope["lookup_key"]: verdict},
        started_at=f"2026-09-15T{hour:02d}:00:00Z",
        completed_at=f"2026-09-15T{hour:02d}:01:00Z",
        sealed_at=f"2026-09-15T{hour:02d}:02:00Z",
        catalog_sha256="b" * 64,
        admission={"status": "OFFICIAL", "evidence_set_sha256": "e" * 64, "audit_decision_sha256": "f" * 64},
    )


def _project(store):
    return project_scoped_history(store, qa_history.active_experiments(qa_history.read_history(store)))


@pytest.mark.parametrize("reverse", [False, True])
def test_sibling_scenarios_preserve_both_results(tmp_path, contract, reverse):
    equity, conventional = _scope(contract), _scope(contract, "conventional")
    cases = [(equity, "FAIL"), (conventional, "PASS")]
    if reverse:
        cases.reverse()
    for index, (scope, verdict) in enumerate(cases):
        _append(tmp_path, scope, run_id=f"run-{index}", verdict=verdict, hour=index)
    projection = _project(tmp_path)
    parent = projection[equity["claim"]["parent_cell_id"]]
    assert parent["equity"][equity["contract_sha256"]]["status"] == "FAIL"
    assert parent["conventional"][conventional["contract_sha256"]]["status"] == "PASS"
    assert "status" not in parent


def test_projection_rebuild_is_deterministic(tmp_path, contract):
    _append(tmp_path, _scope(contract), run_id="first", verdict="PASS", hour=1)
    before = json.dumps(_project(tmp_path), sort_keys=True)
    assert json.dumps(_project(tmp_path), sort_keys=True) == before


def test_failure_retraction_preserves_prior_evidence(tmp_path, contract):
    scope = _scope(contract)
    _append(tmp_path, scope, run_id="pass", verdict="PASS", hour=1)
    failure = _append(tmp_path, scope, run_id="fail", verdict="FAIL", hour=2)
    original = (tmp_path / "intents/fail/summary.json").read_bytes()
    parent = scope["claim"]["parent_cell_id"]
    assert _project(tmp_path)[parent]["equity"][scope["contract_sha256"]]["status"] == "FAIL"
    qa_history.append_invalidation(
        store=tmp_path,
        invalidates_record_sha256=failure["record_sha256"],
        reason_codes=["verified-harness-defect"],
        operator="test-operator",
        sdk=SDK,
        sealed_at="2026-09-15T03:00:00Z",
    )
    assert _project(tmp_path)[parent]["equity"][scope["contract_sha256"]]["status"] == "PASS"
    assert (tmp_path / "intents/fail/summary.json").read_bytes() == original
    assert len(qa_history.read_history(tmp_path)) == 3


@pytest.mark.parametrize("tamper", ["summary-bytes", "key", "verdict"])
def test_scoped_projection_tampering_refused(tmp_path, contract, tamper):
    scope = _scope(contract)
    _append(tmp_path, scope, run_id="run", verdict="PASS", hour=1)
    records = qa_history.read_history(tmp_path)
    source = tmp_path / "intents/run/summary.json"
    summary = json.loads(source.read_text())
    if tamper == "summary-bytes":
        source.write_text(source.read_text() + "\n")
    else:
        row = summary["cells"][0]
        if tamper == "key":
            row["asset_scope"]["lookup_key"] = "d" * 64
        else:
            row["status"] = "FAIL"
        raw = json.dumps(summary).encode()
        source.write_bytes(raw)
        # Even a byte-matching summary cannot change the ledger's structured claim.
        artifact = next(item for item in records[0]["artifacts"] if item["relpath"].endswith("summary.json"))
        artifact.update(sha256=hashlib.sha256(raw).hexdigest(), bytes=len(raw))
    with pytest.raises(ValueError):
        project_scoped_history(tmp_path, records)


def test_scoped_node_without_predispatch_plan_refused(contract):
    scope = _scope(contract)
    catalog = {
        "cells": [{"proof_recipe": {"roles": [{"nodes": [{"nodeid": scope["proof_node"], "scenario_id": "equity"}]}]}}]
    }
    rows = [{"intent_cell_id": scope["claim"]["parent_cell_id"], "nodeids": [scope["proof_node"]]}]
    with pytest.raises(ValueError, match="pre-dispatch"):
        admit_scoped_rows(rows, None, catalog)


def test_missing_decimal_witness_refuses_scoped_qualification():
    payload = {"chain": "base", "tx": {"block_number": 123, "block_hash": "0x" + "a" * 64}}
    verified = {
        "status": "VERIFIED",
        "profile": "swap.v1",
        "facts": {
            "asset_address": "0x" + "a" * 40,
            "output_asset_address": "0x" + "b" * 40,
            "resource_address": "0x" + "c" * 40,
        },
    }
    with pytest.raises(ValueError, match="asset_identity"):
        _identity_observation(payload, verified)
    payload["external_provenance"] = {"block": {"hash": payload["tx"]["block_hash"]}}
    payload["asset_identity"] = {"schema_version": 1, **payload["tx"], "calls": []}
    with pytest.raises(ValueError, match="bijectively"):
        _identity_observation(payload, verified)


@pytest.mark.parametrize("mutation", ["changed", "missing"])
def test_receipt_artifact_mutation_refuses_projection(tmp_path, contract, mutation):
    _append(
        tmp_path,
        _scope(contract),
        run_id="run",
        verdict="PASS",
        hour=1,
        raw_receipt={"transaction_hash": "0x" + "a" * 64, "amount_received": "123"},
    )
    receipt = tmp_path / "intents/run/receipt.json"
    if mutation == "changed":
        receipt.write_text(json.dumps({"transaction_hash": "0x" + "a" * 64, "amount_received": "999"}))
    else:
        receipt.unlink()
    with pytest.raises(ValueError):
        _project(tmp_path)


@pytest.mark.parametrize("mismatch", ["contract", "run", "source", "payload-source", "missing-origin"])
def test_receipt_cannot_adopt_a_new_plan(tmp_path, contract, mismatch):
    from qa_lab.asset_scope_evidence import validate_receipt_plan

    scope = _scope(contract)
    plan = {"asset_scope": scope, "run_id": "original-run", "sdk": SDK}
    payload = {
        "git_sha": SDK["commit"],
        "asset_identity": {
            "scope_lookup_key": scope["lookup_key"],
            "contract_sha256": scope["contract_sha256"],
            "run_id": plan["run_id"],
            "sdk_commit": SDK["commit"],
        },
    }
    validate_receipt_plan(payload, plan)
    if mismatch == "contract":
        changed = copy.deepcopy(contract)
        changed["assets"][0]["issuer"] = "Different curated classification"
        plan["asset_scope"] = _scope(changed)
    elif mismatch == "run":
        plan["run_id"] = "new-run"
    elif mismatch == "source":
        plan["sdk"] = {**SDK, "commit": "b" * 40}
    elif mismatch == "payload-source":
        payload["git_sha"] = "b" * 40
    else:
        del payload["asset_identity"]["scope_lookup_key"]
    with pytest.raises(ValueError):
        validate_receipt_plan(payload, plan)


@pytest.mark.parametrize("mutation", ["changed", "missing"])
def test_historical_index_rebuild_does_not_admit_drifted_receipts(tmp_path, contract, mutation):
    from qa_lab.asset_scope_matrix import matrix_scenarios

    scope = _scope(contract)
    _append(tmp_path, scope, run_id="first", verdict="PASS", hour=1, raw_receipt={"amount": "123"})
    receipt = tmp_path / "intents/first/receipt.json"
    if mutation == "changed":
        receipt.write_text('{"amount":"999"}')
    else:
        receipt.unlink()
    projection = qa_history.rebuild_projections(tmp_path)
    assert projection["record_count"] == 1
    _append(tmp_path, _scope(contract, "sibling"), run_id="second", verdict="PASS", hour=2)
    assert len(qa_history.read_history(tmp_path)) == 2
    with pytest.raises(ValueError):
        qa_history.verify_history(tmp_path)
    with pytest.raises(ValueError):
        matrix_scenarios(tmp_path, {"cells": []}, qa_history.active_experiments(qa_history.read_history(tmp_path)))


@pytest.mark.parametrize("mutation", ["changed", "missing"])
def test_ordinary_projection_still_refuses_modified_summary(tmp_path, contract, mutation):
    _append(tmp_path, _scope(contract), run_id="first", verdict="PASS", hour=1)
    summary = tmp_path / "intents/first/summary.json"
    if mutation == "changed":
        summary.write_text('{"cells":[]}')
    else:
        summary.unlink()
    with pytest.raises(ValueError, match="summary"):
        qa_history.rebuild_projections(tmp_path)
