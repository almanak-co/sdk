"""Matrix qualification is scoped to the enrolled subject market and exact axes."""

import copy

import pytest

from qa_lab import asset_scope_matrix
from qa_lab.asset_scenarios import build_scope
from tests.unit.scripts import test_qa_asset_scenarios as scenario_examples

contract = scenario_examples.contract


def _catalog(scenario):
    return {
        "cells": [
            {
                "id": "intent.aerodrome.base.SWAP",
                "asset_scenarios": [scenario],
                "proof_recipe": {
                    "roles": [
                        {
                            "nodes": [
                                {
                                    "scenario_id": scenario["scenario_id"],
                                    "nodeid": "test.py::test_stock",
                                    "exec_path": "eoa",
                                },
                            ]
                        }
                    ]
                },
            }
        ]
    }


def _projection(scenario, *, overrides=None):
    parent = scenario["parent_bindings"][0]
    scope = build_scope(scenario, parent, "test.py::test_stock")
    row = {
        "status": "PASS",
        "attribution_mode": "exact-runtime",
        "evidence_status": "COMPLETE",
        "contract_status": "VERIFIED",
        "provenance_status": "VERIFIED",
        "report_path": "intents/run/report.html",
        "git": {"commit": "a" * 40},
    }
    row.update(overrides or {})
    return {parent["parent_cell_id"]: {scenario["scenario_id"]: {scope["contract_sha256"]: row}}}


def test_quote_stablecoin_does_not_qualify_stablecoin_family(tmp_path, monkeypatch, contract):
    monkeypatch.setattr(asset_scope_matrix, "project_scoped_history", lambda *_: _projection(contract))
    rows = asset_scope_matrix.matrix_scenarios(tmp_path, _catalog(contract), [])
    row = rows[contract["parent_bindings"][0]["parent_cell_id"]][0]
    assert row["qualified"]
    assert row["families"] == ["tokenized-equities"]
    assert any("stablecoins" in asset["families"] for asset in row["assets"])


def test_multiple_bindings_keep_network_and_wallet_independent(tmp_path, monkeypatch, contract):
    baseline = contract["parent_bindings"][0]
    for network, wallet in [("mainnet", "eoa"), ("anvil", "safe")]:
        binding = {
            **baseline,
            "network": network,
            "exec_path": wallet,
            "parent_cell_id": f"intent.aerodrome.base.SWAP.{network}.{wallet}",
        }
        contract["parent_bindings"].append(binding)
    monkeypatch.setattr(asset_scope_matrix, "project_scoped_history", lambda *_: _projection(contract))
    rows = asset_scope_matrix.matrix_scenarios(tmp_path, _catalog(contract), [])
    assert rows[baseline["parent_cell_id"]][0]["qualified"]
    for parent in contract["parent_bindings"][1:]:
        row = rows[parent["parent_cell_id"]][0]
        assert not row["qualified"]
        assert not row["runnable"]
        assert row["report_path"] is None
        assert row["state"] == ("MAINNET RUNNER MISSING" if parent["network"] == "mainnet" else "NO EXACT TEST")


def test_contract_change_keeps_old_proof_as_gap(tmp_path, monkeypatch, contract):
    historical = _projection(contract)
    contract["resource"]["address"] = "0x" + "d" * 40
    monkeypatch.setattr(asset_scope_matrix, "project_scoped_history", lambda *_: historical)
    row = asset_scope_matrix.matrix_scenarios(tmp_path, _catalog(contract), [])[
        contract["parent_bindings"][0]["parent_cell_id"]
    ][0]
    assert row["state"] == "CONTRACT GAP"
    assert not row["qualified"]
    assert row["report_path"] is None


def test_multiple_subjects_and_families_count_scenario_once(tmp_path, monkeypatch, contract):
    contract["subject_assets"] = ["stock", "quote"]
    contract["assets"][1]["families"] = ["tokenized-equities", "vault-shares"]
    contract["assets"][0]["families"].append("vault-shares")
    monkeypatch.setattr(asset_scope_matrix, "project_scoped_history", lambda *_: _projection(contract))
    rows = asset_scope_matrix.matrix_scenarios(tmp_path, _catalog(contract), [])
    parent_rows = rows[contract["parent_bindings"][0]["parent_cell_id"]]
    assert len(parent_rows) == 1
    assert parent_rows[0]["families"] == ["tokenized-equities", "vault-shares"]


@pytest.mark.parametrize(
    "field,value",
    [
        ("status", "FAIL"),
        ("attribution_mode", "legacy"),
        ("evidence_status", "INCOMPLETE"),
        ("contract_status", "UNVERIFIED"),
        ("provenance_status", "UNVERIFIED"),
    ],
)
def test_nonqualifying_evidence_cannot_paint_scoped_pass(tmp_path, monkeypatch, contract, field, value):
    projection = _projection(contract, overrides={field: value})
    monkeypatch.setattr(asset_scope_matrix, "project_scoped_history", lambda *_: projection)
    row = asset_scope_matrix.matrix_scenarios(tmp_path, _catalog(contract), [])[
        contract["parent_bindings"][0]["parent_cell_id"]
    ][0]
    assert not row["qualified"]
    assert row["state"] != "PASS"
    expected = {
        "status": "FAIL",
        "attribution_mode": "ATTRIBUTION GAP",
        "evidence_status": "EVIDENCE GAP",
        "contract_status": "CONTRACT GAP",
        "provenance_status": "PROVENANCE GAP",
    }
    assert row["state"] == expected[field]


def test_parent_removal_does_not_reattribute_other_market(tmp_path, monkeypatch, contract):
    historical = _projection(contract)
    catalog = _catalog(copy.deepcopy(contract))
    catalog["cells"][0]["id"] = "intent.other_protocol.base.SWAP"
    monkeypatch.setattr(asset_scope_matrix, "project_scoped_history", lambda *_: historical)
    assert asset_scope_matrix.matrix_scenarios(tmp_path, catalog, []) == {}
