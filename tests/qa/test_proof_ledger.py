"""Obligation status requires every mapped input lane and preserves missing case identity."""

import json
from dataclasses import replace

import pytest

from almanak.caip import ChainId
from almanak.connectors.identifiers import ConnectorId, ProtocolVariant
from almanak.connectors.support import EconomicAction
from scripts.qa_bench import proof_mapping
from tests.qa.catalog import Case, ProofScope
from tests.qa.results import row, save_results


def scoped_case():
    return Case(
        id="fork/pinned/composed/safe_roles",
        lane="fork",
        mode="fork",
        profile="safe_roles",
        scopes=(
            ProofScope(
                connector=ConnectorId("curve"),
                chains=(ChainId("eip155:8453"),),
                actions=(EconomicAction.SWAP,),
                variants=(ProtocolVariant("stableswap_ng"),),
            ),
            ProofScope(
                connector=ConnectorId("aave_v3"),
                chains=(ChainId("eip155:8453"),),
                actions=(EconomicAction.BORROW,),
            ),
        ),
    )


@pytest.mark.parametrize("status", ["failed", "blocked", "skipped", "missing", "passed"])
def test_fresh_failure_or_omission_cannot_hide_behind_pinned_success(tmp_path, status):
    pinned = scoped_case()
    fresh = replace(pinned, id="fork/fresh/composed/safe_roles", input_policy="fresh")
    mapping = proof_mapping((pinned, fresh), pinned.obligations())
    assert len(mapping) == 2
    rows = [{**row(pinned), "artifact": "pinned", "status": "passed"}]
    if status != "missing":
        rows.append({**row(fresh), "artifact": "fresh", "status": status})
    result = save_results(tmp_path, {"proof_mapping": mapping, "execution": "run"}, rows)
    expected = "passed" if status == "passed" else "incomplete_or_failed"
    assert result["status"] == expected
    ledger = json.loads((tmp_path / "proof-ledger.json").read_text())
    assert ledger["status"] == expected
    assert {item["status"] for item in ledger["obligations"]} == {expected}
    assert {item["obligation"]["connector"] for item in ledger["obligations"]} == {
        "curve",
        "aave_v3",
    }
    for item in ledger["obligations"]:
        assert item["case_ids"] == sorted((pinned.id, fresh.id))
        assert item["outstanding_case_ids"] == ([] if status == "passed" else [fresh.id])


def test_no_mapping_and_wrong_variant_do_not_establish_obligation_success(tmp_path):
    case = scoped_case()
    required = case.obligations()
    wrong = replace(case, scopes=(replace(case.scopes[0], variants=(ProtocolVariant("other"),)),))
    mapping = proof_mapping((wrong,), required)
    result = save_results(
        tmp_path,
        {"proof_mapping": mapping},
        [{**row(wrong), "artifact": "wrong", "status": "passed"}],
    )
    assert result["status"] == "incomplete_or_failed"
    assert all(not item["case_ids"] for item in mapping)
