"""Declared production profiles have real reference proofs on identified forks."""

import gzip
import hashlib
import json

import pytest

from almanak.connectors.curve.connector import connector
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.support import RunMode
from tests.qa.catalog import ROOT, catalog
from tests.qa.frozen_reference import archived_reference
from tests.qa.worker import assert_fork_actions

EVIDENCE = ROOT / "docs/internal/reports/evidence"
BASE = EVIDENCE / "2026-09-07-rollout/curve-production-rehearsals"
# Optimism and Base were promoted after the base package was sealed; their production
# EOA rehearsals are sealed separately from hosted Fork Qualification run 35886295777.
SUPPLEMENT = EVIDENCE / "2026-09-24-rollout/curve-op-production-rehearsals"
PROOFS = {
    package: json.loads((package / "manifest.json").read_text()) for package in (BASE, SUPPLEMENT)
}
SUPPLEMENTED = {
    f"curve_{chain}_{pool}_lp"
    for chain in ("optimism", "base")
    for pool in ("stable", "tricrypto", "twocrypto")
}
CASES = tuple(
    case
    for case in catalog()
    if case.connector == ConnectorId("curve")
    and case.lane == "fork"
    and case.input_policy == "pinned"
    and case.profile == "eoa"
    and case.scenario == ""
    and all(chain in connector().supported_chains(RunMode.PRODUCTION) for chain in case.chains)
)


def holding(reference):
    """The package and manifest whose passed rows hold one reference."""
    return next(
        (package, proof) for package, proof in PROOFS.items() if reference in proof["passed"]
    )


def artifact(package, name):
    item = next(row for row in PROOFS[package]["files"] if row["path"] == name)
    raw = (package / name).read_bytes()
    assert len(raw) == item["bytes"] and hashlib.sha256(raw).hexdigest() == item["sha256"]
    return gzip.decompress(raw)


def test_every_declared_production_curve_reference_has_retained_closure():
    base, supplement = (set(proof["passed"]) for proof in PROOFS.values())
    assert not base & supplement
    assert {case.reference for case in CASES} == base | supplement
    assert PROOFS[BASE]["status"] == "all_16_references_passed"
    assert len(PROOFS[BASE]["failed_attempts"]) == 3
    assert supplement == SUPPLEMENTED
    assert PROOFS[SUPPLEMENT]["status"] == "all_6_references_passed"
    assert PROOFS[SUPPLEMENT]["failed_attempts"] == []


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.reference)
def test_production_path_retains_actual_strategy_actions_and_payer_receipts(case, tmp_path):
    package, proof = holding(case.reference)
    prefix = proof["passed"][case.reference]["path"]
    result = json.loads(artifact(package, prefix + "/result.json.gz"))
    state = json.loads(artifact(package, prefix + "/checkpoint.json.gz"))
    reference = ROOT / "strategies" / case.reference
    archive = package / proof["source"]["archive"]
    assert state["strategy"] == archived_reference(
        archive.parent, archive.name, case.reference
    ).model_dump(mode="json")
    assert result["status"] == "passed"
    assert result["phases"] == [
        "production_eoa_entered",
        "runtime_resumed",
        "gateway_replaced",
        "canonical_receipts_and_zero_obligations",
    ]
    assert state["mode"] == "closed" and state["pending"] is None
    assert state["outcomes"] and all(outcome["success"] for outcome in state["outcomes"])
    assert state["delivered"] == len(state["outcomes"])
    assert all(
        outcome["execution"]["binding"]["profile"]["kind"] == "eoa" for outcome in state["outcomes"]
    )
    config = tmp_path / "config.yaml"
    config.write_bytes((reference / "config.yaml").read_bytes())
    assert_fork_actions(case, state, config)
