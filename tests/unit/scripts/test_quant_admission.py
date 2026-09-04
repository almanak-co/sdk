"""Adversarial contracts for official Quant admission."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from qa_lab.quant_admission import (
    EVIDENCE_KIND,
    evidence_set_digest,
    validate_audit_decision,
    validate_claim_scope,
)


def _contract(bundle: Path, *, required: list[str] | None = None) -> tuple[dict, str]:
    required = required or ["strategy"]
    contract = {
        "schema_version": 1,
        "goal": "action-density",
        "claim_scope": {
            "required": required,
            "not_applicable": [axis for axis in ("strategy", "books", "dashboard", "harness") if axis not in required],
        },
    }
    path = bundle / "lifecycle-contract.json"
    path.write_text(json.dumps(contract))
    return contract, hashlib.sha256(path.read_bytes()).hexdigest()


def _decision(bundle: Path, *, required: list[str] | None = None, status: str = "PASS") -> dict:
    required = required or ["strategy"]
    transcript = bundle / "audit.md"
    transcript.write_text("independent audit transcript\n")
    digest, inventory = evidence_set_digest(bundle)
    claims = {axis: {"status": status, "measurements": 1, "reason_codes": []} for axis in required}
    if status == "FAIL":
        witness = next(row for row in inventory if row["path"] == "finding.json")
        for claim in claims.values():
            claim.update(reason_codes=["measured_product_failure"], failure_evidence=[witness])
    payload = {
        "schema_version": 1,
        "evidence_kind": EVIDENCE_KIND,
        "audit_verdict": "AUDIT_CONFIRMED",
        "seal_eligible": True,
        "experiment_completed": True,
        "evidence_set_sha256": digest,
        "lifecycle_contract_sha256": hashlib.sha256((bundle / "lifecycle-contract.json").read_bytes()).hexdigest(),
        "required_claims": required,
        "auditor": {
            "role": "quant-admission-auditor",
            "run_id": "audit-run-1",
            "identity": "independent-test-auditor",
            "transcript_path": "audit.md",
            "transcript_sha256": hashlib.sha256(transcript.read_bytes()).hexdigest(),
        },
        "claims": claims,
    }
    (bundle / "audit-decision.json").write_text(json.dumps(payload))
    return payload


def _bundle(tmp_path: Path, *, required: list[str] | None = None) -> tuple[Path, dict, str]:
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "finding.json").write_text(json.dumps({"measured": True}))
    contract, digest = _contract(bundle, required=required)
    _decision(bundle, required=required)
    return bundle, contract, digest


def test_decision_is_bound_to_exact_frozen_evidence_set(tmp_path: Path) -> None:
    bundle, contract, digest = _bundle(tmp_path)

    admission = validate_audit_decision(bundle, contract=contract, contract_sha256=digest)

    assert admission["status"] == "OFFICIAL"
    assert admission["claims"]["strategy"]["status"] == "PASS"
    assert (
        admission["evidence_set_sha256"]
        == json.loads((bundle / "audit-decision.json").read_text())["evidence_set_sha256"]
    )


def test_decision_file_is_not_part_of_its_own_evidence_digest(tmp_path: Path) -> None:
    bundle, _, _ = _bundle(tmp_path)
    before, _ = evidence_set_digest(bundle)
    decision = json.loads((bundle / "audit-decision.json").read_text())
    decision["diagnostic_note"] = "decision metadata does not change observed bytes"
    (bundle / "audit-decision.json").write_text(json.dumps(decision))

    after, _ = evidence_set_digest(bundle)

    assert after == before


def test_post_audit_evidence_mutation_is_rejected(tmp_path: Path) -> None:
    bundle, contract, digest = _bundle(tmp_path)
    (bundle / "finding.json").write_text(json.dumps({"measured": False}))

    with pytest.raises(ValueError, match="not bound to the frozen evidence set"):
        validate_audit_decision(bundle, contract=contract, contract_sha256=digest)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ({"seal_eligible": False}, "seal-eligible"),
        ({"audit_verdict": "AUDIT_OVERTURNED"}, "seal-eligible"),
        ({"experiment_completed": False}, "experiment_completed"),
        ({"evidence_set_sha256": "0" * 64}, "frozen evidence set"),
        ({"lifecycle_contract_sha256": "0" * 64}, "lifecycle contract"),
    ],
)
def test_ineligible_or_unbound_decision_is_rejected(tmp_path: Path, mutation: dict, message: str) -> None:
    bundle, contract, digest = _bundle(tmp_path)
    path = bundle / "audit-decision.json"
    decision = json.loads(path.read_text())
    decision.update(mutation)
    path.write_text(json.dumps(decision))

    with pytest.raises(ValueError, match=message):
        validate_audit_decision(bundle, contract=contract, contract_sha256=digest)


def test_required_claim_must_be_measured_pass_or_fail(tmp_path: Path) -> None:
    bundle, contract, digest = _bundle(tmp_path)
    path = bundle / "audit-decision.json"
    decision = json.loads(path.read_text())
    decision["claims"]["strategy"].update(status="UNMEASURED", measurements=0)
    path.write_text(json.dumps(decision))

    with pytest.raises(ValueError, match="must be PASS or FAIL"):
        validate_audit_decision(bundle, contract=contract, contract_sha256=digest)


def test_official_fail_requires_hashed_failure_evidence(tmp_path: Path) -> None:
    bundle, contract, digest = _bundle(tmp_path)
    _decision(bundle, status="FAIL")
    path = bundle / "audit-decision.json"
    decision = json.loads(path.read_text())
    decision["claims"]["strategy"]["failure_evidence"][0]["sha256"] = "0" * 64
    path.write_text(json.dumps(decision))

    with pytest.raises(ValueError, match="not bound to the frozen evidence set"):
        validate_audit_decision(bundle, contract=contract, contract_sha256=digest)


def test_claim_scope_classifies_every_axis_once() -> None:
    with pytest.raises(ValueError, match="every Quant axis"):
        validate_claim_scope({"claim_scope": {"required": ["strategy"], "not_applicable": ["books"]}})
    with pytest.raises(ValueError, match="both required and not_applicable"):
        validate_claim_scope(
            {
                "claim_scope": {
                    "required": ["strategy"],
                    "not_applicable": ["strategy", "books", "dashboard", "harness"],
                }
            }
        )
