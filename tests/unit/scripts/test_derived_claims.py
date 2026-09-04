from __future__ import annotations

import hashlib

import pytest

from qa_lab.derived_claims import derive_observed_claim


def _claim(tmp_path, *, status: str = "PASS", measurement: object = 1) -> dict:
    artifact = tmp_path / "witness.json"
    artifact.write_text('{"measured":true}\n', encoding="utf-8")
    return {
        "schema_version": 1,
        "claim_id": "quant.books",
        "observer": "quant-auditor",
        "checks": [
            {
                "id": "sqlite_accounting_consistency",
                "status": status,
                "measured": status in {"PASS", "FAIL"},
                "measurement": measurement,
                "reason_codes": [],
                "artifacts": [
                    {
                        "path": "witness.json",
                        "sha256": hashlib.sha256(artifact.read_bytes()).hexdigest(),
                    }
                ],
            }
        ],
    }


def test_producer_status_and_observer_pass_are_not_lab_authoritative(tmp_path) -> None:
    raw = _claim(tmp_path)
    raw["status"] = "FAIL"

    claim = derive_observed_claim(
        raw,
        bundle=tmp_path,
        claim_id="quant.books",
        allowed_observers={"quant-auditor"},
        required_check_ids={"sqlite_accounting_consistency"},
    )

    assert claim["review_status"] == "PASS"
    assert claim["status"] == "UNMEASURED"
    assert claim["measurements"] == 1
    assert "observer_review_not_mechanically_entailed" in claim["reason_codes"]


def test_pass_requires_measurement_and_byte_bound_artifact(tmp_path) -> None:
    raw = _claim(tmp_path, measurement=None)

    with pytest.raises(ValueError, match="without a measurement and artifact"):
        derive_observed_claim(
            raw,
            bundle=tmp_path,
            claim_id="quant.books",
            allowed_observers={"quant-auditor"},
        )


def test_closed_check_contract_rejects_trivial_green_claim(tmp_path) -> None:
    raw = _claim(tmp_path)

    with pytest.raises(ValueError, match="check contract mismatch"):
        derive_observed_claim(
            raw,
            bundle=tmp_path,
            claim_id="quant.books",
            allowed_observers={"quant-auditor"},
            required_check_ids={"sqlite_accounting_consistency", "accountant_applicable_coverage"},
        )


def test_tampered_artifact_invalidates_claim(tmp_path) -> None:
    raw = _claim(tmp_path)
    (tmp_path / "witness.json").write_text('{"measured":false}\n', encoding="utf-8")

    with pytest.raises(ValueError, match="artifact digest mismatch"):
        derive_observed_claim(
            raw,
            bundle=tmp_path,
            claim_id="quant.books",
            allowed_observers={"quant-auditor"},
        )


def test_observer_cannot_cite_its_own_verdict_report_as_evidence(tmp_path) -> None:
    raw = _claim(tmp_path)
    report = tmp_path / "audit.md"
    report.write_text("I say PASS\n", encoding="utf-8")
    raw["checks"][0]["artifacts"] = [{"path": "audit.md", "sha256": hashlib.sha256(report.read_bytes()).hexdigest()}]

    with pytest.raises(ValueError, match="instead of raw evidence"):
        derive_observed_claim(
            raw,
            bundle=tmp_path,
            claim_id="quant.books",
            allowed_observers={"quant-auditor"},
        )
