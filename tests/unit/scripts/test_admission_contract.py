"""Acceptance tests for the shared admission contract (VIB-6712).

The contract is a fail-closed guard, so every test here comes in a pair: one
that the guard admits, and one that it must refuse.  A guard with no refusing
case is indistinguishable from a guard that cannot fire.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from qa_lab.admission_contract import (
    AdmissionRefused,
    admit_claim,
    artifact_authority,
    prove_instrument_discriminates,
    registry_authority,
    verify_authorities,
)


@pytest.fixture
def bundle(tmp_path: Path) -> Path:
    root = tmp_path / "bundle"
    root.mkdir()
    (root / "measurement.json").write_text(json.dumps({"transactions": 3, "reverted": 0}), encoding="utf-8")
    (root / "other.json").write_text(json.dumps({"unused": True}), encoding="utf-8")
    return root


def _honest_derive(bundle: Path) -> dict[str, Any]:
    """A derivation that genuinely reads the artifact it cites."""
    path = bundle / "measurement.json"
    if not path.is_file():
        return {"claim_id": "test.axis", "status": "UNMEASURED", "authorities": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {"claim_id": "test.axis", "status": "FAIL", "authorities": []}
    status = "PASS" if payload.get("reverted") == 0 and payload.get("transactions", 0) > 0 else "FAIL"
    return {
        "claim_id": "test.axis",
        "status": status,
        "measurements": 1,
        "authorities": [artifact_authority(bundle, path)],
    }


def _vacuous_derive(bundle: Path) -> dict[str, Any]:
    """A derivation that claims an authority it never reads."""
    return {
        "claim_id": "test.axis",
        "status": "PASS",
        "measurements": 1,
        "authorities": [{"kind": "artifact", "source": "measurement.json", "identity": _digest(bundle)}],
    }


def _digest(bundle: Path) -> str:
    from qa_lab.derived_claims import sha256_file

    return sha256_file(bundle / "measurement.json")


class TestDerivedNotDeclared:
    def test_official_status_comes_from_the_derivation(self, bundle: Path) -> None:
        claim = admit_claim("axis", _honest_derive, bundle=bundle, declared_status="PASS")
        assert claim["status"] == "PASS"
        assert claim["admission_control"]["derived_status"] == "PASS"

    def test_a_declaration_that_disagrees_refuses_the_seal(self, bundle: Path) -> None:
        (bundle / "measurement.json").write_text(json.dumps({"transactions": 3, "reverted": 1}), encoding="utf-8")
        with pytest.raises(AdmissionRefused, match="declared PASS but the sealer derived FAIL"):
            admit_claim("axis", _honest_derive, bundle=bundle, declared_status="PASS")

    def test_a_declaration_cannot_promote_an_unmeasured_claim(self, bundle: Path) -> None:
        (bundle / "measurement.json").unlink()
        with pytest.raises(AdmissionRefused, match="declared PASS but the sealer derived UNMEASURED"):
            admit_claim("axis", _honest_derive, bundle=bundle, declared_status="PASS")

    def test_an_absent_declaration_still_admits_a_derived_claim(self, bundle: Path) -> None:
        claim = admit_claim("axis", _honest_derive, bundle=bundle, declared_status=None)
        assert claim["status"] == "PASS"
        assert claim["admission_control"]["declared_status"] is None


class TestDeclaredAuthority:
    def test_a_green_without_a_declared_authority_is_refused(self, bundle: Path) -> None:
        def anonymous(_: Path) -> dict[str, Any]:
            return {"claim_id": "test.axis", "status": "PASS", "measurements": 1}

        with pytest.raises(AdmissionRefused, match="does not declare the authority"):
            admit_claim("axis", anonymous, bundle=bundle, declared_status="PASS")

    def test_an_authority_absent_from_the_bundle_is_refused(self, bundle: Path) -> None:
        claim = {"authorities": [{"kind": "artifact", "source": "missing.json", "identity": "a" * 64}]}
        with pytest.raises(AdmissionRefused, match="absent from the evidence set"):
            verify_authorities(claim, bundle=bundle, axis="axis")

    def test_an_authority_whose_bytes_moved_is_refused(self, bundle: Path) -> None:
        claim = {"authorities": [{"kind": "artifact", "source": "measurement.json", "identity": "b" * 64}]}
        with pytest.raises(AdmissionRefused, match="does not match its declared digest"):
            verify_authorities(claim, bundle=bundle, axis="axis")

    def test_an_authority_escaping_the_bundle_is_refused(self, bundle: Path) -> None:
        claim = {"authorities": [{"kind": "artifact", "source": "../escape.json", "identity": "c" * 64}]}
        with pytest.raises(AdmissionRefused, match="unsafe path"):
            verify_authorities(claim, bundle=bundle, axis="axis")

    def test_a_registry_authority_requires_a_content_identity(self) -> None:
        assert registry_authority("gmx.markets", "sha256:abc")["kind"] == "registry"
        with pytest.raises(AdmissionRefused, match="source and an identity"):
            registry_authority("gmx.markets", "")


class TestInstrumentLiveness:
    """The negative control, and the negative control on the negative control."""

    def test_an_honest_derivation_goes_non_green_when_its_evidence_is_corrupted(self, bundle: Path) -> None:
        claim = admit_claim("axis", _honest_derive, bundle=bundle, declared_status="PASS")
        liveness = claim["admission_control"]["liveness"]
        assert liveness["status"] == "PASS"
        assert [row["mutant_status"] for row in liveness["mutations"]] == ["FAIL"]

    def test_a_derivation_that_ignores_its_cited_evidence_is_refused(self, bundle: Path) -> None:
        with pytest.raises(AdmissionRefused, match="does not read the evidence it cites"):
            admit_claim("axis", _vacuous_derive, bundle=bundle, declared_status="PASS")

    def test_liveness_is_not_required_of_a_non_green_claim(self, bundle: Path) -> None:
        (bundle / "measurement.json").write_text(json.dumps({"transactions": 0, "reverted": 0}), encoding="utf-8")
        claim = admit_claim("axis", _honest_derive, bundle=bundle, declared_status="FAIL")
        assert claim["status"] == "FAIL"
        assert claim["admission_control"]["liveness"]["status"] == "NOT_APPLICABLE"

    def test_a_green_citing_only_an_unmutable_registry_is_refused(self, bundle: Path) -> None:
        authorities = [registry_authority("gmx.markets", "sha256:abc")]
        with pytest.raises(AdmissionRefused, match="cites no mutable artifact"):
            prove_instrument_discriminates(_honest_derive, bundle=bundle, authorities=authorities, axis="axis")

    def test_the_original_bundle_is_never_mutated(self, bundle: Path) -> None:
        before = (bundle / "measurement.json").read_bytes()
        admit_claim("axis", _honest_derive, bundle=bundle, declared_status="PASS")
        assert (bundle / "measurement.json").read_bytes() == before

    def test_a_derivation_that_raises_on_corruption_counts_as_discriminating(self, bundle: Path) -> None:
        def strict(target: Path) -> dict[str, Any]:
            payload = json.loads((target / "measurement.json").read_text(encoding="utf-8"))
            return {
                "claim_id": "test.axis",
                "status": "PASS" if payload["reverted"] == 0 else "FAIL",
                "measurements": 1,
                "authorities": [artifact_authority(target, target / "measurement.json")],
            }

        claim = admit_claim("axis", strict, bundle=bundle, declared_status="PASS")
        assert claim["admission_control"]["liveness"]["mutations"][0]["mutant_status"] == "REFUSED"
