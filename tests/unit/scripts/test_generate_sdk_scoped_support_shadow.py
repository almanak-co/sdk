from __future__ import annotations

import json
import socket
from pathlib import Path

import pytest

from almanak.connectors._connector_descriptor import ImportRef
from scripts.ci.generate_sdk_scoped_lifecycle_claims import DEFAULT_ARTIFACT as CLAIMS_ARTIFACT
from scripts.ci.generate_sdk_scoped_support_shadow import (
    DEFAULT_ARTIFACT,
    ShadowPolicyError,
    _decision_row,
    build_payload,
    check_artifact,
    main,
    render_payload,
)
from scripts.ci.lifecycle_evidence_integrity import EvidenceIntegrityError


@pytest.fixture(scope="module")
def shadow_payload() -> dict[str, object]:
    return build_payload()


def test_shadow_policy_has_exact_reviewed_shape(shadow_payload: dict[str, object]) -> None:
    payload = shadow_payload

    assert payload["summary"] == {
        "inventoryCells": 174,
        "declarationSatisfiedCells": 11,
        "declarationWithheldCells": 163,
        "newRiskEligible": 7,
        "newRiskWithheld": 56,
        "riskReductionContinuityPreserved": 4,
        "riskReductionContinuityUnproven": 61,
        "contextRequired": 46,
    }
    assert len(payload["decisions"]) == 174


def test_risk_reduction_uses_its_own_declaration_without_becoming_a_denial(
    shadow_payload: dict[str, object],
) -> None:
    decisions = shadow_payload["decisions"]
    risk_reduction = [
        row for row in decisions if row["semantics"] in {"position_decrease_or_close", "non_position_recovery"}
    ]

    assert len(risk_reduction) == 65
    assert {row["shadowDecision"] for row in risk_reduction} == {
        "preserve_continuity",
        "continuity_unproven",
    }
    assert sum(row["declarationStatus"] == "withheld" for row in risk_reduction) == 61
    assert all(
        row["shadowDecision"] == "preserve_continuity"
        for row in risk_reduction
        if row["declarationStatus"] == "satisfied"
    )
    assert all(
        row["shadowDecision"] == "continuity_unproven"
        for row in risk_reduction
        if row["declarationStatus"] == "withheld"
    )
    assert all(row["blockingObligations"] for row in risk_reduction if row["declarationStatus"] == "withheld")


def test_opening_declaration_gap_cannot_deny_an_independently_satisfied_repay(
    shadow_payload: dict[str, object],
) -> None:
    decisions = shadow_payload["decisions"]
    aave_arbitrum = {
        row["intent"]: row for row in decisions if row["protocol"] == "aave_v3" and row["chain"] == "arbitrum"
    }

    borrow = aave_arbitrum["BORROW"]
    repay = next(
        row
        for row in json.loads(CLAIMS_ARTIFACT.read_text(encoding="utf-8"))["withheldCells"]
        if row["protocol"] == "aave_v3" and row["chain"] == "arbitrum" and row["intent"] == "REPAY"
    )
    independently_satisfied_repay = _decision_row(repay, declaration_satisfied=True)

    assert borrow["shadowDecision"] == "withheld"
    assert independently_satisfied_repay["shadowDecision"] == "preserve_continuity"


def test_atomic_and_maintenance_actions_require_context_instead_of_risk_inference(
    shadow_payload: dict[str, object],
) -> None:
    decisions = shadow_payload["decisions"]
    contextual = [row for row in decisions if row["semantics"] in {"atomic_execution", "position_maintenance"}]

    assert len(contextual) == 46
    assert {row["shadowDecision"] for row in contextual} == {"context_required"}
    assert {row["policyLane"] for row in contextual} == {"context_required"}


def test_output_is_explicitly_inert_and_unapproved(shadow_payload: dict[str, object]) -> None:
    payload = shadow_payload

    assert payload["mode"] == "shadow"
    assert payload["activationEffect"] == "none"
    assert payload["humanReview"] == {
        "decision": "unapproved",
        "activationAuthorized": False,
        "reviewRef": None,
    }
    assert payload["scope"]["ciSuccessMeaning"] == "artifact_integrity_only"
    assert payload["scope"]["deploymentCompatibility"] == "not_evaluated"
    assert payload["scope"]["riskReductionContinuityDoesNotAssertExecutability"] is True
    assert payload["scope"]["riskReductionContinuityNeverDependsOnOpeningEligibility"] is True


def test_every_declaration_mismatch_has_blockers_and_an_explanation(
    shadow_payload: dict[str, object],
) -> None:
    decisions = shadow_payload["decisions"]
    withheld = [row for row in decisions if row["declarationStatus"] == "withheld"]

    assert len(withheld) == 163
    assert all(row["blockingObligations"] for row in withheld)
    assert all(row["reasonCode"] for row in withheld)


def test_shadow_artifact_is_byte_deterministic_and_current() -> None:
    assert render_payload() == render_payload()
    assert render_payload().endswith("\n")
    assert check_artifact(DEFAULT_ARTIFACT)


def test_stale_or_mutated_claims_artifact_fails_closed(tmp_path: Path) -> None:
    payload = json.loads(CLAIMS_ARTIFACT.read_text(encoding="utf-8"))
    payload["withheldCells"][0]["blockingObligations"] = ["invented"]
    claims = tmp_path / "claims.json"
    claims.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    with pytest.raises(ShadowPolicyError, match="stale or noncanonical"):
        build_payload(claims)


def test_generation_remains_offline(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("shadow policy generation must remain offline")

    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(ImportRef, "load", forbidden)
    assert build_payload()["summary"]["inventoryCells"] == 174


def test_source_claim_validation_failure_aborts_cli_write(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def reject_source() -> None:
        raise EvidenceIntegrityError("invalid source evidence")

    monkeypatch.setattr(
        "scripts.ci.generate_sdk_scoped_support_shadow.build_claims_payload",
        reject_source,
    )
    artifact = tmp_path / "shadow-policy.json"

    assert main(["--artifact", str(artifact), "--write"]) == 1
    assert not artifact.exists()
    assert "FAIL: scoped support shadow generation failed: source lifecycle claims failed validation" in (
        capsys.readouterr().out
    )


def test_cli_check_and_write_are_byte_exact(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    artifact = tmp_path / "shadow-policy.json"
    assert main(["--artifact", str(artifact)]) == 1
    assert main(["--artifact", str(artifact), "--write"]) == 0
    assert main(["--artifact", str(artifact)]) == 0
    assert artifact.read_text(encoding="utf-8") == render_payload()
    assert "7 new-risk eligible / 56 new-risk withheld" in capsys.readouterr().out
