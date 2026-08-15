from __future__ import annotations

import json
import socket
from dataclasses import replace
from datetime import date
from pathlib import Path

import pytest

from almanak.connectors._connector_descriptor import ImportRef
from almanak.core.capability_obligations import Unsupported
from almanak.framework.capabilities.effective_matrix import (
    EffectiveCapabilityMatrix,
    MatrixUniverse,
    SourceRole,
    UniverseKind,
)
from scripts.ci.generate_sdk_scoped_lifecycle_claims import (
    DEFAULT_ARTIFACT,
    ScopedClaimsError,
    build_payload,
    check_artifact,
    main,
    render_payload,
)
from scripts.ci.lifecycle_evidence_integrity import EvidenceIntegrityError
from scripts.ci.production_claim_universe import build_production_core_execution_matrix

EXPECTED_DECLARATION_SATISFIED = [
    ("curve", "ethereum", "LP_OPEN"),
    ("morpho_blue", "arbitrum", "BORROW"),
    ("morpho_blue", "arbitrum", "REPAY"),
    ("morpho_blue", "base", "BORROW"),
    ("morpho_blue", "base", "REPAY"),
    ("morpho_blue", "ethereum", "BORROW"),
    ("morpho_blue", "ethereum", "REPAY"),
    ("morpho_blue", "polygon", "BORROW"),
    ("morpho_blue", "polygon", "REPAY"),
    ("spark", "ethereum", "BORROW"),
    ("spark", "ethereum", "SUPPLY"),
]


def test_payload_projects_exact_reviewed_claims_and_withholds_every_other_cell() -> None:
    payload = build_payload()

    assert payload["summary"] == {"auditedCells": 174, "declarationSatisfiedCells": 11, "withheldCells": 163}
    satisfied = payload["declarationSatisfiedClaims"]
    assert [(row["protocol"], row["chain"], row["intent"]) for row in satisfied] == (EXPECTED_DECLARATION_SATISFIED)
    assert len(payload["withheldCells"]) == 163
    assert all(row["status"] == "claim_not_satisfied" for row in payload["withheldCells"])
    assert all(row["blockingObligations"] for row in payload["withheldCells"])


def test_payload_is_explicitly_shadow_only_and_does_not_conflate_deployability() -> None:
    payload = build_payload()

    assert payload["mode"] == "shadow"
    assert payload["publicationEffect"] == "none"
    assert payload["scope"]["deploymentCompatibility"] == "not_evaluated"
    assert payload["scope"]["omittedClaimMeaning"] == "not_declaration_satisfied"
    assert payload["sourceUniverse"]["liveDeploymentStatus"] == "not_queried"


def test_payload_exposes_no_obligation_evidence_or_deferral_metadata() -> None:
    rendered = render_payload()
    forbidden = (
        '"obligations"',
        '"providerRef"',
        '"testEvidence"',
        '"trackingRef"',
        '"reviewBy"',
        '"owner"',
        '"reason"',
    )
    assert not any(value in rendered for value in forbidden)


def test_payload_is_byte_deterministic_and_checked_artifact_is_current() -> None:
    assert render_payload() == render_payload()
    assert render_payload().endswith("\n")
    assert check_artifact(DEFAULT_ARTIFACT)


def test_projection_fails_closed_on_wrong_universe() -> None:
    matrix = build_production_core_execution_matrix()
    wrong = replace(matrix, universe=MatrixUniverse(UniverseKind.REGISTERED_STRATEGY_SUPPORT, "wrong:v1"))

    with pytest.raises(ScopedClaimsError, match="sealed injected production universe"):
        build_payload(wrong)


def test_projection_fails_closed_on_missing_cell() -> None:
    matrix = build_production_core_execution_matrix()
    missing = EffectiveCapabilityMatrix(matrix.cells[:-1], matrix.universe)

    with pytest.raises(ScopedClaimsError, match="exactly 174"):
        build_payload(missing)


def test_projection_fails_closed_on_substituted_cell_identity() -> None:
    matrix = build_production_core_execution_matrix()
    target = matrix.cells[-1]
    substituted = replace(target, key=replace(target.key, chain="unreviewed-chain"))
    cells = tuple(sorted((*matrix.cells[:-1], substituted), key=lambda cell: cell.key.sort_key()))
    mutation = EffectiveCapabilityMatrix(cells, matrix.universe)

    with pytest.raises(ScopedClaimsError, match="exactly match the sealed production inventory"):
        build_payload(mutation)


def test_projection_removes_a_cell_when_one_obligation_becomes_unsupported() -> None:
    matrix = build_production_core_execution_matrix()
    target = next(cell for cell in matrix.cells if cell.claim_satisfied)
    first = target.obligations[0]
    unsupported = Unsupported("mutation", "VIB-6671", "SDK Capability Audit", date(2026, 10, 15))
    mutated_row = replace(first, audited=replace(first.audited, disposition=unsupported))
    mutated_cell = replace(target, obligations=(mutated_row, *target.obligations[1:]))
    mutated = replace(matrix, cells=tuple(mutated_cell if cell is target else cell for cell in matrix.cells))

    payload = build_payload(mutated)
    assert payload["summary"] == {"auditedCells": 174, "declarationSatisfiedCells": 10, "withheldCells": 164}


def test_projection_rejects_any_undeclared_obligation() -> None:
    matrix = build_production_core_execution_matrix()
    target = matrix.cells[0]
    first = target.obligations[0]
    undeclared_row = replace(
        first,
        audited=replace(first.audited, disposition=None),
        sources=tuple(source for source in first.sources if source.role is not SourceRole.DECLARATION),
    )
    undeclared_cell = replace(target, obligations=(undeclared_row, *target.obligations[1:]))
    mutated = replace(matrix, cells=(undeclared_cell, *matrix.cells[1:]))

    with pytest.raises(ScopedClaimsError, match="cannot project UNDECLARED"):
        build_payload(mutated)


def test_projection_remains_offline(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("scoped claims generation must remain offline")

    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(ImportRef, "load", forbidden)
    assert build_payload()["summary"]["auditedCells"] == 174


def test_evidence_integrity_failure_aborts_projection_and_cli_write(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def reject_evidence(_matrix: EffectiveCapabilityMatrix) -> None:
        raise EvidenceIntegrityError("invalid reviewed evidence")

    monkeypatch.setattr(
        "scripts.ci.generate_sdk_scoped_lifecycle_claims.validate_matrix_evidence",
        reject_evidence,
    )

    with pytest.raises(EvidenceIntegrityError, match="invalid reviewed evidence"):
        build_payload()

    artifact = tmp_path / "claims.json"
    assert main(["--artifact", str(artifact), "--write"]) == 1
    assert not artifact.exists()
    assert "FAIL: scoped lifecycle claims generation failed: invalid reviewed evidence" in capsys.readouterr().out


def test_corrupt_artifact_is_reported_as_unreadable(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    artifact = tmp_path / "claims.json"
    artifact.write_bytes(b"\xff")

    assert not check_artifact(artifact)
    assert main(["--artifact", str(artifact)]) == 1
    assert "FAIL: scoped lifecycle claims artifact is missing or unreadable" in capsys.readouterr().out


def test_cli_check_and_write_are_byte_exact(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    artifact = tmp_path / "claims.json"
    assert main(["--artifact", str(artifact)]) == 1
    assert main(["--artifact", str(artifact), "--write"]) == 0
    assert main(["--artifact", str(artifact)]) == 0
    assert json.loads(artifact.read_text()) == build_payload()
    assert "PASS: SDK shadow claims match 11 declaration-satisfied / 163 withheld cells" in capsys.readouterr().out
