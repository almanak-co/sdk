"""Contract tests for the lifecycle-capability CI ratchet."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.core.capability_obligations import (
    IntentSemantics,
    ObligationDeclaration,
    ObligationId,
    SupportClaim,
    Unsupported,
)
from almanak.core.intent_types import IntentType
from almanak.framework.capabilities.effective_matrix import CapabilityCellKey
from almanak.framework.capabilities.obligation_profiles import (
    OBLIGATION_POLICY_VERSION,
    ReportedObligationState,
    audit_profile,
    profile_for,
)
from almanak.framework.primitives.types import Primitive
from scripts.ci import check_lifecycle_capability_ratchet as ratchet


def _disposition(state: ReportedObligationState) -> str | None:
    if state is ReportedObligationState.UNDECLARED:
        return None
    if state is ReportedObligationState.SATISFIED:
        return json.dumps(
            {
                "contract_version": "provider.v1",
                "provider_ref": "almanak.example:Provider",
                "test_evidence": [{"kind": "contract_test", "ref": "tests/example.py::test_provider"}],
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    if state is ReportedObligationState.NOT_APPLICABLE:
        return json.dumps(
            {"reason_code": "permission_plan_not_required", "rule_ref": "permission_plan_not_required"},
            sort_keys=True,
            separators=(",", ":"),
        )
    return json.dumps(
        {
            "owner": "sdk-connectors",
            "reason": "known gap",
            "review_by": "2026-09-01",
            "tracking_ref": "VIB-6651",
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def _cell(
    state: ReportedObligationState = ReportedObligationState.UNDECLARED,
    *,
    protocol: str = "test_swap",
    obligation: str = "compiler",
    disposition: str | None = None,
    sources: tuple[str, ...] = ("requirement:core_profile:profile.v1:compiler",),
) -> ratchet.CellSnapshot:
    identity = ratchet.CellIdentity(
        protocol=protocol,
        chain="ethereum",
        intent="SWAP",
        semantics="atomic_execution",
        primitive="swap",
        claim="core_execution",
    )
    return ratchet.CellSnapshot(
        identity,
        (
            ratchet.ObligationSnapshot(
                obligation,
                state,
                _disposition(state) if disposition is None else disposition,
                sources,
            ),
        ),
    )


def test_exact_legacy_undeclared_baseline_passes() -> None:
    baseline = (_cell(),)
    assert ratchet.compare_snapshots(baseline, baseline) == ()


def test_candidate_provenance_changes_do_not_change_the_state_floor() -> None:
    baseline = (_cell(sources=("candidate:one",)),)
    current = (_cell(sources=("candidate:two",)),)
    assert ratchet.compare_snapshots(baseline, current) == ()


@pytest.mark.parametrize(
    "state",
    (
        ReportedObligationState.SATISFIED,
        ReportedObligationState.NOT_APPLICABLE,
        ReportedObligationState.UNSUPPORTED,
    ),
)
def test_resolved_legacy_gap_is_stale_until_baseline_advances(state: ReportedObligationState) -> None:
    findings = ratchet.compare_snapshots((_cell(),), (_cell(state),))
    assert [item.kind for item in findings] == ["STALE_LEGACY_GAP"]


def test_new_undeclared_and_same_count_substitution_fail_exact_identity() -> None:
    findings = ratchet.compare_snapshots((_cell(protocol="old"),), (_cell(protocol="new"),))
    assert {item.kind for item in findings} == {"NEW_UNDECLARED", "REMOVED_BASELINE_ROW"}
    assert {item.cell.protocol for item in findings} == {"old", "new"}


def test_new_profile_obligation_is_a_new_gap() -> None:
    baseline = (_cell(),)
    current_cell = ratchet.CellSnapshot(
        baseline[0].identity,
        (*baseline[0].obligations, ratchet.ObligationSnapshot("receipt_evidence", ReportedObligationState.UNDECLARED)),
    )
    findings = ratchet.compare_snapshots(baseline, (current_cell,))
    assert [(item.kind, item.obligation) for item in findings] == [("NEW_UNDECLARED", "receipt_evidence")]


@pytest.mark.parametrize(
    "state",
    (
        ReportedObligationState.UNDECLARED,
        ReportedObligationState.UNSUPPORTED,
        ReportedObligationState.NOT_APPLICABLE,
    ),
)
def test_satisfied_cannot_regress(state: ReportedObligationState) -> None:
    findings = ratchet.compare_snapshots((_cell(ReportedObligationState.SATISFIED),), (_cell(state),))
    assert [item.kind for item in findings] == ["SATISFIED_REGRESSION"]


def test_satisfied_provider_or_evidence_change_is_visible_contract_drift() -> None:
    baseline = _cell(ReportedObligationState.SATISFIED)
    changed = _cell(
        ReportedObligationState.SATISFIED,
        disposition=baseline.obligations[0].disposition.replace("provider.v1", "provider.v2"),  # type: ignore[union-attr]
    )
    findings = ratchet.compare_snapshots((baseline,), (changed,))
    assert [item.kind for item in findings] == ["DECLARATION_DRIFT"]
    assert "provider.v1" in findings[0].render()
    assert "provider.v2" in findings[0].render()


def test_diagnostic_contains_full_identity_and_live_provenance() -> None:
    finding = ratchet.compare_snapshots((), (_cell(sources=("candidate:manifest:compiler.v1:Connector.compiler",)),))[0]
    rendered = finding.render()
    assert "test_swap/ethereum/SWAP/swap/core_execution :: compiler" in rendered
    assert "candidate:manifest:compiler.v1:Connector.compiler" in rendered


def _baseline_payload() -> dict[str, object]:
    obligations = {
        "asset_resolution": "U",
        "venue_resolution": "U",
        "amount_protection": "U",
        "compiler": "U",
        "receipt_evidence": "U",
        "money_legs": "U",
        "permission_plan": "U",
    }
    return {
        "schemaVersion": ratchet.BASELINE_SCHEMA_VERSION,
        "matrixSchemaVersion": 1,
        "obligationPolicyVersion": OBLIGATION_POLICY_VERSION,
        "generatorContract": "effective_lifecycle_capabilities.v1",
        "universe": {
            "kind": "registered_strategy_support",
            "sourceRef": "almanak.connectors._connector:CONNECTOR_DESCRIPTOR_REGISTRY",
        },
        "stateCodes": {
            "satisfied": "S",
            "not_applicable": "N",
            "unsupported": "X",
            "undeclared": "U",
        },
        "cells": [
            {
                **_cell().identity.to_dict(),
                "obligations": obligations,
            }
        ],
    }


def test_baseline_loader_is_fail_closed_on_schema_universe_duplicates_and_order(tmp_path: Path) -> None:
    path = tmp_path / "baseline.json"
    for mutation, message in (
        (lambda payload: payload.update(schemaVersion=99), "schemaVersion"),
        (lambda payload: payload.update(universe={"kind": "wrong"}), "universe"),
        (lambda payload: payload["cells"].append(payload["cells"][0].copy()), "duplicate cell"),  # type: ignore[union-attr,index]
    ):
        payload = _baseline_payload()
        mutation(payload)
        path.write_text(json.dumps(payload), encoding="utf-8")
        with pytest.raises(ratchet.BaselineFormatError, match=message):
            ratchet.load_baseline(path)


def test_baseline_loader_rejects_final_state_without_disposition(tmp_path: Path) -> None:
    payload = _baseline_payload()
    payload["cells"][0]["obligations"]["compiler"] = "S"  # type: ignore[index]
    path = tmp_path / "baseline.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ratchet.BaselineFormatError, match="require a canonical disposition"):
        ratchet.load_baseline(path)


def test_baseline_loader_rejects_missing_malformed_unknown_and_noncanonical_input(tmp_path: Path) -> None:
    missing = tmp_path / "missing.json"
    with pytest.raises(ratchet.BaselineFormatError, match="baseline not found"):
        ratchet.load_baseline(missing)

    path = tmp_path / "baseline.json"
    path.write_text("{broken", encoding="utf-8")
    with pytest.raises(ratchet.BaselineFormatError, match="cannot read"):
        ratchet.load_baseline(path)

    for mutate, message in (
        (lambda payload: payload.update(unknown=True), "unknown baseline fields"),
        (lambda payload: payload.update(stateCodes={}), "stateCodes"),
        (lambda payload: payload.update(matrixSchemaVersion=99), "matrixSchemaVersion"),
        (lambda payload: payload.update(matrixSchemaVersion=True), "matrixSchemaVersion"),
        (lambda payload: payload.update(schemaVersion=1.0), "schemaVersion"),
        (lambda payload: payload.update(obligationPolicyVersion=99), "obligationPolicyVersion"),
        (lambda payload: payload.update(obligationPolicyVersion=True), "obligationPolicyVersion"),
        (lambda payload: payload.update(generatorContract="wrong"), "generatorContract"),
        (lambda payload: payload["cells"][0].update(intent="NOT_AN_INTENT"), "non-canonical identity"),  # type: ignore[index,union-attr]
        (lambda payload: payload["cells"][0].update(semantics="position_open_or_increase"), "semantics for SWAP"),  # type: ignore[index,union-attr]
        (lambda payload: payload["cells"][0].update(obligations={"not_real": "U"}), "unknown obligation"),  # type: ignore[index,union-attr]
        (lambda payload: payload["cells"][0]["obligations"].update(compiler="?"), "invalid state code"),  # type: ignore[index,union-attr]
    ):
        payload = _baseline_payload()
        mutate(payload)
        path.write_text(json.dumps(payload), encoding="utf-8")
        with pytest.raises(ratchet.BaselineFormatError, match=message):
            ratchet.load_baseline(path)


def test_baseline_loader_rejects_noncanonical_cell_order(tmp_path: Path) -> None:
    payload = _baseline_payload()
    first = payload["cells"][0]  # type: ignore[index]
    second = {**first, "protocol": "aaa_swap"}  # type: ignore[arg-type]
    payload["cells"] = [first, second]
    path = tmp_path / "baseline.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ratchet.BaselineFormatError, match="canonical order"):
        ratchet.load_baseline(path)


def test_baseline_loader_rejects_typed_na_outside_core_profile_policy(tmp_path: Path) -> None:
    payload = _baseline_payload()
    payload["cells"][0]["obligations"]["compiler"] = {  # type: ignore[index]
        "state": "N",
        "disposition": {
            "rule_ref": "permission_plan_not_required",
            "reason_code": "permission_plan_not_required",
        },
    }
    path = tmp_path / "baseline.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ratchet.BaselineFormatError, match="cannot justify compiler"):
        ratchet.load_baseline(path)


def test_refresh_refuses_new_gap_but_writes_forward_resolution(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    path = tmp_path / "baseline.json"
    matrix = object()
    writes: list[object] = []
    monkeypatch.setattr(ratchet, "load_baseline", lambda _path: (_cell(),))
    monkeypatch.setattr(ratchet, "snapshot_matrix", lambda _matrix: (_cell(protocol="new"),))
    monkeypatch.setattr(ratchet, "write_baseline", lambda _path, value: writes.append(value))
    with pytest.raises(ratchet.BaselineFormatError, match="NEW_UNDECLARED"):
        ratchet.refresh_baseline(path, matrix)  # type: ignore[arg-type]
    assert writes == []

    monkeypatch.setattr(
        ratchet,
        "snapshot_matrix",
        lambda _matrix: (_cell(ReportedObligationState.SATISFIED),),
    )
    ratchet.refresh_baseline(path, matrix)  # type: ignore[arg-type]
    assert writes == [matrix]


def test_refresh_allows_removed_legacy_undeclared_rows(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    path = tmp_path / "baseline.json"
    matrix = object()
    writes: list[object] = []
    monkeypatch.setattr(ratchet, "snapshot_matrix", lambda _matrix: ())
    monkeypatch.setattr(ratchet, "write_baseline", lambda _path, value: writes.append(value))

    monkeypatch.setattr(ratchet, "load_baseline", lambda _path: (_cell(),))
    ratchet.refresh_baseline(path, matrix)  # type: ignore[arg-type]
    assert writes == [matrix]


@pytest.mark.parametrize(
    "state",
    (
        ReportedObligationState.SATISFIED,
        ReportedObligationState.NOT_APPLICABLE,
        ReportedObligationState.UNSUPPORTED,
    ),
)
def test_refresh_rejects_removed_reviewed_rows(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    state: ReportedObligationState,
) -> None:
    path = tmp_path / "baseline.json"
    matrix = object()
    monkeypatch.setattr(
        ratchet,
        "load_baseline",
        lambda _path: (_cell(state),),
    )
    monkeypatch.setattr(ratchet, "snapshot_matrix", lambda _matrix: ())
    with pytest.raises(ratchet.BaselineFormatError, match="REMOVED_BASELINE_ROW"):
        ratchet.refresh_baseline(path, matrix)  # type: ignore[arg-type]


def test_committed_baseline_matches_the_offline_registered_matrix(capsys: pytest.CaptureFixture[str]) -> None:
    assert ratchet.main([]) == 0
    output = capsys.readouterr().out
    assert "PASS: lifecycle capability ratchet matches" in output
    assert "cells /" in output


def _matrix_with_unsupported(review_by: date) -> SimpleNamespace:
    key = CapabilityCellKey(
        protocol="test_swap",
        chain="ethereum",
        intent=IntentType.SWAP,
        semantics=IntentSemantics.ATOMIC_EXECUTION,
        primitive=Primitive.SWAP,
        claim=SupportClaim.CORE_EXECUTION,
    )
    disposition = Unsupported("known gap", "VIB-6650", "sdk-connectors", review_by)
    audit = audit_profile(
        profile_for(key.profile_key),
        (ObligationDeclaration(ObligationId.COMPILER, disposition),),
    )
    return SimpleNamespace(
        cells=(
            SimpleNamespace(
                key=key,
                obligations=tuple(SimpleNamespace(audited=row) for row in audit.obligations),
            ),
        )
    )


def test_stale_unsupported_declaration_reports_exact_owned_gap() -> None:
    matrix = _matrix_with_unsupported(date(2026, 9, 1))
    disposition = next(
        row.audited.disposition
        for row in matrix.cells[0].obligations
        if row.audited.obligation is ObligationId.COMPILER
    )
    assert isinstance(disposition, Unsupported)
    assert ratchet.stale_unsupported_rows(matrix, as_of=disposition.review_by) == (
        "test_swap/ethereum/SWAP/swap/core_execution :: compiler: "
        "review_by=2026-09-01 owner=sdk-connectors tracking_ref=VIB-6650",
    )


def test_cli_fails_when_unsupported_review_deadline_is_due(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(ratchet, "build_effective_capability_matrix", lambda: _matrix_with_unsupported(date.today()))
    assert ratchet.main([]) == 1
    output = capsys.readouterr().out
    assert "FAIL: lifecycle capability ratchet found 1 unsupported declaration(s) due for review" in output
    assert "[STALE_UNSUPPORTED] test_swap/ethereum/SWAP/swap/core_execution :: compiler" in output


def test_cli_external_baseline_refresh_reports_success_after_write(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    path = tmp_path / "baseline.json"
    matrix = SimpleNamespace(cells=(object(),))
    monkeypatch.setattr(ratchet, "build_effective_capability_matrix", lambda: matrix)
    monkeypatch.setattr(ratchet, "stale_unsupported_rows", lambda _matrix, *, as_of: ())

    def refresh(target: Path, _matrix: object) -> None:
        target.write_text("written\n", encoding="utf-8")

    monkeypatch.setattr(ratchet, "refresh_baseline", refresh)
    assert ratchet.main(["--baseline", str(path), "--write-baseline"]) == 0
    assert path.read_text(encoding="utf-8") == "written\n"
    assert str(path) in capsys.readouterr().out
