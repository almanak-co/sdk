"""Contracts for append-only QA triage dispositions."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT_DIR = REPO_ROOT / "scripts" / "quant-test"


def _load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, SCRIPT_DIR / filename)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def modules():
    return _load("qa_triage_test", "qa_triage.py"), _load("qa_coverage_triage_test", "qa_coverage.py")


@pytest.fixture
def store(tmp_path: Path, modules) -> Path:
    _, qa = modules
    catalog = tmp_path / "cells.yaml"
    catalog.write_text(
        yaml.safe_dump(
            {
                "version": 1,
                "chains": ["arbitrum"],
                "defaults": {"networks": ["mainnet"], "exec_paths": ["eoa"]},
                "cells": [
                    {
                        "id": "perp.gmx_v2.arbitrum.simple",
                        "primitive": "perp",
                        "protocol": "gmx_v2",
                        "chain": "arbitrum",
                        "lifecycle": "simple",
                    }
                ],
            }
        )
    )
    root = tmp_path / "store"
    qa.bootstrap_store(root, catalog)
    evidence = root / "runs/2026/08/06/gmx"
    evidence.mkdir(parents=True)
    (evidence / "finding.json").write_text('{"finding":"false zero"}\n')
    return root


def _decision(*, approval: dict, triage_id: str, supersedes: str | None = None) -> dict:
    row = {
        "schema_version": 1,
        "evidence_kind": "almanak.qa.triage",
        "triage_id": triage_id,
        "created_at": "2026-08-07T12:00:00Z" if not supersedes else "2026-08-07T13:00:00Z",
        "targets": [{"kind": "cell", "id": "perp.gmx_v2.arbitrum.simple.mainnet.eoa"}],
        "classification": "TEST_DEFECT",
        "disposition": "DRAFT_NEW",
        "confidence": "HIGH",
        "fingerprint": "TEST_DEFECT:FALSE-ZERO-NAV:positions-only-field",
        "observation": "The auditor treated position value as full-wallet NAV.",
        "contract": "total_value_usd is position scoped; cash is separate.",
        "evidence": [{"label": "Finding", "relpath": "runs/2026/08/06/gmx/finding.json"}],
        "superseded_ticket_ids": ["VIB-5183"],
        "approval": approval,
    }
    if supersedes:
        row["supersedes_triage_id"] = supersedes
        row["ticket"] = {
            "identifier": "VIB-7000",
            "title": "Auditor misreads position value as NAV",
            "url": "https://linear.app/almanak/issue/VIB-7000/example",
        }
    return row


def test_draft_is_immutable_and_does_not_change_ticket_associations(modules, store: Path, tmp_path: Path) -> None:
    triage, _ = modules
    source = tmp_path / "draft.json"
    source.write_text(json.dumps(_decision(approval={"state": "DRAFT"}, triage_id="triage-gmx-draft")))

    target = triage.seal_decision(decision_json=source, store=store)

    sealed = json.loads((target / "decision.json").read_text())
    latest = json.loads((store / "index" / triage.INDEX_NAME).read_text())
    assert sealed["triage_state"] == "DRAFT_READY"
    assert latest["perp.gmx_v2.arbitrum.simple.mainnet.eoa"]["triage_state"] == "DRAFT_READY"
    assert triage.ticket_associations(store) == ([], set())
    assert "does not change ticket ownership" in (target / "report.html").read_text()
    with pytest.raises(FileExistsError):
        triage.seal_decision(decision_json=source, store=store)


def test_approved_followup_supersedes_association_without_mutating_old_decision(
    modules, store: Path, tmp_path: Path
) -> None:
    triage, qa = modules
    draft = tmp_path / "draft.json"
    draft.write_text(json.dumps(_decision(approval={"state": "DRAFT"}, triage_id="triage-gmx-draft")))
    draft_target = triage.seal_decision(decision_json=draft, store=store)
    approved = tmp_path / "approved.json"
    approved.write_text(
        json.dumps(
            _decision(
                approval={"state": "APPROVED", "by": "qa-owner", "at": "2026-08-07T13:00:00Z"},
                triage_id="triage-gmx-approved",
                supersedes="triage-gmx-draft",
            )
        )
    )

    approved_target = triage.seal_decision(decision_json=approved, store=store)
    active, superseded = triage.ticket_associations(store)
    index = qa.rebuild_ticket_index(store)

    assert json.loads((draft_target / "decision.json").read_text())["approval"]["state"] == "DRAFT"
    assert json.loads((approved_target / "decision.json").read_text())["approval"]["attestation_kind"] == (
        "operator-self-attested"
    )
    assert [row["ticket"]["identifier"] for row in active] == ["VIB-7000"]
    assert superseded == {("perp.gmx_v2.arbitrum.simple.mainnet.eoa", "VIB-5183")}
    assert index["tickets"]["VIB-7000"]["reports"][0]["association_status"] == "active"


def test_rejects_pattern_supersedes_triage_id(modules, store: Path, tmp_path: Path) -> None:
    triage, _ = modules
    source = tmp_path / "decision.json"
    source.write_text(
        json.dumps(
            _decision(
                approval={"state": "APPROVED", "by": "qa-owner", "at": "2026-08-07T13:00:00Z"},
                triage_id="triage-gmx-approved",
                supersedes="triage-gmx-*",
            )
        )
    )

    with pytest.raises(ValueError, match="supersedes_triage_id contains unsupported characters"):
        triage.seal_decision(decision_json=source, store=store)


def test_rejects_evidence_outside_store(modules, store: Path, tmp_path: Path) -> None:
    triage, _ = modules
    outside = tmp_path / "outside.json"
    outside.write_text("{}")
    row = _decision(approval={"state": "DRAFT"}, triage_id="triage-invalid-evidence")
    row["evidence"] = [{"relpath": "../outside.json"}]
    source = tmp_path / "decision.json"
    source.write_text(json.dumps(row))

    # NOTE: this alternation accepts either guard firing, so a regression to a
    # plain is_file() failure would still pass here.
    with pytest.raises(ValueError, match="store file|escapes"):
        triage.seal_decision(decision_json=source, store=store)


def test_late_import_of_older_decision_does_not_replace_latest(modules, store: Path, tmp_path: Path) -> None:
    triage, _ = modules
    newest = _decision(approval={"state": "DRAFT"}, triage_id="triage-newest")
    newest["created_at"] = "2026-08-07T14:00:00Z"
    newest_path = tmp_path / "newest.json"
    newest_path.write_text(json.dumps(newest))
    triage.seal_decision(decision_json=newest_path, store=store)
    older = _decision(approval={"state": "DRAFT"}, triage_id="triage-older")
    older["created_at"] = "2026-08-07T11:00:00Z"
    older_path = tmp_path / "older.json"
    older_path.write_text(json.dumps(older))

    triage.seal_decision(decision_json=older_path, store=store)

    latest = json.loads((store / "index" / triage.INDEX_NAME).read_text())
    assert latest["perp.gmx_v2.arbitrum.simple.mainnet.eoa"]["triage_id"] == "triage-newest"
