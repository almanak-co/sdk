"""Canonical-evidence rendering contracts for quant-test reports."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType

SCRIPT = Path(__file__).parents[3] / "scripts" / "quant-test" / "render_report.py"


def _load():
    spec = importlib.util.spec_from_file_location("render_report_evidence_test", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _write(path: Path, value):
    path.write_text(json.dumps(value))


def test_canonical_summary_displays_every_accountant_class_without_promotion(tmp_path, monkeypatch):
    module = _load()
    _write(
        tmp_path / "accountant.json",
        {
            "cell_details": [
                {"id": "G1", "description": "money", "status": "PASS", "diagnostic": "ties"},
                {"id": "G2", "description": "cost", "status": "FAIL", "diagnostic": "gap"},
                {"id": "G3", "description": "yield", "status": "SKIP", "diagnostic": "N/A: no yield"},
                {"id": "G4", "description": "track C", "status": "XFAIL", "diagnostic": "expected gap"},
            ]
        },
    )
    _write(
        tmp_path / "receipt-reconciliation.json",
        {
            "canonical_hash_count": 3,
            "native_unit_reconciliation": {"status": "PASS"},
            "usd_price_basis_reconciliation": {"status": "PASS", "basis": "per-intent oracle"},
        },
    )
    _write(
        tmp_path / "evidence-manifest.json",
        {"created_at": "2026-08-09T12:00:00Z", "git": {"commit": "a" * 40, "sdk_version": "1.2.3"},
         "run_id": "sealed-run-1"},
    )
    fake = ModuleType("qa_run_evidence")
    fake.verify_manifest = lambda *_: {}
    monkeypatch.setitem(sys.modules, "qa_run_evidence", fake)
    data = {
        "meta": {
            "slug": "benqi_lending_lifecycle",
            "dashboard_evidence": {
                "applicability": "APPLICABLE",
                "capture_mode": "live",
                "config_sha256": "b" * 64,
            },
        }
    }
    _write(
        tmp_path / "batch-nav-proof.json",
        {"strategies": {"benqi_lending_lifecycle": {"nav_verdict": "PASS", "nav_cells": {
            "NAV1": {"status": "PASS", "diagnostic": "ties"}
        }}}},
    )
    summary = module.canonical_evidence_summary(tmp_path, data)
    assert summary["accountant"]["status"] == "FAIL"
    assert summary["accountant"]["counts"] == {"PASS": 1, "FAIL": 1, "SKIP": 1, "XFAIL": 1}
    assert summary["reproducibility"]["status"] == "LOCAL PASS"
    assert summary["identity"]["run_id"] == "sealed-run-1"
    html = module.canonical_evidence_html(summary)
    for text in ("applicable PASS", "applicable FAIL", "inapplicable / skipped", "expected gap (not PASS)"):
        assert text in html
    assert "N/A: no yield" in html and "per-intent oracle" in html


def test_xfail_is_incomplete_while_justified_skip_is_not_promoted(tmp_path):
    module = _load()
    _write(
        tmp_path / "accountant.json",
        {"cell_details": [
            {"id": "G1", "description": "money", "status": "PASS", "diagnostic": "ties"},
            {"id": "G14", "description": "on-chain", "status": "XFAIL", "diagnostic": "expected"},
            {"id": "G11", "description": "failed tx", "status": "SKIP", "diagnostic": "N/A"},
        ]},
    )
    summary = module.canonical_evidence_summary(tmp_path, {"meta": {}})
    assert summary["accountant"]["status"] == "INCOMPLETE"
    assert summary["accountant"]["counts"]["PASS"] == 1
    assert summary["accountant"]["counts"]["XFAIL"] == 1


def test_absent_canonical_outputs_render_absent_or_unmeasured_never_pass(tmp_path):
    module = _load()
    summary = module.canonical_evidence_summary(tmp_path, {"meta": {}})
    assert summary["accountant"]["status"] == "ABSENT"
    assert summary["receipt"]["native_status"] == "ABSENT"
    assert summary["reproducibility"]["status"] == "ABSENT"
    assert summary["nav_status"] == "UNMEASURED"
    assert summary["identity"] == {
        "commit": "ABSENT",
        "sdk_version": "ABSENT",
        "sealed_at": "ABSENT",
        "run_id": "ABSENT",
    }
    html = module.canonical_evidence_html(summary)
    assert "ABSENT" in html and "UNMEASURED" in html


def test_retained_manifest_is_required_for_full_pass_and_mutation_fails(tmp_path, monkeypatch):
    module = _load()
    bundle = tmp_path / "bundle"
    retained = tmp_path / "sealed" / "evidence-manifest.json"
    bundle.mkdir()
    retained.parent.mkdir()
    manifest = {
        "created_at": "2026-08-09T12:00:00Z",
        "git": {"commit": "a" * 40, "sdk_version": "1.2.3"},
        "run_id": "run-1",
    }
    _write(bundle / "evidence-manifest.json", manifest)
    _write(retained, manifest)
    fake = ModuleType("qa_run_evidence")
    fake.verify_manifest = lambda *_: {}
    monkeypatch.setitem(sys.modules, "qa_run_evidence", fake)
    summary = module.canonical_evidence_summary(bundle, {"meta": {}}, retained_manifest_path=retained)
    assert summary["reproducibility"]["status"] == "PASS"

    manifest["run_id"] = "rewritten-run"
    _write(bundle / "evidence-manifest.json", manifest)
    mutated = module.canonical_evidence_summary(bundle, {"meta": {}}, retained_manifest_path=retained)
    assert mutated["reproducibility"]["status"] == "FAIL"
    assert "differs" in mutated["reproducibility"]["note"]

    self_anchored = module.canonical_evidence_summary(
        bundle,
        {"meta": {}},
        retained_manifest_path=bundle / "evidence-manifest.json",
    )
    assert self_anchored["reproducibility"]["status"] == "FAIL"
    assert "outside the rewritable evidence bundle" in self_anchored["reproducibility"]["note"]


def test_batch_index_exposes_canonical_line_of_defense_and_only_full_verification_is_green(tmp_path):
    module = _load()

    def evidence(repro):
        return {
            "accountant": {
                "status": "PASS",
                "counts": {"PASS": 20, "FAIL": 0, "SKIP": 1, "XFAIL": 0},
                "note": "all applicable cells pass",
            },
            "dashboard": {"applicability": "APPLICABLE", "capture_mode": "live", "config_sha256": "c" * 64},
            "identity": {"commit": "a" * 40, "sdk_version": "1.2.3", "run_id": "run-1", "sealed_at": "t"},
            "nav_status": "PASS",
            "nav_cells": [],
            "receipt": {"native_status": "PASS", "usd_status": "PASS", "hash_count": 3, "note": "oracle"},
            "reproducibility": {"status": repro, "note": "retained" if repro == "PASS" else "local only"},
        }

    datas = {
        slug: {"meta": {"chain": "arbitrum"}, "n_actions": 2, "acc_events": 4, "pnl": "0"}
        for slug in ("full", "local")
    }
    finds = {
        slug: {"verdict": "PASS", "headline": "ok"}
        for slug in ("full", "local")
    }
    html = module.render_index(
        "B",
        tmp_path,
        ["full", "local"],
        datas,
        finds,
        evidence_summaries={"full": evidence("PASS"), "local": evidence("LOCAL PASS")},
    )
    assert "Accountant P/F/S/X" in html and "PASS · P20/F0/S1/X0" in html
    assert "receipt native/USD" in html and "SDK / sealed run" in html
    assert "Canonical evidence 1/2 fully verified" in html
    assert "LOCAL PASS" in html
