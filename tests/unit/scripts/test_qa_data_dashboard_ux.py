"""Focused contracts for the operator-facing Data Tests dashboard."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
QA_COVERAGE = REPO_ROOT / "scripts" / "quant-test" / "qa_coverage.py"


@pytest.fixture(scope="module")
def qa():
    spec = importlib.util.spec_from_file_location("qa_coverage_data_dashboard_ux", QA_COVERAGE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_data_catalog_separates_pipeline_readiness_from_schedule(qa) -> None:
    catalog = qa.build_data_catalog()

    assert {cell["pipeline_status"] for cell in catalog["cells"]} == {"ready"}
    # No scheduler is installed anywhere (VIB-6820): a schedule claim may only
    # return alongside a verifiably installed scheduler, never as aspiration.
    assert not any(cell["scheduled"] for cell in catalog["cells"])
    assert {cell["schedule"] for cell in catalog["cells"]} == {"operator / unscheduled"}
    assert all(cell["runner"] == "make test-nightly-visual" for cell in catalog["cells"])
    assert all("data-seal --bundle <nightly-output-directory>" in cell["sealer"] for cell in catalog["cells"])
    assert all(cell["sealer_is_template"] is True for cell in catalog["cells"])

    provider_suite = next(suite for suite in catalog["suites"] if suite["id"] == "qa_data")
    assert provider_suite["runner"] == "uv run python -m almanak.framework.data.qa.cli"
    assert provider_suite["evidence_status"] == "declared_unsealed"
    assert provider_suite["pipeline_status"] == "pipe_gap"


def test_data_catalog_projects_exact_production_identities_with_stable_digest(qa, monkeypatch) -> None:
    monkeypatch.setattr(qa, "_utc_now", lambda: qa.datetime(2026, 8, 15, 12, 0, tzinfo=qa.UTC))
    first = qa.build_data_catalog()
    monkeypatch.setattr(qa, "_utc_now", lambda: qa.datetime(2026, 8, 16, 12, 0, tzinfo=qa.UTC))
    second = qa.build_data_catalog()

    assert first["generated_at"] != second["generated_at"]
    assert first["catalog_sha256"] == second["catalog_sha256"]
    assert first["identity_requirements_sha256"] == second["identity_requirements_sha256"]
    assert {row["kind"] for row in first["identity_requirements"]} == {
        "direct_chainlink_feed",
        "token",
        "v3_pool",
    }


def test_data_cell_state_prioritizes_sealed_evidence_over_structure(qa) -> None:
    ready = {"pipeline_status": "ready"}
    gap = {"pipeline_status": "pipe_gap"}

    assert qa._data_cell_state(ready, None) == {"key": "ready", "label": "READY", "sealed": False}
    assert qa._data_cell_state(gap, None) == {"key": "pipe_gap", "label": "PIPE GAP", "sealed": False}
    assert qa._data_cell_state(gap, {"status": "PASS"}) == {
        "key": "pass",
        "label": "PASS",
        "sealed": True,
        "raw_status": "PASS",
    }
    # UNMEASURED stays fail-closed (red) but is labeled as what it is.
    assert qa._data_cell_state(ready, {"status": "UNMEASURED"}) == {
        "key": "fail",
        "label": "UNMEASURED",
        "sealed": True,
        "raw_status": "UNMEASURED",
    }


def test_data_dashboard_renders_operator_states_and_honest_provider_inventory(qa, tmp_path: Path) -> None:
    store = tmp_path / "store"
    (store / "index").mkdir(parents=True)
    (store / "index" / "data_latest.json").write_text(json.dumps({}))
    catalog = qa.build_data_catalog()

    page = qa.render_data_lab(
        store=store,
        qa_catalog={"chain_logos": {}},
        data_catalog=catalog,
    ).read_text()

    assert "Ready · never run" in page
    assert "Pipeline gaps" in page
    assert "Needs action" in page
    assert "Schedule badges are operational metadata, not evidence" in page
    assert "provider categories declared · unsealed" in page
    assert "PIPE GAP · DECLARED, UNSEALED" in page
    assert "Contract check" in page
    assert "Provider health" in page
    assert "Seal command template" in page
    assert "uv run python -m almanak.framework.data.qa.cli" in page
    assert '"key":"ready","label":"READY","sealed":false' in page
    assert "Exact production identity inventory" in page
    assert "Declaration is not proof" in page
    assert "Search exact production identities" in page
    assert "pipe exists · never run" in page
    assert "block-pinned matching observation paints PASS" in page
    assert "/^0x[0-9a-f]{40}$/.test(q)" in page
    assert "addresses.includes(q)" in page
    assert "health==='PASS'?'pass':health==='DEGRADED'?'degraded':'fail'" in page
