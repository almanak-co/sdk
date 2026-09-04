"""Fail-closed contracts for Backtesting QA catalogs, seals, and Lab paint."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = REPO_ROOT / "qa_lab" / "qa_backtest.py"
TEST_COMMIT = "a" * 40
TEST_SDK = {
    "commit": TEST_COMMIT,
    "branch": "test",
    "dirty": False,
    "sdk_version": "0.0-test",
    "source": "executing-worktree",
}


@pytest.fixture(scope="module")
def module():
    spec = importlib.util.spec_from_file_location("qa_backtest_test", SCRIPT)
    assert spec is not None and spec.loader is not None
    loaded = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = loaded
    spec.loader.exec_module(loaded)
    return loaded


def _trust_bundle(module, root: Path, *, status: str = "PASS") -> tuple[Path, dict]:
    catalog = module.build_backtest_catalog()
    bundle = root / "bundle"
    bundle.mkdir()
    cells = {
        cell["source_cell_id"]: {
            "invariant": cell["invariant"],
            "column": cell["primitive"],
            "status": status,
            "xfail_ticket": cell["xfail_ticket"],
            "description": cell["description"],
        }
        for cell in catalog["cells"]
    }
    (bundle / "trust-matrix.json").write_text(
        json.dumps({"matrix": "backtest-trust-matrix", "ticket": "VIB-5081", "cells": cells})
    )
    (bundle / "pytest.log").write_text("41 passed, 178 warnings in 3.55s\n")
    return bundle, catalog


def test_catalog_and_empty_board_keep_tiers_independent(module, tmp_path: Path) -> None:
    store = tmp_path / "store"
    catalog = module.build_backtest_catalog()

    page = module.render_backtest_lab(store=store, lab_css="")

    assert catalog["summary"] == {"registered_cells": len(catalog["cells"]), "tiers": 3}
    assert catalog["summary"]["registered_cells"] >= 40
    assert {tier["tier"] for tier in catalog["tiers"]} == {
        "trust-matrix",
        "keyed-validation",
        "paper-anvil",
    }
    assert json.loads((store / "index" / module.BACKTEST_INDEX_NAME).read_text()) == {}
    rendered = page.read_text()
    assert "Backtesting truth by evidence tier" in rendered
    assert "historical accuracy, fork execution, or profitable live behavior" in rendered
    assert "Keyed tests that skip for missing credentials remain visibly SKIP" in rendered
    assert 'class="btn active" href="backtesting.html">Backtesting</a>' in rendered


def test_trust_matrix_seal_is_immutable_and_paints_only_registered_cells(module, tmp_path: Path) -> None:
    store = tmp_path / "store"
    module.bootstrap_backtest(store)
    bundle, catalog = _trust_bundle(module, tmp_path)

    target = module.seal_backtest_bundle(
        bundle=bundle,
        store=store,
        catalog=catalog,
        tier="trust-matrix",
        sdk_provenance=TEST_SDK,
        run_id="trust-proof",
    )

    manifest = json.loads((target / "manifest.json").read_text())
    latest = json.loads((store / "index" / module.BACKTEST_INDEX_NAME).read_text())
    assert manifest["verdict"] == "PASS"
    assert manifest["scores"]["passed"] == len(catalog["cells"])
    assert manifest["warnings"] == 178
    assert len([key for key in latest if key.startswith("backtest.trust-matrix.")]) == len(catalog["cells"]) + 1
    assert latest["backtest.trust-matrix.suite"]["verdict"] == "PASS"
    cell_rows = [row for key, row in latest.items() if key != "backtest.trust-matrix.suite"]
    assert {row["status"] for row in cell_rows} == {"PASS"}
    assert all(row["attribution_mode"] == "exact-runtime" for row in cell_rows)
    assert (target / "report.html").is_file()
    with pytest.raises(FileExistsError, match="already sealed"):
        module.seal_backtest_bundle(
            bundle=bundle,
            store=store,
            catalog=catalog,
            tier="trust-matrix",
            sdk_provenance=TEST_SDK,
            run_id="trust-proof",
        )


def test_keyed_all_skip_is_visible_skip_and_never_paints_trust_cells(module, tmp_path: Path) -> None:
    store = tmp_path / "store"
    module.bootstrap_backtest(store)
    catalog = module.build_backtest_catalog()
    bundle = tmp_path / "keyed"
    bundle.mkdir()
    (bundle / "keyed.xml").write_text(
        '<testsuite tests="2" failures="0" errors="0" skipped="2">'
        '<testcase name="one"><skipped message="missing key"/></testcase>'
        '<testcase name="two"><skipped message="missing key"/></testcase>'
        "</testsuite>"
    )

    module.seal_backtest_bundle(
        bundle=bundle,
        store=store,
        catalog=catalog,
        tier="keyed-validation",
        sdk_provenance=TEST_SDK,
        run_id="keyed-skip",
    )

    latest = json.loads((store / "index" / module.BACKTEST_INDEX_NAME).read_text())
    assert latest["backtest.keyed-validation.suite"]["verdict"] == "SKIP"
    assert latest["backtest.keyed-validation.suite"]["scores"]["passed"] == 0
    assert set(latest) == {"backtest.keyed-validation.suite"}
