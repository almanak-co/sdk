"""Historical green must never masquerade as current eligible coverage."""

import json
from datetime import UTC, datetime, timedelta

import pytest

from qa_lab.qa_canary import CELL_IDS, MESSAGE, attest_failure
from qa_lab.qa_eligibility import intent_eligibility, intent_product_green
from qa_lab.qa_history import append_experiment

CATALOG = {"catalog_sha256": "c" * 64, "cells": [{"id": "intent.uni.base.SWAP"}]}
CELL = "intent.uni.base.SWAP.anvil.safe"
GREEN = {
    "catalog_sha256": "c" * 64,
    "status": "PASS",
    "attribution_mode": "exact-runtime",
    "evidence_status": "COMPLETE",
    "contract_status": "VERIFIED",
    "provenance_status": "VERIFIED",
    "network": "anvil",
}


@pytest.mark.parametrize(
    "field,value",
    [
        ("catalog_sha256", "old"),
        ("attribution_mode", "legacy"),
        ("status", "FAIL"),
        ("evidence_status", "SOFT"),
        ("contract_status", "UNMEASURED"),
        ("provenance_status", "UNMEASURED"),
        ("network", "mainnet"),
    ],
)
def test_green_requires_every_admission_axis(field, value):
    assert intent_product_green(GREEN, CATALOG["catalog_sha256"])
    assert not intent_product_green({**GREEN, field: value}, CATALOG["catalog_sha256"])


def test_history_catalog_eligibility_and_product_admission_stay_distinct(tmp_path):
    rows = {
        CELL: GREEN,
        "intent.uni.base.SWAP.anvil.eoa": {**GREEN, "catalog_sha256": "old"},
        "intent.uni.base.SWAP.mainnet.safe": {**GREEN, "network": "mainnet"},
        "intent.removed.base.SWAP.anvil.safe": GREEN,
        CELL_IDS[0]: {"status": "FAIL"},
    }
    rows = {key: _seal_product(tmp_path, key, row) if key not in CELL_IDS else row for key, row in rows.items()}
    counts = intent_eligibility(tmp_path, CATALOG, rows)
    assert counts["historical_sealed"] == 4
    assert counts["historical_pass"] == 4
    assert counts["current_catalog_eligible"] == 2
    assert counts["map_drift"] == 2
    assert counts["current_product_green"] == 1
    assert counts["trusted_product_green"] == 0
    assert not counts["recorder_verified"]


def _seal_control(store, cell_id, stamp):
    path = cell_id.rsplit(".", 1)[1]
    run_dir = store / "intents" / path
    run_dir.mkdir(parents=True)
    junit = run_dir / "results.xml"
    junit.write_text(
        f'<testsuite><testcase classname="tests.qa_lab.test_harness_canary" name="test_harness_canary_must_fail_{path}">'
        f'<failure message="AssertionError: {MESSAGE}">AssertionError</failure></testcase></testsuite>'
    )
    summary = {
        "cells": [
            {
                "intent_cell_id": cell_id,
                "status": "FAIL",
                "canary_attestation": attest_failure(junit, exec_path=path, returncode=1),
            }
        ]
    }
    (run_dir / "summary.json").write_text(json.dumps(summary))
    sdk = {"commit": "a" * 40, "branch": "test", "dirty": False, "sdk_version": "test", "source": "executing-worktree"}
    manifest = run_dir / "manifest.json"
    manifest.write_text(json.dumps({"run_id": path, "sdk": sdk}))
    append_experiment(
        store=store,
        surface="intent",
        run_id=path,
        run_dir=run_dir,
        manifest_path=manifest,
        sdk=sdk,
        cell_verdicts={cell_id: "FAIL"},
        catalog_sha256=CATALOG["catalog_sha256"],
        started_at=stamp,
        completed_at=stamp,
        sealed_at=stamp,
    )


def test_real_ledger_controls_gate_trust_but_expiry_preserves_measured_green(tmp_path):
    now = datetime(2026, 9, 5, 13, tzinfo=UTC)
    for cell_id in CELL_IDS:
        _seal_control(tmp_path, cell_id, now.isoformat())
    row = _seal_product(tmp_path, CELL, GREEN)
    current = intent_eligibility(tmp_path, CATALOG, {CELL: row}, now)
    assert current["recorder_verified"] and current["trusted_product_green"] == 1
    expired = intent_eligibility(tmp_path, CATALOG, {CELL: row}, now + timedelta(days=1))
    assert not expired["recorder_verified"] and expired["trusted_product_green"] == 0
    assert expired["current_product_green"] == 1
    (tmp_path / "intents" / "safe" / "results.xml").write_text("tampered")
    assert not intent_eligibility(tmp_path, CATALOG, {CELL: row}, now)["recorder_verified"]


def _seal_product(store, cell_id, row, suffix=""):

    row = {**row, "intent_cell_id": cell_id}
    run_id = cell_id.replace(".", "-") + suffix
    run_dir = store / "intents" / run_id
    run_dir.mkdir(parents=True)
    sdk = {"commit": "a" * 40, "branch": "test", "dirty": False, "sdk_version": "test", "source": "executing-worktree"}
    manifest = run_dir / "manifest.json"
    manifest.write_text(json.dumps({"run_id": run_id, "sdk": sdk}))
    (run_dir / "report.html").write_text("<html>Sealed test report</html>")
    for receipt in row.get("receipts", []):
        for field in ("path", "decode_path"):
            target = run_dir / receipt[field]
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text("sealed receipt fixture")
    fingerprint = row["catalog_sha256"]
    if fingerprint == "old":
        fingerprint = "b" * 64
        row["catalog_sha256"] = fingerprint
    (run_dir / "summary.json").write_text(json.dumps({"run_id": run_id, "catalog_sha256": fingerprint, "cells": [row]}))
    stamp = "2026-09-05T12:00:00Z"
    record = append_experiment(
        store=store,
        surface="intent",
        run_id=run_id,
        run_dir=run_dir,
        manifest_path=manifest,
        sdk=sdk,
        cell_verdicts={cell_id: row["status"]},
        catalog_sha256=fingerprint,
        started_at=stamp,
        completed_at=stamp,
        sealed_at=stamp,
    )
    prefix = record["store_path"]
    return {
        **row,
        **{field: record[field] for field in ("run_id", "store_path", "catalog_sha256", "sealed_at")},
        "report_path": f"{prefix}/report.html",
        "receipt_paths": [f"{prefix}/{path}" for path in row.get("receipt_paths", [])],
        "receipt_path": f"{prefix}/{row['receipt_paths'][0]}" if row.get("receipt_paths") else None,
        "receipts": [
            {**receipt, "path": f"{prefix}/{receipt['path']}", "decode_path": f"{prefix}/{receipt['decode_path']}"}
            for receipt in row.get("receipts", [])
        ],
        "git": record["sdk"],
        "artifacts": [
            {**artifact, "kind": "report.html" if artifact["relpath"] == f"{prefix}/report.html" else "artifact"}
            for artifact in record["artifacts"]
        ],
    }


def test_fabricated_or_modified_index_does_not_count_as_sealed_or_green(tmp_path):
    assert intent_eligibility(tmp_path, CATALOG, {CELL: GREEN})["unverified_index"] == 1
    row = _seal_product(tmp_path, CELL, GREEN)
    assert intent_eligibility(tmp_path, CATALOG, {CELL: row})["current_product_green"] == 1
    for field, value in (("run_id", "invented"), ("status", "FAIL"), ("provenance_status", "UNMEASURED")):
        counts = intent_eligibility(tmp_path, CATALOG, {CELL: {**row, field: value}})
        assert counts["unverified_index"] == 1 and counts["historical_sealed"] == 0
    (tmp_path / row["store_path"] / "summary.json").write_text("tampered")
    assert intent_eligibility(tmp_path, CATALOG, {CELL: row})["current_product_green"] == 0


def test_stale_index_cannot_retain_green_after_newer_failure(tmp_path):
    old = _seal_product(tmp_path, CELL, GREEN)
    new = _seal_product(tmp_path, CELL, {**GREEN, "status": "FAIL"}, suffix="-new")
    stale = intent_eligibility(tmp_path, CATALOG, {CELL: old})
    assert stale["unverified_index"] == 1 and stale["current_product_green"] == 0
    current = intent_eligibility(tmp_path, CATALOG, {CELL: new})
    assert current["current_fail"] == 1 and current["unverified_index"] == 0


@pytest.mark.parametrize(
    "field,value",
    [
        ("intent_cell_id", "intent.other.base.SWAP.anvil.safe"),
        ("protocol", "other"),
        ("chain", "arbitrum"),
        ("intent", "LP_CLOSE"),
        ("exec_path", "eoa"),
        ("report_path", "other/run/report.html"),
        ("receipt_path", "other/run/receipt.json"),
        ("receipt_paths", ["other/run/receipt.json"]),
        ("git", {"commit": "f" * 40}),
    ],
)
def test_changed_identity_or_evidence_links_cannot_keep_green(tmp_path, field, value):
    row = _seal_product(tmp_path, CELL, GREEN)
    assert intent_eligibility(tmp_path, CATALOG, {CELL: row})["current_product_green"] == 1
    changed = {**row, field: value}
    assert intent_eligibility(tmp_path, CATALOG, {CELL: changed})["unverified_index"] == 1


def test_receipt_links_metadata_and_artifacts_are_bound_but_human_summary_can_be_enriched(tmp_path):
    import copy

    receipt = {"path": "receipts/one.json", "decode_path": "receipts/one.html", "tx_hash": "0x123", "grade": "hard"}
    row = _seal_product(tmp_path, CELL, {**GREEN, "receipt_paths": [receipt["path"]], "receipts": [receipt]})
    row["receipts"][0]["human_summary"] = {"headline": "Readable enrichment"}
    assert intent_eligibility(tmp_path, CATALOG, {CELL: row})["current_product_green"] == 1
    for field in ("path", "decode_path", "tx_hash", "grade"):
        changed = copy.deepcopy(row)
        changed["receipts"][0][field] = "changed"
        assert intent_eligibility(tmp_path, CATALOG, {CELL: changed})["unverified_index"] == 1
    changed = copy.deepcopy(row)
    changed["artifacts"][0]["relpath"] = "other/run/report.html"
    assert intent_eligibility(tmp_path, CATALOG, {CELL: changed})["unverified_index"] == 1


def test_renderer_never_enriches_receipt_path_from_an_unauthenticated_index(tmp_path):
    from qa_lab.qa_coverage import render_intent_lab

    receipt = {"path": "receipts/one.json", "decode_path": "receipts/one.html"}
    row = _seal_product(tmp_path, CELL, {**GREEN, "receipt_paths": [receipt["path"]], "receipts": [receipt]})
    row["receipts"][0]["path"] = row["report_path"]
    index = tmp_path / "index" / "intent_latest.json"
    index.write_text(json.dumps({CELL: row}))
    catalog = {
        **CATALOG,
        "chains": ["base"],
        "cells": [{"id": "intent.uni.base.SWAP", "protocol": "uni", "chain": "base", "intent": "SWAP"}],
    }
    rendered = render_intent_lab(store=tmp_path, qa_catalog={}, intent_catalog=catalog).read_text()
    assert '"unverified_index":1' in rendered
    assert '"current_product_green":0' in rendered
    assert "Receipt and report links are withheld" in rendered


@pytest.mark.parametrize("cells", [None, "invalid", {}, [None], [{}], [{"id": 1}]])
def test_malformed_catalog_is_refused_not_reported_as_zero_coverage(tmp_path, cells):
    with pytest.raises(ValueError, match="catalog cells"):
        intent_eligibility(tmp_path, {"cells": cells}, {})
