"""A scope-aware store upgrade must preserve every historical evidence byte."""

import json

import pytest

from qa_lab import qa_coverage, qa_history
from tests.unit.scripts.test_asset_scope_evidence import SDK


def test_v2_to_v3_upgrade_preserves_history_and_legacy_index(tmp_path):
    run_dir = tmp_path / "intents/historical"
    run_dir.mkdir(parents=True)
    (run_dir / "manifest.json").write_text(json.dumps({"run_id": "historical", "sdk": SDK}))
    (run_dir / "summary.json").write_text(
        json.dumps(
            {
                "cells": [
                    {
                        "intent_cell_id": "intent.aerodrome.base.SWAP.anvil.eoa",
                        "status": "FAIL",
                    }
                ]
            }
        )
    )
    qa_history.append_experiment(
        store=tmp_path,
        surface="intent",
        run_id="historical",
        run_dir=run_dir,
        manifest_path=run_dir / "manifest.json",
        sdk=SDK,
        cell_verdicts={"intent.aerodrome.base.SWAP.anvil.eoa": "FAIL"},
        started_at="2026-09-15T01:00:00Z",
        completed_at="2026-09-15T01:01:00Z",
        sealed_at="2026-09-15T01:02:00Z",
        catalog_sha256="b" * 64,
    )
    manifest = tmp_path / qa_coverage.STORE_MANIFEST_NAME
    manifest.write_text(json.dumps({"schema_version": 2, "kind": qa_coverage.STORE_KIND}))
    legacy = tmp_path / "index/intent_latest.json"
    legacy.write_text(json.dumps({"intent.aerodrome.base.SWAP.anvil.eoa": {"status": "PASS"}}))
    preserved = {path: path.read_bytes() for path in tmp_path.rglob("*") if path.is_file() and path != manifest}
    plan = qa_coverage.migrate_store_layout(tmp_path)
    assert plan["status"] == "PLANNED"
    assert qa_coverage.read_store_schema_version(tmp_path) == 2
    with pytest.raises(qa_coverage.StoreSchemaError):
        qa_coverage._assert_store_schema_current(tmp_path)
    result = qa_coverage.migrate_store_layout(tmp_path, apply=True)
    assert result["status"] == "MIGRATED"
    assert result["moved"] == []
    assert result["pre_migration_terminal_sha256"] == result["post_migration_terminal_sha256"]
    assert qa_coverage.read_store_schema_version(tmp_path) == 3
    assert all(path.read_bytes() == original for path, original in preserved.items())
    assert len(qa_history.read_history(tmp_path)) == 1
    assert qa_coverage.migrate_store_layout(tmp_path, apply=True)["status"] == "ALREADY_CURRENT"


def test_newer_store_refuses_downgrade(tmp_path):
    (tmp_path / qa_coverage.STORE_MANIFEST_NAME).write_text(
        json.dumps({"schema_version": 4, "kind": qa_coverage.STORE_KIND})
    )
    with pytest.raises(qa_coverage.StoreSchemaError, match="downgrade"):
        qa_coverage.migrate_store_layout(tmp_path, apply=True)
