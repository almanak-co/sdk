"""Tests for nightly-test-builds/merge_nightly_reports.py — the per-shard JSON
merge that carries the VIB-5373 machine-readable signals downstream.

The merge step rebuilds the markdown report from a fixed-column table (dropping
every other section), so the scope allowlist (a), per-result fingerprints +
root cause (b/c), and skipped-unsupported combos (d) MUST travel through the
merged ``nightly_results.json`` instead. These tests pin that contract.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

_NIGHTLY_DIR = Path(__file__).resolve().parents[2] / "nightly-test-builds"


def _load(name: str):
    if str(_NIGHTLY_DIR) not in sys.path:
        sys.path.insert(0, str(_NIGHTLY_DIR))
    spec = importlib.util.spec_from_file_location(f"{name}_mnr", _NIGHTLY_DIR / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def merge():
    return _load("merge_nightly_reports")


def _write_shard(base: Path, chain: str, artifact: dict) -> Path:
    shard = base / f"shard_{chain}"
    shard.mkdir(parents=True)
    (shard / "results.json").write_text(json.dumps(artifact))
    return shard


def test_merge_unions_in_scope_fingerprints_across_shards(merge, tmp_path):
    """(a) The merged in_scope_fingerprints is the UNION across shards — the
    allowlist the downstream filer diffs past tickets against to auto-close moot
    ones. A fingerprint present in ANY shard is in scope."""
    _write_shard(tmp_path, "arbitrum", {
        "schema_version": 1, "chain": "arbitrum", "mode": "anvil", "commit": "abc",
        "in_scope_fingerprints": ["a_arb:arbitrum", "b_arb:arbitrum"],
        "results": [
            {"name": "a_arb", "chain": "arbitrum", "status": "PASS", "outcome": "EXECUTED",
             "tx_count": 1, "failure_type": "", "root_cause": "", "fingerprint": "a_arb:arbitrum"},
        ],
        "skipped_unsupported": [],
    })
    _write_shard(tmp_path, "base", {
        "schema_version": 1, "chain": "base", "mode": "anvil", "commit": "abc",
        "in_scope_fingerprints": ["c_base:base"],
        "results": [
            {"name": "c_base", "chain": "base", "status": "FAIL", "outcome": "ERROR",
             "tx_count": 0, "failure_type": "anvil_fork_start_failure",
             "root_cause": "Managed Anvil fork failed to start", "fingerprint": "c_base:base"},
        ],
        "skipped_unsupported": [
            {"name": "pancake_opt", "chain": "optimism", "reason": "unsupported (protocol, chain)"},
        ],
    })

    out = tmp_path / "nightly_results.json"
    merged = merge.merge_results_artifacts(
        [tmp_path / "shard_arbitrum", tmp_path / "shard_base"], out,
    )

    assert set(merged["in_scope_fingerprints"]) == {"a_arb:arbitrum", "b_arb:arbitrum", "c_base:base"}
    assert {r["name"] for r in merged["results"]} == {"a_arb", "c_base"}
    # (c) root cause survives the merge.
    failing = next(r for r in merged["results"] if r["name"] == "c_base")
    assert failing["root_cause"] == "Managed Anvil fork failed to start"
    # (d) skipped-unsupported is aggregated and never promoted to a result.
    assert merged["skipped_unsupported"] == [
        {"name": "pancake_opt", "chain": "optimism", "reason": "unsupported (protocol, chain)"},
    ]
    # File on disk matches the returned dict.
    assert json.loads(out.read_text())["in_scope_fingerprints"] == merged["in_scope_fingerprints"]
    # All shards present and parseable -> scope is complete (safe to auto-close).
    assert merged["scope_complete"] is True
    assert merged["missing_shards"] == []


def test_merge_tolerates_missing_and_corrupt_shard_json(merge, tmp_path):
    """A shard that failed before writing results.json (or wrote garbage) must
    not crash the merge — the other shards' signals still aggregate — but the
    merged artifact MUST flag scope as incomplete so the downstream filer
    suppresses auto-close (an incomplete fingerprint union would otherwise
    resolve tickets for strategies that merely lived in the dropped shard)."""
    good = _write_shard(tmp_path, "arbitrum", {
        "schema_version": 1, "chain": "arbitrum", "mode": "anvil", "commit": "abc",
        "in_scope_fingerprints": ["a:arbitrum"],
        "results": [{"name": "a", "chain": "arbitrum", "status": "PASS", "outcome": "EXECUTED",
                     "tx_count": 0, "failure_type": "", "root_cause": "", "fingerprint": "a:arbitrum"}],
        "skipped_unsupported": [],
    })
    # Corrupt shard
    corrupt = tmp_path / "shard_base"
    corrupt.mkdir()
    (corrupt / "results.json").write_text("{not json")
    # Missing-artifact shard
    missing = tmp_path / "shard_polygon"
    missing.mkdir()

    out = tmp_path / "nightly_results.json"
    merged = merge.merge_results_artifacts([good, corrupt, missing], out)
    assert merged["in_scope_fingerprints"] == ["a:arbitrum"]
    assert len(merged["results"]) == 1
    # Critical safety signal: scope is incomplete and the offending shards named.
    assert merged["scope_complete"] is False
    assert set(merged["missing_shards"]) == {str(corrupt), str(missing)}
    assert json.loads(out.read_text())["scope_complete"] is False
