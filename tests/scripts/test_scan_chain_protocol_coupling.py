"""Tests for the ``scripts/ci/scan_chain_protocol_coupling.py`` baseline identity.

VIB-4851: the coupling ratchet originally keyed its committed baseline on
``(category, path, line, column, name)``. Every line-shifting refactor then
reported hundreds of spurious "net-new" findings (416 net-new for a tree with
only 31 genuinely-new coupling sites), so the gate was disabled for the duration
of the cleanup wave (commit ``aa3d345f3``). It was re-enabled with a
**line-insensitive** identity — ``(category, path, name)``. These tests pin that
contract so the line-shift false-positive cannot regress.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest


def _load_module():
    repo_root = Path(__file__).resolve().parents[2]
    script_path = repo_root / "scripts" / "ci" / "scan_chain_protocol_coupling.py"
    spec = importlib.util.spec_from_file_location("scan_chain_protocol_coupling", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # Register so dataclasses.fields() can resolve forward refs on Finding.
    sys.modules["scan_chain_protocol_coupling"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def scanner():
    return _load_module()


def _finding(
    scanner,
    *,
    category="PROTOCOL_STRING",
    path="almanak/framework/foo.py",
    line=10,
    column=4,
    name="aave_v3",
):
    return scanner.Finding(
        category=category,
        path=path,
        line=line,
        column=column,
        snippet=f'x = "{name}"',
        name=name,
    )


def test_baseline_key_is_line_and_column_insensitive(scanner):
    a = _finding(scanner, line=10, column=4)
    b = _finding(scanner, line=999, column=77)  # same coupling site, just moved
    assert scanner._baseline_key(a) == scanner._baseline_key(b)
    assert scanner._baseline_key(a) == ("PROTOCOL_STRING", "almanak/framework/foo.py", "aave_v3")


def test_baseline_key_distinguishes_category_path_name(scanner):
    base = scanner._baseline_key(_finding(scanner))
    assert scanner._baseline_key(_finding(scanner, name="uniswap_v3")) != base
    assert scanner._baseline_key(_finding(scanner, path="almanak/framework/bar.py")) != base
    assert scanner._baseline_key(_finding(scanner, category="CONNECTOR_IMPORT")) != base


def test_findings_to_records_dedupes_and_drops_line_column(scanner):
    findings = [
        _finding(scanner, line=10),
        _finding(scanner, line=20),  # same site, different line -> collapses
        _finding(scanner, name="uniswap_v3", line=30),
    ]
    records = scanner.findings_to_records(findings)
    assert records == [
        {"category": "PROTOCOL_STRING", "path": "almanak/framework/foo.py", "name": "aave_v3"},
        {"category": "PROTOCOL_STRING", "path": "almanak/framework/foo.py", "name": "uniswap_v3"},
    ]
    assert all("line" not in r and "column" not in r for r in records)


def test_findings_to_records_order_is_line_independent(scanner):
    """Record order must be the sorted (category, path, name) key, not first-seen line.

    Regression guard for the Gemini review on #2588: sorting raw findings
    (line-sensitive) *before* dedup leaked first-occurrence line order into
    the committed baseline, producing noisy reorder diffs on refresh.
    """
    # uniswap_v3 occurs at an EARLIER line than aave_v3; output must still be
    # alphabetical by name, proving the order does not depend on line numbers.
    early_first = scanner.findings_to_records(
        [
            _finding(scanner, name="uniswap_v3", line=5),
            _finding(scanner, name="aave_v3", line=50),
            _finding(scanner, name="aave_v3", line=99),  # dup site -> collapses
        ]
    )
    # Same sites, opposite line order -> identical record list.
    late_first = scanner.findings_to_records(
        [
            _finding(scanner, name="aave_v3", line=5),
            _finding(scanner, name="uniswap_v3", line=99),
        ]
    )
    assert early_first == late_first
    assert [r["name"] for r in early_first] == ["aave_v3", "uniswap_v3"]


def test_line_shift_produces_no_net_new(scanner, tmp_path):
    """The core regression: moving coupled code to new lines must not trip the gate."""
    baseline_findings = [_finding(scanner, line=10), _finding(scanner, name="uniswap_v3", line=20)]
    dest = tmp_path / "baseline.json"
    scanner.write_baseline_json(baseline_findings, dest)
    baseline_keys = scanner.load_baseline_json(dest)

    shifted = [_finding(scanner, line=110), _finding(scanner, name="uniswap_v3", line=220)]
    current_keys = {scanner._baseline_key(f) for f in shifted}
    assert current_keys - baseline_keys == set()


def test_new_site_is_net_new(scanner, tmp_path):
    baseline_findings = [_finding(scanner, line=10)]
    dest = tmp_path / "baseline.json"
    scanner.write_baseline_json(baseline_findings, dest)
    baseline_keys = scanner.load_baseline_json(dest)

    current = [_finding(scanner, line=10), _finding(scanner, name="gmx_v2", line=11)]
    net_new = {scanner._baseline_key(f) for f in current} - baseline_keys
    assert net_new == {("PROTOCOL_STRING", "almanak/framework/foo.py", "gmx_v2")}


def test_load_baseline_reads_legacy_v1_with_line_column(scanner, tmp_path):
    """Pre-v2 baselines carried line/column; the loader must ignore them, not crash."""
    legacy = {
        "version": 1,
        "total": 1,
        "findings": [
            {"category": "CHAIN_STRING", "path": "almanak/x.py", "line": 5, "column": 9, "name": "arbitrum"}
        ],
    }
    dest = tmp_path / "legacy.json"
    dest.write_text(json.dumps(legacy), encoding="utf-8")
    assert scanner.load_baseline_json(dest) == {("CHAIN_STRING", "almanak/x.py", "arbitrum")}


def test_write_baseline_schema_is_v2_and_deduped(scanner, tmp_path):
    findings = [_finding(scanner, line=10), _finding(scanner, line=20)]  # one site, two lines
    dest = tmp_path / "baseline.json"
    scanner.write_baseline_json(findings, dest)
    payload = json.loads(dest.read_text())
    assert payload["version"] == 2
    assert payload["total"] == 1  # deduped sites
    assert payload["raw_findings"] == 2  # underlying line-level findings
    assert len(payload["findings"]) == 1
