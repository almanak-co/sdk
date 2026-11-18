"""Retained evidence proves a case only through an explicit passing record."""

import gzip
import json
import subprocess
from pathlib import Path

import pytest

from scripts import proof_ledger
from scripts.proof_ledger import (
    Catalog,
    build,
    captured_at,
    listed_files,
    obligation_ledger,
    read_package,
    render_markdown,
    resolve_case_id,
    source_identity,
    summarize_cases,
    verdict,
)
from scripts.qa_bench import proof_mapping
from tests.qa.catalog import catalog, validate

FORK = "fork/pinned/example_lending/eoa/configured"
FRESH = "fork/fresh/example_lending/eoa/configured"
BACKTEST = "backtest/pinned/example_lending/economic"
SCHEDULED = Catalog(frozenset({FORK, FRESH, BACKTEST}), frozenset({"example_lending"}))


def write_json(path: Path, value, compress=False):
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(value)
    if compress:
        with gzip.open(path, "wt", encoding="utf-8") as target:
            target.write(text)
    else:
        path.write_text(text)


def package(evidence: Path, name: str, manifest) -> Path:
    path = evidence / name / "manifest.json"
    write_json(path, manifest)
    return path


def read(evidence: Path, name: str, manifest, scheduled=SCHEDULED):
    return read_package(package(evidence, name, manifest), evidence, scheduled)


@pytest.mark.parametrize(
    ("record", "expected"),
    [
        ({"returncode": 0}, ("passed", "returncode")),
        ({"returncode": 1}, ("failed", "returncode")),
        ({"returncode": 0, "status": "failed"}, ("failed", "returncode")),
        ({"returncode": True}, ("no_result", "none")),
        ({"status": "passed"}, ("passed", "status")),
        ({"status": "blocked"}, ("failed", "status")),
        ({"result": "passed"}, ("passed", "status")),
        ({"phases": ["entered_and_held", "closed"]}, ("passed", "phases")),
        ({"phases": ["entered_and_held"]}, ("incomplete", "phases")),
        ({"phases": []}, ("incomplete", "phases")),
        ({"closed": True, "completed": 7}, ("no_result", "none")),
        ({"returncode": 0, "status": {"phase": "closed"}}, ("unrecognised", "status")),
        ({"status": ["passed"]}, ("unrecognised", "status")),
    ],
)
def test_only_explicit_passing_records_pass(record, expected):
    assert verdict(record)[:2] == expected


@pytest.mark.parametrize(
    ("candidates", "expected"),
    [
        ([FORK], FORK),
        (["native/pinned-example_lending-eoa-configured/evidence/result.json.gz"], FORK),
        (["cases/fork-fresh-example_lending-eoa-configured"], FRESH),
        (["example_lending-pinned-eoa-configured"], FORK),
        (["runs/pinned-example_lending-economic"], BACKTEST),
        (["example_lending-0a601ae797d44f52bd24426efc5ad71f"], None),
        (["pinned-other_lending-eoa-configured"], None),
        (["eoa-configured"], None),
        ([], None),
    ],
)
def test_case_ids_resolve_only_to_scheduled_cases(candidates, expected):
    assert resolve_case_id(candidates, SCHEDULED.case_ids) == expected


def test_path_segments_resolve_any_lane_but_never_an_ambiguous_suffix():
    cleanup = "cleanup/pinned/example_lp/eoa"
    gmx_fork = "fork/pinned/example_position/eoa/configured"
    gmx_cleanup = "cleanup/pinned/example_position/eoa/configured"
    ids = frozenset({FORK, cleanup, gmx_fork, gmx_cleanup})
    assert resolve_case_id(["runs/pinned-example_lp-eoa"], ids) == cleanup
    assert resolve_case_id(["cleanup-pinned-example_lp-eoa"], ids) == cleanup
    assert resolve_case_id(["example_position-pinned-eoa-configured"], ids) is None
    assert resolve_case_id(["fork-pinned-example_position-eoa-configured"], ids) == gmx_fork
    assert resolve_case_id([gmx_cleanup], ids) == gmx_cleanup


def test_returncode_records_with_result_paths(tmp_path):
    write_json(tmp_path / "native/x/evidence/result.json.gz", {"status": "passed"}, compress=True)
    read_package_result = read(
        tmp_path,
        "native",
        {
            "baseline_commit": "abc123",
            "cases": [
                {
                    "case": FORK,
                    "directory": "x",
                    "returncode": 0,
                    "seconds": 1.5,
                    "result_path": "x/evidence/result.json.gz",
                    "checkpoint_path": "x/evidence/checkpoint.json.gz",
                },
                {"case": FRESH, "directory": "y", "returncode": 1, "seconds": 2.0},
            ],
        },
    )
    assert read_package_result.kind == "case_records"
    assert read_package_result.source == {"key": "baseline_commit", "values": ["abc123"]}
    assert [(r.case_id, r.verdict, r.basis) for r in read_package_result.records] == [
        (FORK, "passed", "returncode"),
        (FRESH, "failed", "returncode"),
    ]


def test_result_path_without_returncode_reads_the_referenced_result(tmp_path):
    write_json(tmp_path / "pkg/a/result.json", {"status": "passed", "phases": ["closed"]})
    write_json(tmp_path / "pkg/b/result.json.gz", {"status": "failed"}, compress=True)
    write_json(tmp_path / "pkg/d/result.json", {"closed": True})
    result = read(
        tmp_path,
        "pkg",
        {
            "cases": [
                {"case": FORK, "result_path": "a/result.json.gz"},
                {"case": FRESH, "result_path": "b/result.json"},
                {"case": BACKTEST, "result_path": "c/result.json"},
                {"case": FORK, "directory": "d", "result_path": "d/result.json"},
            ]
        },
    )
    assert [(r.verdict, r.basis) for r in result.records] == [
        ("passed", "result_file"),
        ("failed", "result_file"),
        ("unreadable", "result_file"),
        ("no_result", "result_file"),
    ]


def test_status_and_phase_records_without_a_lane_are_unattributed(tmp_path):
    result = read(
        tmp_path,
        "older",
        {
            "scope": "older capture",
            "cases": [
                {
                    "name": "example_lending-1d7ada1a",
                    "reference": "example_lending",
                    "profile": "safe",
                    "status": "passed",
                },
                {"case": "example_lending-cleanup-eoa-56f0", "phases": ["closed"]},
                {"case": "example_lending-9b1c", "status": "passed", "phases": ["entered"]},
                {"case": "example_lending-77aa", "phases": ["entered_and_held"]},
            ],
        },
    )
    assert [r.case_id for r in result.records] == [None] * 4
    assert [r.verdict for r in result.records] == ["passed", "passed", "passed", "incomplete"]
    assert {r.reference for r in result.records} == {"example_lending"}
    unattributed = proof_ledger.unattributed_passes(result.records)
    assert unattributed["by_reference"]["example_lending"]["records"] == 3
    assert unattributed["without_reference"] == 0


def test_file_listed_results_identify_cases_from_paths_or_content(tmp_path):
    evidence = tmp_path
    write_json(
        evidence / "files/native/pinned-example_lending-eoa-configured/evidence/result.json.gz",
        {"strategy": "example_lending", "status": "passed", "phases": ["closed"]},
        compress=True,
    )
    write_json(
        evidence / "files/proofs/run-1/result.json.gz",
        {"case": BACKTEST, "status": "passed"},
        compress=True,
    )
    write_json(evidence / "files/quality/result.json", {"status": "passed"})
    write_json(evidence / "files/broken/result.json", {"status": "passed"})
    (evidence / "files/broken/result.json.gz").write_bytes(b"not gzip")
    write_json(evidence / "files/list/result.json", [])
    result = read(
        evidence,
        "files",
        {
            "native/pinned-example_lending-eoa-configured/evidence/result.json": {
                "sha256": "0" * 64
            },
            "native/pinned-example_lending-eoa-configured/evidence/checkpoint.json.gz": {
                "sha256": "1" * 64
            },
            "proofs/run-1/result.json.gz": {"sha256": "2" * 64},
            "quality/result.json": {"sha256": "3" * 64},
            "broken/result.json.gz": {"sha256": "4" * 64},
            "list/result.json": {"sha256": "5" * 64},
            "tested-source.tar.gz": {"sha256": "6" * 64},
        },
    )
    assert result.kind == "result_files"
    assert result.source == {"key": "files:source_archive_sha256", "values": ["6" * 64]}
    by_path = {r.path: r for r in result.records}
    assert (
        by_path["native/pinned-example_lending-eoa-configured/evidence/result.json"].case_id == FORK
    )
    assert by_path["proofs/run-1/result.json.gz"].case_id == BACKTEST
    assert by_path["quality/result.json"].case_id is None
    assert by_path["quality/result.json"].verdict == "passed"
    assert by_path["quality/result.json"].reference == ""
    assert by_path["broken/result.json.gz"].verdict == "unreadable"
    assert by_path["list/result.json"].verdict == "unreadable"
    assert proof_ledger.unattributed_passes(result.records)["without_reference"] == 1


def test_unknown_shapes_are_reported_not_skipped(tmp_path):
    unknown = read(tmp_path, "unknown", {"notes": "hand written"})
    assert (unknown.kind, unknown.records, unknown.error) == ("unrecognised", [], "")
    broken = tmp_path / "broken/manifest.json"
    broken.parent.mkdir()
    broken.write_text("{not json")
    assert read_package(broken, tmp_path, SCHEDULED).kind == "unrecognised"
    assert read_package(broken, tmp_path, SCHEDULED).error.startswith("unreadable")
    scalar = tmp_path / "scalar/manifest.json"
    scalar.parent.mkdir()
    scalar.write_text("3")
    assert read_package(scalar, tmp_path, SCHEDULED).error == "manifest is not an object"
    nested = read(tmp_path, "run", {"schema_version": 1, "dataset": "d", "strategy": {}})
    assert nested.kind == "nested_artifact"
    listing = read(tmp_path, "research", [{"file": "a.html", "sha256": "0" * 64}])
    assert (listing.kind, listing.unrecognised_records) == ("no_case_records", 0)
    partial = read(
        tmp_path,
        "partial",
        {
            "status": "passed",
            "cases": [{"scope": "discovery", "status": "passed"}, "drained"],
            "results": {"controller_completed": [{"case": FORK, "returncode": 0}]},
        },
    )
    assert (partial.kind, partial.unrecognised_records) == ("case_records", 2)
    assert [r.case_id for r in partial.records] == [FORK]


def test_source_identity_and_capture_time_fallbacks(tmp_path):
    assert source_identity({"tested_source": {"sha256": "x"}}, {}) == {
        "key": "tested_source.sha256",
        "values": ["x"],
    }
    assert source_identity({"source_archive": "../a.tar.gz"}, {}) == {
        "key": "source_archive",
        "values": ["../a.tar.gz"],
    }
    assert source_identity({}, {"a/tested-source.tar.gz": "1", "b/source.tar.gz": "2"}) == {
        "key": "files:source_archive_sha256",
        "values": ["1", "2"],
    }
    assert source_identity({}, {"a/tested-source.tar.gz": None}) is None
    assert captured_at({"junit": {"timestamp": "2026-09-08T15:41:13"}}, Path("x")) == {
        "value": "2026-09-08T15:41:13",
        "source": "junit:timestamp",
    }
    assert captured_at({"results": {"recorded_at": "t"}}, Path("2026-09-09-x/y")) == {
        "value": "t",
        "source": "results:recorded_at",
    }
    assert captured_at({}, Path("2026-09-09-rollout/a")) == {
        "value": "2026-09-09",
        "source": "directory_name",
    }
    assert captured_at({}, Path("rollout/a")) == {"value": None, "source": "unrecorded"}
    assert listed_files(
        {"files": [{"path": "a", "sha256": "1" * 64}, {"file": "b"}, "c"], "artifacts": {"d": 1}}
    ) == {"a": "1" * 64, "b": None, "d": None}


def test_nested_package_results_are_recorded_once(tmp_path):
    write_json(
        tmp_path / "parent/child/result.json", {"case": FORK, "status": "passed"}, compress=False
    )
    write_json(tmp_path / "parent/own/result.json", {"case": FRESH, "status": "passed"})
    package(tmp_path, "parent/child", {"files": [{"path": "result.json", "sha256": "0" * 64}]})
    package(
        tmp_path,
        "parent",
        {
            "files": [
                {"path": "child/result.json", "sha256": "0" * 64},
                {"path": "own/result.json", "sha256": "1" * 64},
            ]
        },
    )
    packages = proof_ledger.read_packages(tmp_path, SCHEDULED)
    assert [(p.path, [r.case_id for r in p.records]) for p in packages] == [
        ("parent/child/manifest.json", [FORK]),
        ("parent/manifest.json", [FRESH]),
    ]


def test_bench_results_rows_are_cases_with_the_run_identity(tmp_path):
    write_json(
        tmp_path / "bench/public/results.json",
        {
            "schema_version": 1,
            "identity": {"commit": "deadbeef", "tree_sha256": "t"},
            "status": "incomplete_or_failed",
            "cases": [
                {"id": FORK, "lane": "fork", "status": "passed", "artifact": "cases/000"},
                {"id": FRESH, "lane": "fork", "status": "blocked", "category": "not_started"},
                {"id": BACKTEST, "lane": "backtest", "status": "skipped"},
            ],
        },
    )
    write_json(tmp_path / "bench/broken/results.json", ["not", "a", "run"])
    write_json(
        tmp_path / "bench/batch/results.json.gz",
        [{"case": BACKTEST, "returncode": 0, "seconds": 3.0}, {"network": "isolated"}],
        compress=True,
    )
    result = read(
        tmp_path,
        "bench",
        {
            "baseline_commit": "cafe",
            "scope": "retained bench run",
            "files": [
                {"path": "public/results.json", "sha256": "0" * 64},
                {"path": "broken/results.json", "sha256": "1" * 64},
                {"path": "missing/results.json.gz", "sha256": "2" * 64},
                {"path": "batch/results.json", "sha256": "3" * 64},
            ],
        },
    )
    assert result.kind == "result_files"
    assert [(r.case_id, r.verdict, r.source_key, r.source_values) for r in result.records] == [
        (BACKTEST, "passed", "baseline_commit", ("cafe",)),
        (FORK, "passed", "identity.commit", ("deadbeef",)),
        (FRESH, "failed", "identity.commit", ("deadbeef",)),
        (BACKTEST, "failed", "identity.commit", ("deadbeef",)),
    ]
    assert result.records[1].path == "cases/000"
    assert result.unrecognised_records == 3
    cases = summarize_cases(result.records)
    assert cases[FORK]["newest_pass"]["source"] == {
        "key": "identity.commit",
        "values": ["deadbeef"],
    }


def test_source_divergence_compares_hashes_within_one_identity_kind(tmp_path):
    archive = {"path": "tested-source.tar.gz", "sha256": "a" * 64}
    second = {"path": "batch/source.tar.gz", "sha256": "b" * 64}
    record = {"cases": [{"case": FORK, "returncode": 0}]}
    read(tmp_path, "2026-09-01-commit", {"baseline_commit": "c" * 40, **record})
    read(tmp_path, "2026-09-02-one-archive", {"files": [archive], **record})
    read(tmp_path, "2026-09-03-two-archives", {"files": [archive, second], **record})
    packages = proof_ledger.read_packages(tmp_path, SCHEDULED)
    cases = summarize_cases([r for p in packages for r in p.records])
    assert cases[FORK]["source_divergent"] is False
    assert cases[FORK]["source_identities"] == [
        {"key": "baseline_commit", "values": ["c" * 40]},
        {"key": "files:source_archive_sha256", "values": ["a" * 64, "b" * 64]},
    ]
    read(tmp_path, "2026-09-04-other-archive", {"files": [second], **record})
    packages = proof_ledger.read_packages(tmp_path, SCHEDULED)
    cases = summarize_cases([r for p in packages for r in p.records])
    assert cases[FORK]["source_divergent"] is True


def test_ledger_classification_and_source_divergence(tmp_path):
    read(
        tmp_path,
        "2026-09-07-first",
        {"base_commit": "one", "cases": [{"case": FORK, "returncode": 0}]},
    )
    read(
        tmp_path,
        "2026-09-08-second",
        {
            "base_commit": "two",
            "cases": [{"case": FORK, "returncode": 0}, {"case": FRESH, "returncode": 1}],
        },
    )
    read(tmp_path, "2026-09-09-third", {"cases": [{"case": BACKTEST, "status": "passed"}]})
    packages = proof_ledger.read_packages(tmp_path, SCHEDULED)
    cases = summarize_cases([r for p in packages for r in p.records])
    assert cases[FORK]["source_divergent"] is True
    assert cases[FORK]["source_identities"] == [{"key": "base_commit", "values": ["one", "two"]}]
    assert cases[FORK]["newest_pass"]["package"] == "2026-09-08-second/manifest.json"
    assert cases[FRESH]["retained_pass"] is False
    assert cases[FRESH]["non_passing_records"] == {"failed": 1}
    assert cases[BACKTEST]["source_identities"] == []
    mapping = [
        {"obligation": {"connector": "a", "chain": "c", "mode": "fork"}, "case_ids": [FORK]},
        {"obligation": {"connector": "a", "chain": "c", "mode": "fork"}, "case_ids": [FORK, FRESH]},
        {"obligation": {"connector": "b", "chain": "c", "mode": "fork"}, "case_ids": [FRESH]},
        {"obligation": {"connector": "b", "chain": "c", "mode": "backtest"}, "case_ids": []},
    ]
    ledger = obligation_ledger(mapping, cases)
    assert [entry["status"] for entry in ledger] == ["proven", "partial", "none", "none"]
    assert ledger[1]["outstanding_case_ids"] == [FRESH]
    assert proof_ledger.totals(ledger, "connector") == {
        "a": {"proven": 1, "partial": 1, "none": 0},
        "b": {"proven": 0, "partial": 0, "none": 2},
    }


def test_build_uses_the_catalog_mapping_and_renders(tmp_path, monkeypatch, capsys):
    cases = catalog()
    mapping = proof_mapping(cases, validate(cases))
    proven = min((m for m in mapping if m["case_ids"]), key=lambda m: len(m["case_ids"]))
    partial = next(
        m
        for m in mapping
        if len(m["case_ids"]) > 1 and not set(m["case_ids"]) & set(proven["case_ids"])
    )
    evidence = tmp_path / "evidence"
    package(
        evidence,
        "2026-09-09-proven",
        {
            "baseline_commit": "abc",
            "cases": [{"case": i, "returncode": 0} for i in proven["case_ids"]],
        },
    )
    package(
        evidence,
        "2026-09-09-partial",
        {"cases": [{"case": partial["case_ids"][0], "status": "passed"}]},
    )
    package(evidence, "2026-09-09-odd", {"unexpected": True})
    payload = proof_ledger.plain(build(evidence))
    statuses = {
        json.dumps(e["obligation"], sort_keys=True): e["status"] for e in payload["obligations"]
    }
    assert (
        statuses[json.dumps(proof_ledger.plain(proven["obligation"]), sort_keys=True)] == "proven"
    )
    assert (
        statuses[json.dumps(proof_ledger.plain(partial["obligation"]), sort_keys=True)] == "partial"
    )
    counts = payload["summary"]["obligations"]
    assert sum(counts.values()) == len(mapping)
    assert counts["proven"] >= 1 and counts["partial"] >= 1
    passed = set(proven["case_ids"]) | {partial["case_ids"][0]}
    assert counts["none"] == sum(1 for m in mapping if not set(m["case_ids"]) & passed)
    assert payload["summary"]["packages"] == {"case_records": 2, "unrecognised": 1}
    assert len(payload["blocked_candidates"]) == sum(1 for c in cases if c.lane == "blocked")
    assert all(b["case_id"].startswith("blocked/") for b in payload["blocked_candidates"])
    text = render_markdown(payload)
    assert text.startswith("# Obligation proof ledger\n\nA retained pass on an older source")
    assert "## Blocked candidates" in text and "2026-09-09-odd/manifest.json" in text
    monkeypatch.setattr(
        "sys.argv", ["proof_ledger", "--evidence", str(evidence), "--output", str(tmp_path / "out")]
    )
    monkeypatch.setattr(subprocess, "check_output", _raise)
    assert proof_ledger.main() == 0
    assert "unrecognised packages: 1" in capsys.readouterr().out
    written = json.loads((tmp_path / "out/proof-ledger.json").read_text())
    assert written["catalog_identity"] is None
    assert written["summary"]["obligations"] == payload["summary"]["obligations"]
    assert (tmp_path / "out/proof-ledger.md").read_text().startswith("# Obligation proof ledger")


def _raise(*args, **kwargs):
    raise OSError("no git")


def test_missing_or_file_evidence_root_is_refused(tmp_path, monkeypatch, capsys):
    stray = tmp_path / "not-a-directory"
    stray.write_text("{}")
    for evidence in (tmp_path / "absent", stray):
        monkeypatch.setattr(
            "sys.argv",
            ["proof_ledger", "--evidence", str(evidence), "--output", str(tmp_path / "out")],
        )
        with pytest.raises(SystemExit) as refused:
            proof_ledger.main()
        assert refused.value.code == 2
        assert "evidence root is not a directory" in capsys.readouterr().err
    assert not (tmp_path / "out").exists()
