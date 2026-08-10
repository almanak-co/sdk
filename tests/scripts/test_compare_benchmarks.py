from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest

from scripts.ci.compare_benchmarks import compare_benchmarks, main


def _results(
    elapsed: float,
    *,
    limit: float = 30.0,
    passed: bool = True,
    error: str | None = None,
) -> dict[str, Any]:
    benchmark: dict[str, Any] = {
        "elapsed_seconds": elapsed,
        "limit_seconds": limit,
        "passed": passed,
    }
    if error is not None:
        benchmark["error"] = error
    return {
        "all_passed": passed,
        "benchmarks": {"hold": benchmark},
    }


def test_compare_benchmarks_classifies_relative_regression() -> None:
    compared = compare_benchmarks(
        current=_results(12.1),
        baseline=_results(10.0),
        warn_threshold=10.0,
        fail_threshold=20.0,
    )

    assert len(compared) == 1
    assert compared[0].status == "regression"
    assert compared[0].change_percent == pytest.approx(21.0)


def test_main_fails_when_absolute_sla_fails_even_if_relative_result_is_stable(
    tmp_path: Path,
    monkeypatch,
) -> None:
    current = tmp_path / "current.json"
    baseline = tmp_path / "baseline.json"
    report = tmp_path / "report.md"
    current.write_text(json.dumps(_results(31.0, passed=False)), encoding="utf-8")
    baseline.write_text(json.dumps(_results(30.5, passed=False)), encoding="utf-8")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "compare_benchmarks.py",
            "--current",
            str(current),
            "--baseline",
            str(baseline),
            "--output",
            str(report),
        ],
    )

    assert main() == 1
    assert "Absolute SLA Failures" in report.read_text(encoding="utf-8")


def test_main_fails_closed_when_baseline_is_missing(tmp_path: Path, monkeypatch) -> None:
    current = tmp_path / "current.json"
    report = tmp_path / "report.md"
    current.write_text(json.dumps(_results(5.0)), encoding="utf-8")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "compare_benchmarks.py",
            "--current",
            str(current),
            "--baseline",
            str(tmp_path / "missing.json"),
            "--output",
            str(report),
        ],
    )

    assert main() == 1
    assert "required baseline file" in report.read_text(encoding="utf-8")


def test_main_reports_current_execution_errors(tmp_path: Path, monkeypatch) -> None:
    current = tmp_path / "current.json"
    baseline = tmp_path / "baseline.json"
    report = tmp_path / "report.md"
    current.write_text(json.dumps(_results(0.0, passed=False, error="engine failed")), encoding="utf-8")
    baseline.write_text(json.dumps(_results(5.0)), encoding="utf-8")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "compare_benchmarks.py",
            "--current",
            str(current),
            "--baseline",
            str(baseline),
            "--output",
            str(report),
        ],
    )

    assert main() == 1
    assert "Current benchmark execution was invalid: hold: engine failed" in report.read_text(encoding="utf-8")


def test_main_fails_when_benchmark_name_sets_differ(tmp_path: Path, monkeypatch) -> None:
    current_results = _results(5.0)
    current_results["benchmarks"]["current-only"] = {
        "elapsed_seconds": 1.0,
        "limit_seconds": 30.0,
        "passed": True,
    }
    current = tmp_path / "current.json"
    baseline = tmp_path / "baseline.json"
    report = tmp_path / "report.md"
    current.write_text(json.dumps(current_results), encoding="utf-8")
    baseline.write_text(json.dumps(_results(5.0)), encoding="utf-8")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "compare_benchmarks.py",
            "--current",
            str(current),
            "--baseline",
            str(baseline),
            "--output",
            str(report),
        ],
    )

    assert main() == 1
    report_text = report.read_text(encoding="utf-8")
    assert "Benchmark result sets are incompatible" in report_text
    assert "Missing from current: []" in report_text
    assert "missing from baseline: ['current-only']" in report_text
