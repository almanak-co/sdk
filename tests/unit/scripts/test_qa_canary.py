"""A broken test process must never impersonate a healthy recorder control."""

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from xml.sax.saxutils import quoteattr

import pytest

from qa_lab.qa_canary import CELL_IDS, MESSAGE, attest_failure, health


def _junit(path: Path, *, message: str = MESSAGE, child: str = "failure", name: str = "safe") -> None:
    path.write_text(
        '<testsuite><testcase classname="tests.qa_lab.test_harness_canary" '
        f'name="test_harness_canary_must_fail_{name}">'
        f"<{child} message={quoteattr('AssertionError: ' + message)}/></testcase></testsuite>"
    )


@pytest.mark.parametrize(
    "message,child,code,name",
    [
        (MESSAGE, "failure", 1, "safe"),
        ("fixture exploded", "failure", 1, "safe"),
        (MESSAGE, "error", 1, "safe"),
        (MESSAGE, "skipped", 0, "safe"),
        (MESSAGE, "failure", 2, "safe"),
        (MESSAGE, "failure", 1, "eoa"),
    ],
)
def test_only_exact_assertion_is_healthy(tmp_path, message, child, code, name):
    junit = tmp_path / "results.xml"
    _junit(junit, message=message, child=child, name=name)
    result = attest_failure(junit, exec_path="safe", returncode=code)
    assert result["expected_failure_verified"] is (
        message == MESSAGE and child == "failure" and code == 1 and name == "safe"
    )


@pytest.mark.parametrize("contents", ["", "not xml", "<testsuite/>", "<testsuite><testcase/></testsuite>"])
def test_missing_failure_cannot_certify(tmp_path, contents):
    junit = tmp_path / "results.xml"
    junit.write_text(contents)
    assert not attest_failure(junit, exec_path="safe", returncode=1)["expected_failure_verified"]


def _sealed(tmp_path):
    directory = tmp_path / "intents" / "canary"
    directory.mkdir(parents=True)
    junit = directory / "results.xml"
    _junit(junit)
    row = {
        "intent_cell_id": CELL_IDS[0],
        "status": "FAIL",
        "canary_attestation": attest_failure(junit, exec_path="safe", returncode=1),
    }
    (directory / "summary.json").write_text(json.dumps({"cells": [row]}))
    record = {
        "surface": "intent",
        "store_path": "intents/canary",
        "run_id": "canary",
        "record_sha256": "record",
        "catalog_sha256": "catalog",
        "cell_verdicts": {CELL_IDS[0]: "FAIL"},
        "sealed_at": "2026-09-05T12:00:00Z",
        "artifacts": [],
    }
    for path in directory.iterdir():
        record["artifacts"].append(
            {"relpath": path.relative_to(tmp_path).as_posix(), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
        )
    return record


def test_health_requires_fresh_current_intact_noninvalidated_evidence(tmp_path):
    record = _sealed(tmp_path)
    now = datetime(2026, 9, 5, 13, tzinfo=UTC)

    def check(records=None, catalog="catalog", at=now):
        return health(store=tmp_path, catalog_sha256=catalog, records=records or [record], now=at)[CELL_IDS[0]]

    assert check()["healthy"]
    assert not health(store=tmp_path, catalog_sha256="catalog", records=[], now=now)[CELL_IDS[0]]["healthy"]
    assert not check(catalog="new catalog")["healthy"]
    assert not check(at=now + timedelta(days=1))["healthy"]
    assert not check(at=now - timedelta(hours=2))["healthy"]
    assert not check(records=[record, {"record_kind": "invalidation", "invalidates_record_sha256": "record"}])[
        "healthy"
    ]
    (tmp_path / "intents/canary/results.xml").write_text("tampered")
    assert not check()["healthy"]


def test_daily_prioritizes_missing_canary_and_does_not_call_expected_failure_a_product_failure(tmp_path):
    from qa_lab.qa_daily import build_daily_report

    record = _sealed(tmp_path)
    (tmp_path / "index").mkdir()
    (tmp_path / "index/experiment_runs.jsonl").write_text(json.dumps(record) + "\n")
    (tmp_path / "catalog").mkdir()
    (tmp_path / "catalog/intent_cells.json").write_text(json.dumps({"catalog_sha256": "catalog"}))
    report = build_daily_report(store=tmp_path, day="2026-09-05")
    assert report["attention"][0]["kind"] == "canary_unhealthy"
    assert report["attention"][0]["subject"] == CELL_IDS[1]
    assert not [a for a in report["attention"] if a["subject"] == CELL_IDS[0]]
