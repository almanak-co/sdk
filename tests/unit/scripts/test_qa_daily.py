"""Contracts for the Daily Report projection.

The report exists because a Slack message cannot say "this lane stopped
running". These tests pin the three distinctions that make the page worth
reading: silence is a state, a regression is not the same as a standing
failure, and specimen output can never be mistaken for evidence.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture(scope="module")
def daily_module() -> ModuleType:
    path = REPO_ROOT / "scripts" / "quant-test" / "qa_daily.py"
    spec = importlib.util.spec_from_file_location("qa_daily_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _ledger(store: Path, records: list[dict]) -> None:
    index = store / "index"
    index.mkdir(parents=True, exist_ok=True)
    with (index / "experiment_runs.jsonl").open("w", encoding="utf-8") as stream:
        for record in records:
            stream.write(json.dumps(record) + "\n")


def _record(surface: str, day: str, verdicts: dict[str, str], run: str = "r1") -> dict:
    stamp = f"{day}T12:00:00+00:00"
    return {
        "surface": surface,
        "run_id": f"{surface}-{run}",
        "cell_verdicts": verdicts,
        "completed_at": stamp,
        "sealed_at": stamp,
    }


def _kinds(report: dict, subject: str | None = None) -> list[str]:
    return [item["kind"] for item in report["attention"] if subject is None or item["subject"] == subject]


# ---------------------------------------------------------------------------
# Silence — the failure mode the nightly Slack post could not express
# ---------------------------------------------------------------------------


def test_lane_that_never_ran_is_reported_not_omitted(daily_module: ModuleType, tmp_path: Path) -> None:
    _ledger(tmp_path, [_record("demo", "2026-08-31", {"demo.a.anvil.eoa": "PASS"})])
    report = daily_module.build_daily_report(store=tmp_path, day="2026-08-31")
    silent = [item["subject"] for item in report["attention"] if item["kind"] == "silent_lane"]
    assert "Data" in silent
    assert "Intent" in silent


def test_silence_outranks_every_ordinary_failure(daily_module: ModuleType, tmp_path: Path) -> None:
    """A lane that is not running cannot fail, so it must be triaged first."""
    _ledger(tmp_path, [_record("demo", "2026-08-31", {"demo.a.anvil.eoa": "FAIL"})])
    report = daily_module.build_daily_report(store=tmp_path, day="2026-08-31")
    kinds = [item["kind"] for item in report["attention"]]
    assert kinds[0] == "silent_lane"
    assert kinds.index("silent_lane") < kinds.index("new_fail")


def test_lane_past_its_cadence_is_stale_not_silent(daily_module: ModuleType, tmp_path: Path) -> None:
    """A lane that ran recently is quiet; one past its cadence is overdue."""
    _ledger(
        tmp_path,
        [
            _record("accounting", "2026-08-01", {"acct.a": "PASS"}),
            _record("demo", "2026-08-31", {"demo.a.anvil.eoa": "PASS"}),
        ],
    )
    report = daily_module.build_daily_report(store=tmp_path, day="2026-08-31")
    accounting = [item for item in report["attention"] if item["subject"] == "Accounting"]
    assert [item["kind"] for item in accounting] == ["stale_lane"]
    assert "30 days ago" in accounting[0]["detail"]


def test_lane_inside_its_cadence_raises_nothing(daily_module: ModuleType, tmp_path: Path) -> None:
    _ledger(
        tmp_path,
        [
            _record("accounting", "2026-08-29", {"acct.a": "PASS"}),
            _record("demo", "2026-08-31", {"demo.a.anvil.eoa": "PASS"}),
        ],
    )
    report = daily_module.build_daily_report(store=tmp_path, day="2026-08-31")
    assert not [item for item in report["attention"] if item["subject"] == "Accounting"]


# ---------------------------------------------------------------------------
# Movement — a regression is not a standing failure
# ---------------------------------------------------------------------------


def test_green_yesterday_red_today_is_a_regression(daily_module: ModuleType, tmp_path: Path) -> None:
    _ledger(
        tmp_path,
        [
            _record("demo", "2026-08-30", {"demo.a.anvil.eoa": "PASS"}, run="r1"),
            _record("demo", "2026-08-31", {"demo.a.anvil.eoa": "FAIL"}, run="r2"),
        ],
    )
    report = daily_module.build_daily_report(store=tmp_path, day="2026-08-31")
    assert _kinds(report, "demo.a.anvil.eoa") == ["regression"]


def test_first_ever_result_being_red_is_a_new_failure_not_a_regression(
    daily_module: ModuleType, tmp_path: Path
) -> None:
    _ledger(tmp_path, [_record("demo", "2026-08-31", {"demo.a.anvil.eoa": "FAIL"})])
    report = daily_module.build_daily_report(store=tmp_path, day="2026-08-31")
    assert _kinds(report, "demo.a.anvil.eoa") == ["new_fail"]


def test_standing_failure_is_not_reported_as_a_regression(daily_module: ModuleType, tmp_path: Path) -> None:
    """Red yesterday and red today is not new information about a regression."""
    _ledger(
        tmp_path,
        [
            _record("demo", "2026-08-30", {"demo.a.anvil.eoa": "FAIL"}, run="r1"),
            _record("demo", "2026-08-31", {"demo.a.anvil.eoa": "FAIL"}, run="r2"),
        ],
    )
    report = daily_module.build_daily_report(store=tmp_path, day="2026-08-31")
    assert _kinds(report, "demo.a.anvil.eoa") == ["new_fail"]


def test_void_is_its_own_kind_not_a_failure(daily_module: ModuleType, tmp_path: Path) -> None:
    """VOID means "we cannot grade this", which is a different action than FAIL."""
    _ledger(tmp_path, [_record("demo", "2026-08-31", {"demo.a.anvil.eoa": "VOID"})])
    report = daily_module.build_daily_report(store=tmp_path, day="2026-08-31")
    assert _kinds(report, "demo.a.anvil.eoa") == ["void"]


def test_passing_cells_raise_no_attention(daily_module: ModuleType, tmp_path: Path) -> None:
    _ledger(tmp_path, [_record("demo", "2026-08-31", {"demo.a.anvil.eoa": "PASS"})])
    report = daily_module.build_daily_report(store=tmp_path, day="2026-08-31")
    assert not _kinds(report, "demo.a.anvil.eoa")


def test_attention_is_sorted_by_declared_triage_order(daily_module: ModuleType, tmp_path: Path) -> None:
    _ledger(
        tmp_path,
        [
            _record("demo", "2026-08-30", {"demo.regressed.anvil.eoa": "PASS"}, run="r0"),
            _record(
                "demo",
                "2026-08-31",
                {
                    "demo.new.anvil.eoa": "FAIL",
                    "demo.regressed.anvil.eoa": "FAIL",
                    "demo.void.anvil.eoa": "VOID",
                },
                run="r1",
            ),
        ],
    )
    report = daily_module.build_daily_report(store=tmp_path, day="2026-08-31")
    ranks = [daily_module.ATTENTION_ORDER.index(item["kind"]) for item in report["attention"]]
    assert ranks == sorted(ranks)


# ---------------------------------------------------------------------------
# Denominators and honesty
# ---------------------------------------------------------------------------


def test_every_lane_appears_in_the_rollcall_even_when_quiet(daily_module: ModuleType, tmp_path: Path) -> None:
    """A lane dropped from the roll-call is a lane nobody notices going dark."""
    _ledger(tmp_path, [_record("demo", "2026-08-31", {"demo.a.anvil.eoa": "PASS"})])
    report = daily_module.build_daily_report(store=tmp_path, day="2026-08-31")
    assert {lane["key"] for lane in report["lanes"]} == {lane["key"] for lane in daily_module.LANES}
    data = next(lane for lane in report["lanes"] if lane["key"] == "data")
    assert data["runs"] == 0
    assert data["last_seen"] is None


def test_empty_ledger_reports_silence_not_success(daily_module: ModuleType, tmp_path: Path) -> None:
    """Zero runs must never project as a clean day."""
    report = daily_module.build_daily_report(store=tmp_path, day="2026-08-31")
    assert report["totals"]["runs"] == 0
    assert report["totals"]["attention"] == len(daily_module.LANES)
    assert {item["kind"] for item in report["attention"]} == {"silent_lane"}


def test_real_report_is_never_flagged_specimen(daily_module: ModuleType, tmp_path: Path) -> None:
    report = daily_module.build_daily_report(store=tmp_path, day="2026-08-31")
    assert report["specimen"] is False
    assert all(item["specimen"] is False for item in report["attention"])


def test_specimen_report_is_flagged_at_every_level(daily_module: ModuleType) -> None:
    """An unlabelled mock-up would deepen the exact distrust this page fixes."""
    report = daily_module.specimen_report(day="2026-08-31")
    assert report["specimen"] is True
    assert report["attention"] and all(item["specimen"] is True for item in report["attention"])
    assert report["lanes"] and all(lane["specimen"] is True for lane in report["lanes"])


def test_specimen_demonstrates_every_attention_kind(daily_module: ModuleType) -> None:
    """The worked example is only useful if it shows the whole vocabulary."""
    report = daily_module.specimen_report(day="2026-08-31")
    assert {item["kind"] for item in report["attention"]} == set(daily_module.ATTENTION_ORDER)


def test_every_attention_kind_has_copy_and_a_rank(daily_module: ModuleType) -> None:
    assert set(daily_module.ATTENTION_COPY) == set(daily_module.ATTENTION_ORDER)


# ---------------------------------------------------------------------------
# Calendar
# ---------------------------------------------------------------------------


def test_calendar_buckets_verdicts_by_day(daily_module: ModuleType, tmp_path: Path) -> None:
    _ledger(
        tmp_path,
        [
            _record("demo", "2026-08-30", {"a": "PASS", "b": "FAIL"}, run="r1"),
            _record("intent", "2026-08-30", {"c": "PASS"}, run="r2"),
            _record("demo", "2026-08-31", {"d": "VOID"}, run="r3"),
        ],
    )
    calendar = daily_module.build_calendar(store=tmp_path, today="2026-08-31")
    assert calendar["days"]["2026-08-30"] == {
        "date": "2026-08-30",
        "runs": 2,
        "pass": 2,
        "fail": 1,
        "other": 0,
        "surfaces": ["demo", "intent"],
        "specimen": False,
    }
    assert calendar["days"]["2026-08-31"]["other"] == 1


def test_calendar_omits_days_with_nothing_sealed(daily_module: ModuleType, tmp_path: Path) -> None:
    """A quiet day must have no entry, so it can render as a gap and not green."""
    _ledger(tmp_path, [_record("demo", "2026-08-31", {"a": "PASS"})])
    calendar = daily_module.build_calendar(store=tmp_path, today="2026-08-31")
    assert "2026-08-30" not in calendar["days"]


def test_calendar_grid_covers_the_requested_months(daily_module: ModuleType, tmp_path: Path) -> None:
    calendar = daily_module.build_calendar(store=tmp_path, months=3, today="2026-08-31")
    assert [grid["label"] for grid in calendar["months"]] == ["June 2026", "July 2026", "August 2026"]


def test_calendar_grid_rolls_over_a_year_boundary(daily_module: ModuleType, tmp_path: Path) -> None:
    calendar = daily_module.build_calendar(store=tmp_path, months=3, today="2026-01-15")
    assert [grid["label"] for grid in calendar["months"]] == ["November 2025", "December 2025", "January 2026"]


def test_torn_ledger_line_does_not_lose_the_whole_report(daily_module: ModuleType, tmp_path: Path) -> None:
    index = tmp_path / "index"
    index.mkdir(parents=True)
    good = json.dumps(_record("demo", "2026-08-31", {"a": "PASS"}))
    (index / "experiment_runs.jsonl").write_text(f"{good}\n{{partial", encoding="utf-8")
    assert len(daily_module.read_ledger(tmp_path)) == 1


def test_ledger_order_follows_the_same_precedence_the_day_derivation_uses(
    daily_module: ModuleType, tmp_path: Path
) -> None:
    """`history[-1]` means "previous verdict" only if ledger order and day order agree.

    A record carrying both stamps must sort by `completed_at or sealed_at` —
    the exact precedence `_day_of` consumers apply — or a run completed earlier
    but sealed later is read as the newer verdict and a real regression is
    misclassified as `new_fail` (or a stale failure as a regression).
    """
    early = {
        "surface": "intent",
        "run_id": "intent-early",
        "cell_verdicts": {"cell": "PASS"},
        "completed_at": "2026-01-01T12:00:00+00:00",
        "sealed_at": "2026-01-03T12:00:00+00:00",
    }
    late = {
        "surface": "intent",
        "run_id": "intent-late",
        "cell_verdicts": {"cell": "FAIL"},
        "completed_at": "2026-01-02T12:00:00+00:00",
        "sealed_at": "2026-01-02T12:00:00+00:00",
    }
    _ledger(tmp_path, [late, early])  # written out of order on purpose

    records = daily_module.read_ledger(tmp_path)

    assert [row["run_id"] for row in records] == ["intent-early", "intent-late"]


def test_ledger_sort_parses_timestamps_rather_than_comparing_strings(daily_module: ModuleType, tmp_path: Path) -> None:
    """Lexicographically "...T10:00:00.5Z" sorts before "...T10:00:00Z" ('.' < 'Z'),
    so mixed fractional-second precision would reorder rows and history[-1]
    would read the wrong "previous verdict"."""
    whole = {"surface": "intent", "run_id": "whole", "cell_verdicts": {}, "completed_at": "2026-01-01T10:00:00Z"}
    frac = {"surface": "intent", "run_id": "frac", "cell_verdicts": {}, "completed_at": "2026-01-01T10:00:00.5Z"}
    _ledger(tmp_path, [frac, whole])

    records = daily_module.read_ledger(tmp_path)

    assert [row["run_id"] for row in records] == ["whole", "frac"]
