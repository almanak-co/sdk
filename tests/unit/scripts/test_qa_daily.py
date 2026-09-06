"""Contracts for the Daily Report projection.

The report exists because a Slack message cannot say "this lane stopped
running". These tests pin the three distinctions that make the page worth
reading: silence is a state, a regression is not the same as a standing
failure, and only evidence observed from the declared tracking epoch appears.
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
    path = REPO_ROOT / "qa_lab" / "qa_daily.py"
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
    _ledger(tmp_path, [_record("demo", "2026-09-04", {"demo.a.anvil.eoa": "PASS"})])
    report = daily_module.build_daily_report(store=tmp_path, day="2026-09-04")
    silent = [item["subject"] for item in report["attention"] if item["kind"] == "silent_lane"]
    assert "Data" in silent
    assert "Intent" in silent


def test_silence_outranks_every_ordinary_failure(daily_module: ModuleType, tmp_path: Path) -> None:
    """A lane that is not running cannot fail, so it must be triaged first."""
    _ledger(tmp_path, [_record("demo", "2026-09-04", {"demo.a.anvil.eoa": "FAIL"})])
    report = daily_module.build_daily_report(store=tmp_path, day="2026-09-04")
    kinds = [item["kind"] for item in report["attention"]]
    assert kinds[0] == "canary_unhealthy"
    assert kinds.index("silent_lane") < kinds.index("new_fail")


def test_lane_past_its_cadence_is_stale_not_silent(daily_module: ModuleType, tmp_path: Path) -> None:
    """A lane that ran recently is quiet; one past its cadence is overdue."""
    _ledger(
        tmp_path,
        [
            _record("accounting", "2026-09-04", {"acct.a": "PASS"}),
            _record("demo", "2026-10-04", {"demo.a.anvil.eoa": "PASS"}),
        ],
    )
    report = daily_module.build_daily_report(store=tmp_path, day="2026-10-04")
    accounting = [item for item in report["attention"] if item["subject"] == "Accounting"]
    assert [item["kind"] for item in accounting] == ["stale_lane"]
    assert "30 days ago" in accounting[0]["detail"]


def test_lane_inside_its_cadence_raises_nothing(daily_module: ModuleType, tmp_path: Path) -> None:
    _ledger(
        tmp_path,
        [
            _record("accounting", "2026-09-04", {"acct.a": "PASS"}),
            _record("demo", "2026-09-06", {"demo.a.anvil.eoa": "PASS"}),
        ],
    )
    report = daily_module.build_daily_report(store=tmp_path, day="2026-09-06")
    assert not [item for item in report["attention"] if item["subject"] == "Accounting"]


# ---------------------------------------------------------------------------
# Movement — a regression is not a standing failure
# ---------------------------------------------------------------------------


def test_green_yesterday_red_today_is_a_regression(daily_module: ModuleType, tmp_path: Path) -> None:
    _ledger(
        tmp_path,
        [
            _record("demo", "2026-09-04", {"demo.a.anvil.eoa": "PASS"}, run="r1"),
            _record("demo", "2026-09-05", {"demo.a.anvil.eoa": "FAIL"}, run="r2"),
        ],
    )
    report = daily_module.build_daily_report(store=tmp_path, day="2026-09-05")
    assert _kinds(report, "demo.a.anvil.eoa") == ["regression"]


def test_first_ever_result_being_red_is_a_new_failure_not_a_regression(
    daily_module: ModuleType, tmp_path: Path
) -> None:
    _ledger(tmp_path, [_record("demo", "2026-09-04", {"demo.a.anvil.eoa": "FAIL"})])
    report = daily_module.build_daily_report(store=tmp_path, day="2026-09-04")
    assert _kinds(report, "demo.a.anvil.eoa") == ["new_fail"]


def test_standing_failure_is_not_reported_as_a_regression(daily_module: ModuleType, tmp_path: Path) -> None:
    """Red yesterday and red today is not new information about a regression."""
    _ledger(
        tmp_path,
        [
            _record("demo", "2026-09-04", {"demo.a.anvil.eoa": "FAIL"}, run="r1"),
            _record("demo", "2026-09-05", {"demo.a.anvil.eoa": "FAIL"}, run="r2"),
        ],
    )
    report = daily_module.build_daily_report(store=tmp_path, day="2026-09-05")
    assert _kinds(report, "demo.a.anvil.eoa") == ["new_fail"]


def test_void_is_its_own_kind_not_a_failure(daily_module: ModuleType, tmp_path: Path) -> None:
    """VOID means "we cannot grade this", which is a different action than FAIL."""
    _ledger(tmp_path, [_record("demo", "2026-09-04", {"demo.a.anvil.eoa": "VOID"})])
    report = daily_module.build_daily_report(store=tmp_path, day="2026-09-04")
    assert _kinds(report, "demo.a.anvil.eoa") == ["void"]


def test_passing_cells_raise_no_attention(daily_module: ModuleType, tmp_path: Path) -> None:
    _ledger(tmp_path, [_record("demo", "2026-09-04", {"demo.a.anvil.eoa": "PASS"})])
    report = daily_module.build_daily_report(store=tmp_path, day="2026-09-04")
    assert not _kinds(report, "demo.a.anvil.eoa")


def test_attention_is_sorted_by_declared_triage_order(daily_module: ModuleType, tmp_path: Path) -> None:
    _ledger(
        tmp_path,
        [
            _record("demo", "2026-09-04", {"demo.regressed.anvil.eoa": "PASS"}, run="r0"),
            _record(
                "demo",
                "2026-09-05",
                {
                    "demo.new.anvil.eoa": "FAIL",
                    "demo.regressed.anvil.eoa": "FAIL",
                    "demo.void.anvil.eoa": "VOID",
                },
                run="r1",
            ),
        ],
    )
    report = daily_module.build_daily_report(store=tmp_path, day="2026-09-05")
    ranks = [daily_module.ATTENTION_ORDER.index(item["kind"]) for item in report["attention"]]
    assert ranks == sorted(ranks)


# ---------------------------------------------------------------------------
# Denominators and honesty
# ---------------------------------------------------------------------------


def test_every_lane_appears_in_the_rollcall_even_when_quiet(daily_module: ModuleType, tmp_path: Path) -> None:
    """A lane dropped from the roll-call is a lane nobody notices going dark."""
    _ledger(tmp_path, [_record("demo", "2026-09-04", {"demo.a.anvil.eoa": "PASS"})])
    report = daily_module.build_daily_report(store=tmp_path, day="2026-09-04")
    assert {lane["key"] for lane in report["lanes"]} == {lane["key"] for lane in daily_module.LANES}
    data = next(lane for lane in report["lanes"] if lane["key"] == "data")
    assert data["runs"] == 0
    assert data["last_seen"] is None


def test_empty_ledger_reports_silence_not_success(daily_module: ModuleType, tmp_path: Path) -> None:
    """Zero runs must never project as a clean day."""
    report = daily_module.build_daily_report(store=tmp_path, day="2026-09-04")
    assert report["totals"]["runs"] == 0
    assert report["totals"]["attention"] == len(daily_module.LANES) + 2
    assert {item["kind"] for item in report["attention"]} == {"silent_lane", "canary_unhealthy"}


def test_real_report_is_never_flagged_specimen(daily_module: ModuleType, tmp_path: Path) -> None:
    report = daily_module.build_daily_report(store=tmp_path, day="2026-09-04")
    assert report["specimen"] is False
    assert all(item["specimen"] is False for item in report["attention"])


def test_every_attention_kind_has_copy_and_a_rank(daily_module: ModuleType) -> None:
    assert set(daily_module.ATTENTION_COPY) == set(daily_module.ATTENTION_ORDER)


# ---------------------------------------------------------------------------
# Calendar
# ---------------------------------------------------------------------------


def test_calendar_buckets_verdicts_by_day(daily_module: ModuleType, tmp_path: Path) -> None:
    _ledger(
        tmp_path,
        [
            _record("demo", "2026-09-04", {"a": "PASS", "b": "FAIL"}, run="r1"),
            _record("intent", "2026-09-04", {"c": "PASS"}, run="r2"),
            _record("demo", "2026-09-05", {"d": "VOID"}, run="r3"),
        ],
    )
    calendar = daily_module.build_calendar(store=tmp_path, today="2026-09-05")
    assert calendar["days"]["2026-09-04"] == {
        "date": "2026-09-04",
        "runs": 2,
        "pass": 2,
        "fail": 1,
        "other": 0,
        "surfaces": ["demo", "intent"],
        "specimen": False,
    }
    assert calendar["days"]["2026-09-05"]["other"] == 1


def test_calendar_omits_days_with_nothing_sealed(daily_module: ModuleType, tmp_path: Path) -> None:
    """A quiet day must have no entry, so it can render as a gap and not green."""
    _ledger(tmp_path, [_record("demo", "2026-09-05", {"a": "PASS"})])
    calendar = daily_module.build_calendar(store=tmp_path, today="2026-09-05")
    assert "2026-09-04" not in calendar["days"]


def test_calendar_begins_with_september_2026(daily_module: ModuleType, tmp_path: Path) -> None:
    calendar = daily_module.build_calendar(store=tmp_path, today="2026-09-04")
    assert calendar["observation_start"] == "2026-09-04"
    assert [grid["label"] for grid in calendar["months"]] == ["September 2026"]


def test_calendar_appends_months_forward_across_years(daily_module: ModuleType, tmp_path: Path) -> None:
    calendar = daily_module.build_calendar(store=tmp_path, today="2027-01-15")
    assert [grid["label"] for grid in calendar["months"]] == [
        "September 2026",
        "October 2026",
        "November 2026",
        "December 2026",
        "January 2027",
    ]


def test_pre_epoch_evidence_is_excluded_from_daily_and_calendar(daily_module: ModuleType, tmp_path: Path) -> None:
    _ledger(
        tmp_path,
        [
            _record("demo", "2026-09-03", {"old": "FAIL"}, run="old"),
            _record("demo", "2026-09-04", {"new": "PASS"}, run="new"),
        ],
    )

    report = daily_module.build_daily_report(store=tmp_path, day="2026-09-04")
    calendar = daily_module.build_calendar(store=tmp_path, today="2026-09-04")

    assert report["totals"]["runs"] == 1
    assert _kinds(report, "old") == []
    assert "2026-09-03" not in calendar["days"]
    assert calendar["days"]["2026-09-04"]["runs"] == 1


@pytest.mark.parametrize("builder", ["daily", "calendar"])
def test_dates_before_evidence_epoch_are_rejected(daily_module: ModuleType, tmp_path: Path, builder: str) -> None:
    with pytest.raises(ValueError, match="evidence begins on 2026-09-04"):
        if builder == "daily":
            daily_module.build_daily_report(store=tmp_path, day="2026-09-03")
        else:
            daily_module.build_calendar(store=tmp_path, today="2026-09-03")


def test_rendered_calendar_links_to_canonical_today_page(daily_module: ModuleType, tmp_path: Path) -> None:
    output = daily_module.render_calendar_lab(store=tmp_path, lab_css="", day="2026-09-04")
    page = output.read_text(encoding="utf-8")
    assert 'href="index.html">Open today\'s report' in page
    assert "SPECIMEN" not in page
    assert "September 2026" in page


def test_today_intent_attention_deep_links_to_the_exact_cell(daily_module: ModuleType, tmp_path: Path) -> None:
    cell = "intent.uniswap_v3.base.LP_CLOSE.anvil.safe"
    from qa_lab import qa_history

    run_dir = tmp_path / "runs" / "test-fail"
    run_dir.mkdir(parents=True)
    sdk = {"commit": "a" * 40, "branch": "test", "dirty": False, "sdk_version": "test", "source": "executing-worktree"}
    manifest = run_dir / "manifest.json"
    manifest.write_text(json.dumps({"run_id": "test-fail", "sdk": sdk}))
    qa_history.append_experiment(
        store=tmp_path,
        surface="intent",
        run_id="test-fail",
        run_dir=run_dir,
        manifest_path=manifest,
        sdk=sdk,
        cell_verdicts={cell: "FAIL"},
        started_at="2026-09-04T12:00:00Z",
        completed_at="2026-09-04T12:01:00Z",
        sealed_at="2026-09-04T12:01:00Z",
        catalog_sha256="c" * 64,
        admission={
            "status": "OFFICIAL",
            "validator": "qa_lab.qa_coverage.seal_intent_harness_failure",
            "schema_version": 1,
        },
    )

    output = daily_module.render_daily_lab(store=tmp_path, lab_css="", day="2026-09-04")
    page = output.read_text(encoding="utf-8")

    assert "function attentionHref(item)" in page
    assert "String(item.lane||'').toLowerCase()==='intent'" in page
    assert "?cell=${encodeURIComponent(item.subject)}" in page
    assert cell in page


def test_today_documents_qal_ticket_and_cross_project_linking_rules(daily_module: ModuleType, tmp_path: Path) -> None:
    output = daily_module.render_daily_lab(store=tmp_path, lab_css="", day="2026-09-04")
    page = output.read_text(encoding="utf-8")

    assert "QA finding ownership" in page
    assert "Create the QA Lab record" in page
    assert "Search every Almanak project" in page
    assert "QAL Fix Agent" in page
    assert "Project boundary" in page
    assert "currently use <code>ALM-…</code>" in page
    assert "https://linear.app/almanak/project/qa-lab-f8b9efc0b0cc" in page


def test_torn_ledger_line_makes_today_unavailable(daily_module: ModuleType, tmp_path: Path) -> None:
    index = tmp_path / "index"
    index.mkdir(parents=True)
    good = json.dumps(_record("demo", "2026-08-31", {"a": "PASS"}))
    (index / "experiment_runs.jsonl").write_text(f"{good}\n{{partial", encoding="utf-8")
    with pytest.raises(ValueError, match="malformed JSON"):
        daily_module.read_ledger(tmp_path)
    page = daily_module.render_daily_lab(store=tmp_path, lab_css="").read_text()
    assert "Data mode: UNAVAILABLE" in page
    assert "Today is unavailable" in page
    assert "function renderLanes" not in page


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


def test_today_empty_store_has_fixed_provenance_header(daily_module: ModuleType, tmp_path: Path) -> None:
    import hashlib

    output = daily_module.render_daily_lab(store=tmp_path, lab_css="", day="2026-09-04")
    assert output.name == "index.html"
    page = output.read_text()
    assert "Data mode: REAL" in page
    assert "Generated (UTC)" in page
    assert "2026-09-04T00:00:00Z" in page
    assert hashlib.sha256(b"").hexdigest() in page
    assert "position:sticky" in page
    assert "Historical sealed" in page and "Current-catalog eligible" in page


def test_specimen_payload_cannot_replace_today(daily_module: ModuleType, tmp_path: Path, monkeypatch) -> None:
    report = daily_module.build_daily_report(store=tmp_path)
    report["specimen"] = True
    monkeypatch.setattr(daily_module, "build_daily_report", lambda **kwargs: report)
    out = tmp_path / "lab" / "index.html"
    out.parent.mkdir()
    out.write_text("old specimen")
    daily_module.render_daily_lab(store=tmp_path, lab_css="")
    assert "Data mode: UNAVAILABLE" in out.read_text()
    assert "old specimen" not in out.read_text()


def test_full_lab_failure_replaces_stale_today_with_unavailable(tmp_path: Path) -> None:
    from qa_lab.qa_coverage import render_lab

    (tmp_path / "index").mkdir()
    (tmp_path / "index" / "experiment_runs.jsonl").write_text("{broken")
    (tmp_path / "lab").mkdir()
    today = tmp_path / "lab" / "index.html"
    today.write_text("SPECIMEN old route")
    guide = today.parent / "home.html"
    guide.write_text("STATIC GUIDE")
    immutable = tmp_path / "intents/run/report.html"
    immutable.parent.mkdir(parents=True)
    immutable.write_text("SEALED REPORT")
    custom = tmp_path / "custom-board.html"
    custom.write_text("STALE CUSTOM GREEN")
    for name in ("calendar.html", "intent.html", "evidence.html"):
        (today.parent / name).write_text("STALE GREEN")
    with pytest.raises(ValueError, match="history JSON"):
        render_lab(store=tmp_path, output=custom)
    assert "Data mode: UNAVAILABLE" in custom.read_text()
    assert guide.read_text() == "STATIC GUIDE"
    assert immutable.read_text() == "SEALED REPORT"
    assert "Data mode: UNAVAILABLE" in today.read_text()
    assert "SPECIMEN old route" not in today.read_text()
    for name in ("calendar.html", "intent.html", "evidence.html"):
        page = (today.parent / name).read_text()
        assert "Data mode: UNAVAILABLE" in page
        assert "STALE GREEN" not in page
        assert 'href="home.html"' in page


def test_lifecycle_acceptance_is_not_daily_product_activity(daily_module, tmp_path):
    _ledger(
        tmp_path,
        [
            _record("intent_lifecycle_acceptance", "2026-09-05", {"acceptance.intent.swap": "PASS"}),
            _record("intent_lifecycle_acceptance", "2026-09-05", {"acceptance.intent.fail": "FAIL"}, run="r2"),
        ],
    )
    assert daily_module.build_calendar(store=tmp_path, today="2026-09-05")["days"] == {}
    report = daily_module.build_daily_report(store=tmp_path, day="2026-09-05")
    assert report["totals"]["runs"] == 0
    assert not any(item["subject"].startswith("acceptance.") for item in report["attention"])


def test_malformed_catalog_projection_replaces_stale_real_today(daily_module, tmp_path):
    (tmp_path / "catalog").mkdir()
    (tmp_path / "catalog/intent_cells.json").write_text('{"cells":[null]}')
    out = tmp_path / "lab/index.html"
    out.parent.mkdir()
    out.write_text("Data mode: REAL -- old snapshot")
    daily_module.render_daily_lab(store=tmp_path, lab_css="", day="2026-09-05")
    assert "Data mode: UNAVAILABLE" in out.read_text()
    assert "old snapshot" not in out.read_text()


def test_full_lab_projection_error_replaces_stale_today(tmp_path, monkeypatch):
    from qa_lab import qa_coverage

    out = tmp_path / "lab/index.html"
    out.parent.mkdir()
    out.write_text("Data mode: REAL -- old snapshot")

    def broken_projection(**kwargs):
        raise TypeError("invalid catalog projection")

    monkeypatch.setattr(qa_coverage, "build_accounting_catalog", broken_projection)
    with pytest.raises(TypeError, match="invalid catalog projection"):
        qa_coverage.render_lab(store=tmp_path)
    assert "Data mode: UNAVAILABLE" in out.read_text()
    assert "old snapshot" not in out.read_text()


def test_daily_uses_one_historical_evaluation_time(daily_module, tmp_path, monkeypatch):
    from datetime import UTC, datetime

    from qa_lab import qa_canary, qa_eligibility

    measured = {}

    def canary_health(**kwargs):
        measured["canaries"] = kwargs["now"]
        return {}

    def eligibility(store, catalog, latest, now=None):
        measured["eligibility"] = now
        return {}

    monkeypatch.setattr(qa_canary, "health", canary_health)
    monkeypatch.setattr(qa_eligibility, "intent_eligibility", eligibility)
    daily_module.build_daily_report(store=tmp_path, day="2026-09-04")
    assert measured["canaries"] == measured["eligibility"] == datetime(2026, 9, 4, 23, 59, 59, 999999, tzinfo=UTC)


@pytest.mark.parametrize("ledger", ["{broken", '{"surface":"intent","cell_verdicts":{"cell":"PASS"}}\n'])
def test_calendar_replaces_stale_claims_when_history_is_not_verified(daily_module, tmp_path, ledger):
    index = tmp_path / "index"
    index.mkdir()
    (index / "experiment_runs.jsonl").write_text(ledger)
    output = tmp_path / "lab/calendar.html"
    output.parent.mkdir()
    output.write_text("STALE GREEN")
    assert daily_module.render_calendar_lab(store=tmp_path, lab_css="") == output
    page = output.read_text()
    assert "Calendar is unavailable" in page
    assert "Data mode: UNAVAILABLE" in page
    assert "STALE GREEN" not in page


@pytest.fixture
def verified_cli_store(tmp_path):
    from qa_lab.qa_history import append_experiment

    run = tmp_path / "runs/cli-proof"
    run.mkdir(parents=True)
    sdk = {"commit": "a" * 40, "branch": "test", "dirty": False, "sdk_version": "test", "source": "executing-worktree"}
    manifest = run / "manifest.json"
    manifest.write_text(json.dumps({"run_id": "cli-proof", "sdk": sdk}))
    append_experiment(
        store=tmp_path,
        surface="demo",
        run_id="cli-proof",
        run_dir=run,
        manifest_path=manifest,
        sdk=sdk,
        cell_verdicts={"demo.cli": "PASS"},
        started_at="2026-09-04T12:00:00Z",
        completed_at="2026-09-04T12:01:00Z",
        sealed_at="2026-09-04T12:01:00Z",
        catalog_sha256="c" * 64,
    )
    return tmp_path


@pytest.mark.parametrize("command", ["report", "calendar"])
@pytest.mark.parametrize("damage", ["invalid_hash", "missing_artifact", "changed_ledger"])
def test_json_cli_emits_nothing_for_unverified_snapshot(
    daily_module, verified_cli_store, command, damage, monkeypatch, capsys
):
    store = verified_cli_store
    ledger = store / "index/experiment_runs.jsonl"
    if damage == "invalid_hash":
        record = json.loads(ledger.read_text())
        record["cell_verdicts"]["demo.cli"] = "FAIL"
        ledger.write_text(json.dumps(record) + "\n")
    elif damage == "missing_artifact":
        (store / "runs/cli-proof/manifest.json").unlink()
    else:
        name = "build_daily_report" if command == "report" else "build_calendar"
        original = getattr(daily_module, name)

        def mutate_during_projection(**kwargs):
            payload = original(**kwargs)
            with ledger.open("a") as stream:
                stream.write("\n")
            return payload

        monkeypatch.setattr(daily_module, name, mutate_during_projection)
    with pytest.raises(ValueError):
        daily_module.main(["--store", str(store), "--day", "2026-09-04", command])
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize("command", ["report", "calendar"])
def test_json_cli_publishes_verified_snapshot(daily_module, verified_cli_store, command, capsys):
    from qa_lab.qa_history import verify_history

    store = verified_cli_store
    assert daily_module.main(["--store", str(store), "--day", "2026-09-04", command]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ledger_sha256"] == verify_history(store)["ledger_sha256"]
    if command == "report":
        assert payload["data_mode"] == "real"
        assert payload["totals"]["runs"] == 1
    else:
        assert payload["days"]["2026-09-04"]["runs"] == 1
