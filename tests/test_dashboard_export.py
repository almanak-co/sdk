"""The supervised runtime retains dashboard inputs without owning presentation."""

import json
from types import SimpleNamespace

import pytest

from almanak import cli
from almanak.cli_arguments import build_parser
from almanak.dashboard.export import export_input, read_revision
from almanak.runtime import Runtime
from almanak.state import Journal


def test_service_start_and_resume_parse_dashboard_export(tmp_path):
    common = [
        "--gateway-url",
        "http://127.0.0.1:1",
        "--gateway-auth-file",
        str(tmp_path / "auth"),
        "--export",
        str(tmp_path / "dashboard"),
    ]
    start = build_parser().parse_args(
        [
            "service",
            "start",
            *common,
            "--config",
            str(tmp_path / "deployment.json"),
            "--strategy",
            str(tmp_path / "strategy.py"),
        ]
    )
    resume = build_parser().parse_args(["service", "resume", *common])
    assert start.export == resume.export == tmp_path / "dashboard"


def test_parsed_service_start_passes_export_to_runtime(tmp_path, monkeypatch):
    export = tmp_path / "dashboard"
    config = tmp_path / "deployment.json"
    strategy = tmp_path / "strategy.py"
    config.write_text("{}")
    strategy.write_text("")
    args = build_parser().parse_args(
        [
            "service",
            "start",
            "--gateway-url",
            "http://127.0.0.1:1",
            "--gateway-auth-file",
            str(tmp_path / "auth"),
            "--export",
            str(export),
            "--config",
            str(config),
            "--strategy",
            str(strategy),
        ]
    )
    seen = {}

    class FakeRuntime:
        def __init__(self, *unused, **kwargs):
            seen.update(kwargs)
            self.journal = SimpleNamespace(load=lambda: SimpleNamespace(deployment_id="dep-1"))

        def run(self, *, interrupt):
            interrupt()
            export.mkdir()
            (export / "input.json").write_text("{}")
            return SimpleNamespace()

    monkeypatch.setattr("almanak.runtime.Runtime", FakeRuntime)
    monkeypatch.setattr("almanak.connectors.composition.configuration_from_json", lambda _: {})
    monkeypatch.setattr("almanak.strategy.StrategyDefinition.read", lambda _: object())
    cli.coordinated_lifecycle(args, lambda: None)
    assert seen["export"] == export
    assert (export / "input.json").exists()


def test_dashboard_input_is_retained_with_monotonic_revisions(
    tmp_path, recorded, reference_strategy
):
    state = recorded.model_copy(update={"strategy": reference_strategy})
    directory = tmp_path / "dashboard"
    first = export_input(directory, state, captured_at=42)
    second = export_input(directory, state, captured_at=43)
    assert (first["revision"], second["revision"]) == (1, 2)
    stored = json.loads((directory / "input.json").read_text())
    assert stored["revision"] == 2 and stored["snapshot"]["record_hash"] == stored["record_hash"]
    assert "raw_transaction" not in json.dumps(stored)
    assert read_revision(tmp_path / "missing.json") == 0


def test_an_unchanged_record_is_not_exported_again_until_the_refresh(
    tmp_path, recorded, reference_strategy, monkeypatch
):
    from almanak.dashboard import export

    now = [1000.0]
    monkeypatch.setattr(export.time, "monotonic", lambda: now[0])
    state = recorded.model_copy(update={"strategy": reference_strategy})
    directory = tmp_path / "dashboard"
    assert export_input(directory, state)["revision"] == 1
    # The record is unchanged: no write, so a watcher of the file sees no change.
    now[0] += export.UNCHANGED_REFRESH_SECONDS - 1
    assert export_input(directory, state) is None
    assert read_revision(directory / "input.json") == 1
    # The multi-chain scheduler cursor is not part of the displayed record.
    moved = state.model_copy(update={"scheduler_cursor": state.scheduler_cursor + 1})
    assert export_input(directory, moved) is None
    changed = state.model_copy(update={"mode": "teardown"})
    assert export_input(directory, changed)["revision"] == 2
    # Past the refresh interval an unchanged record is captured again.
    now[0] += export.UNCHANGED_REFRESH_SECONDS
    assert export_input(directory, changed)["revision"] == 3


def test_the_last_running_input_survives_the_teardown_that_follows(
    tmp_path, recorded, reference_strategy
):
    from almanak.dashboard.export import RUNNING_INPUT

    directory = tmp_path / "dashboard"
    running = recorded.model_copy(update={"strategy": reference_strategy, "mode": "running"})
    export_input(directory, running, captured_at=42)
    export_input(directory, running.model_copy(update={"mode": "closed"}), captured_at=43)
    kept = json.loads((directory / RUNNING_INPUT).read_text())
    assert kept["snapshot"]["mode"] == "running" and kept["captured_at"] == 42
    assert json.loads((directory / "input.json").read_text())["snapshot"]["mode"] == "closed"


def test_runtime_export_failure_is_reported_and_never_fatal(
    tmp_path, recorded, reference_strategy, capsys
):
    runtime = Runtime.__new__(Runtime)
    runtime.export = tmp_path / "dashboard"
    journal = Journal(tmp_path / "state.sqlite")
    journal.create(recorded.config, reference_strategy)
    runtime.journal = journal
    runtime.retain_dashboard_input()
    assert (runtime.export / "input.json").exists()
    runtime.journal = SimpleNamespace(load=lambda: (_ for _ in ()).throw(RuntimeError("boom")))
    runtime.retain_dashboard_input()
    assert json.loads(capsys.readouterr().err.strip()) == {
        "event": "dashboard_export_failed",
        "reason": "RuntimeError",
    }


@pytest.mark.parametrize("export_fails", [False, True])
def test_run_retains_committed_state_when_tick_raises(
    tmp_path, recorded, reference_strategy, monkeypatch, capsys, export_fails
):
    runtime = Runtime.__new__(Runtime)
    runtime.export = tmp_path / "dashboard"
    runtime.journal = Journal(tmp_path / "state.sqlite")
    runtime.journal.create(recorded.config, reference_strategy)
    runtime.closed = False
    closed = []
    runtime.gateway = SimpleNamespace(close=lambda: closed.append(True))
    runtime.retain_dashboard_input()
    previous = json.loads((runtime.export / "input.json").read_text())
    failure = RuntimeError("execution failed after committing")

    def commit_then_fail():
        runtime.journal.update(lambda state: state.model_copy(update={"mode": "teardown"}))
        raise failure

    def refuse_export(*args, **kwargs):
        raise OSError("dashboard storage unavailable")

    monkeypatch.setattr(runtime, "tick", commit_then_fail)
    if export_fails:
        monkeypatch.setattr("almanak.dashboard.export.export_input", refuse_export)
    with pytest.raises(RuntimeError) as caught:
        runtime.run()
    assert caught.value is failure
    assert runtime.closed and closed == [True]
    assert runtime.journal.load().mode == "teardown"
    retained = json.loads((runtime.export / "input.json").read_text())
    if export_fails:
        assert retained == previous
        assert json.loads(capsys.readouterr().err.strip()) == {
            "event": "dashboard_export_failed",
            "reason": "OSError",
        }
    else:
        assert retained["revision"] == previous["revision"] + 1
        assert retained["record_hash"] != previous["record_hash"]
        assert retained["snapshot"]["mode"] == "teardown"


def test_retained_reports_an_unchanged_checkpoint_without_projecting_it_again(
    tmp_path, recorded, reference_strategy, monkeypatch
):
    import sqlite3

    from almanak import reporting, reports

    calls = []
    original = reporting.report

    def counted(state):
        calls.append(state)
        return original(state)

    monkeypatch.setattr(reporting, "report", counted)
    monkeypatch.setattr(reports, "_last_report", None)
    state = recorded.model_copy(update={"strategy": reference_strategy})

    def commit(checkpoint):
        with sqlite3.connect(tmp_path / "state.sqlite") as db:
            db.execute("CREATE TABLE IF NOT EXISTS checkpoint (id INTEGER PRIMARY KEY, body TEXT)")
            db.execute(
                "INSERT OR REPLACE INTO checkpoint VALUES (1, ?)", (checkpoint.model_dump_json(),)
            )

    commit(state)
    first = reports.retained(tmp_path)
    # A caller may change its copy; the next status still reports the checkpoint.
    first["mode"] = "changed by a caller"
    second = reports.retained(tmp_path)
    assert len(calls) == 1 and second["mode"] == state.mode
    commit(state.model_copy(update={"mode": "teardown"}))
    assert reports.retained(tmp_path)["mode"] == "teardown" and len(calls) == 2
