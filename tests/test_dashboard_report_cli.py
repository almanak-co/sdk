"""The supported report command exports connector-specific state without mutation."""

import json
import sqlite3
import sys

import httpx
import pytest

from almanak.cli import main
from tests.test_dx_reports import retain
from tests.uniswap_support import live_state


@pytest.mark.parametrize("family", ("aave", "lp"))
@pytest.mark.parametrize("format_", ("json", "html"))
def test_report_cli_keeps_connector_scope_and_database_unchanged(
    tmp_path, recorded, monkeypatch, family, format_
):
    def forbidden(*_, **__):
        raise AssertionError("offline report attempted a provider request")

    monkeypatch.setattr(httpx.Client, "send", forbidden)
    state = recorded if family == "aave" else live_state("lp")
    retain(tmp_path, state)
    before = (tmp_path / "state.sqlite").read_bytes()
    output = tmp_path / ("report." + format_)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "almanak",
            "report",
            "--run",
            str(tmp_path),
            "--format",
            format_,
            "--output",
            str(output),
        ],
    )
    assert main() == 0
    rendered = output.read_text()
    assert "eip155:" in rendered and "gas" in rendered
    assert (tmp_path / "state.sqlite").read_bytes() == before
    if format_ == "json":
        assert json.loads(rendered)["evidence"] == "Recorded evidence only; no fresh chain audit"
    else:
        assert "Strategy report" in rendered


def test_report_cli_refuses_unknown_connector_without_creating_output(tmp_path, monkeypatch):
    state = live_state("lp").model_dump(mode="json")
    state["config"]["connector"] = "unknown_connector"
    with sqlite3.connect(tmp_path / "state.sqlite") as connection:
        connection.execute("CREATE TABLE checkpoint(id INTEGER PRIMARY KEY, body TEXT)")
        connection.execute("INSERT INTO checkpoint VALUES(1, ?)", (json.dumps(state),))
    before = (tmp_path / "state.sqlite").read_bytes()
    output = tmp_path / "report.json"
    monkeypatch.setattr(
        sys,
        "argv",
        ["almanak", "report", "--run", str(tmp_path), "--json", "--output", str(output)],
    )
    assert main() == 1
    assert not output.exists()
    assert (tmp_path / "state.sqlite").read_bytes() == before


def test_report_dashboard_draws_a_fork_test_from_its_last_running_input(
    tmp_path, recorded, reference_strategy, monkeypatch, capsys
):
    from almanak.dashboard.export import export_input

    running = recorded.model_copy(update={"strategy": reference_strategy, "mode": "running"})
    closed = running.model_copy(update={"mode": "closed"})
    retain(tmp_path, closed)
    export_input(tmp_path / "dashboard", running, captured_at=42)
    export_input(tmp_path / "dashboard", closed, captured_at=43)
    monkeypatch.setattr(
        sys, "argv", ["almanak", "report", "--run", str(tmp_path), "--dashboard", "--json"]
    )
    assert main() == 0
    response = json.loads(capsys.readouterr().out)
    assert response["snapshot"]["mode"] == "running"
    assert response["status"] in ("ready", "absent")


def test_report_dashboard_draws_the_workspaces_current_dashboard_over_the_run(
    tmp_path, recorded, reference_strategy, monkeypatch, capsys
):
    from almanak.dashboard.export import export_input

    run, workspace = tmp_path / "run", tmp_path / "ws"
    run.mkdir()
    workspace.mkdir()
    running = recorded.model_copy(update={"strategy": reference_strategy, "mode": "running"})
    retain(run, running)
    export_input(run / "dashboard", running, captured_at=42)
    # The reference keeps its hook in strategy.py; this workspace has moved it to dashboard.py.
    source = reference_strategy.source.replace("def dashboard(", "def retired_dashboard(")
    (workspace / "strategy.py").write_text(source)
    (workspace / "dashboard.py").write_text(
        "from almanak import dashboard as ui\n\n\n"
        "def dashboard(context):\n"
        "    return ui.Dashboard(title='Edited', blocks=(ui.Text(id='note', text='new'),))\n"
    )
    argv = ["almanak", "report", "--run", str(run), "--dashboard", "--json"]
    monkeypatch.setattr(sys, "argv", [*argv, "--workspace", str(workspace)])
    assert main() == 0
    response = json.loads(capsys.readouterr().out)
    assert response["status"] == "ready"
    assert response["document"]["title"] == "Edited"
    assert response["snapshot"]["mode"] == "running"


def test_report_dashboard_writes_its_response_to_output(
    tmp_path, recorded, reference_strategy, monkeypatch, capsys
):
    from almanak.dashboard.export import export_input

    running = recorded.model_copy(update={"strategy": reference_strategy, "mode": "running"})
    retain(tmp_path, running)
    export_input(tmp_path / "dashboard", running, captured_at=42)
    output = tmp_path / "dashboard.json"
    argv = ["almanak", "report", "--run", str(tmp_path), "--dashboard", "--output", str(output)]
    monkeypatch.setattr(sys, "argv", argv)
    assert main() == 0
    assert json.loads(output.read_text())["snapshot"]["mode"] == "running"
    assert capsys.readouterr().out == ""


def test_a_workspace_dashboard_over_the_checkpoint_keeps_the_runs_identity(
    tmp_path, recorded, reference_strategy, monkeypatch, capsys
):
    run, workspace = tmp_path / "run", tmp_path / "ws"
    run.mkdir()
    workspace.mkdir()
    retain(run, recorded.model_copy(update={"strategy": reference_strategy}))
    source = reference_strategy.source.replace("def dashboard(", "def retired_dashboard(")
    (workspace / "strategy.py").write_text(source)
    (workspace / "dashboard.py").write_text(
        "from almanak import dashboard as ui\n\n\n"
        "def dashboard(context):\n"
        "    return ui.Dashboard(title='Edited', blocks=(ui.Text(id='note', text='new'),))\n"
    )
    base = ["almanak", "report", "--run", str(run), "--dashboard", "--json"]
    monkeypatch.setattr(sys, "argv", base)
    assert main() == 0
    captured = json.loads(capsys.readouterr().out)["snapshot"]
    monkeypatch.setattr(sys, "argv", [*base, "--workspace", str(workspace)])
    assert main() == 0
    edited = json.loads(capsys.readouterr().out)
    assert edited["document"]["title"] == "Edited"
    assert edited["snapshot"]["strategy_hash"] == captured["strategy_hash"]
    assert edited["snapshot"]["record_hash"] == captured["record_hash"]
