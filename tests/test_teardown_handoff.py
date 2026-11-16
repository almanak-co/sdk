"""`almanak teardown` on a latched workspace, through the relaunch, setup and control.

Only the external boundaries are replaced: the supervisor process the CLI launches runs
in this process behind its real control server, the gateway is a fake that records the
teardown request, and coordinator spawns are recorded instead of started.
"""

import json
import threading
import time
from http.server import HTTPServer
from types import SimpleNamespace

import pytest

from almanak import workspace
from almanak.supervisor import handler
from tests.test_dx_supervision import supervisor as supervisor  # noqa: F401 (fixture)

LATCH = {
    "error": "execution_failed",
    "message": 'recorded execution failure (Error("Price slippage check")) blocks new '
    "exposure; request teardown",
}


class Alive:
    """The launched supervisor process; it keeps running for the whole command."""

    def poll(self):
        return None


def latched(supervisor, monkeypatch):
    """A retained workspace whose last coordinator stopped on a recorded revert."""
    root = supervisor.root
    record = {"mode": "running"}
    monkeypatch.setattr("almanak.reports.retained", lambda _: dict(record))
    supervisor.deployment.mkdir(parents=True)
    (supervisor.deployment / "deployment.json").write_text("{}")
    (root / "state.sqlite").touch()
    (root / "logs" / "coordinator.log").write_text(json.dumps(LATCH) + "\n")
    # The previous pod's supervisor is gone; its control URL no longer answers.
    supervisor.persist(
        stage="running",
        provisioned=True,
        configuration="prepared",
        processes={},
        coordinator_log_offset=0,
        gateway_url="http://127.0.0.1:1",
        control_url="http://127.0.0.1:1",
    )
    status = supervisor.status()
    assert status["stage"] == "failed" and status["failure"] == "teardown_required"
    return record


def boundaries(supervisor, monkeypatch, record):
    """Fake the process launch, the gateway and coordinator spawns; record their order."""
    events, live, servers = [], set(), []
    monkeypatch.setattr(supervisor, "prepared", lambda: True)
    monkeypatch.setattr("almanak.supervisor_preparation.provision", lambda _: None)
    monkeypatch.setattr(supervisor, "start_gateway", lambda: None)
    monkeypatch.setattr("almanak.supervisor.running", lambda r: bool(r) and r["pid"] in live)

    def admission():
        events.append(("admission", supervisor.state["stage"]))
        return True

    def teardown():
        events.append(("gateway_teardown", record["mode"]))
        record["mode"] = "teardown"

    def spawn(root, name, args):
        events.append(("spawn", args[1], record["mode"]))
        live.add(len(events))
        return {"pid": len(events)}

    def launch(root, **options):
        # `python -m almanak.supervisor --run ROOT [flags]`, as `supervisor.main` runs it.
        events.append(("launch", options))
        assert options["teardown"] and not options["resume"]
        supervisor.start_teardown_setup()
        server = HTTPServer(("127.0.0.1", 0), handler(supervisor))
        supervisor.server = server
        threading.Thread(target=server.serve_forever, daemon=True).start()
        servers.append(server)
        supervisor.persist(control_url=f"http://127.0.0.1:{server.server_port}")
        return Alive()

    monkeypatch.setattr(supervisor, "admission", admission)
    monkeypatch.setattr(
        supervisor, "client", lambda: SimpleNamespace(teardown=teardown, close=lambda: None)
    )
    monkeypatch.setattr("almanak.supervisor.spawn", spawn)
    monkeypatch.setattr(workspace, "launch", launch)
    return events, servers


def teardown_command(root):
    return workspace.command(SimpleNamespace(product="teardown", run=root, format="json"))


def test_teardown_of_a_latched_workspace_applies_teardown_before_any_coordinator(
    supervisor, monkeypatch, capsys
):
    record = latched(supervisor, monkeypatch)
    events, servers = boundaries(supervisor, monkeypatch, record)
    observed = []
    original = supervisor.setup

    def setup(**options):
        # What a host polling the relaunched supervisor sees while its setup runs.
        deadline = time.monotonic() + 5
        while supervisor.state.get("control_url") == "http://127.0.0.1:1":
            assert time.monotonic() < deadline
            time.sleep(0.01)
        observed.append(workspace.request(supervisor.root, "status"))
        return original(**options)

    monkeypatch.setattr(supervisor, "setup", setup)
    try:
        assert teardown_command(supervisor.root) == 0
    finally:
        for server in servers:
            server.shutdown()
            server.server_close()
    ((during,),) = [observed]
    assert during["stage"] == "provisioned" and during["failure"] is None
    assert during["provisioning"]
    assert [event[0] for event in events] == [
        "launch",
        "admission",
        "gateway_teardown",
        "admission",
        "spawn",
    ]
    # Setup never resumed or started execution: the only coordinator is the one the
    # teardown control started, on the retained journal and in teardown mode.
    assert events[1] == ("admission", "provisioned")
    assert events[2] == ("gateway_teardown", "running")
    assert events[-1] == ("spawn", "resume", "teardown")
    out = capsys.readouterr().out
    printed = json.loads(out[out.index("\n{") + 1 :])
    assert printed["stage"] == "running" and printed["coordinator"] == "running"
    assert printed["recorded"] == {"mode": "teardown"}


def test_teardown_of_a_workspace_that_never_executed_is_refused(supervisor, monkeypatch):
    record = latched(supervisor, monkeypatch)
    (supervisor.root / "state.sqlite").unlink()
    events, servers = boundaries(supervisor, monkeypatch, record)
    try:
        with pytest.raises(ValueError, match="nothing to tear down"):
            teardown_command(supervisor.root)
    finally:
        for server in servers:
            server.shutdown()
            server.server_close()
    assert [event[0] for event in events] == ["launch"]
    assert supervisor.state["stage"] == "failed"
    assert "nothing to tear down" in supervisor.state["diagnostic"]
