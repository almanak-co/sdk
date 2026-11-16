"""Supervision authority, control authentication and lifecycle failure semantics."""

import json
import os
import subprocess
import sys
import threading
import time
from http.server import HTTPServer
from types import SimpleNamespace

import httpx
import pytest

from almanak import supervisor_process, workspace
from almanak.gateway.ethereum import DataError
from almanak.supervisor import Supervisor, handler
from tests.test_operator_fixture import anchors as anchors
from tests.test_operator_fixture import context as context
from tests.test_operator_fixture import fixture as fixture
from tests.test_operator_fixture import operator as operator
from tests.test_operator_fixture import original as original


@pytest.fixture
def supervisor(tmp_path, monkeypatch):
    (tmp_path / "private").mkdir()
    (tmp_path / "logs").mkdir()
    (tmp_path / "control.token").write_text("a" * 64)
    (tmp_path / "control.token").chmod(0o600)
    workspace.save(
        tmp_path / "workspace.json",
        {
            "version": 1,
            "identity": "workspace-test",
            "mode": "fork",
            "stage": "created",
            "credentials": {"env_file": "/private/env"},
            "processes": {},
        },
    )
    selected = Supervisor(tmp_path)
    monkeypatch.setattr("almanak.reports.retained", lambda _: {"mode": "running"})
    yield selected
    selected.lock.close()


def test_process_identity_refuses_pid_reuse_and_acknowledges_pause():
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import sys, time; sys.stdout.write('.'); sys.stdout.flush(); time.sleep(30)",
        ],
        stdout=subprocess.PIPE,
    )
    try:
        # The command line of a child is visible only once its exec has completed.
        assert process.stdout is not None and process.stdout.read(1) == b"."
        record = {"pid": process.pid, "identity": supervisor_process.identity(process.pid)}
        assert record["identity"]["command"]
        assert supervisor_process.running(record)
        changed = {**record, "identity": {"started": "another process"}}
        supervisor_process.stop(changed)
        assert process.poll() is None
        assert not supervisor_process.running({"pid": os.getpid(), "identity": None})
        supervisor_process.stop(record)
        assert not supervisor_process.running(record)
    finally:
        if process.poll() is None:
            process.terminate()
        process.wait(timeout=5)


def test_workspace_lock_refuses_competing_supervisors(supervisor, monkeypatch):
    monkeypatch.setattr(workspace, "ACQUIRE_SECONDS", 0.1)
    with pytest.raises(RuntimeError, match="already has a supervisor"):
        workspace.acquire(supervisor.root)


def test_a_starting_supervisor_outwaits_a_shutdown_probe(supervisor):
    import fcntl

    supervisor.lock.close()
    probe = (supervisor.root / "supervisor.lock").open("a+")
    fcntl.flock(probe, fcntl.LOCK_SH | fcntl.LOCK_NB)
    threading.Timer(0.2, probe.close).start()
    workspace.acquire(supervisor.root).close()
    assert probe.closed


def test_authenticated_control_requires_workspace_and_bearer_identity(supervisor):
    with HTTPServer(("127.0.0.1", 0), handler(supervisor)) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        supervisor.persist(control_url=f"http://127.0.0.1:{server.server_port}")
        try:
            assert workspace.request(supervisor.root, "status")["supervisor"] == "available"
            url = supervisor.state["control_url"] + "/control"
            assert httpx.post(url, json={"action": "shutdown"}).status_code == 403
            response = httpx.post(
                url,
                headers={"Authorization": "Bearer " + "a" * 64},
                json={"identity": "other", "action": "shutdown"},
            )
            assert "identity mismatch" in response.json()["error"]
            with pytest.raises(ValueError, match="unknown workspace control"):
                workspace.request(supervisor.root, "arbitrary")
        finally:
            server.shutdown()
            thread.join()


def test_pause_during_provisioning_prevents_coordinator_admission(supervisor):
    supervisor.setup_active = True
    response = supervisor.control("pause", False)
    assert response["stage"] == "paused" and supervisor.state["pause_requested"]
    supervisor.start_coordinator(create=True)
    assert "coordinator" not in supervisor.state["processes"]
    with pytest.raises(ValueError, match="provisioning is active"):
        supervisor.control("resume", False)


def gated_admission(supervisor, monkeypatch, outcome=True):
    """Hold admission until released, as production admission holds for minutes."""
    entered, release, commands = threading.Event(), threading.Event(), []

    def admission():
        entered.set()
        assert release.wait(10)
        if isinstance(outcome, Exception):
            raise outcome
        if not outcome:
            supervisor.persist(stage="action_required", diagnostic="install permissions")
        return outcome

    supervisor.persist(provisioned=True, stage="action_required", diagnostic="install")
    supervisor.state["gateway_url"] = "http://127.0.0.1:1"
    monkeypatch.setattr(supervisor, "admission", admission)
    monkeypatch.setattr(supervisor, "start_gateway", lambda: None)
    monkeypatch.setattr("almanak.supervisor.running", lambda record: record is not None)
    monkeypatch.setattr(
        "almanak.supervisor.spawn",
        lambda root, name, command: commands.append(command) or {"pid": 1},
    )
    return entered, release, commands


def settled(supervisor):
    deadline = time.monotonic() + 10
    while supervisor.setup_active and time.monotonic() < deadline:
        time.sleep(0.01)
    return not supervisor.setup_active


def test_provisioned_resume_admits_in_the_background_while_status_answers(supervisor, monkeypatch):
    entered, release, commands = gated_admission(supervisor, monkeypatch)
    with HTTPServer(("127.0.0.1", 0), handler(supervisor)) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        supervisor.persist(control_url=f"http://127.0.0.1:{server.server_port}")
        try:
            accepted = workspace.request(supervisor.root, "resume", password="unlock")
            assert entered.wait(5) and not release.is_set()
            assert accepted["provisioning"] and accepted["stage"] == "provisioned"
            assert accepted["diagnostic"] is None
            status = workspace.request(supervisor.root, "status")
            assert status["supervisor"] == "available" and status["provisioning"]
            with pytest.raises(ValueError, match="provisioning is active"):
                workspace.request(supervisor.root, "resume")
            assert commands == []
        finally:
            release.set()
            assert settled(supervisor)
            server.shutdown()
            thread.join()
    assert supervisor.state["stage"] == "running" and supervisor.password is None
    assert [command[1] for command in commands] == ["start"]


def test_background_resume_failure_persists_failed_stage_with_diagnostic(supervisor, monkeypatch):
    _, release, commands = gated_admission(
        supervisor, monkeypatch, RuntimeError("roles history read failed")
    )
    supervisor.control("resume", False)
    release.set()
    assert settled(supervisor)
    assert supervisor.state["stage"] == "failed"
    assert supervisor.state["diagnostic"] == "roles history read failed"
    assert commands == [] and not supervisor.status()["provisioning"]
    # No coordinator ran, so a host may launch again as any restart would.
    assert supervisor.status()["failure"] == "start"


def test_background_resume_keeps_action_required_and_honors_a_pause(supervisor, monkeypatch):
    _, release, commands = gated_admission(supervisor, monkeypatch, outcome=False)
    supervisor.control("resume", False)
    release.set()
    assert settled(supervisor)
    assert supervisor.state["stage"] == "action_required" and commands == []
    entered, release, commands = gated_admission(supervisor, monkeypatch)
    supervisor.control("resume", False)
    assert entered.wait(5)
    assert supervisor.control("pause", False)["stage"] == "paused"
    release.set()
    assert settled(supervisor)
    assert supervisor.state["stage"] == "paused" and commands == []


def test_first_start_and_resume_each_run_admission_once(supervisor, monkeypatch):
    _, release, commands = gated_admission(supervisor, monkeypatch)
    release.set()
    admissions = []
    monkeypatch.setattr(supervisor, "admission", lambda: admissions.append(True) or True)
    monkeypatch.setattr(supervisor, "prepared", lambda: True)
    monkeypatch.setattr("almanak.supervisor_preparation.provision", lambda _: None)
    (supervisor.deployment).mkdir(parents=True)
    (supervisor.deployment / "deployment.json").write_text("{}")
    supervisor.setup()
    assert len(admissions) == 1 and [command[1] for command in commands] == ["start"]
    supervisor.persist(stage="paused", processes={})
    supervisor.control("resume", False)
    assert settled(supervisor)
    assert len(admissions) == 2 and supervisor.state["stage"] == "running"


def test_resume_of_a_running_coordinator_is_refused_without_changing_stage(supervisor, monkeypatch):
    gated_admission(supervisor, monkeypatch)
    supervisor.persist(stage="running", processes={"coordinator": {"pid": 1}})
    (supervisor.root / "state.sqlite").touch()
    supervisor.state["pause_requested"] = True
    with pytest.raises(ValueError, match="coordinator is already running"):
        supervisor.control("resume", False)
    assert supervisor.state["stage"] == "running" and not supervisor.setup_active
    assert supervisor.state["pause_requested"]


def test_resuming_stopped_execution_reports_provisioned_not_failed(supervisor, monkeypatch):
    # A coordinator that stopped leaves stage running; status would read that as failed.
    entered, release, commands = gated_admission(supervisor, monkeypatch)
    supervisor.persist(stage="running", diagnostic="stale", processes={})
    (supervisor.root / "state.sqlite").touch()
    accepted = supervisor.control("resume", False)
    assert entered.wait(5)
    assert accepted["stage"] == "provisioned" and accepted["diagnostic"] is None
    assert supervisor.status()["stage"] == "provisioned"
    release.set()
    assert settled(supervisor)
    assert supervisor.state["stage"] == "running"
    assert [command[1] for command in commands] == ["resume"]


def test_failed_coordinator_is_reported_without_automatic_restart(supervisor):
    supervisor.persist(stage="running", failure="start")
    response = supervisor.status()
    assert response["stage"] == "failed" and response["failure"] == "execution"
    assert "explicitly resume" in response["diagnostic"]
    assert supervisor.state["processes"] == {}


QUOTA_REFUSAL = {
    "error": "quota",
    "message": "swap action exceeds its non-refilling spending quota for "
    "eip155:8453/erc20:0x4200000000000000000000000000000000000006: requested 5 with 0 "
    "already spent of the 1 quota (1 remaining, base units)",
}


def test_stopped_coordinator_reports_its_own_refusal(supervisor, monkeypatch):
    log = supervisor.root / "logs" / "coordinator.log"
    # An earlier run's refusal stays in the appended log and must not be reported.
    log.write_text(json.dumps({"error": "gas_price", "message": "earlier run"}) + "\n")
    supervisor.state["gateway_url"] = "http://127.0.0.1:1"
    monkeypatch.setattr("almanak.supervisor.running", lambda _: False)
    monkeypatch.setattr(supervisor, "admission", lambda: True)
    monkeypatch.setattr(supervisor, "start_gateway", lambda: None)
    monkeypatch.setattr("almanak.supervisor.spawn", lambda *args: {"pid": 1})
    supervisor.start_coordinator(create=False)
    assert supervisor.state["stage"] == "running"
    with log.open("a") as stream:
        stream.write("Traceback-free progress line\n")
        stream.write(json.dumps(QUOTA_REFUSAL) + "\n")
        stream.write(json.dumps({"event": "dashboard_export_failed", "error": "x"}) + "\n")
    response = supervisor.status()
    assert response["stage"] == "failed" and response["failure"] == "execution"
    assert response["diagnostic"] == (
        f"Coordinator stopped on quota: {QUOTA_REFUSAL['message']}. Details are in "
        "logs/coordinator.log; resolve the cause, then explicitly resume."
    )
    # The supervisor's recorded reason survives it: a caller can read it offline.
    offline = workspace.status(supervisor.root)
    assert offline["supervisor"] == "unavailable"
    assert offline["stage"] == "failed" and offline["diagnostic"] == response["diagnostic"]


def test_stopped_coordinator_without_a_refusal_keeps_the_generic_diagnostic(supervisor):
    log = supervisor.root / "logs" / "coordinator.log"
    log.write_text(json.dumps({"error": "gas_price", "message": "earlier run"}) + "\n")
    supervisor.persist(stage="running", coordinator_log_offset=log.stat().st_size)
    with log.open("a") as stream:
        stream.write("Traceback (most recent call last):\n  RuntimeError: unexpected\n")
    generic = "Coordinator stopped; inspect its log and explicitly resume."
    assert supervisor.status()["diagnostic"] == generic
    # A workspace recorded before the offset existed never guesses from an old log.
    supervisor.persist(stage="running", coordinator_log_offset=None)
    assert supervisor.status()["diagnostic"] == generic


def test_a_generic_gateway_refusal_points_at_the_gateway_log(supervisor):
    log = supervisor.root / "logs" / "coordinator.log"
    supervisor.persist(stage="running", coordinator_log_offset=0)
    refused = {"error": "invalid_request", "message": "gateway request refused"}
    log.write_text(json.dumps(refused) + "\n")
    diagnostic = supervisor.status()["diagnostic"]
    assert diagnostic.startswith("Coordinator stopped on invalid_request: gateway request refused")
    assert "logs/gateway.log" in diagnostic


LATCHES = [
    {
        "error": "execution_failed",
        "message": 'recorded execution failure (Error("Price slippage check")) blocks new '
        "exposure; request teardown",
    },
    {
        "error": "accounting",
        "message": "recorded outcome has accounting issues; teardown remains available",
    },
]


@pytest.mark.parametrize("latch", LATCHES, ids=lambda latch: latch["error"])
def test_a_latched_coordinator_requires_teardown_and_accepts_it(supervisor, monkeypatch, latch):
    live, spawned, teardowns = set(), [], []
    supervisor.state["gateway_url"] = "http://127.0.0.1:1"
    (supervisor.root / "state.sqlite").touch()
    monkeypatch.setattr("almanak.supervisor.running", lambda r: r is not None and r["pid"] in live)
    monkeypatch.setattr(supervisor, "admission", lambda: True)
    monkeypatch.setattr(supervisor, "start_gateway", lambda: None)

    def spawn(root, name, args):
        spawned.append(args)
        live.add(len(spawned))
        return {"pid": len(spawned)}

    monkeypatch.setattr("almanak.supervisor.spawn", spawn)
    monkeypatch.setattr(
        supervisor,
        "client",
        lambda: SimpleNamespace(teardown=lambda: teardowns.append(1), close=lambda: None),
    )
    # A resumed coordinator stops at once on a refusal its recorded outcomes repeat.
    supervisor.start_coordinator(create=False)
    with (supervisor.root / "logs" / "coordinator.log").open("a") as log:
        log.write(json.dumps(latch) + "\n")
    live.clear()
    status = supervisor.status()
    assert status["stage"] == "failed" and status["failure"] == "teardown_required"
    assert status["next_commands"] == [f"almanak teardown --run {supervisor.root}"]
    assert status["diagnostic"].startswith(f"Coordinator stopped on {latch['error']}: ")
    assert "teardown is required" in status["diagnostic"]
    assert "explicitly resume" not in status["diagnostic"]
    offline = workspace.status(supervisor.root)
    assert offline["failure"] == "teardown_required"
    assert offline["next_commands"] == status["next_commands"]
    # The failed stage's supervisor still accepts the teardown and restarts the coordinator.
    accepted = supervisor.control("teardown", False)
    assert teardowns == [1] and [args[1] for args in spawned] == ["resume", "resume"]
    assert accepted["stage"] == "running" and accepted["failure"] is None


def test_other_coordinator_refusals_stay_execution_failures(supervisor):
    log = supervisor.root / "logs" / "coordinator.log"
    supervisor.persist(stage="running", coordinator_log_offset=0)
    log.write_text(json.dumps(QUOTA_REFUSAL) + "\n")
    status = supervisor.status()
    assert status["failure"] == "execution"
    assert status["next_commands"] == [f"almanak resume --run {supervisor.root}"]


@pytest.mark.parametrize(
    "ended",
    [
        {"event": "gas_ceiling_recovered", "attempts": 3},
        {"event": "gas_ceiling_dropped", "attempts": 3},
        {"event": "transient_read_hold", "kind": "http"},
        {"event": "strategy_worker_timeout", "kind": "wall"},
    ],
)
def test_a_running_coordinator_reports_its_fee_ceiling_hold_until_it_ends(
    supervisor, monkeypatch, ended
):
    log = supervisor.root / "logs" / "coordinator.log"
    # An earlier run's hold stays in the appended log and must not be reported.
    earlier = {"event": "gas_ceiling_hold", "kind": "gas_price", "reason": "earlier run"}
    log.write_text(json.dumps(earlier) + "\n")
    supervisor.persist(stage="running", coordinator_log_offset=log.stat().st_size)
    monkeypatch.setattr("almanak.supervisor.running", lambda _: True)
    assert supervisor.status()["diagnostic"] is None

    def hold(kind, quote):
        reason = f"quoted {quote} wei"
        with log.open("a") as stream:
            stream.write(json.dumps({"event": "gas_ceiling_hold", "kind": kind, "reason": reason}))
            stream.write("\n" + json.dumps({"event": "dashboard_export_failed"}) + "\n")
        return supervisor.status()

    response = hold("gas_price", 5)
    assert response["stage"] == "running" and response["coordinator"] == "running"
    assert response["diagnostic"] == (
        "Coordinator holding on gas_price: the market fee quote is above the gas policy's "
        "max_gas_price_wei. Nothing is signed while it holds; it quotes again on every "
        "attempt and proceeds once the quote is within the ceiling. Each quote is in "
        "logs/coordinator.log."
    )
    # The diagnostic names the ceiling, not the quote, so a new quote does not change it.
    assert hold("gas_price", 6)["diagnostic"] == response["diagnostic"]
    assert "max_transaction_fee" in hold("gas_fee_quote", 7)["diagnostic"]
    # A hold is never persisted as the workspace's diagnostic.
    assert supervisor.state.get("diagnostic") is None
    with log.open("a") as stream:
        stream.write(json.dumps(ended) + "\n")
    assert supervisor.status()["diagnostic"] is None


def test_failure_kind_is_reported_only_for_the_failed_stage(supervisor):
    # A start failure retried into a running deployment keeps the recorded kind on disk.
    supervisor.persist(stage="paused", failure="start")
    assert supervisor.status()["failure"] is None
    supervisor.persist(stage="failed")
    assert supervisor.status()["failure"] == "start"
    # Without an answering supervisor the recorded stage and kind are reported.
    unavailable = workspace.status(supervisor.root)
    assert unavailable["supervisor"] == "unavailable" and unavailable["failure"] == "start"
    supervisor.state.pop("failure")
    supervisor.persist(stage="failed")
    assert supervisor.status()["failure"] is None


def test_coordinator_start_and_resume_retain_dashboard_input(supervisor, monkeypatch):
    commands = []
    supervisor.state["gateway_url"] = "http://127.0.0.1:1"
    monkeypatch.setattr("almanak.supervisor.running", lambda _: False)
    monkeypatch.setattr(supervisor, "admission", lambda: True)
    monkeypatch.setattr(supervisor, "start_gateway", lambda: None)
    monkeypatch.setattr(
        "almanak.supervisor.spawn",
        lambda root, name, command: commands.append((root, name, command)) or {"pid": 1},
    )
    supervisor.start_coordinator(create=True)
    supervisor.start_coordinator(create=False)
    expected = ["--export", str(supervisor.root / "dashboard")]
    assert commands[0][2][6:8] == expected
    assert commands[1][2][6:8] == expected


def test_closed_coordinator_status_and_shutdown(supervisor, monkeypatch):
    monkeypatch.setattr("almanak.reports.retained", lambda _: {"mode": "closed"})
    stopped = threading.Event()
    supervisor.server = SimpleNamespace(shutdown=stopped.set)
    supervisor.persist(stage="running")
    assert supervisor.status()["stage"] == "closed"
    supervisor.shutdown(False)
    assert stopped.wait(2)
    assert supervisor.state["stage"] == "shutdown"
    assert "cannot be resumed" in supervisor.state["diagnostic"]
    # The stopped server's port can be reused; no later control may be sent to it.
    assert workspace.read(supervisor.root)["control_url"] is None
    with pytest.raises(ValueError, match="workspace is shut down"):
        workspace.request(supervisor.root, "pause")


def test_shutdown_refuses_unverified_obligations_and_force_preserves_evidence(supervisor):
    with pytest.raises(ValueError, match="remaining or unverified"):
        supervisor.shutdown(False)
    stopped = threading.Event()
    supervisor.server = SimpleNamespace(shutdown=stopped.set)
    supervisor.shutdown(True)
    assert stopped.wait(2)
    assert workspace.read(supervisor.root)["identity"] == "workspace-test"


def control_args(supervisor, product, **options):
    return SimpleNamespace(product=product, run=supervisor.root, format="json", **options)


def unreachable(self, url, **_):
    raise httpx.ConnectError("unreachable", request=httpx.Request("POST", url))


@pytest.mark.parametrize("force", [False, True])
def test_repeated_shutdown_reports_the_recorded_state(supervisor, monkeypatch, capsys, force):
    supervisor.lock.close()
    # An older supervisor left its URL on disk; another process may own that port now.
    supervisor.persist(stage="shutdown", control_url="http://127.0.0.1:1")
    monkeypatch.setattr(httpx.Client, "post", pytest.fail)
    assert workspace.command(control_args(supervisor, "shutdown", force=force)) == 0
    reported = json.loads(capsys.readouterr().out)
    assert reported["stage"] == "shutdown" and reported["supervisor"] == "unavailable"


def test_repeated_shutdown_waits_for_the_supervisor_that_shut_down_to_exit(
    supervisor, monkeypatch, capsys
):
    # The first shutdown's answer arrives before its supervisor releases the lock.
    supervisor.persist(stage="shutdown", control_url=None)
    monkeypatch.setattr(workspace, "request", pytest.fail)
    threading.Timer(0.2, supervisor.lock.close).start()
    assert workspace.command(control_args(supervisor, "shutdown", force=True)) == 0
    assert supervisor.lock.closed
    assert json.loads(capsys.readouterr().out)["stage"] == "shutdown"


@pytest.mark.skipif(os.geteuid() == 0, reason="root opens a read-only lock file")
@pytest.mark.parametrize(("published", "expected"), [(False, True), (True, False)])
def test_an_unprobeable_lock_counts_only_a_workspace_without_a_url(supervisor, published, expected):
    supervisor.persist(stage="shutdown", control_url="http://127.0.0.1:1" if published else None)
    (supervisor.root / "supervisor.lock").chmod(0o400)
    assert workspace.lock_held(supervisor.root) is None
    assert workspace.shut_down(supervisor.root) is expected


def test_shutdown_while_a_relaunched_supervisor_holds_the_workspace_is_sent(
    supervisor, monkeypatch
):
    # A teardown's relaunched supervisor keeps the shutdown stage until it provisions,
    # and publishes its URL only once setup has started.
    monkeypatch.setattr(workspace, "SHUTDOWN_SETTLE_SECONDS", 0.1)
    supervisor.persist(stage="shutdown", control_url=None)
    sent = []

    def refused(root, action, **_):
        sent.append(action)
        raise ValueError("provisioning is active; wait for completion before this command")

    monkeypatch.setattr(workspace, "request", refused)
    with pytest.raises(ValueError, match="provisioning is active"):
        workspace.command(control_args(supervisor, "shutdown", force=True))
    assert sent == ["shutdown"]


@pytest.mark.parametrize("interrupted", [False, True])
def test_follow_ends_with_the_recorded_status_after_a_concurrent_shutdown(
    supervisor, monkeypatch, interrupted
):
    supervisor.persist(stage="running", control_url="http://127.0.0.1:1")
    monkeypatch.setattr(workspace, "wait_ready", lambda root: {"stage": "running"})
    original = workspace.request

    def shut_down(root, action, **options):
        # Another command shuts the workspace down while the follow polls it.
        supervisor.persist(stage="shutdown", control_url=None)
        if interrupted and action == "status":
            raise KeyboardInterrupt
        return original(root, action, **options)

    monkeypatch.setattr(workspace, "request", shut_down)
    emitted = []
    monkeypatch.setattr(workspace, "emit", lambda value, args: emitted.append(value))
    expected = 130 if interrupted else 0
    assert workspace.follow(supervisor.root, control_args(supervisor, "run")) == expected
    assert emitted[-1]["stage"] == "shutdown" and emitted[-1]["supervisor"] == "unavailable"


@pytest.mark.parametrize("mode", ["closed", "running"])
def test_a_shut_down_fork_shows_its_recorded_run_and_offers_only_its_report(
    supervisor, monkeypatch, mode
):
    from almanak.presentation import render_text

    supervisor.persist(
        stage="shutdown",
        control_url=None,
        diagnostic="Fork nodes stopped; this fork cannot be resumed.",
    )
    monkeypatch.setattr("almanak.reports.retained", lambda _: {"mode": mode})
    status = workspace.status(supervisor.root)
    assert status["supervisor"] == "unavailable"
    assert status["next_commands"] == [f"almanak report --run {supervisor.root}"]
    rendered = render_text(status)
    assert "stage: shutdown" in rendered and "cannot be resumed" in rendered
    assert f"Lifecycle: {mode}" in rendered and "almanak resume" not in rendered


@pytest.mark.parametrize(("product", "options"), [("pause", {}), ("shutdown", {"force": True})])
def test_control_to_an_unreachable_supervisor_is_one_error_document(
    supervisor, monkeypatch, product, options
):
    from almanak.cli import error_document

    supervisor.persist(stage="paused", control_url="http://127.0.0.1:1")
    monkeypatch.setattr(httpx.Client, "post", unreachable)
    with pytest.raises(workspace.Unanswered) as raised:
        workspace.command(control_args(supervisor, product, **options))
    document = error_document(raised.value)
    assert document is not None and document["error"] == "supervisor_unavailable"
    assert document["workspace"] == str(supervisor.root) and document["action"] == product
    assert "status_code" not in document


def test_an_unreachable_supervisor_prints_one_error_line(supervisor, monkeypatch, capsys):
    from almanak import cli

    supervisor.persist(stage="paused", control_url="http://127.0.0.1:1")
    monkeypatch.setattr(httpx.Client, "post", unreachable)
    monkeypatch.setattr(sys, "argv", ["almanak", "pause", "--run", str(supervisor.root)])
    assert cli.main() == 1
    captured = capsys.readouterr()
    assert captured.out == "" and len(captured.err.splitlines()) == 1
    document = json.loads(captured.err)
    assert document["error"] == "supervisor_unavailable" and document["action"] == "pause"


def test_an_http_error_from_the_supervisor_reports_only_its_status(supervisor, monkeypatch):
    from almanak.cli import error_document

    supervisor.persist(stage="paused", control_url="http://127.0.0.1:1")

    def answered(self, url, **_):
        # Whatever now listens on a reused port may send a body of its own.
        request = httpx.Request("POST", url)
        return httpx.Response(409, json={"error": "foreign"}, request=request)

    monkeypatch.setattr(httpx.Client, "post", answered)
    with pytest.raises(workspace.Unanswered) as raised:
        workspace.command(control_args(supervisor, "pause"))
    document = error_document(raised.value)
    assert document is not None and document["status_code"] == 409
    assert "HTTP 409" in document["message"] and "foreign" not in json.dumps(document)


def test_stopped_fork_recovery_never_creates_replacement(supervisor, context, operator, anchors):
    from almanak.gateway.fixture_preparation import compile_fixture
    from almanak.supervisor_forks import reconnect

    plan = compile_fixture(context, operator, anchors)
    with pytest.raises(RuntimeError, match="original fork node is stopped"):
        reconnect(supervisor, plan)
    assert supervisor.state["processes"] == {}


def test_fixture_rollback_is_forbidden_after_execution_exists(supervisor):
    from almanak.supervisor_forks import rollback_fixture

    (supervisor.root / "state.sqlite").touch()
    with pytest.raises(RuntimeError, match="authoritative execution"):
        rollback_fixture(supervisor, None, supervisor.root / "private/deployment/fixture")
    assert "ephemeral" not in (supervisor.root / "workspace.json").read_text()


def test_setup_refuses_a_workspace_without_prepared_configuration(supervisor):
    with pytest.raises(ValueError, match="no prepared configuration"):
        supervisor.setup()
    assert supervisor.state["processes"] == {} and supervisor.state["stage"] == "created"


def test_failed_provisioning_retains_diagnostic_and_does_not_retry(supervisor, monkeypatch, capsys):
    calls = []

    def failed():
        calls.append(True)
        raise RuntimeError("retained setup needs reconciliation")

    monkeypatch.setattr(supervisor, "setup", failed)
    supervisor.setup_active = True
    supervisor.setup_worker(supervisor.setup)
    assert calls == [True] and not supervisor.setup_active
    assert supervisor.state["stage"] == "failed"
    finished = json.loads(capsys.readouterr().out)
    assert finished["event"] == "setup_finished" and finished["stage"] == "failed"
    supervisor.status()
    assert calls == [True]


def test_status_does_not_discard_ephemeral_password(supervisor):
    supervisor.password = "pending-unlock"
    supervisor.control("status", False)
    assert supervisor.password == "pending-unlock"
    assert "pending-unlock" not in (supervisor.root / "workspace.json").read_text()


def test_gateway_readiness_closes_clients_on_success_and_failure(supervisor, monkeypatch):
    calls = []

    def configuration():
        calls.append("read")
        raise ValueError("gateway not ready")

    client = SimpleNamespace(configuration=configuration, close=lambda: calls.append("close"))
    monkeypatch.setattr(supervisor, "client", lambda: client)
    record = SimpleNamespace(
        require_configuration=lambda *_: calls.append("checked"),
        preparation=SimpleNamespace(prepared=SimpleNamespace(strategy=None)),
    )
    monkeypatch.setattr("almanak.supervisor_preparation.checked_preparation", lambda _: record)
    with pytest.raises(ValueError, match="not ready"):
        supervisor.check_gateway()
    assert calls == ["read", "close"]
    client.configuration = lambda: calls.append("ready")
    supervisor.wait_gateway(None)
    assert calls[-3:] == ["ready", "checked", "close"]


def test_gateway_wait_outlasts_a_slow_start_and_reports_exit_or_hang(
    supervisor, monkeypatch, capsys
):
    clock = {"now": 0.0, "answers_at": 120.0, "alive": True}

    def check():
        if clock["now"] < clock["answers_at"]:
            raise DataError("gateway_unavailable", "gateway request failed")

    def sleep(seconds):
        clock["now"] += seconds * 100

    monkeypatch.setattr(supervisor, "check_gateway", check)
    monkeypatch.setattr("almanak.supervisor.time.monotonic", lambda: clock["now"])
    monkeypatch.setattr("almanak.supervisor.time.sleep", sleep)
    monkeypatch.setattr("almanak.supervisor.running", lambda _: clock["alive"])
    # The rescheduled pod of 0814dcf6 answered after 25 seconds; a live gateway is awaited.
    supervisor.wait_gateway({"pid": 1})
    assert clock["now"] >= 120
    ready = json.loads(capsys.readouterr().out)
    assert ready["event"] == "gateway_ready" and ready["seconds"] >= 120
    clock.update(now=0.0, answers_at=float("inf"))
    with pytest.raises(RuntimeError, match="did not answer within 300 seconds"):
        supervisor.wait_gateway({"pid": 1})
    clock.update(now=0.0, alive=False)
    with pytest.raises(RuntimeError, match="exited during start"):
        supervisor.wait_gateway({"pid": 1})
    assert clock["now"] == 0.0


def test_a_spawned_read_gateway_logs_how_long_it_took_to_answer(supervisor, monkeypatch, capsys):
    from almanak import supervisor_preparation

    clock = {"now": 0.0, "alive": False}
    spawned, closed = [], []

    def spawn(root, name, arguments, *, listener=None):
        spawned.append(name)
        clock["now"] += 3.0
        return {"pid": 1, "identity": {"started": "read gateway"}}

    def answered(client, context, process):
        clock["now"] += 42.0

    client = SimpleNamespace(close=lambda: closed.append(True))
    monkeypatch.setattr(supervisor_preparation, "spawn", spawn)
    monkeypatch.setattr(supervisor_preparation, "running", lambda _: clock["alive"])
    monkeypatch.setattr(supervisor_preparation, "gateway_arguments", lambda *_, **__: [])
    monkeypatch.setattr(supervisor_preparation, "GatewayClient", lambda *_: client)
    monkeypatch.setattr(supervisor_preparation, "wait_read_gateway", answered)
    monkeypatch.setattr("almanak.supervisor_preparation.time.monotonic", lambda: clock["now"])
    # 35ea0f96 and 58cef849 failed on the read gateway, whose start was never timed.
    assert supervisor_preparation.read_gateway(supervisor, None) is client
    assert spawned == ["read-gateway"]
    ready = json.loads(capsys.readouterr().out)
    # The spawn itself is part of the cold start.
    assert ready == {"event": "read_gateway_ready", "seconds": 45.0}
    # A read gateway that is already running was not started here, so nothing is timed.
    clock["alive"] = True
    assert supervisor_preparation.read_gateway(supervisor, None) is client
    assert spawned == ["read-gateway"] and capsys.readouterr().out == "" and not closed


def test_gateway_wait_refuses_a_rejected_configuration_at_once(supervisor, monkeypatch):
    def check():
        raise ValueError("gateway configuration differs from the prepared deployment")

    monkeypatch.setattr(supervisor, "check_gateway", check)
    monkeypatch.setattr("almanak.supervisor.running", lambda _: True)
    monkeypatch.setattr("almanak.supervisor.time.sleep", pytest.fail)
    with pytest.raises(ValueError, match="differs from the prepared deployment"):
        supervisor.wait_gateway({"pid": 1})


def relaunch(supervisor, monkeypatch, action, result, *, process=None, published=True, answers=()):
    """Relaunch after a failed status; the replacement publishes its own URL.

    `answers` scripts the status requests after the first; none answer by default.
    """
    launches, waits = [], []
    process = process or Launched()
    answers = [ValueError("supervisor unavailable"), *answers]

    def unavailable(*_):
        answer = answers.pop(0) if answers else ValueError("supervisor unavailable")
        if isinstance(answer, Exception):
            raise answer
        return answer

    def wait_ready(root, **options):
        waits.append(options)
        if published:
            supervisor.persist(control_url="http://127.0.0.1:2")
        return result

    monkeypatch.setattr(workspace, "request", unavailable)
    monkeypatch.setattr(workspace, "launch", lambda *a, **k: launches.append(k) or process)
    monkeypatch.setattr(workspace, "wait_ready", wait_ready)
    return workspace.ensure_supervisor(supervisor.root, action), launches, waits


@pytest.mark.parametrize(
    "result",
    [
        {"supervisor": "available", "provisioning": True, "stage": "provisioned"},
        {"supervisor": "available", "provisioning": False, "stage": "action_required"},
    ],
)
def test_relaunch_carries_the_resume_without_a_second_command(supervisor, monkeypatch, result):
    supervisor.persist(stage="shutdown", control_url="http://127.0.0.1:1")
    (password, resumed), launches, waits = relaunch(supervisor, monkeypatch, "resume", result)
    assert password is None and resumed == result
    assert launches == [{"password": None, "resume": True, "teardown": False}]
    assert waits[0]["replaced"]["control_url"] == "http://127.0.0.1:1"
    assert isinstance(waits[0]["launched"], Launched)


def test_status_of_the_replaced_supervisor_is_not_an_accepted_resume(supervisor, monkeypatch):
    supervisor.persist(stage="paused", control_url="http://127.0.0.1:1")
    paused = {"supervisor": "available", "provisioning": False, "stage": "paused"}
    with pytest.raises(RuntimeError, match="did not answer"):
        relaunch(supervisor, monkeypatch, "resume", paused, published=False)


@pytest.mark.parametrize("action", ["resume", "teardown"])
def test_a_supervisor_that_answered_late_receives_the_control(supervisor, monkeypatch, action):
    # The replacement exits because the original supervisor still holds the lock.
    (password, resumed), launches, _ = relaunch(
        supervisor,
        monkeypatch,
        action,
        {},
        process=Launched(exited_after=0),
        published=False,
        answers=[{"stage": "paused"}],
    )
    assert len(launches) == 1 and (password, resumed) == (None, None)


def test_a_relaunch_that_exited_with_no_supervisor_answering_is_an_error(supervisor, monkeypatch):
    with pytest.raises(RuntimeError, match="relaunched supervisor exited"):
        relaunch(supervisor, monkeypatch, "resume", {}, process=Launched(exited_after=0))


def test_wait_ready_ends_when_the_launched_process_exits(supervisor, monkeypatch):
    supervisor.persist(control_url="http://127.0.0.1:1")
    replaced = workspace.read(supervisor.root)
    monkeypatch.setattr(workspace, "status", lambda root: {"stage": "paused"})
    launched = Launched(exited_after=0)
    assert workspace.wait_ready(supervisor.root, replaced=replaced, launched=launched) == {
        "stage": "paused"
    }
    assert launched.polls == 1


@pytest.mark.parametrize(
    ("result", "reason"),
    [
        (
            {"supervisor": "available", "stage": "failed", "diagnostic": "boom"},
            "setup failed: boom",
        ),
        ({"supervisor": "unavailable", "stage": "shutdown"}, "did not answer"),
    ],
)
def test_relaunched_resume_that_failed_is_an_error(supervisor, monkeypatch, result, reason):
    with pytest.raises(RuntimeError, match=reason):
        relaunch(supervisor, monkeypatch, "resume", result)


@pytest.mark.parametrize(
    ("password", "resume", "teardown", "flags"),
    [
        (None, False, False, []),
        ("unlock", True, False, ["--password-stdin", "--resume"]),
        (None, False, True, ["--teardown"]),
    ],
)
def test_launch_starts_a_detached_supervisor_and_hands_over_the_password(
    supervisor, monkeypatch, password, resume, teardown, flags
):
    started = []

    class Stdin:
        def __init__(self):
            self.written, self.closed = b"", False

        def write(self, data):
            self.written += data

        def close(self):
            self.closed = True

    class Popen:
        def __init__(self, argv, **options):
            self.stdin = Stdin() if options["stdin"] == subprocess.PIPE else None
            started.append((argv, options, self))

    monkeypatch.setattr(workspace.subprocess, "Popen", Popen)
    process = workspace.launch(supervisor.root, password=password, resume=resume, teardown=teardown)
    ((argv, options, launched),) = started
    assert process is launched
    assert argv == [
        sys.executable,
        "-m",
        "almanak.supervisor",
        "--run",
        str(supervisor.root),
        *flags,
    ]
    assert options["start_new_session"] and options["cwd"] == "/"
    if password is None:
        assert options["stdin"] == subprocess.DEVNULL and process.stdin is None
    else:
        assert process.stdin.written == b'"unlock"\n' and process.stdin.closed


def test_relaunch_for_teardown_waits_for_setup_before_the_control(supervisor, monkeypatch):
    settled = []
    monkeypatch.setattr(workspace.time, "monotonic", lambda: 1000.0)
    monkeypatch.setattr(workspace, "wait_settled", lambda *args: settled.append(args))
    (password, resumed), launches, _ = relaunch(supervisor, monkeypatch, "teardown", {})
    assert password is None and resumed is None
    assert launches == [{"password": None, "resume": False, "teardown": True}]
    # The whole relaunch shares one command budget, less the control's reserve.
    ((root, process, deadline),) = settled
    assert (root, deadline) == (supervisor.root, 1000.0 + 600 - 60)
    assert isinstance(process, Launched)


class Launched:
    def __init__(self, exited_after=None):
        self.polls = 0
        self.exited_after = exited_after

    def poll(self):
        self.polls += 1
        exited = self.exited_after is not None and self.polls > self.exited_after
        return 0 if exited else None


def scripted_status(monkeypatch, answers):
    def request(root, action):
        answer = answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer

    monkeypatch.setattr(workspace.time, "sleep", lambda _: None)
    monkeypatch.setattr(workspace, "request", request)


def test_wait_settled_retries_failed_polls_until_setup_ends(supervisor, monkeypatch):
    answers = [
        {"provisioning": True},
        httpx.ConnectError("refused"),
        ValueError("provisioning is active"),
        {"provisioning": False},
    ]
    scripted_status(monkeypatch, answers)
    workspace.wait_settled(supervisor.root, Launched(), float("inf"))
    assert answers == []


def test_wait_settled_ends_when_the_supervisor_exits_or_the_deadline_passes(
    supervisor, monkeypatch
):
    answers = [httpx.ConnectError("refused")] * 2 + [{"provisioning": True}]
    scripted_status(monkeypatch, answers)
    workspace.wait_settled(supervisor.root, Launched(exited_after=2), float("inf"))
    assert answers == [{"provisioning": True}]
    workspace.wait_settled(supervisor.root, Launched(), float("-inf"))
    assert answers == [{"provisioning": True}]


def test_wait_ready_ignores_the_stage_a_replaced_supervisor_left(supervisor, monkeypatch):
    supervisor.persist(stage="failed", control_url="http://127.0.0.1:1")
    original = workspace.read
    replaced = original(supervisor.root)
    reads = []

    def read(root):
        # The relaunched supervisor publishes its own control URL on the third poll.
        reads.append(True)
        if len(reads) == 3:
            supervisor.persist(stage="provisioned", control_url="http://127.0.0.1:2")
        return original(root)

    monkeypatch.setattr(workspace, "read", read)
    monkeypatch.setattr(workspace.time, "sleep", lambda _: None)
    monkeypatch.setattr(workspace, "request", lambda root, action: {"stage": "provisioned"})
    assert workspace.wait_ready(supervisor.root, replaced=replaced) == {"stage": "provisioned"}
    assert len(reads) == 3


def resumable(supervisor, monkeypatch):
    """A provisioned workspace with execution state, as a restarted pod finds it."""
    _, release, commands = gated_admission(supervisor, monkeypatch)
    release.set()
    monkeypatch.setattr(supervisor, "prepared", lambda: True)
    monkeypatch.setattr("almanak.supervisor_preparation.provision", lambda _: None)
    supervisor.deployment.mkdir(parents=True)
    (supervisor.deployment / "deployment.json").write_text("{}")
    (supervisor.root / "state.sqlite").touch()
    supervisor.persist(
        stage="shutdown", diagnostic="Services stopped", pause_requested=True, processes={}
    )
    return commands


def test_launched_resume_reports_provisioned_then_resumes_execution(supervisor, monkeypatch):
    commands = resumable(supervisor, monkeypatch)
    started = threading.Event()
    monkeypatch.setattr(supervisor, "admission", lambda: started.wait(10))
    supervisor.start_resumed_setup()
    status = supervisor.status()
    assert status["stage"] == "provisioned" and status["provisioning"]
    assert status["diagnostic"] is None and not supervisor.state["pause_requested"]
    started.set()
    assert settled(supervisor)
    assert supervisor.state["stage"] == "running"
    assert [command[1] for command in commands] == ["resume"]


def test_launch_for_teardown_masks_the_failed_stage_until_setup_ends(supervisor, monkeypatch):
    # A pod started to tear a latched deployment down: hosts treat failed as final, so the
    # previous supervisor's stage must not be reported while this one sets up.
    commands = resumable(supervisor, monkeypatch)
    supervisor.persist(stage="failed", failure="teardown_required", diagnostic="latched")
    started = threading.Event()
    monkeypatch.setattr(supervisor, "admission", lambda: started.wait(10))
    supervisor.start_teardown_setup()
    status = supervisor.status()
    assert status["stage"] == "provisioned" and status["provisioning"]
    assert status["failure"] is None and status["diagnostic"] is None
    started.set()
    assert settled(supervisor)
    # Never paused: the teardown control that follows starts the coordinator.
    assert supervisor.state["stage"] == "provisioned" and commands == []


def test_a_teardown_without_execution_state_never_creates_a_coordinator(supervisor, monkeypatch):
    # workspace.json exists but nothing ever executed: a close-positions action must not
    # start the strategy from scratch.
    commands = resumable(supervisor, monkeypatch)
    (supervisor.root / "state.sqlite").unlink()
    provisioned, gateways = [], []
    monkeypatch.setattr("almanak.supervisor_preparation.provision", provisioned.append)
    monkeypatch.setattr(supervisor, "start_gateway", lambda: gateways.append(1))
    monkeypatch.setattr(supervisor, "client", lambda: pytest.fail("no teardown request"))
    supervisor.start_teardown_setup()
    assert settled(supervisor)
    status = supervisor.status()
    assert status["stage"] == "failed" and status["failure"] == "start"
    assert "nothing to tear down" in status["diagnostic"]
    with pytest.raises(ValueError, match="nothing to tear down"):
        supervisor.control("teardown", False)
    assert commands == [] and provisioned == [] and gateways == []
    assert supervisor.state["processes"] == {}
    assert not (supervisor.root / "state.sqlite").exists()


def test_plain_launch_keeps_existing_execution_paused(supervisor, monkeypatch):
    commands = resumable(supervisor, monkeypatch)
    supervisor.setup()
    assert supervisor.state["stage"] == "paused" and commands == []


@pytest.mark.parametrize(("paused", "stage"), [(False, "running"), (True, "paused")])
def test_launched_resume_adopts_a_coordinator_that_outlived_its_supervisor(
    supervisor, monkeypatch, paused, stage
):
    commands = resumable(supervisor, monkeypatch)
    supervisor.persist(processes={"coordinator": {"pid": 1}}, pause_requested=paused)
    supervisor.setup(resume=True)
    assert supervisor.state["stage"] == stage and commands == []


def test_launched_resume_restarts_a_surviving_coordinator_on_a_new_gateway(supervisor, monkeypatch):
    commands = resumable(supervisor, monkeypatch)
    live, stopped = {7}, []

    def stop(record, *, interrupt=False):
        stopped.append((record, interrupt))
        live.discard(record["pid"])

    monkeypatch.setattr("almanak.supervisor.running", lambda r: bool(r) and r["pid"] in live)
    monkeypatch.setattr("almanak.supervisor.stop", stop)
    # The surviving coordinator holds the old gateway's URL; the gateway died with it.
    monkeypatch.setattr(
        supervisor, "start_gateway", lambda: supervisor.persist(gateway_url="http://127.0.0.1:2")
    )
    supervisor.persist(processes={"coordinator": {"pid": 7}}, pause_requested=False)
    supervisor.setup(resume=True)
    assert stopped == [({"pid": 7}, True)]
    assert [command[1] for command in commands] == ["resume"]
    assert commands[0][commands[0].index("--gateway-url") + 1] == "http://127.0.0.1:2"
    assert supervisor.state["stage"] == "running"


@pytest.mark.parametrize(
    ("flag", "first"),
    [(None, "setup"), ("--resume", "resumed setup"), ("--teardown", "teardown setup")],
)
def test_supervisor_process_starts_setup_before_publishing_its_control_url(
    tmp_path, monkeypatch, flag, first
):
    from almanak import supervisor as module

    events = []

    class Recorded:
        def __init__(self, root, password):
            self.server = None

        def start_setup(self):
            events.append("setup")

        def start_resumed_setup(self):
            events.append("resumed setup")

        def start_teardown_setup(self):
            events.append("teardown setup")

        def persist(self, **updates):
            events.append(sorted(updates))

    monkeypatch.setattr(module, "Supervisor", Recorded)
    monkeypatch.setattr(module.HTTPServer, "serve_forever", lambda *a, **k: events.append("serve"))
    arguments = ["supervisor", "--run", str(tmp_path), *([flag] if flag else [])]
    monkeypatch.setattr(sys, "argv", arguments)
    module.main()
    assert events == [first, ["control_url"], "serve"]


def test_pause_during_a_launched_resume_keeps_the_coordinator_stopped(supervisor, monkeypatch):
    commands = resumable(supervisor, monkeypatch)
    entered, release = threading.Event(), threading.Event()
    monkeypatch.setattr(supervisor, "admission", lambda: entered.set() or release.wait(10))
    supervisor.start_resumed_setup()
    assert entered.wait(5)
    assert supervisor.control("pause", False)["stage"] == "paused"
    release.set()
    assert settled(supervisor)
    assert supervisor.state["stage"] == "paused" and commands == []


def test_follow_ctrl_c_pauses_and_returns_130(supervisor, monkeypatch):
    emitted, actions = [], []

    def request(root, action):
        actions.append(action)
        if action == "status":
            raise KeyboardInterrupt
        return {"stage": "paused"}

    monkeypatch.setattr(workspace, "wait_ready", lambda _: {"stage": "running"})
    monkeypatch.setattr(workspace, "request", request)
    monkeypatch.setattr(workspace, "emit", lambda value, _: emitted.append(value))
    assert workspace.follow(supervisor.root, SimpleNamespace()) == 130
    assert actions == ["status", "pause"] and emitted[-1] == {"stage": "paused"}


def public_reference(tmp_path):
    from tests.test_author_resolution import REFERENCE, gasless_reference

    reference = tmp_path / "reference"
    reference.mkdir()
    (reference / "strategy.py").write_bytes((REFERENCE / "strategy.py").read_bytes())
    (reference / "config.yaml").write_bytes(gasless_reference(tmp_path).read_bytes())
    return reference


def run_arguments(reference, operator_path, env, output, *, production=False):
    return SimpleNamespace(
        directory=reference,
        config=None,
        strategy=None,
        production=production,
        fork=not production,
        gateway_config=operator_path,
        env_file=env,
        output=output,
    )


def test_workspace_captures_inputs_before_launch_and_uses_private_files(
    tmp_path, monkeypatch, operator
):
    reference = public_reference(tmp_path)
    env = tmp_path / "explicit.env"
    env.write_text("")
    operator_path = tmp_path / "operator.json"
    operator_path.write_text(operator.model_dump_json())
    launched = []
    monkeypatch.setattr(
        workspace, "launch", lambda root, **_: launched.append(workspace.read(root))
    )
    root = workspace.create(run_arguments(reference, operator_path, env, tmp_path / "run"))
    state = launched[0]
    assert state["stage"] == "created" and state["mode"] == "fork"
    assert state["configuration"] == "prepared-fork-v1" and state["processes"] == {}
    assert state["credentials"] == {"gateway_config": str(operator_path), "env_file": str(env)}
    assert (root / "strategy.py").read_text().rstrip() == (
        reference / "strategy.py"
    ).read_text().rstrip()
    assert (root / "control.token").stat().st_mode & 0o777 == 0o600
    assert not (root / "author.json").exists()
    context = json.loads((root / "private/read-context.json").read_text())
    assert context["mode"] == "fork"
    assert context["anchors"] == [a.model_dump(mode="json") for a in operator.fork.anchors]
    assert [a["name"] for a in context["accounts"]] == ["lending_0", "lending_1"]
    authored = json.loads((root / "private/author-inputs.json").read_text())
    assert authored["strategy"]["allocations"]["main_weth"]["capital"] == "0.01"


@pytest.mark.parametrize("fault", ["fork_without_fixture", "production_without_signing"])
def test_create_refuses_operators_missing_the_mode_authority(
    tmp_path, monkeypatch, operator, fault
):
    from almanak.connectors.support import RunMode
    from tests.test_resolution_context import revalidate

    reference = public_reference(tmp_path)
    env = tmp_path / "explicit.env"
    env.write_text("")
    if fault == "fork_without_fixture":
        selected = revalidate(operator, fork=None)
        production = False
        message = "requires operator fixture inputs"
    else:
        selected = revalidate(operator, modes=(RunMode.PRODUCTION,), fork=None)
        production = True
        message = "requires operator signing configuration"
    operator_path = tmp_path / "operator.json"
    operator_path.write_text(selected.model_dump_json())
    monkeypatch.setattr(workspace, "launch", lambda *_, **__: pytest.fail("launched"))
    args = run_arguments(reference, operator_path, env, tmp_path / "run", production=production)
    with pytest.raises(ValueError, match=message):
        workspace.create(args)
    assert not args.output.exists()


def test_production_admission_funding_failure_and_eoa_ready(monkeypatch):
    from almanak import production_setup

    config = SimpleNamespace(accounts=SimpleNamespace(bindings=()))
    monkeypatch.setattr(production_setup, "preflight_config", lambda *_: {"funded": False})
    assert not production_setup.admit(config, None, None, None)["ready"]
    monkeypatch.setattr(production_setup, "preflight_config", lambda *_: {"funded": True})
    assert production_setup.admit(config, None, None, None)["ready"]


@pytest.fixture
def safe_admission(monkeypatch):
    """Admission of one Safe account with generation and verification recorded."""
    from almanak import production_setup
    from almanak.gateway import permissions

    class Profile:
        pass

    calls = []
    pending = []
    manifest = SimpleNamespace(partitions=(SimpleNamespace(transactions=pending),), digest="d" * 64)
    monkeypatch.setattr(production_setup, "SafeRoles", Profile)
    monkeypatch.setattr(
        production_setup, "production_routes", lambda *_: SimpleNamespace(close=lambda: None)
    )
    monkeypatch.setattr(
        production_setup, "connector_for", lambda _: SimpleNamespace(permissions=None)
    )
    monkeypatch.setattr(production_setup.StrategyDefinition, "read", lambda _: None)
    monkeypatch.setattr(production_setup, "export_permissions", lambda *_: calls.append("exported"))
    monkeypatch.setattr(
        permissions, "generate", lambda *_, **__: calls.append("generated") or manifest
    )
    monkeypatch.setattr(
        permissions, "verify", lambda *_, **__: calls.append("verified") or {"partitions": []}
    )
    config = SimpleNamespace(
        accounts=SimpleNamespace(bindings=(SimpleNamespace(profile=Profile()),))
    )
    return SimpleNamespace(module=production_setup, config=config, calls=calls, pending=pending)


@pytest.mark.parametrize(
    ("funded", "owner_transactions", "reason"),
    [
        (False, True, "funding or market checks failed; owner must install exported permissions"),
        (False, False, "funding or market checks failed"),
        (True, True, "owner must install exported permissions"),
    ],
)
def test_admission_reports_funding_and_permissions_together(
    safe_admission, monkeypatch, funded, owner_transactions, reason
):
    setup = safe_admission
    setup.pending.extend([object()] * owner_transactions)
    monkeypatch.setattr(setup.module, "preflight_config", lambda *_: {"funded": funded})
    result = setup.module.admit(setup.config, None, None, None)
    assert not result["ready"] and result["reason"] == reason
    # Owner bundles are exported even while funding is missing; nothing verifies yet.
    assert setup.calls == ["generated", "exported"]


def test_admission_verifies_installed_permissions_once_funded(safe_admission, monkeypatch):
    setup = safe_admission
    monkeypatch.setattr(setup.module, "preflight_config", lambda *_: {"funded": True})
    assert setup.module.admit(setup.config, None, None, None)["ready"]
    assert setup.calls == ["generated", "exported", "verified"]
