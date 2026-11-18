"""Bounded local process management shared by fork proofs."""

import json
import os
import signal
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path

import pytest

from almanak.runtime import report
from almanak.state import Journal

ROOT = Path(__file__).resolve().parents[2]
CLI = str(Path(sys.executable).with_name("almanak"))


@contextmanager
def processes(tmp_path):
    processes: list[subprocess.Popen] = []
    handles = []

    def start(arguments, ready=True):
        log = tmp_path / f"process-{len(processes)}.log"
        handle = log.open("w")
        handles.append(handle)
        environment = {"PATH": os.environ["PATH"], "PYTHONDONTWRITEBYTECODE": "1"}
        # Preserve the trusted scratch location without inheriting operator credentials.
        if "TMPDIR" in os.environ:
            environment["TMPDIR"] = os.environ["TMPDIR"]
        process = subprocess.Popen(
            arguments,
            cwd=ROOT,
            stdout=handle,
            stderr=handle,
            env=environment,
        )
        processes.append(process)
        if ready:
            wait_ready(process, log, "ready" if ready is True else ready)
        return process

    try:
        yield start
    finally:
        for process in reversed(processes):
            stop_process(process)
        for handle in handles:
            handle.close()


@pytest.fixture
def launch(tmp_path):
    with processes(tmp_path) as start:
        yield start


def stop_process(process):
    if process.poll() is None:
        process.kill()
    process.wait(timeout=10)


def control_arguments(arguments):
    return arguments[2:]


def resume(launch, journal, arguments):
    deadline = time.monotonic() + 40
    while time.monotonic() < deadline:
        with journal.connect() as connection:
            identity, expires = connection.execute(
                "SELECT identity, expires FROM coordinator"
            ).fetchone()
        if identity is None or expires <= time.time():
            return launch([CLI, "service", "resume", *arguments])
        time.sleep(0.05)
    raise AssertionError("terminated runtime retained a live lease")


def request_teardown(arguments):
    result = subprocess.run(
        [CLI, "service", "teardown", *control_arguments(arguments)],
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert_teardown_response(result)


def wait_state(journal, predicate, process, *, timeout=180):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        state = journal.load()
        if predicate(state):
            return state
        if process.poll() is not None:
            raise AssertionError(
                "coordinator exited before expected state: " + json.dumps(report(state))
            )
        time.sleep(0.05)
    raise AssertionError("state deadline exceeded: " + json.dumps(report(journal.load())))


def wait_ready(process, log, event):
    deadline = time.monotonic() + 40
    while time.monotonic() < deadline:
        if json.dumps({"event": event})[1:-1] in log.read_text():
            return process
        if process.poll() is not None:
            raise AssertionError(log.read_text())
        time.sleep(0.05)
    raise AssertionError("coordinator did not become ready: " + log.read_text())


def assert_teardown_response(result):
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["mode"] == "teardown"


def start_gateway(fork, directory, launch, *, lease_seconds=2):
    config_path = directory / "deployment.json"
    key_path = directory / "fork-keys.json"
    database = directory / "state.sqlite"
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    gateway_args = [
        CLI,
        "service",
        "gateway",
        "--database",
        str(database),
        "--fork-url",
        fork.rpc.endpoint if not hasattr(fork, "nodes") else "collection",
        "--fork-key-file",
        str(key_path),
        "--auth-token-file",
        str(directory / "gateway-auth.token"),
        "--port",
        str(port),
        "--lease-seconds",
        str(lease_seconds),
    ]
    preparation = directory / "prepared-deployment.json"
    if preparation.is_file():
        gateway_args.extend(
            [
                "--gateway-config",
                str(directory / "operator.json"),
                "--preparation-file",
                str(preparation),
            ]
        )
    else:
        gateway_args.extend(["--config", str(config_path)])
    if (directory / "fork-routes.json").is_file():
        index = gateway_args.index("--fork-url")
        gateway_args[index : index + 2] = ["--fork-routes", str(directory / "fork-routes.json")]
    process = launch(gateway_args, ready="gateway_ready")
    fork.gateway_process = process
    fork.gateway_arguments = gateway_args
    arguments = [
        "--config",
        str(config_path),
        "--gateway-url",
        f"http://127.0.0.1:{port}",
        "--gateway-auth-file",
        str(directory / "gateway-auth.token"),
    ]
    return Journal(database), arguments


def exercise_approval_cleanup(
    case,
    config,
    env,
    temporary,
    record,
    *,
    operation,
    assert_committed,
    assert_cleanup,
    prepare=None,
):
    """Kill the runtime at approval commit, replace it and the gateway, then close by revoking.

    `operation` is the connector's first approval operation kind. `assert_committed`
    checks the interrupted checkpoint; `assert_cleanup(node, before, closed)` checks the
    closed state against the fork. Both lanes retain the same record phases.
    """
    from contextlib import ExitStack

    from almanak.strategy import StrategyDefinition
    from tests.strategy_fork.harness import (
        bind_workspace,
        install_permissions,
        start_gateway,
    )

    with ExitStack() as stack:
        nodes, node = bind_workspace(config, env, stack, record)
        launch = stack.enter_context(processes(temporary))
        directory = temporary / "deployment"
        deployment = (
            nodes.prepare(config, directory) if prepare is None else prepare(nodes, directory)
        )
        journal, arguments = start_gateway(node, directory, launch, lease_seconds=30)
        install_cleanup_permissions(
            case, nodes, directory, deployment, arguments, record, install_permissions
        )
        accepted = interrupt_after_commit(case, launch, arguments, operation, journal)
        assert_committed(accepted)
        record["interrupted_checkpoint"] = accepted.model_dump(mode="json")
        record["phases"].append("approval_committed_before_outcome_delivery")
        closed = resume_cleanup(node, launch, journal, arguments)
        assert closed.strategy == StrategyDefinition.read(case.strategy)
        assert_cleanup(node, accepted, closed)
        record["phases"].append("replaced_runtime_and_gateway_revoked_without_trading")


def install_cleanup_permissions(case, nodes, directory, deployment, arguments, record, install):
    if record["account_profile"] == "safe_roles":
        install(case, nodes, directory, deployment, arguments)
        record["phases"].append("generated_permissions_installed_and_verified")


def interrupt_after_commit(case, launch, arguments, operation, journal):
    interrupted = launch(
        [
            sys.executable,
            str(ROOT / "tests/fork/driver.py"),
            "--strategy",
            str(case.strategy),
            *arguments,
            "--crash",
            "commit",
            "--operation",
            operation,
        ]
    )
    assert interrupted.wait(timeout=180) == -signal.SIGKILL
    return journal.load()


def resume_cleanup(node, launch, journal, arguments):
    from tests.strategy_fork.harness import restart_gateway

    restart_gateway(node, launch)
    request_teardown(arguments)
    running = resume(launch, journal, arguments)
    closed = wait_state(journal, lambda state: state.mode == "closed", running, timeout=180)
    assert running.wait(timeout=20) == 0
    return closed
