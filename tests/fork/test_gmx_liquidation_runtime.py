"""Capture real unsolicited liquidation semantics before enabling runtime attribution."""

import signal
import sys
import time
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from tests import gmx_fork
from tests.fork.support import processes, request_teardown, wait_state
from tests.fork.test_gmx_liquidation_capture import capture_liquidation, source_after
from tests.fork.test_gmx_requests import case, configuration
from tests.strategy_fork.harness import (
    bind_workspace,
    install_permissions,
    resolve_config,
    restart_gateway,
    resume,
    retain,
    start_gateway,
    start_runtime,
)

ROOT = Path(__file__).parents[2]
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]


@pytest.mark.parametrize("boundary", ["external_evidence", "external_commit", "outcome"])
@pytest.mark.parametrize("chain", ["arbitrum", "avalanche"])
@pytest.mark.parametrize("profile", ["eoa", "safe_roles"])
def test_liquidation_runtime(request, tmp_path, chain, profile, boundary):
    selected = case(f"gmx_v2_{chain}_position")
    env = request.config.getoption("--fork-env").resolve()
    config = resolve_config(configuration(selected, profile, env, tmp_path / "resolution"), env, {})
    artifact = (
        ROOT
        / "artifacts/strategy-fork"
        / (selected.name + "-liquidation-runtime-" + profile + "-" + boundary + "-" + uuid4().hex)
    )
    artifact.mkdir(parents=True)
    temporary = tmp_path / artifact.name
    temporary.mkdir()
    record = {
        "strategy": selected.name,
        "profile": profile,
        "boundary": boundary,
        "status": "failed",
        "scope": "actual reference liquidation runtime recovery",
        "phases": [],
    }
    try:
        with ExitStack() as stack:
            nodes, node = bind_workspace(config, env, stack, record)
            launch = stack.enter_context(processes(temporary))
            directory = temporary / "deployment"
            deployment = nodes.prepare(config, directory)
            journal, arguments = start_gateway(node, directory, launch, lease_seconds=30)
            if profile == "safe_roles":
                install_permissions(selected, node, directory, deployment, arguments)
            node.rpc("anvil_setBlockTimestampInterval", [1])
            running, _ = start_runtime(
                selected, node, SimpleNamespace(), journal, arguments, launch, record["phases"]
            )
            gmx_fork.drive_external(
                node,
                journal,
                running,
                lambda r: source_after(chain, r.events[0].identity.block.timestamp),
            )
            held = wait_state(journal, gmx_fork.entered, running)
            running.kill()
            assert running.wait(timeout=20) == -signal.SIGKILL
            capture_liquidation(node, held, journal.path.parent, chain)
            record["phases"].append("actual_reference_position_liquidated_with_authentic_oracle")
            restart_gateway(node, launch)
            wait_expired_lease(journal)
            crashing = launch(
                [
                    sys.executable,
                    str(ROOT / "tests/fork/liquidation_driver.py"),
                    *arguments,
                    "--crash",
                    boundary,
                ],
                ready=False,
            )
            assert crashing.wait(timeout=180) == -signal.SIGKILL
            interrupted = journal.load()
            assert_interrupted(interrupted, held, boundary)
            (artifact / "interrupted-checkpoint.json").write_text(
                interrupted.model_dump_json(indent=2)
            )
            restart_gateway(node, launch)
            running = resume(launch, journal, arguments)
            delivered = wait_state(journal, liquidated, running)
            assert_delivered_liquidation(delivered, held)
            request_teardown(arguments)
            closed = wait_state(journal, lambda s: s.mode == "closed", running)
            assert running.wait(timeout=20) == 0
            assert_closed_liquidation(closed)
            record["phases"].append(
                "gateway_replacement_external_reconcile_once_delivery_and_clean_teardown"
            )
            record["status"] = "passed"
    finally:
        retain(temporary, artifact, record)
        retain_liquidation_capture(temporary, artifact)
        print("GMX liquidation runtime evidence:", artifact)


def liquidated(state):
    return any(
        o.kind == "gmx_liquidation_outcome" for o in state.outcomes
    ) and state.delivered == len(state.outcomes)


def assert_interrupted(state, held, boundary):
    assert_original_author_context(state, held)
    if boundary == "external_evidence":
        assert state.owned == held.owned
        assert state.outcomes == held.outcomes
    else:
        assert_committed_liquidation(state, held)


def assert_original_author_context(state, held):
    assert state.requests == held.requests
    assert state.memory == held.memory
    assert state.delivered == held.delivered


def assert_committed_liquidation(state, held):
    assert len(state.outcomes) == len(held.outcomes) + 1
    assert state.outcomes[-1].kind == "gmx_liquidation_outcome"
    assert state.owned[0].position is None


def assert_delivered_liquidation(state, held):
    assert state.owned[0].position is None
    assert not state.outcomes[-1].accounting_issues
    assert state.requests == held.requests


def assert_closed_liquidation(state):
    assert sum(o.kind == "gmx_liquidation_outcome" for o in state.outcomes) == 1
    assert state.memory.values["finished"] == ["Main"]
    assert_cleanup(state)


def assert_cleanup(state):
    assert all(not request.unresolved for request in state.requests)
    assert all(not owned.claimable_funding for owned in state.owned)


def wait_expired_lease(journal):
    deadline = time.monotonic() + 40
    while time.monotonic() < deadline:
        with journal.connect() as connection:
            identity, expires = connection.execute(
                "SELECT identity, expires FROM coordinator"
            ).fetchone()
        if identity is None or expires <= time.time():
            return
        time.sleep(0.05)
    raise AssertionError("terminated runtime retained a live lease")


def retain_liquidation_capture(temporary, artifact):
    captured = temporary / "deployment/external-liquidation.json"
    if captured.exists():
        (artifact / "external-liquidation.json").write_bytes(captured.read_bytes())
