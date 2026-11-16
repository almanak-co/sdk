"""Swap cancellation and keeper delivery survive native process recovery."""

import signal
import sys
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from tests import gmx_fork, gmx_position_path, gmx_swap_path
from tests.fork.support import processes, request_teardown, wait_state
from tests.fork.test_gmx_requests import case, configuration, operation_count
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


@pytest.mark.parametrize("chain", ["arbitrum", "avalanche"])
@pytest.mark.parametrize("profile", ["safe_roles", "eoa"])
@pytest.mark.parametrize("scenario", ["cancellation", "keeper_delivery"])
def test_gmx_swap_recovery(request, tmp_path, chain, profile, scenario):
    selected = case(f"gmx_v2_{chain}_swap")
    env = request.config.getoption("--fork-env").resolve()
    config = resolve_config(configuration(selected, profile, env, tmp_path / "resolution"), env, {})
    artifact = (
        ROOT
        / "artifacts/strategy-fork"
        / (selected.name + "-" + scenario + "-" + profile + "-" + uuid4().hex)
    )
    artifact.mkdir(parents=True)
    temporary = tmp_path / artifact.name
    temporary.mkdir()
    record = {
        "strategy": selected.name,
        "profile": profile,
        "scenario": scenario,
        "status": "failed",
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
                record["phases"].append("generated_permissions_installed_and_verified")
            if scenario == "cancellation":
                cancel_swap(selected, node, journal, arguments, launch, record["phases"])
            else:
                recover_keeper(selected, node, journal, arguments, launch, record["phases"], chain)
            record["status"] = "passed"
    finally:
        retain(temporary, artifact, record)
        print("GMX swap recovery evidence:", artifact)


def cancel_swap(selected, node, journal, arguments, launch, phases):
    node.rpc("anvil_setBlockTimestampInterval", [1])
    assertions = SimpleNamespace(crash_operation="gmx_create_swap", crash_stage="broadcast")
    running, _ = start_runtime(selected, node, assertions, journal, arguments, launch, phases)
    accepted = wait_state(journal, accepted_request, running)
    assert len(accepted.requests) == 1 and accepted.requests[0].unresolved
    request = accepted.requests[0]
    request_teardown(arguments)
    node.rpc("evm_setNextBlockTimestamp", [request.admission_deadline + 301])
    node.rpc("evm_mine")
    closed = wait_state(journal, lambda s: s.mode == "closed", running)
    assert running.wait(timeout=20) == 0
    assert_cancelled_request(closed, request)
    assert_cancelled_inventory(closed)
    phases.append(
        "broadcast_recovered_exact_cancellation_after_deadline_with_input_refund_and_revocation"
    )


def recover_keeper(selected, node, journal, arguments, launch, phases, chain):
    node.rpc("anvil_setBlockTimestampInterval", [1])
    running = launch(
        [
            sys.executable,
            str(ROOT / "tests/fork/driver.py"),
            "--strategy",
            str(selected.strategy),
            *arguments,
            "--crash",
            "outcome",
            "--operation",
            "gmx_create_swap",
            "--result-kind",
            "gmx_request_outcome",
        ]
    )
    gmx_fork.drive_external(
        node, journal, running, lambda request: gmx_position_path.source_for(request, chain)
    )
    assert running.wait(timeout=180) == -signal.SIGKILL
    interrupted = journal.load()
    assert_undelivered_keeper(interrupted)
    restart_gateway(node, launch)
    running = resume(launch, journal, arguments)
    holding = wait_state(journal, gmx_swap_path.entered, running)
    assert_delivered_keeper(holding, interrupted)
    phases.append(
        "keeper_effect_committed_before_hook_delivery_crash_then_delivered_once_after_gateway_replacement"
    )
    request_teardown(arguments)
    gmx_swap_path.drive_external(node, journal, running, chain)
    closed = wait_state(journal, lambda state: state.mode == "closed", running)
    assert running.wait(timeout=20) == 0
    gmx_swap_path.assert_closed(node, closed)
    phases.append("return_swap_and_revocation_closed")


def accepted_request(state):
    return bool(state.requests) and state.delivered == len(state.outcomes)


def assert_cancelled_request(state, request):
    assert len(state.requests) == 1 and state.requests[0].key == request.key
    assert state.requests[0].status == "cancelled"


def assert_cancelled_inventory(closed):
    assert operation_count(closed, "gmx_create_swap") == 1
    assert operation_count(closed, "gmx_cancel_exchange_order") == 1
    assert all(not o.accounting_issues for o in closed.outcomes)
    assert_cancelled_cleanup(closed)
    assert_input_refund(closed)


def assert_cancelled_cleanup(closed):
    assert all(not c.needs_cleanup for c in closed.closure)
    assert closed.memory.values["failed"] == [closed.config.allocations[0].name]


def assert_input_refund(closed):
    allocation = closed.config.allocations[0]
    inventory = {a.asset: a.amount for a in closed.owned[0].inventory}
    for initial in allocation.initial_inventory.root:
        expected = initial.amount - (
            allocation.execution_fee.amount
            if initial.asset == allocation.execution_fee.asset
            else 0
        )
        assert inventory[initial.asset] == expected


def assert_undelivered_keeper(state):
    assert len(state.requests) == 1 and state.requests[0].status == "executed"
    assert_keeper_delivery_pending(state)
    assert not state.memory.values.get("held_at")


def assert_keeper_delivery_pending(state):
    assert state.delivered == len(state.outcomes) - 1
    assert state.outcomes[-1].kind == "gmx_request_outcome"


def assert_delivered_keeper(holding, interrupted):
    assert holding.requests == interrupted.requests and holding.owned == interrupted.owned
    assert holding.outcomes == interrupted.outcomes
    assert len(holding.memory.values["held_at"]) == 1
