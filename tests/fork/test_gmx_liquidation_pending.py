"""A retained local close broadcast survives liquidation and subsequent keeper cancellation."""

import signal
import sys
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from tests import gmx_fork
from tests.fork.support import processes, request_teardown, wait_state
from tests.fork.test_gmx_liquidation_capture import capture_liquidation, source_after
from tests.fork.test_gmx_liquidation_runtime import wait_expired_lease
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


@pytest.mark.parametrize("chain", ["arbitrum", "avalanche"])
@pytest.mark.parametrize("profile", ["eoa", "safe_roles"])
def test_liquidation_preserves_pending_close(request, tmp_path, chain, profile):
    selected = case(f"gmx_v2_{chain}_position")
    env = request.config.getoption("--fork-env").resolve()
    config = resolve_config(configuration(selected, profile, env, tmp_path / "resolution"), env, {})
    artifact = (
        ROOT
        / "artifacts/strategy-fork"
        / (selected.name + "-liquidation-pending-" + profile + "-" + uuid4().hex)
    )
    artifact.mkdir(parents=True)
    temporary = tmp_path / artifact.name
    temporary.mkdir()
    record = {
        "strategy": selected.name,
        "profile": profile,
        "status": "failed",
        "phases": [],
        "scope": "actual reference pending-close liquidation recovery",
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
            wait_state(journal, gmx_fork.entered, running)
            running.kill()
            assert running.wait(timeout=20) == -signal.SIGKILL
            request_teardown(arguments)
            pending = retained_close_broadcast(selected, node, journal, arguments, launch)
            (artifact / "pending-checkpoint.json").write_text(pending.model_dump_json(indent=2))
            capture_liquidation(node, pending, directory, chain)
            committed = commit_liquidation(node, journal, arguments, launch)
            assert_created_once_before_liquidation(pending, committed)
            (artifact / "external-checkpoint.json").write_text(committed.model_dump_json(indent=2))
            cancel_empty_position(node, journal, committed, chain)
            restart_gateway(node, launch)
            running = resume(launch, journal, arguments)
            closed = wait_state(journal, lambda s: s.mode == "closed", running)
            assert running.wait(timeout=20) == 0
            assert_pending_closure(closed, pending.pending.tx_hash)
            record["phases"].append(
                "original_close_broadcast_reconciled_before_external_liquidation_then_keeper_cancellation_and_clean_closure"
            )
            record["status"] = "passed"
    finally:
        retain(temporary, artifact, record)
        captured = temporary / "deployment/external-liquidation.json"
        if captured.exists():
            (artifact / "external-liquidation.json").write_bytes(captured.read_bytes())
        print("GMX liquidation pending evidence:", artifact)


def retained_close_broadcast(selected, node, journal, arguments, launch):
    restart_gateway(node, launch)
    wait_expired_lease(journal)
    running = launch(
        [
            sys.executable,
            str(ROOT / "tests/fork/driver.py"),
            "--strategy",
            str(selected.strategy),
            *arguments,
            "--resume",
            "--crash",
            "broadcast",
            "--operation",
            "gmx_create_order",
        ]
    )
    assert running.wait(timeout=180) == -signal.SIGKILL
    state = journal.load()
    assert (
        state.pending.operation.order_type == 4 and state.pending.before.owned.position is not None
    )
    return state


def commit_liquidation(node, journal, arguments, launch):
    restart_gateway(node, launch)
    wait_expired_lease(journal)
    running = launch(
        [
            sys.executable,
            str(ROOT / "tests/fork/liquidation_driver.py"),
            *arguments,
            "--crash",
            "external_commit",
        ],
        ready=False,
    )
    assert running.wait(timeout=180) == -signal.SIGKILL
    return journal.load()


def assert_created_once_before_liquidation(pending, committed):
    original = pending.pending
    transactions = [o for o in committed.outcomes if o.tx_hash == original.tx_hash]
    assert len(transactions) == 1 and transactions[0].operation == original.operation
    assert_admitted_close(committed, original, transactions[0])
    assert_external_commit(committed)


def assert_admitted_close(state, original, transaction):
    assert state.pending is None
    assert state.decision == original.decision
    assert state.requests[-1] == transaction.request
    assert state.requests[-1].unresolved


def assert_external_commit(state):
    assert state.outcomes[-1].kind == "gmx_liquidation_outcome"
    assert state.delivered == len(state.outcomes) - 1
    assert state.owned[0].position is None


def assert_pending_closure(state, original_hash):
    assert state.requests[-1].status == "cancelled"
    assert state.owned[0].position is None
    assert all(not o.accounting_issues for o in state.outcomes)
    assert_once_delivery(state, original_hash)


def assert_once_delivery(state, original_hash):
    assert sum(o.kind == "gmx_liquidation_outcome" for o in state.outcomes) == 1
    assert sum(o.tx_hash == original_hash for o in state.outcomes) == 1
    assert state.delivered == len(state.outcomes)


def cancel_empty_position(node, journal, state, chain):
    request = state.requests[-1]
    latest = int(node.rpc("eth_getBlockByNumber", ["latest", False])["timestamp"], 16)
    source = source_after(chain, latest)
    receipt = gmx_fork.execute_keeper(
        node, state.config.target(request.allocation), request, source
    )
    gmx_fork.retain_keeper(node, journal, request, source, receipt)
    assert receipt["status"] == "0x1"
    from almanak.connectors.gmx_v2.receipts import one_protocol_event

    target = state.config.target(request.allocation)
    event = one_protocol_event(
        receipt, target, "OrderCancelled", target.market.contracts.order_handler
    )
    assert event.topics[0] == request.key
