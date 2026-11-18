"""Actual two-chain borrowing continues through chain-local outages and lost responses."""

import signal
import socket
import sys
import time
from pathlib import Path
from uuid import uuid4

import pytest

from almanak.gateway.client import GatewayClient
from almanak.gateway.fork_workspace import ForkSet
from almanak.state import Journal
from almanak.strategy import StrategyDefinition
from almanak.work import work_for
from tests.fork.support import CLI, processes, request_teardown, resume, wait_state
from tests.strategy_fork.adapter import fixture_plan_for, prepare_admitted_fork, resolve_reference
from tests.strategy_fork.harness import (
    discover,
    install_permissions,
    load_assertions,
    retain,
)

ROOT = Path(__file__).parents[2]
CASE = next(
    c
    for c in discover(ROOT / "strategies", Path(__file__).parent / "scenarios")
    if c.name == "aave_v4_multichain_borrowing"
)
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]


def launch_gateway(directory, launch):
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    arguments = [
        sys.executable,
        str(ROOT / "tests/fork/outage_gateway_driver.py"),
        "--directory",
        str(directory),
        "--port",
        str(port),
    ]
    process = launch(arguments, ready="gateway_ready")
    control = [
        "--config",
        str(directory / "deployment.json"),
        "--gateway-url",
        f"http://127.0.0.1:{port}",
        "--gateway-auth-file",
        str(directory / "gateway-auth.token"),
    ]
    return process, arguments, control


def healthy_borrowed(state, chain):
    return any(
        o.operation.kind == "borrow" and o.operation.target.fund.chain == chain
        for o in state.outcomes
    ) and state.delivered == len(state.outcomes)


def pending_unknown(directory, failed, state):
    pending = work_for(state, failed).pending
    return pending is not None and (directory / (failed.chain_id.reference + "-mined")).exists()


def independent_recovery(case, nodes, directory, failed_index, deployment, launch, record):
    assertions = load_assertions(case)
    ready = getattr(assertions, "independent_ready", healthy_borrowed)
    independent = getattr(assertions, "assert_independent_closed", assert_independent_teardown)
    gateway, arguments, control = launch_gateway(directory, launch)
    install_permissions(case, nodes, directory, deployment, control)
    register_strategy(case, directory, deployment, control)
    chains = tuple(b.chain for b in deployment.fork_bindings)
    failed, healthy = chains[failed_index], chains[1 - failed_index]
    down = directory / (failed.chain_id.reference + "-down")
    receipt_gate = directory / (failed.chain_id.reference + "-receipt")
    down.touch()
    journal = Journal(directory / "state.sqlite")
    runtime = launch([CLI, "service", "resume", *control])
    state = wait_state(journal, lambda s: ready(s, healthy), runtime)
    assert_failed_chain_unentered(state, failed)
    record["phases"].append("healthy_entry_during_other_chain_outage")
    receipt_gate.touch()
    down.unlink()
    started = time.monotonic()
    state = wait_state(
        journal, lambda s: pending_unknown(directory, failed, s), runtime, timeout=600
    )
    record["restored_entry_seconds"] = time.monotonic() - started
    pending = work_for(state, failed).pending
    inventory = retained_inventory(state, failed)
    down.touch()
    replace_interrupted_processes(runtime, gateway, arguments, launch)
    request_teardown(control)
    runtime = resume(launch, journal, control)
    state = wait_state(journal, lambda s: work_for(s, healthy).closed, runtime)
    independent(state, failed, healthy, pending)
    assert retained_inventory(state, failed) == inventory
    record["phases"].append("healthy_teardown_with_other_chain_unknown_submission")
    receipt_gate.unlink()
    down.unlink()
    closed = wait_state(journal, lambda s: s.mode == "closed", runtime)
    assert runtime.wait(timeout=20) == 0
    assert_recovered_once(closed, failed, pending)
    assertions.assert_closed(nodes, closed)
    record["phases"].append("same_pending_identity_recovered_once_and_all_obligations_closed")


def retained_inventory(state, chain):
    return tuple(b for b in getattr(state, "inventory", ()) if b.key.chain == chain)


def assert_failed_chain_unentered(state, failed):
    assert work_for(state, failed).fault.kind == "transport"
    assert not any(o.operation.target.fund.chain == failed for o in state.outcomes)


def register_strategy(case, directory, deployment, control):
    client = GatewayClient(control[3], directory / "gateway-auth.token")
    try:
        client.create(deployment, StrategyDefinition.read(case.strategy))
    finally:
        client.close()


def replace_interrupted_processes(runtime, gateway, arguments, launch):
    runtime.kill()
    assert runtime.wait(timeout=10) == -signal.SIGKILL
    gateway.send_signal(signal.SIGINT)
    assert gateway.wait(timeout=20) == 0
    launch(arguments, ready="gateway_ready")


def assert_independent_teardown(state, failed, healthy, pending):
    assert state.mode == "teardown" and work_for(state, failed).pending == pending
    assert all(
        not any((o.shares, o.drawn_shares, o.collateral_enabled))
        for o in state.owned
        if o.target.fund.chain == healthy
    )


def assert_recovered_once(closed, failed, pending):
    assert (
        sum(
            o.tx_hash == pending.tx_hash and o.operation.target.fund.chain == failed
            for o in closed.outcomes
        )
        == 1
    )


@pytest.mark.parametrize("failed_index", [0, 1], ids=["ethereum_outage", "avalanche_outage"])
def test_independent_fork_recovery(failed_index, request, tmp_path):
    env = request.config.getoption("--fork-env").resolve()
    operator = ROOT / "examples" / "operators" / f"{CASE.name}.fork.yaml"
    progress = resolve_reference(CASE.config, operator, env, tmp_path / "resolution")
    plan = fixture_plan_for(CASE.config, operator, progress)
    artifact = (
        ROOT
        / "artifacts/strategy-fork"
        / (CASE.name + "-outage-" + str(failed_index) + "-" + uuid4().hex)
    )
    artifact.mkdir(parents=True)
    record = {
        "strategy": CASE.name,
        "failed_chain_index": failed_index,
        "status": "failed",
        "phases": [],
    }
    try:
        with (
            ForkSet(plan, env) as nodes,
            processes(tmp_path) as launch,
        ):
            directory = tmp_path / "deployment"
            deployment = prepare_admitted_fork(
                CASE.config, CASE.strategy, operator, progress, plan, nodes, directory
            )
            assert deployment.fork_gas is not None
            assert deployment.fork_gas.gas == deployment.gas
            assert deployment.production is None
            independent_recovery(CASE, nodes, directory, failed_index, deployment, launch, record)
        record["status"] = "passed"
    finally:
        retain(tmp_path, artifact, record)
