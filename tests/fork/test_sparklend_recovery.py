"""Actual Spark process death before submission, lost broadcast response and delivery."""

import signal
import sys
import time
from contextlib import ExitStack
from pathlib import Path
from uuid import uuid4

import pytest

from tests.fork.support import processes, request_teardown, resume, wait_state
from tests.fork.test_aave_v3_base_eoa import eoa_configuration
from tests.strategy_fork.harness import (
    assert_recovered_once,
    bind_workspace,
    case_fork_config,
    discover,
    install_permissions,
    load_assertions,
    resolve_config,
    restart_gateway,
    retain,
    start_gateway,
)

ROOT = Path(__file__).parents[2]
DRIVER = ROOT / "tests/fork/driver.py"
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]


def interrupt(launch, case, arguments, stage, *, resumed=False):
    command = [sys.executable, str(DRIVER), "--strategy", str(case.strategy), *arguments]
    if resumed:
        command.append("--resume")
    child = launch([*command, "--crash", stage, "--operation", "borrow"])
    assert child.wait(timeout=180) == -signal.SIGKILL


def reserve_before_signing(case, node, journal, arguments, launch, record):
    interrupt(launch, case, arguments, "reserved")
    state = journal.load()
    assert state.pending is None and state.plan.operation.kind == "borrow"
    assert state.plan.before.position.debt_scaled == 0
    assert all(outcome.operation.kind != "borrow" for outcome in state.outcomes)
    record["reserved"] = state.model_dump(mode="json")
    record["phases"].append("borrow_reserved_before_signing_or_submission")
    return state.plan.attempt_id


def lose_broadcast_response(case, node, journal, arguments, launch, record, directory):
    node.gateway_process.kill()
    assert node.gateway_process.wait(timeout=20) == -signal.SIGKILL
    port = node.gateway_arguments[node.gateway_arguments.index("--port") + 1]
    killed_gateway = launch(
        [
            sys.executable,
            str(ROOT / "tests/fork/sparklend_gateway_driver.py"),
            "--directory",
            str(directory),
            "--port",
            port,
        ],
        ready="gateway_ready",
    )
    running = resume(launch, journal, arguments)
    assert killed_gateway.wait(timeout=180) == -signal.SIGKILL
    assert running.wait(timeout=30) != 0
    state = journal.load()
    pending, transaction = observed_broadcast(node, state)
    record["lost_response"] = state.model_dump(mode="json")
    record["broadcast_transaction"] = transaction
    record["phases"].append("gateway_died_after_broadcast_before_http_response")
    node.gateway_process = launch(node.gateway_arguments, ready="gateway_ready")
    assert journal.load() == state
    return pending


def observed_broadcast(node, state):
    pending = state.pending
    assert pending is not None and pending.operation.kind == "borrow"
    transaction = node.rpc("eth_getTransactionByHash", [pending.tx_hash])
    assert transaction is not None and transaction["hash"] == pending.tx_hash
    return pending, transaction


def commit_before_delivery(case, node, journal, arguments, launch, record, pending):
    wait_for_lease(journal)
    interrupt(launch, case, arguments, "commit", resumed=True)
    state = journal.load()
    assert state.delivered < len(state.outcomes)
    assert state.pending is None and state.outcomes[-1].tx_hash == pending.tx_hash
    assert state.outcomes[-1].operation == pending.operation
    record["committed"] = state.model_dump(mode="json")
    record["phases"].append("borrow_effects_committed_before_strategy_delivery")
    restart_gateway(node, launch)
    running = resume(launch, journal, arguments)
    assertions = load_assertions(case)
    holding = wait_state(journal, assertions.entered, running)
    assert_recovered_once(holding, pending.tx_hash)
    assertions.assert_holding(node, holding)
    request_teardown(arguments)
    closed = wait_state(journal, lambda state: state.mode == "closed", running)
    assert running.wait(timeout=20) == 0
    assertions.assert_closed(node, closed)
    assert_recovered_once(closed, pending.tx_hash)
    record["phases"].append("single_borrow_reconciled_delivered_and_closed")


def exercise(case, config, env, temporary, record):
    with ExitStack() as stack:
        nodes, node = bind_workspace(config, env, stack, record)
        launch = stack.enter_context(processes(temporary))
        directory = temporary / "deployment"
        deployment = nodes.prepare(config, directory)
        journal, arguments = start_gateway(node, directory, launch, lease_seconds=30)
        if record["account_profile"] == "safe_roles":
            install_permissions(case, nodes, directory, deployment, arguments)
            record["phases"].append("generated_permissions_installed_and_verified")
        identity = reserve_before_signing(case, node, journal, arguments, launch, record)
        pending = lose_broadcast_response(case, node, journal, arguments, launch, record, directory)
        assert pending.attempt_id == identity
        commit_before_delivery(case, node, journal, arguments, launch, record, pending)


@pytest.mark.parametrize("profile", ("eoa", "safe_roles"))
def test_spark_recovers_all_three_privileged_process_boundaries(request, tmp_path, profile):
    case = next(
        c
        for c in discover(ROOT / "strategies", ROOT / "tests/strategy_fork/scenarios")
        if c.name == "sparklend_ethereum_lending"
    )
    env = request.config.getoption("--fork-env").resolve()
    config = case_fork_config(case, env, tmp_path / "resolution")
    if profile == "eoa":
        config = eoa_configuration(config)
    config = resolve_config(config, env, {})
    artifact = ROOT / "artifacts/spark-promotion-recovery" / (profile + "-" + uuid4().hex)
    artifact.mkdir(parents=True)
    temporary = tmp_path / artifact.name
    temporary.mkdir()
    record = {"strategy": case.name, "account_profile": profile, "status": "failed", "phases": []}
    try:
        exercise(case, config, env, temporary, record)
        record["status"] = "passed"
    finally:
        retain(temporary, artifact, record)
        print("Spark process boundary evidence:", artifact)


def wait_for_lease(journal):
    deadline = time.monotonic() + 40
    while time.monotonic() < deadline:
        with journal.connect() as connection:
            identity, expires = connection.execute(
                "SELECT identity, expires FROM coordinator"
            ).fetchone()
        if identity is None or expires <= time.time():
            return
        time.sleep(0.05)
    raise AssertionError("dead coordinator retained its execution lease")
