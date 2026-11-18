"""EOA settlement and Safe cancellation use the reusable GMX strategy and trusted services."""

from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from tests.fork.support import processes, request_teardown, wait_state
from tests.fork.test_aave_v3_base_eoa import eoa_configuration, restart_held_runtime
from tests.strategy_fork.harness import (
    Case,
    align_teardown,
    assert_recovered_once,
    bind_workspace,
    install_permissions,
    load_assertions,
    resolve_config,
    retain,
    start_gateway,
    start_runtime,
    verify_hold,
)

ROOT = Path(__file__).parents[2]
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]


def case(name):
    root = ROOT / "strategies" / name
    return Case(
        name,
        root / "strategy.py",
        root / "config.yaml",
        ROOT / "tests/strategy_fork/scenarios" / name / "assertions.py",
    )


@pytest.mark.parametrize("name", ["gmx_v2_arbitrum_position", "gmx_v2_avalanche_position"])
@pytest.mark.parametrize("mode", ["eoa", "safe_cancel_recovery"])
def test_gmx_reference_services(request, tmp_path, mode, name):
    selected = case(name)
    env = request.config.getoption("--fork-env").resolve()
    config = configuration(selected, mode, env, tmp_path / "resolution")
    config = resolve_config(config, env, {})
    artifact = ROOT / "artifacts/strategy-fork" / (selected.name + "-" + mode + "-" + uuid4().hex)
    artifact.mkdir(parents=True)
    temporary = tmp_path / artifact.name
    temporary.mkdir()
    record = {"strategy": selected.name, "profile": mode, "status": "failed", "phases": []}
    try:
        with ExitStack() as stack:
            nodes, node = bind_workspace(config, env, stack, record)
            launch = stack.enter_context(processes(temporary))
            directory = temporary / "deployment"
            deployment = nodes.prepare(config, directory)
            journal, arguments = start_gateway(node, directory, launch, lease_seconds=2)
            run_mode(
                mode, selected, node, directory, deployment, journal, arguments, launch, record
            )
            record["status"] = "passed"
    finally:
        retain(temporary, artifact, record)
        print("GMX reference evidence:", artifact)


def configuration(selected, mode, env, directory):
    """The public root plus the shipped fork operator example decide every value."""
    from tests.strategy_fork.adapter import fork_config_for, resolve_reference

    operator = ROOT / "examples/operators" / (selected.name + ".fork.yaml")
    config = fork_config_for(
        selected.config, operator, resolve_reference(selected.config, operator, env, directory)
    )
    if mode == "eoa":
        config = eoa_configuration(config)
        config = config.model_copy(
            update={
                "funds": tuple(
                    f.model_copy(update={"foundry_account_index": 941078 + i})
                    for i, f in enumerate(config.funds)
                )
            }
        )
    return config


def run_mode(mode, selected, node, directory, deployment, journal, arguments, launch, record):
    assertions = load_assertions(selected)
    if mode == "eoa":
        assert all(b.profile.kind == "eoa" for b in deployment.accounts.bindings)
        settle_eoa(selected, node, assertions, journal, arguments, launch, record["phases"])
        return
    install_permissions(selected, node, directory, deployment, arguments)
    record["phases"].append("generated_permissions_installed_and_verified")
    cancel_recovered(selected, node, journal, arguments, launch, record["phases"])


def cancel_recovered(selected, node, journal, arguments, launch, phases):
    node.rpc("anvil_setBlockTimestampInterval", [1])
    assertions = SimpleNamespace(crash_operation="gmx_create_order", crash_stage="broadcast")
    running, recovered_hash = start_runtime(
        selected, node, assertions, journal, arguments, launch, phases
    )
    accepted = wait_state(
        journal, lambda s: bool(s.requests) and s.delivered == len(s.outcomes), running
    )
    assert_recovered_once(accepted, recovered_hash)
    assert len(accepted.requests) == 1 and accepted.requests[0].unresolved
    key = accepted.requests[0].key
    phases.append("accepted_request_recovered_without_duplicate_creation")
    request_teardown(arguments)
    timestamp = int(node.rpc("eth_getBlockByNumber", ["latest", False])["timestamp"], 16)
    node.rpc("evm_setNextBlockTimestamp", [timestamp + 301])
    node.rpc("evm_mine")
    closed = wait_state(journal, lambda s: s.mode == "closed", running)
    assert running.wait(timeout=20) == 0
    assert_cancelled(closed, key)
    phases.append("exact_retained_request_cancelled_and_closed")


def assert_cancelled(state, key):
    assert len(state.requests) == 1
    assert state.requests[0].key == key and state.requests[0].status == "cancelled"
    assert all(o.position is None for o in state.owned)
    assert not any(o.accounting_issues for o in state.outcomes)
    assert operation_count(state, "gmx_create_order") == 1
    assert operation_count(state, "gmx_cancel_order") == 1


def settle_eoa(selected, node, assertions, journal, arguments, launch, phases):
    running, recovered = start_runtime(
        selected, node, assertions, journal, arguments, launch, phases
    )
    assertions.drive_external(node, journal, running)
    holding = wait_state(journal, assertions.entered, running)
    assert_recovered_once(holding, recovered)
    verify_hold(node, assertions, journal, running, holding)
    phases.append("entered_and_held")
    running = restart_held_runtime(
        node, assertions, journal, arguments, launch, phases, running, holding
    )
    align_teardown(assertions, node, journal, running, phases)
    request_teardown(arguments)
    assertions.drive_external(node, journal, running)
    closed = wait_state(journal, lambda state: state.mode == "closed", running)
    assert running.wait(timeout=20) == 0
    assertions.assert_closed(node, closed)
    phases.append("closed")


def operation_count(state, kind):
    return sum(o.operation.kind == kind for o in state.outcomes)
