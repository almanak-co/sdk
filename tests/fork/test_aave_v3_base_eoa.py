"""Explicit EOA Base fork proof, without requesting production payer authority."""

from contextlib import ExitStack
from pathlib import Path
from uuid import uuid4

import pytest

from almanak.gateway.fork_config import ForkEoaFund
from almanak.strategy import StrategyDefinition
from tests.fork.support import processes, request_teardown, resume, wait_state
from tests.strategy_fork.harness import (
    align_teardown,
    assert_recovered_once,
    bind_workspace,
    case_fork_config,
    discover,
    load_assertions,
    resolve_config,
    restart_gateway,
    retain,
    start_gateway,
    start_runtime,
    stop_runtime,
    verify_hold,
)

ROOT = Path(__file__).parents[2]
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]


def eoa_configuration(config):
    funds = []
    for index, fund in enumerate(config.funds):
        operator = next(o for o in config.operators if o.name == fund.operator)
        funds.append(
            ForkEoaFund(
                account=fund.account,
                native_balance=operator.native_balance,
                assets=fund.assets,
                foundry_account_index=941167 + index,
            )
        )
    return type(config).model_validate(
        {**config.model_dump(), "funds": tuple(funds), "operators": ()}
    )


def eoa_lifecycle(case, node, assertions, journal, arguments, launch, phases):
    running, recovered = start_runtime(case, node, assertions, journal, arguments, launch, phases)
    holding = wait_state(
        journal, assertions.entered, running, timeout=getattr(assertions, "entry_timeout", 180)
    )
    assert_recovered_once(holding, recovered)
    assert holding.strategy == StrategyDefinition.read(case.strategy)
    verify_hold(node, assertions, journal, running, holding)
    phases.append("entered_and_held")
    running = restart_held_runtime(
        node, assertions, journal, arguments, launch, phases, running, holding
    )
    exercise = getattr(assertions, "exercise_hold", None)
    if exercise is not None:
        exercise(node, journal, running)
        phases.append("timed_strategy_exit_verified")
    align_teardown(assertions, node, journal, running, phases)
    request_teardown(arguments)
    closed = wait_state(
        journal,
        lambda state: state.mode == "closed",
        running,
        timeout=getattr(assertions, "teardown_timeout", 180),
    )
    assert running.wait(timeout=getattr(assertions, "process_timeout", 20)) == 0
    assertions.assert_closed(node, closed)
    phases.append("closed")


def restart_held_runtime(node, assertions, journal, arguments, launch, phases, running, holding):
    for replace_gateway in (False, True):
        stop_runtime(running, timeout=getattr(assertions, "process_timeout", 20))
        if replace_gateway:
            restart_gateway(node, launch, timeout=getattr(assertions, "process_timeout", 20))
        running = resume(launch, journal, arguments)
        verify_hold(node, assertions, journal, running, holding)
        phases.append("gateway_replaced_and_resumed" if replace_gateway else "runtime_resumed")
    return running


@pytest.mark.parametrize("chain", ["base", "arbitrum"])
def test_base_eoa_fork_lifecycle(request, tmp_path, chain):
    case = next(
        c
        for c in discover(ROOT / "strategies", ROOT / "tests/strategy_fork/scenarios")
        if c.name == f"aave_v3_{chain}_lending"
    )
    env = request.config.getoption("--fork-env").resolve()
    config = resolve_config(
        eoa_configuration(case_fork_config(case, env, tmp_path / "resolution")), env, {}
    )
    artifact = ROOT / "artifacts/strategy-fork" / (case.name + "-eoa-" + uuid4().hex)
    artifact.mkdir(parents=True)
    temporary = tmp_path / artifact.name
    temporary.mkdir()
    record = {
        "strategy": case.name,
        "account_profile": "eoa",
        "authority": "identified Anvil fixture only; no production authority",
        "onchain_roles_quotas": "not_applicable_to_explicit_eoa_profile",
        "status": "failed",
        "phases": [],
    }
    try:
        with ExitStack() as stack:
            nodes, node = bind_workspace(config, env, stack, record)
            launch = stack.enter_context(processes(temporary))
            directory = temporary / "deployment"
            deployment = nodes.prepare(config, directory)
            assert all(b.profile.kind == "eoa" for b in deployment.accounts.bindings)
            journal, arguments = start_gateway(node, directory, launch, lease_seconds=30)
            eoa_lifecycle(
                case, node, load_assertions(case), journal, arguments, launch, record["phases"]
            )
            record["status"] = "passed"
    finally:
        retain(temporary, artifact, record)
        print("EOA fork evidence:", artifact)
