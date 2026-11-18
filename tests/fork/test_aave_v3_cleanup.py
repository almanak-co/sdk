"""Approval-only interruption leaves no position and must revoke before closure."""

import signal
import sys
from contextlib import ExitStack
from pathlib import Path
from uuid import uuid4

import pytest

from almanak.strategy import StrategyDefinition
from tests.fork.support import processes, request_teardown, resume, wait_state
from tests.fork.test_aave_v3_base_eoa import eoa_configuration
from tests.strategy_fork.harness import (
    bind_workspace,
    case_fork_config,
    discover,
    install_permissions,
    resolve_config,
    restart_gateway,
    retain,
    start_gateway,
)
from tests.strategy_fork.pool_lending_lifecycle import (
    assert_payer_effects,
    assert_successful_outcomes,
    assert_zero_obligations,
    assert_zero_owned,
)

ROOT = Path(__file__).parents[2]
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]


@pytest.mark.parametrize(
    "chain", ["ethereum", "base", "arbitrum", "optimism", "bnb", "polygon", "avalanche"]
)
@pytest.mark.parametrize("profile", ["eoa", "safe_roles"])
def test_aave_approval_cleanup_after_committed_effect(request, tmp_path, chain, profile):
    name = f"aave_v3_{chain}_lending"
    case = next(
        value
        for value in discover(ROOT / "strategies", ROOT / "tests/strategy_fork/scenarios")
        if value.name == name
    )
    env = request.config.getoption("--fork-env").resolve()
    config = case_fork_config(case, env, tmp_path / "resolution")
    if profile == "eoa":
        config = eoa_configuration(config)
    config = resolve_config(config, env, {})
    artifact = ROOT / "artifacts/strategy-fork" / (name + "-cleanup-" + profile + "-" + uuid4().hex)
    artifact.mkdir(parents=True)
    temporary = tmp_path / artifact.name
    temporary.mkdir()
    record = dict(
        strategy=name,
        account_profile=profile,
        scenario="approval_commit_then_teardown",
        status="failed",
        phases=[],
    )
    try:
        exercise(case, config, env, temporary, record)
        record["status"] = "passed"
    finally:
        retain(temporary, artifact, record)
        print("Aave cleanup evidence:", artifact)


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
                "approve",
            ]
        )
        assert interrupted.wait(timeout=180) == -signal.SIGKILL
        accepted = journal.load()
        assert_approval_committed(accepted)
        record["interrupted_checkpoint"] = accepted.model_dump(mode="json")
        record["phases"].append("approval_committed_before_outcome_delivery")
        restart_gateway(node, launch)
        request_teardown(arguments)
        running = resume(launch, journal, arguments)
        closed = wait_state(journal, lambda state: state.mode == "closed", running, timeout=180)
        assert running.wait(timeout=20) == 0
        assert closed.strategy == StrategyDefinition.read(case.strategy)
        assert_cleanup(accepted, closed)
        record["phases"].append("replaced_runtime_and_gateway_revoked_without_economic_exposure")


def assert_approval_committed(state):
    assert len(state.outcomes) == 1 and state.delivered == 0
    outcome = state.outcomes[0]
    assert outcome.success and outcome.operation.kind == "approve"
    assert outcome.operation.amount.amount > 0 and outcome.after.allowance > 0
    assert state.pending is None
    assert_zero_owned(state)


def assert_cleanup(before, closed):
    assert (closed.mode, closed.pending, closed.plan, closed.decision) == (
        "closed",
        None,
        None,
        None,
    )
    assert closed.delivered == len(closed.outcomes) == 2
    assert closed.outcomes[0] == before.outcomes[0]
    assert_revocation(before, closed.outcomes[1])
    assert_successful_outcomes(closed)
    assert_zero_owned(closed)
    assert_cleanup_balances(closed)


def assert_revocation(before, revoked):
    assert revoked.operation.kind == "revoke" and revoked.after.allowance == 0
    assert revoked.operation.target == before.outcomes[0].operation.target


def assert_cleanup_balances(closed):
    for initial, final in zip(closed.baseline, closed.closure, strict=True):
        assert_zero_obligations(final)
        assert final.wallet_balance == initial.wallet_balance
        assert_payer_effects(closed, initial, final)
