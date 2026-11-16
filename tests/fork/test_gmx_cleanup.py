"""A committed approval remains a cleanup obligation after process replacement.

Request-sized approvals are escrowed in full by a settled request, so this crash
after the approval commits is how a perpetual and an exchange allocation each
reach their revocation.
"""

import signal
import sys
from contextlib import ExitStack
from pathlib import Path
from uuid import uuid4

import pytest

from almanak.strategy import StrategyDefinition
from tests.fork.support import processes, request_teardown, resume, wait_state
from tests.fork.test_gmx_requests import configuration
from tests.qa.gmx import eoa_configuration
from tests.strategy_fork.curve_assertions import assert_gas_cost
from tests.strategy_fork.harness import (
    bind_workspace,
    discover,
    install_permissions,
    resolve_config,
    restart_gateway,
    retain,
    start_gateway,
)

ROOT = Path(__file__).parents[2]
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]


# The approval each reference commits first, and the revocation teardown then completes.
CLEANUP = {
    "position": ("gmx_approval", "revoke_position"),
    "swap": ("gmx_exchange_approval", "revoke_exchange"),
}

REVOCATION = dict(CLEANUP.values())


def cleanup_kinds(name):
    return CLEANUP[name.rsplit("_", 1)[1]]


@pytest.mark.parametrize("path", ["position", "swap"])
@pytest.mark.parametrize("chain", ["arbitrum", "avalanche"])
@pytest.mark.parametrize("profile", ["eoa", "safe_roles"])
def test_gmx_approval_cleanup_after_committed_effect(request, tmp_path, chain, profile, path):
    name = f"gmx_v2_{chain}_{path}"
    case = next(
        value
        for value in discover(ROOT / "strategies", ROOT / "tests/strategy_fork/scenarios")
        if value.name == name
    )
    env = request.config.getoption("--fork-env").resolve()
    config = configuration(case, "safe_roles", env, tmp_path / "resolution")
    if profile == "eoa":
        config = eoa_configuration(config)
    config = resolve_config(config, env, {})
    artifact = ROOT / "artifacts/strategy-fork" / (name + "-cleanup-" + profile + "-" + uuid4().hex)
    artifact.mkdir(parents=True)
    temporary = tmp_path / artifact.name
    temporary.mkdir()
    record = {
        "strategy": name,
        "account_profile": profile,
        "scenario": "approval_commit_then_teardown",
        "status": "failed",
        "phases": [],
    }
    try:
        exercise(case, config, env, temporary, record)
        record["status"] = "passed"
    finally:
        retain(temporary, artifact, record)


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
        approval, _ = cleanup_kinds(case.name)
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
                approval,
            ]
        )
        assert interrupted.wait(timeout=180) == -signal.SIGKILL
        accepted = journal.load()
        assert_approval_committed(accepted, approval)
        record["interrupted_checkpoint"] = accepted.model_dump(mode="json")
        record["phases"].append("approval_committed_before_outcome_delivery")
        restart_gateway(node, launch)
        request_teardown(arguments)
        running = resume(launch, journal, arguments)
        closed = wait_state(journal, lambda state: state.mode == "closed", running, timeout=180)
        assert running.wait(timeout=20) == 0
        assert closed.strategy == StrategyDefinition.read(case.strategy)
        assert_cleanup(node, accepted, closed)
        record["phases"].append("replaced_runtime_and_gateway_revoked_without_trading")


def assert_approval_committed(state, approval):
    assert len(state.outcomes) == 1 and state.delivered == 0
    outcome = state.outcomes[0]
    assert outcome.success and outcome.operation.kind == approval
    assert outcome.operation.amount.amount > 0 and outcome.after.needs_cleanup
    assert state.pending is None


def assert_cleanup(node, before, closed):
    from types import SimpleNamespace

    from almanak.connectors.gmx_v2.observations import snapshot
    from almanak.gateway.ethereum import EthereumReader

    assert_closed_effects(before, closed)
    assert not closed.requests
    with EthereumReader(closed.owned[0].target.fund.chain, node.rpc.endpoint) as reader:
        backend = SimpleNamespace(config=closed.config, reader=reader)
        block = reader.pin_block("latest")
        for owned, initial, final in zip(
            closed.owned, closed.baseline, closed.closure, strict=True
        ):
            observed = snapshot(backend, owned.target.allocation, block, owned)
            assert_no_live_obligations(observed)
            assert_retained_cleanup(before, owned, final, observed)
            assert_wallet_costs(closed, owned, initial, final)


def assert_no_live_obligations(observed):
    assert getattr(observed, "position", None) is None
    assert not observed.needs_cleanup
    assert observed.pending_order is None
    assert observed.request is None


def assert_retained_cleanup(before, owned, final, observed):
    assert observed.wallets == final.wallets
    assert getattr(owned, "position", None) is None
    assert not final.needs_cleanup
    assert owned.inventory == before.owned[0].inventory


def assert_wallet_costs(closed, owned, initial, final):
    events = [o for o in closed.outcomes if o.execution.submitter == initial.payer]
    assert_gas_cost(events, initial, final)
    for first, last in zip(initial.wallets, final.wallets, strict=True):
        assert_wallet_delta(first, last, gas_from_fund(events, first.asset, initial, owned))


def assert_wallet_delta(first, last, expected):
    assert first.asset == last.asset
    assert first.amount - last.amount == expected


def gas_from_fund(events, asset, initial, owned):
    if asset.namespace != "slip44" or initial.payer != owned.target.fund:
        return 0
    return sum(outcome.gas_paid.amount for outcome in events)


def assert_closed_effects(before, closed):
    assert_closed_delivery(before, closed)
    revoked = closed.outcomes[1]
    assert_revocation(revoked, before.outcomes[0])
    assert_completed_result(revoked)
    for outcome in closed.outcomes:
        assert_no_protocol_effects(outcome)


def assert_closed_delivery(before, closed):
    assert closed.pending is None
    assert closed.delivered == len(closed.outcomes) == 2
    assert closed.outcomes[0] == before.outcomes[0]


def assert_revocation(revoked, approved):
    assert revoked.success
    assert revoked.operation.kind == approved.operation.kind
    assert revoked.operation.amount.amount == 0
    assert revoked.operation.target == approved.operation.target


def assert_completed_result(revoked):
    assert not revoked.after.needs_cleanup
    assert revoked.strategy_result().intent.kind == REVOCATION[revoked.operation.kind]
    assert revoked.strategy_result().status == "completed"


def assert_no_protocol_effects(outcome):
    assert not outcome.accounting_issues
    assert all(delta.amount == 0 for delta in outcome.assets)
