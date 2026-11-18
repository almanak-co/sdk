"""Authentic keepers cancel unreachable GM bounds without losing escrow or replaying delivery."""

import signal
from contextlib import ExitStack
from pathlib import Path
from uuid import uuid4

import pytest
from eth_utils import keccak

from almanak.asset_amounts import AssetAmounts
from almanak.connectors.gmx_v2.liquidity_reader import handler
from almanak.connectors.gmx_v2.receipts import one_protocol_event
from tests import gmx_fork, gmx_liquidity_path
from tests.fork.support import processes, request_teardown, wait_state
from tests.fork.test_gmx_liquidity_recovery import (
    assert_currency_refund,
    assert_empty_positions,
    drive_keeper,
    launch_driver,
    settle_prior_deposits,
    source_for,
)
from tests.fork.test_gmx_requests import case, configuration
from tests.strategy_fork.harness import (
    bind_workspace,
    install_permissions,
    resolve_config,
    restart_gateway,
    resume,
    retain,
    start_gateway,
)

ROOT = Path(__file__).parents[2]
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]


@pytest.mark.parametrize("chain", ["arbitrum", "avalanche"])
@pytest.mark.parametrize("profile", ["safe_roles", "eoa"])
@pytest.mark.parametrize("kind", ["deposit", "withdrawal"])
def test_native_keeper_cancellation(request, tmp_path, chain, profile, kind):
    selected = case(f"gmx_v2_{chain}_liquidity")
    env = request.config.getoption("--fork-env").resolve()
    config = cancellation_config(selected, profile, kind, env, tmp_path / "resolution")
    config = resolve_config(config, env, {})
    artifact = (
        ROOT
        / "artifacts/strategy-fork"
        / (selected.name + "-keeper-cancel-" + kind + "-" + profile + "-" + uuid4().hex)
    )
    artifact.mkdir(parents=True)
    temporary = tmp_path / artifact.name
    temporary.mkdir()
    record = {
        "strategy": selected.name,
        "profile": profile,
        "kind": kind,
        "scenario": "keeper_cancellation_delivery",
        "status": "failed",
        "phases": [],
    }
    try:
        execute_case(selected, config, env, temporary, record, chain, kind)
        record["status"] = "passed"
    finally:
        retain(temporary, artifact, record)
        print("GMX keeper cancellation evidence:", artifact)


def cancellation_config(selected, profile, kind, env, directory):
    config = configuration(selected, profile, env, directory)
    allocation = config.allocations[0]
    if kind == "deposit":
        updated = allocation.model_copy(
            update={
                "minimum_market_tokens": allocation.minimum_market_tokens.model_copy(
                    update={"amount": 2**120}
                )
            }
        )
    else:
        minimums = list(allocation.withdrawal_minimums.root)
        minimums[1] = minimums[1].model_copy(
            update={"amount": 25 * 10 ** config.asset_metadata[minimums[1].asset].decimals}
        )
        updated = allocation.model_copy(
            update={"withdrawal_minimums": AssetAmounts(tuple(minimums))}
        )
    return type(config).model_validate_json(
        config.model_copy(update={"allocations": (updated,)}).model_dump_json()
    )


def execute_case(selected, config, env, temporary, record, chain, kind):
    with ExitStack() as stack:
        nodes, node = bind_workspace(config, env, stack, record)
        launch = stack.enter_context(processes(temporary))
        directory = temporary / "deployment"
        deployment = nodes.prepare(config, directory)
        journal, arguments = start_gateway(node, directory, launch, lease_seconds=30)
        if record["profile"] == "safe_roles":
            install_permissions(selected, node, directory, deployment, arguments)
            record["phases"].append("generated_permissions_installed_and_verified")
        run_cancellation(selected, node, journal, arguments, launch, record["phases"], chain, kind)


def run_cancellation(selected, node, journal, arguments, launch, phases, chain, kind):
    running = launch_driver(selected, node, arguments, launch, chain, kind, "outcome")
    settle_prior_deposits(node, journal, running, chain, kind)
    request = cancel_with_keeper(node, journal, running, chain)
    assert running.wait(timeout=180) == -signal.SIGKILL
    interrupted = journal.load()
    assert_cancelled_outcome(interrupted, request)
    restart_gateway(node, launch)
    running = resume(launch, journal, arguments)
    wait_state(journal, failed_delivered, running)
    request_teardown(arguments)
    if kind == "withdrawal":
        drive_keeper(node, journal, running, chain)
    closed = wait_state(journal, lambda s: s.mode == "closed", running)
    assert running.wait(timeout=20) == 0
    assert_empty_positions(closed)
    gmx_liquidity_path.assert_accounting(closed)
    gmx_liquidity_path.assert_cleanup(closed)
    phases.append(
        "authentic_keeper_cancellation_refund_committed_then_delivery_recovered_once_and_closed"
    )


def failed_delivered(state):
    return bool(state.memory.values.get("failed")) and state.delivered == len(state.outcomes)


def cancel_with_keeper(node, journal, running, chain):
    interval = 15 if chain == "avalanche" else 10
    state, request = gmx_fork.pending_request(node, journal, running, interval)
    target = state.config.target(request.allocation)
    source = source_for(request, chain)
    receipt = gmx_fork.execute_keeper(node, target, request, source)
    gmx_fork.retain_keeper(node, journal, request, source, receipt, block_interval=interval)
    assert receipt["status"] == "0x1"
    assert_cancelled_event(receipt, target, request)
    return request


def assert_cancelled_event(receipt, target, request):
    event = one_protocol_event(
        receipt, target, request.kind.title() + "Cancelled", handler(target, request.kind)
    )
    assert event.topics[0] == request.key
    error = {
        "deposit": "MinMarketTokens(uint256,uint256)",
        "withdrawal": "InsufficientOutputAmount(uint256,uint256)",
    }[request.kind]
    assert event.value("bytes", "reasonBytes")[:10] == "0x" + keccak(text=error)[:4].hex()


def assert_cancelled_outcome(state, request):
    outcome = state.outcomes[-1]
    assert state.delivered == len(state.outcomes) - 1
    assert outcome.kind == "gmx_request_outcome" and outcome.request.status == "cancelled"
    assert_refunded_outcome(outcome, request)


def assert_refunded_outcome(outcome, request):
    assert not outcome.accounting_issues
    if request.kind == "deposit":
        assert_currency_refund(outcome, request)
    else:
        assert (
            outcome.after.owned.market_tokens.amount
            == outcome.before.owned.market_tokens.amount + request.escrow[0].amount
        )
