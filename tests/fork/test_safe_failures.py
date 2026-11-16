"""Actual gateway refusal and independent observations around changed Safe authority."""

import signal
import sys
from types import SimpleNamespace

import pytest

from almanak.gateway.fork_fixture import Fork
from almanak.gateway.safe_fixture import calldata, owner_transaction
from almanak.runtime import Runtime
from tests.author_evidence import retained_author
from tests.fork.support import processes, start_gateway
from tests.fork.test_safe_recovery import (
    DRIVER,
    ROOT,
    STRATEGY,
    await_expired_lease,
    one_safe_config,
    replace_gateway,
)
from tests.strategy_fork.harness import Case, install_permissions, retain

pytestmark = pytest.mark.fork


@pytest.fixture
def safe_deployment(request, tmp_path):
    config = one_safe_config()
    with (
        Fork(
            request.config.getoption("--fork-env"), config.origin, target=config.single_target()
        ) as node,
        processes(tmp_path) as launch,
    ):
        directory = tmp_path / "deployment"
        deployment = node.prepare(config, directory)
        journal, arguments = start_gateway(node, directory, launch)
        case = Case("safe-failure", STRATEGY, retained_author(STRATEGY, "config.json"), __file__)
        install_permissions(case, node, directory, deployment, arguments)
        yield SimpleNamespace(
            node=node,
            directory=directory,
            config=deployment,
            journal=journal,
            arguments=arguments,
            launch=launch,
            temporary=tmp_path,
        )


def interrupt_operation(run, stage, operation="borrow"):
    crashed = run.launch(
        [
            sys.executable,
            str(DRIVER),
            "--strategy",
            str(STRATEGY),
            *run.arguments,
            "--crash",
            stage,
            "--operation",
            operation,
        ]
    )
    assert crashed.wait(timeout=100) == -signal.SIGKILL
    await_expired_lease(run.journal)
    return run.journal.load()


def owner_change(run, signature, types, args):
    profile = run.config.accounts.bindings[0].profile
    return owner_transaction(
        run.node.rpc,
        run.node.fixture_owners[run.config.accounts.bindings[0].name],
        profile.fund.address,
        profile.roles_module,
        calldata(signature, types, args),
    )


def save_failure(run, name, extra):
    artifact = ROOT / "artifacts/safe-failures" / f"{name}-{run.journal.load().deployment_id}"
    artifact.mkdir(parents=True)
    retain(run.temporary, artifact, {"status": "passed", "case": name, **extra})


@pytest.mark.parametrize("stage", ["reserved", "prepared"])
def test_revocation_blocks_signing_or_broadcast_but_keeps_reconciliation_reads(
    safe_deployment, stage
):
    run = safe_deployment
    before = interrupt_operation(run, stage)
    profile = run.config.accounts.bindings[0].profile
    receipt = owner_change(
        run,
        "assignRoles(address,bytes32[],bool[])",
        ("address", "bytes32[]", "bool[]"),
        (profile.operator.address, [bytes.fromhex(profile.role_key[2:])], [False]),
    )
    replace_gateway(run.node, run.launch)
    resumed = run.launch(
        [sys.executable, str(DRIVER), "--strategy", str(STRATEGY), *run.arguments, "--resume"]
    )
    assert resumed.wait(timeout=40) != 0
    assert run.journal.load() == before
    runner = Runtime(run.arguments[3], run.directory / "gateway-auth.token")
    try:
        observed = runner.gateway.observe()
        assert len(observed) == len(run.config.positions)
        assert_pending_unsubmitted(run, before)
    finally:
        runner.close()
    save_failure(run, f"revoked-{stage}", {"revocation_receipt": receipt})


def assert_pending_unsubmitted(run, state):
    if state.pending is not None:
        assert run.node.rpc("eth_getTransactionByHash", [state.pending.tx_hash]) is None


def enter_and_stop(run):
    from tests.fork.support import wait_state
    from tests.strategy_fork.scenarios.aave_v4_weth_supply.assertions import entered

    running = run.launch([sys.executable, str(DRIVER), "--strategy", str(STRATEGY), *run.arguments])
    holding = wait_state(run.journal, entered, running)
    running.kill()
    running.wait(timeout=10)
    await_expired_lease(run.journal)
    return holding


def resume_driver(run):
    return run.launch(
        [sys.executable, str(DRIVER), "--strategy", str(STRATEGY), *run.arguments, "--resume"]
    )


def set_teardown(run):
    runner = Runtime(run.arguments[3], run.directory / "gateway-auth.token")
    try:
        runner.journal.teardown()
    finally:
        runner.close()


def test_missing_repayment_funding_preserves_main_debt_and_closes_bluechip(safe_deployment):
    from almanak.gateway.safe_funding import USDC, fund_usdc_storage

    run = safe_deployment
    enter_and_stop(run)
    profile = run.config.accounts.bindings[0].profile
    fund_usdc_storage(run.node, profile.fund.address, USDC, 0)
    run.node.rpc("evm_mine")
    set_teardown(run)
    failed = resume_driver(run)
    assert failed.wait(timeout=60) != 0
    state = run.journal.load()
    main, blue = state.config.allocations
    assert state.shares(state.config.target(main.name)) > 0
    assert state.shares(state.config.target(blue.name)) == 0
    assert_unpaid_debt(state)
    save_failure(run, "repayment-funding", {"fault": "Anvil-only removal of simulated USDC cash"})


def assert_unpaid_debt(state):
    assert any(o.drawn_shares > 0 for o in state.owned)
    assert not any(o.operation.kind == "repay" for o in state.outcomes)


def test_timed_exit_repays_main_before_release_and_keeps_bluechip(safe_deployment):
    from tests.fork.support import wait_state
    from tests.strategy_fork.scenarios.aave_v4_weth_supply.assertions import assert_closed

    run = safe_deployment
    enter_and_stop(run)
    run.node.rpc("evm_increaseTime", [301])
    run.node.rpc("evm_mine")
    running = resume_driver(run)
    exited = wait_state(run.journal, main_closed, running)
    assert exited.shares(exited.config.target(exited.config.allocations[1].name)) > 0
    assert_repay_before_release(exited)
    set_teardown_via_client(run)
    closed = wait_state(run.journal, lambda s: s.mode == "closed", running)
    assert running.wait(timeout=20) == 0
    assert_closed(run.node, closed)
    save_failure(run, "timed-exit", {})


def main_closed(state):
    target = state.config.target(state.config.allocations[0].name)
    return state.shares(target) == 0 and state.delivered == len(state.outcomes)


def assert_repay_before_release(state):
    repaid = first_operation(state, "repay")
    main = first_operation(state, "withdraw", state.config.allocations[0].name)
    assert repaid < main


def first_operation(state, kind, allocation=None):
    for index, outcome in enumerate(state.outcomes):
        if outcome.operation.kind != kind:
            continue
        if allocation is None or outcome.operation.allocation == allocation:
            return index
    raise AssertionError("required economic operation was not recorded")


def set_teardown_via_client(run):
    from almanak.gateway.client import GatewayClient

    client = GatewayClient(run.arguments[3], run.directory / "gateway-auth.token")
    try:
        client.teardown()
    finally:
        client.close()


def use_proxy(run, proxy):
    args = list(run.node.gateway_arguments)
    args[args.index("--fork-url") + 1] = proxy.url
    run.node.gateway_arguments = args
    replace_gateway(run.node, run.launch)


def test_price_read_outage_blocks_borrowing(safe_deployment):
    from tests.fork.read_faults import ReadFaultProxy

    run = safe_deployment
    before = interrupt_operation(run, "reserved")
    with ReadFaultProxy(run.node.rpc.endpoint, ["getReservesPrices(uint256[])"]) as proxy:
        use_proxy(run, proxy)
        failed = resume_driver(run)
        assert failed.wait(timeout=40) != 0
        assert proxy.refused > 0
        assert run.journal.load() == before
        save_failure(run, "borrow-price-outage", {"refused_reads": proxy.refused})


def test_health_read_outage_still_allows_funded_repayment(safe_deployment):
    from tests.fork.read_faults import ReadFaultProxy
    from tests.fork.support import wait_state
    from tests.strategy_fork.scenarios.aave_v4_weth_supply.assertions import assert_closed

    run = safe_deployment
    enter_and_stop(run)
    with ReadFaultProxy(run.node.rpc.endpoint, ["getUserAccountData(address)"]) as proxy:
        use_proxy(run, proxy)
        set_teardown(run)
        running = resume_driver(run)
        closed = wait_state(run.journal, lambda s: s.mode == "closed", running)
        assert running.wait(timeout=20) == 0
        assert proxy.refused > 0
        assert_closed(run.node, closed)
        assert any(o.after.health_error for o in closed.outcomes)
        save_failure(run, "repay-health-outage", {"refused_reads": proxy.refused})


def test_injected_health_breach_exits_main_through_the_real_strategy(safe_deployment):
    from tests.fork.read_faults import HealthShockProxy
    from tests.fork.support import wait_state
    from tests.strategy_fork.scenarios.aave_v4_weth_supply.assertions import assert_closed

    run = safe_deployment
    enter_and_stop(run)
    with HealthShockProxy(
        run.node.rpc.endpoint, run.config.debts[0].minimum_health_factor - 1
    ) as proxy:
        use_proxy(run, proxy)
        running = resume_driver(run)
        exited = wait_state(run.journal, main_closed, running)
        assert_repay_before_release(exited)
        assert exited.shares(exited.config.target(exited.config.allocations[1].name)) > 0
        set_teardown_via_client(run)
        closed = wait_state(run.journal, lambda s: s.mode == "closed", running)
        assert running.wait(timeout=20) == 0
        assert_closed(run.node, closed)
        save_failure(
            run,
            "health-triggered-exit",
            {
                "injected_health_observations": proxy.changed,
                "limitation": "Injected health; real fork repayment; no liquidation model.",
            },
        )
