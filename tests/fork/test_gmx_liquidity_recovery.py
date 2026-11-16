"""GM liquidity cancellation and keeper delivery survive native process recovery."""

import gzip
import json
import signal
import sys
from contextlib import ExitStack
from pathlib import Path
from uuid import uuid4

import pytest

from tests import gmx_fork, gmx_liquidity_path, gmx_position_path
from tests.fork.support import processes, request_teardown, wait_state
from tests.fork.test_gmx_requests import case, configuration, operation_count
from tests.retained_fixtures import retained
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
@pytest.mark.parametrize("scenario", ["cancellation", "keeper_delivery"])
@pytest.mark.parametrize("kind", ["deposit", "withdrawal"])
def test_gmx_liquidity_recovery(request, tmp_path, chain, profile, scenario, kind):
    selected = case(f"gmx_v2_{chain}_liquidity")
    env = request.config.getoption("--fork-env").resolve()
    config = resolve_config(configuration(selected, profile, env, tmp_path / "resolution"), env, {})
    artifact = (
        ROOT
        / "artifacts/strategy-fork"
        / (selected.name + "-" + kind + "-" + scenario + "-" + profile + "-" + uuid4().hex)
    )
    artifact.mkdir(parents=True)
    temporary = tmp_path / artifact.name
    temporary.mkdir()
    record = {
        "strategy": selected.name,
        "profile": profile,
        "scenario": scenario,
        "kind": kind,
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
                cancel_liquidity(
                    selected, node, journal, arguments, launch, record["phases"], chain, kind
                )
            else:
                recover_keeper(
                    selected, node, journal, arguments, launch, record["phases"], chain, kind
                )
            record["status"] = "passed"
    finally:
        retain(temporary, artifact, record)
        print("GMX liquidity recovery evidence:", artifact)


def launch_fault(selected, node, journal, arguments, launch, chain, kind, stage):
    running = launch_driver(selected, node, arguments, launch, chain, kind, stage)
    settle_prior_deposits(node, journal, running, chain, kind)
    if stage == "outcome":
        drive_keeper(node, journal, running, chain)
    assert running.wait(timeout=180) == -signal.SIGKILL
    return journal.load()


def launch_driver(selected, node, arguments, launch, chain, kind, stage):
    node.rpc("anvil_setBlockTimestampInterval", [15 if chain == "avalanche" else 10])
    extra = ["--result-kind", "gmx_request_outcome"] if stage == "outcome" else []
    return launch(
        [
            sys.executable,
            str(ROOT / "tests/fork/driver.py"),
            "--strategy",
            str(selected.strategy),
            *arguments,
            "--crash",
            stage,
            "--operation",
            "gmx_create_" + kind,
            *extra,
        ]
    )


def settle_prior_deposits(node, journal, running, chain, kind):
    for count in range(1, 3 if kind == "withdrawal" else 1):
        drive_keeper(node, journal, running, chain)
        wait_state(
            journal, lambda s, count=count: gmx_position_path.delivered_count(s, count), running
        )


def drive_keeper(node, journal, running, chain):
    gmx_fork.drive_external(
        node,
        journal,
        running,
        lambda request: source_for(request, chain),
        block_interval=15 if chain == "avalanche" else 10,
    )


def accepted_request(state):
    return (
        bool(state.requests)
        and state.requests[-1].unresolved
        and state.delivered == len(state.outcomes)
    )


def request_cancelled(state, key):
    return any(r.key == key and r.status == "cancelled" for r in state.requests)


def cancel_liquidity(selected, node, journal, arguments, launch, phases, chain, kind):
    interrupted = launch_fault(selected, node, journal, arguments, launch, chain, kind, "broadcast")
    assert interrupted.pending.operation.kind == "gmx_create_" + kind
    identity = interrupted.pending.tx_hash
    restart_gateway(node, launch)
    running = resume(launch, journal, arguments)
    accepted = wait_state(journal, accepted_request, running)
    request = accepted.requests[-1]
    assert request.events[0].identity.transaction_hash == identity
    request_teardown(arguments)
    node.rpc("evm_setNextBlockTimestamp", [cancellation_timestamp(request, chain)])
    node.rpc("evm_mine")
    cancelled = wait_state(journal, lambda s: request_cancelled(s, request.key), running)
    assert_refund(cancelled, request)
    if kind == "withdrawal":
        drive_keeper(node, journal, running, chain)
    closed = wait_state(journal, lambda s: s.mode == "closed", running)
    assert_cancelled_closure(closed, running)
    phases.append("broadcast_recovery_then_exact_request_cancellation_refund_and_cleanup")


def assert_refund(state, request):
    outcome = cancellation_outcome(state)
    assert outcome.request.key == request.key and not outcome.accounting_issues
    if request.kind == "withdrawal":
        assert (
            outcome.after.owned.market_tokens.amount
            == outcome.before.owned.market_tokens.amount + request.escrow[0].amount
        )
    else:
        assert_currency_refund(outcome, request)


def cancellation_outcome(state):
    return next(o for o in reversed(state.outcomes) if o.operation.kind == "gmx_cancel_liquidity")


def assert_currency_refund(outcome, request):
    delta = {value.key.asset: value.amount for value in outcome.assets}
    for escrow in request.escrow:
        assert delta[escrow.asset] == escrow.amount


def assert_cancelled_closure(state, running):
    assert running.wait(timeout=20) == 0
    assert operation_count(state, "gmx_cancel_liquidity") == 1
    assert state.memory.values["failed"] == [state.config.allocations[0].name]
    assert_empty_positions(state)
    gmx_liquidity_path.assert_accounting(state)
    gmx_liquidity_path.assert_cleanup(state)


def assert_empty_positions(state):
    assert all(owned.market_tokens.amount == 0 for owned in state.owned)


def recover_keeper(selected, node, journal, arguments, launch, phases, chain, kind):
    interrupted = launch_fault(selected, node, journal, arguments, launch, chain, kind, "outcome")
    assert_undelivered_keeper(interrupted)
    restart_gateway(node, launch)
    running = resume(launch, journal, arguments)
    delivered = wait_state(journal, lambda s: s.delivered >= len(interrupted.outcomes), running)
    assert delivered.outcomes[: len(interrupted.outcomes)] == interrupted.outcomes
    gmx_liquidity_path.drive_external(node, journal, running, chain)
    wait_state(journal, gmx_liquidity_path.entered, running)
    request_teardown(arguments)
    gmx_liquidity_path.drive_external(node, journal, running, chain)
    closed = wait_state(journal, lambda state: state.mode == "closed", running)
    assert running.wait(timeout=20) == 0
    gmx_liquidity_path.assert_closed(node, closed)
    phases.append(
        "keeper_effect_committed_before_delivery_crash_then_once_after_gateway_replacement_and_closure"
    )


def assert_undelivered_keeper(state):
    assert state.requests[-1].status == "executed"
    assert state.delivered == len(state.outcomes) - 1
    assert state.outcomes[-1].kind == "gmx_request_outcome"


# Later keeper publications after each reference capture, in time order after its rows.
# Avalanche has no signed WAVAX report between 1788828357 and 1788829186 on any GMX Oracle.
LATE = {
    "arbitrum": "2026-09-09-gmx-gm/arbitrum-late-signed-reports.json.gz",
    "avalanche": "2026-10-19-gmx-avalanche-gm-close/reports/avalanche-late-signed-reports.json.gz",
}
# Teardown starts this many fork seconds before a retained report. A close request created
# up to ten Avalanche 15 s fixture blocks later still precedes the report, which must be
# strictly later than the request, and stays within its 300 s window.
TEARDOWN_BLOCKS = 10
TEARDOWN_LEAD = TEARDOWN_BLOCKS * 15 + 1


def reports(chain):
    original = json.loads(
        gzip.decompress((gmx_position_path.ROOT / (chain + "-signed-reports.json.gz")).read_bytes())
    )
    late = retained(LATE[chain])
    return original, [
        *original["selected"],
        *json.loads(gzip.decompress(late.read_bytes()))["selected"],
    ]


def source_for(request, chain):
    original, rows = reports(chain)
    adjustments = {b["asset"]: b["timestamp_adjustment"] for b in original["bindings"]}
    return next(
        row
        for row in rows
        if gmx_position_path.eligible(
            row,
            adjustments,
            request.events[0].identity.block.timestamp,
            original["request_expiration_seconds"],
        )
    )


def cancellation_timestamp(request, chain):
    minimum = request.admission_deadline + 301
    if request.kind == "deposit":
        return minimum
    # Leave enough fixture blocks for cancellation, delivery, approval and the close request.
    selected = next(value for value in report_times(chain) if value > minimum + 60)
    return max(minimum, selected - 100)


def report_times(chain):
    proof, rows = reports(chain)
    adjustments = {b["asset"]: b["timestamp_adjustment"] for b in proof["bindings"]}
    return [min(r["observed_at"] - adjustments[r["asset"]] for r in row["reports"]) for row in rows]


def align_teardown(node, chain):
    """Move the fork clock so a teardown request created now has a retained report to settle it."""
    now = int(node.rpc("eth_getBlockByNumber", ["latest", False])["timestamp"], 16)
    times = report_times(chain)
    selected = next((value for value in times if value > now + TEARDOWN_LEAD), None)
    if selected is None:
        raise AssertionError(
            f"fork time {now} leaves no retained {chain} report after {now + TEARDOWN_LEAD}; "
            f"the last is at {max(times)}"
        )
    node.rpc("evm_setNextBlockTimestamp", [selected - TEARDOWN_LEAD])
    node.rpc("evm_mine")
