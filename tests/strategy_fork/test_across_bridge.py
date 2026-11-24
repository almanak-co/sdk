"""The Across bridge reference through a refund, a killed coordinator and a destination outage.

Each proof runs the reference itself in a supervised fork workspace, driven through the
public commands. The arrival proof is the discovered lifecycle in `test_strategies.py`.

The refund here is a fork-constructed bundle: the fixture impersonates the SpokePool's
admin to relay a root of one leaf it wrote, which no dataworker proposed and no HubPool
validated. It proves how the SDK reads and attributes a refund, never that the venue
would have paid one. The quote every deposit signs is the fork gateway's constructed
one. Nothing here is promotion or Fork Qualification evidence.
"""

import os
import signal
import time
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from almanak.state import Journal
from almanak.transfers import Refunded
from almanak.work import work_for
from almanak.workspace import read as read_workspace
from tests.strategy_fork.harness import discover, load_assertions
from tests.strategy_fork.supervised import (
    await_checkpoint,
    await_stage,
    await_workspace,
    control_command,
    fork_handle,
    release,
    retain_supervised,
    start_run,
)

ROOT = Path(__file__).parents[2]
NAME = "across_usdc_arbitrum_base_bridge"
CASE = next(
    c for c in discover(ROOT / "strategies", Path(__file__).parent / "scenarios") if c.name == NAME
)
OPERATOR = ROOT / "examples" / "operators" / f"{NAME}.fork.yaml"
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]


@contextmanager
def supervised(request, tmp_path, proof, case=CASE, operator=OPERATOR):
    """One running supervised workspace of a reference, released and retained after."""
    env = request.config.getoption("--fork-env").resolve()
    artifact = ROOT / "artifacts/strategy-fork" / f"{case.name}-{proof}-{uuid4().hex}"
    artifact.mkdir(parents=True)
    root = tmp_path / "run"
    record = {"strategy": case.name, "proof": proof, "phases": [], "status": "failed"}
    follower = start_run(case, operator, env, root, tmp_path / "almanak-run.log")
    try:
        await_workspace(root, follower)
        await_stage(root, ("running",))
        yield SimpleNamespace(
            root=root,
            record=record,
            phases=record["phases"],
            assertions=load_assertions(case),
            journal=Journal(root / "state.sqlite"),
        )
        record["status"] = "passed"
    finally:
        release(root, follower)
        retain_supervised(root, artifact, record)
        print("Supervised strategy fork evidence:", artifact)


def settler(run):
    return run.assertions.settler(fork_handle(run.root), run.journal)


def deposited(run):
    """The checkpoint once the deposit is mined and the strategy has learned of it."""
    return await_checkpoint(
        run.root, lambda s: run.assertions.in_flight(s) and run.assertions.delivered(s)
    )


def fill(run, state):
    (transfer,) = state.transfers
    settler(run).fill(state, transfer)
    return await_checkpoint(run.root, run.assertions.entered)


def close(run, check):
    control_command(run.root, "teardown", run.record)
    closed = await_checkpoint(run.root, lambda s: s.mode == "closed")
    check(fork_handle(run.root), closed)
    assert all(work.closed for work in closed.work)
    run.phases.append("closed")
    control_command(run.root, "shutdown", run.record)
    run.phases.append("workspace_shutdown")


def process(run, name) -> int:
    return read_workspace(run.root)["processes"][name]["pid"]


def await_lease_expiry(journal, seconds=90):
    """A killed coordinator released nothing: its lease must run out before another starts."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        with journal.connect() as connection:
            identity, expires = connection.execute(
                "SELECT identity, expires FROM coordinator"
            ).fetchone()
        if identity is None or expires <= time.time():
            return
        time.sleep(0.2)
    raise AssertionError("the killed coordinator kept a live lease")


def test_an_unfilled_transfer_is_refunded_only_after_the_destination_proves_it(request, tmp_path):
    with supervised(request, tmp_path, "refund") as run:
        state = deposited(run)
        (transfer,) = state.transfers
        run.phases.append("deposit_mined_and_transfer_open")
        deadline = int(transfer.fill_deadline.timestamp())
        # No relayer fills. The destination chain passes the fill deadline first.
        settler(run).expire(transfer.destination.chain, deadline)
        state = await_checkpoint(run.root, lambda s: s.transfers[0].unfilled)
        assert state.transfers[0].open, "the destination proves a transfer unfilled, no more"
        run.phases.append("destination_proved_the_transfer_unfilled")
        row = settler(run).refund(state, state.transfers[0])
        assert row["evidence"].startswith("fork-constructed refund bundle")
        state = await_checkpoint(run.root, run.assertions.refunded)
        assert isinstance(state.transfers[0].resolution, Refunded)
        run.assertions.assert_refunded(fork_handle(run.root), state)
        run.phases.append("fork_constructed_refund_measured_and_delivered")
        close(run, run.assertions.assert_refunded)


def test_a_coordinator_killed_after_the_deposit_sends_no_second_one(request, tmp_path):
    with supervised(request, tmp_path, "restart") as run:
        # Stop at the first checkpoint that holds the mined deposit, delivered or not.
        state = await_checkpoint(run.root, run.assertions.in_flight)
        os.kill(process(run, "coordinator"), signal.SIGKILL)
        (deposit,) = run.assertions.deposits(state)
        run.phases.append("coordinator_killed_between_mined_deposit_and_arrival")
        await_lease_expiry(run.journal)
        control_command(run.root, "resume", run.record)
        await_stage(run.root, ("running",))
        state = deposited(run)
        assert [d.tx_hash for d in run.assertions.deposits(state)] == [deposit.tx_hash]
        run.phases.append("resumed_with_the_same_open_transfer")
        state = fill(run, state)
        run.assertions.assert_holding(fork_handle(run.root), state)
        run.phases.append("arrival_measured_and_delivered")
        close(run, run.assertions.assert_closed)
        closed = run.journal.load()
        assert [d.tx_hash for d in run.assertions.deposits(closed)] == [deposit.tx_hash]


def test_the_source_bridges_while_the_destination_is_unavailable(request, tmp_path):
    with supervised(request, tmp_path, "destination-outage") as run:
        node = process(run, "fork-eip155:8453")
        os.kill(node, signal.SIGSTOP)
        try:
            state = deposited(run)
            destination = state.transfers[0].destination.chain
            fault = work_for(state, destination).fault
            assert fault is not None, "the destination chain answered during its outage"
            run.record["destination_fault"] = fault.model_dump(mode="json")
            assert not state.external_scans, "an unavailable chain recorded a scan"
            run.phases.append("source_bridged_during_destination_outage")
        finally:
            os.kill(node, signal.SIGCONT)
        state = fill(run, state)
        run.assertions.assert_holding(fork_handle(run.root), state)
        assert work_for(state, destination).fault is None
        run.phases.append("destination_recovered_and_arrival_delivered")
        close(run, run.assertions.assert_closed)


def kinds(state) -> list[str]:
    """The deployment's own mined steps, in order."""
    return [o.operation.kind for o in state.outcomes if hasattr(o, "operation") and o.success]


def await_approval(run, seconds=600):
    """The first checkpoint that holds the mined approval, polled faster than a tick."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        state = run.journal.load() if run.journal.path.is_file() else None
        if state is not None and "approve" in kinds(state):
            return state
        time.sleep(0.05)
    raise AssertionError("the bridge reference mined no approval")


def test_teardown_revokes_an_approval_whose_deposit_was_never_signed(request, tmp_path):
    with supervised(request, tmp_path, "revoke") as run:
        coordinator = process(run, "coordinator")
        await_approval(run)
        # Hold the coordinator between the mined approval and the deposit it would sign.
        os.kill(coordinator, signal.SIGSTOP)
        try:
            state = run.journal.load()
            source = state.config.target("move").fund.chain
            assert kinds(state) == ["approve"] and work_for(state, source).pending is None, (
                "the deposit was signed before the coordinator could be held"
            )
            control_command(run.root, "teardown", run.record)
        finally:
            os.kill(coordinator, signal.SIGCONT)
        run.phases.append("teardown_requested_with_a_standing_approval")
        closed = await_checkpoint(run.root, lambda s: s.mode == "closed")
        assert kinds(closed) == ["approve", "revoke"] and not closed.transfers
        node = fork_handle(run.root)
        assert run.assertions.balances(node, closed) == (run.assertions.funded(closed), 0, 0)
        assert run.assertions.remembered(closed) == (None, None, None)
        run.phases.append("strategy_revoked_and_closed_without_a_deposit")
        control_command(run.root, "shutdown", run.record)
        run.phases.append("workspace_shutdown")
