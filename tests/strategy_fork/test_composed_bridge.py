"""The composed bridge-and-lend reference, stage by stage, and its refund.

Each proof runs the reference itself in a supervised fork workspace, driven through the
public commands. The plain lifecycle with pause and resume is the discovered one in
`test_strategies.py`; these two stop at every stage and assert owned inventory there.

The quote every deposit signs is the fork gateway's constructed one, and the refund is a
fork-constructed bundle no dataworker proposed. Nothing here is promotion or Fork
Qualification evidence.
"""

from pathlib import Path

import pytest

from almanak.transfers import Refunded
from tests.strategy_fork.harness import discover
from tests.strategy_fork.supervised import await_checkpoint, control_command, fork_handle
from tests.strategy_fork.test_across_bridge import settler, supervised

ROOT = Path(__file__).parents[2]
NAME = "arbitrum_base_bridge_and_lend"
CASE = next(
    c for c in discover(ROOT / "strategies", Path(__file__).parent / "scenarios") if c.name == NAME
)
OPERATOR = ROOT / "examples" / "operators" / f"{NAME}.fork.yaml"
pytestmark = [pytest.mark.fork, pytest.mark.strategy_fork]


def deposited(run):
    """The checkpoint once the deposit is mined and the strategy has learned of it."""
    state = await_checkpoint(
        run.root, lambda s: run.assertions.in_flight(s) and run.assertions.delivered(s)
    )
    run.assertions.assert_in_flight(fork_handle(run.root), state)
    run.phases.append("deposit_mined_and_value_in_no_inventory")
    return state


def close(run, check):
    control_command(run.root, "teardown", run.record)
    closed = await_checkpoint(run.root, lambda s: s.mode == "closed")
    check(fork_handle(run.root), closed)
    assert all(work.closed for work in closed.work)
    run.phases.append("closed")
    control_command(run.root, "shutdown", run.record)
    run.phases.append("workspace_shutdown")


def test_the_arrived_amount_is_supplied_held_and_withdrawn_on_the_destination(request, tmp_path):
    with supervised(request, tmp_path, "arrival", CASE, OPERATOR) as run:
        state = deposited(run)
        (transfer,) = state.transfers
        settler(run).fill(state, transfer)
        arrived = await_checkpoint(run.root, lambda s: s.transfers[0].resolution is not None)
        # One checkpoint decides it: no supply mined yet means the whole measured credit
        # is owned; a mined supply spent exactly the measured amount and nothing else.
        received = arrived.transfers[0].resolution.received.amount
        assert received == run.assertions.OFFERED
        supplies = run.assertions.mined(arrived, "supply")
        if supplies:
            assert [o.operation.amount.amount for o in supplies] == [received]
            assert run.assertions.inventory(arrived) == (0, 0)
        else:
            assert run.assertions.inventory(arrived) == (0, received)
        run.phases.append("arrival_measured_and_credited_to_the_destination")
        state = await_checkpoint(run.root, run.assertions.entered)
        run.assertions.assert_holding(fork_handle(run.root), state)
        run.phases.append("exactly_the_arrived_amount_supplied_and_held")
        close(run, run.assertions.assert_closed)


def test_a_refund_restores_source_inventory_and_nothing_is_supplied(request, tmp_path):
    with supervised(request, tmp_path, "refund", CASE, OPERATOR) as run:
        state = deposited(run)
        (transfer,) = state.transfers
        deadline = int(transfer.fill_deadline.timestamp())
        # No relayer fills. The destination chain passes the fill deadline first.
        settler(run).expire(transfer.destination.chain, deadline)
        state = await_checkpoint(run.root, lambda s: s.transfers[0].unfilled)
        assert state.transfers[0].open and run.assertions.inventory(state) == (0, 0)
        run.phases.append("destination_proved_the_transfer_unfilled")
        row = settler(run).refund(state, state.transfers[0])
        assert row["evidence"].startswith("fork-constructed refund bundle")
        state = await_checkpoint(run.root, run.assertions.refunded)
        assert isinstance(state.transfers[0].resolution, Refunded)
        run.assertions.assert_refunded(fork_handle(run.root), state)
        run.phases.append("fork_constructed_refund_measured_and_source_inventory_whole")
        close(run, run.assertions.assert_refunded)
