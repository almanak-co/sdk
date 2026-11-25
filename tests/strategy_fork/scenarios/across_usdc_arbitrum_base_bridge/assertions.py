"""Run the Across bridge reference: one deposit, a real fill, and a measured arrival.

No relayer runs on a fork, so this scenario fills the open transfer through the
destination SpokePool's own `fillRelay`. On a fork run the quote the deposit signed is
the fork gateway's constructed one, never the venue's. A production rehearsal signs the
venue's own answer instead; `tests/strategy_fork/bridge_rehearsal.py` says how.
"""

import time

from eth_abi import encode
from eth_utils import keccak

from almanak.connectors.across.connector import AcrossProjection
from almanak.transfers import Arrived, Refunded
from tests.strategy_fork import bridge_rehearsal
from tests.strategy_fork.bridge_rehearsal import prepare_rehearsal, settler  # noqa: F401

SENT = 100_000_000
# The constructed fork quote charges one basis point.
OFFERED = SENT - SENT // 10_000
WAIT_SECONDS = 600


def awaited(journal, running, predicate):
    deadline = time.monotonic() + WAIT_SECONDS
    while time.monotonic() < deadline:
        state = journal.load()
        if predicate(state):
            return state
        assert running.poll() is None, "coordinator stopped while a transfer was awaited"
        time.sleep(0.5)
    raise AssertionError("the bridge reference did not reach the awaited transfer state")


def in_flight(state) -> bool:
    return any(t.open for t in state.transfers)


def drive_external(node, journal, running):
    """Fill the transfer once its deposit is mined. A later call finds nothing open."""
    state = awaited(journal, running, lambda s: bool(s.transfers) or s.mode != "running")
    for transfer in state.transfers:
        if transfer.open:
            settler(node, journal, state).fill(state, transfer)


def deposits(state) -> list:
    return [
        o
        for o in state.outcomes
        if getattr(getattr(o, "operation", None), "kind", None) == "deposit" and o.success
    ]


def delivered(state) -> bool:
    return state.delivered == len(state.outcomes)


def entered(state) -> bool:
    return (
        delivered(state)
        and len(state.transfers) == 1
        and isinstance(state.transfers[0].resolution, Arrived)
    )


def refunded(state) -> bool:
    return (
        delivered(state)
        and len(state.transfers) == 1
        and isinstance(state.transfers[0].resolution, Refunded)
    )


def token_read(node, chain, token, signature, *accounts) -> int:
    data = keccak(text=signature)[:4] + encode(("address",) * len(accounts), accounts)
    rpc = node.for_chain(chain).rpc
    return int(rpc("eth_call", [{"to": token, "data": "0x" + data.hex()}, "latest"]), 16)


def balances(node, state):
    """(source balance, destination balance, SpokePool allowance) of the bridged asset."""
    target = state.config.target("move")
    source, destination = target.fund, target.destination_fund
    return (
        token_read(
            node, source.chain, target.asset.reference, "balanceOf(address)", source.address
        ),
        token_read(
            node,
            destination.chain,
            target.destination_asset.reference,
            "balanceOf(address)",
            destination.address,
        ),
        token_read(
            node,
            source.chain,
            target.asset.reference,
            "allowance(address,address)",
            source.address,
            target.route.spoke_pool,
        ),
    )


def funded(state) -> int:
    """What the fixture gave the source account: the capital and its funding buffer."""
    return SENT + SENT // 100


def remembered(state) -> tuple:
    """What the strategy recorded from its outcomes: sent, received, returned."""
    values = state.memory.values
    return tuple(values.get(name) for name in ("sent", "received", "returned"))


def offered(state) -> int:
    (deposit,) = deposits(state)
    return bridge_rehearsal.offered(state, deposit, SENT, OFFERED)


def assert_one_deposit(state):
    (deposit,) = deposits(state)
    source = bridge_rehearsal.quote_source(state)
    report = AcrossProjection.report(state)
    assert [t["quote_source"] for t in report["transfers"]] == [source]
    context, _ = AcrossProjection.dashboard(state)
    (row,) = context.datasets["transfers"].rows
    assert row["quote_source"] == source
    assert (deposit.operation.amount.amount, deposit.operation.quote.output_amount) == (
        SENT,
        offered(state),
    )


def assert_holding(node, state):
    assert_one_deposit(state)
    (transfer,) = state.transfers
    assert transfer.resolution.received.amount == offered(state)
    assert balances(node, state) == (funded(state) - SENT, offered(state), 0)
    assert remembered(state) == (SENT, offered(state), None)


def assert_closed(node, state):
    assert state.mode == "closed" and all(work.closed for work in state.work)
    assert_holding(node, state)


def assert_refunded(node, state):
    assert_one_deposit(state)
    (transfer,) = state.transfers
    assert transfer.unfilled and transfer.resolution.returned.amount == SENT
    assert balances(node, state) == (funded(state), 0, 0)
    assert remembered(state) == (SENT, None, SENT)
