"""Run the Across bridge reference: one deposit, a real fill, and a measured arrival.

No relayer runs on a fork, so this scenario fills the open transfer through the
destination SpokePool's own `fillRelay`. The quote the deposit signed is the fork
gateway's constructed one, never the venue's.
"""

import time
from pathlib import Path

from eth_abi import encode
from eth_utils import keccak

from almanak.connectors.across.connector import AcrossProjection
from almanak.connectors.across.fork_settler import Settler
from almanak.transfers import Arrived, Refunded

SENT = 100_000_000
# The constructed fork quote charges one basis point.
OFFERED = SENT - SENT // 10_000
WAIT_SECONDS = 600


class Nodes:
    """The run's fork nodes as a fork keeper addresses them: by the chain they fork."""

    def __init__(self, handles):
        self.handles = handles.handles

    def handle(self, chain_id):
        return next(h for h in self.handles if h.origin.chain.chain_id == chain_id)

    def rpc(self, chain_id):
        return self.handle(chain_id).rpc

    def origin(self, chain_id):
        return self.handle(chain_id).origin


def settler(node, journal) -> Settler:
    return Settler(Nodes(node), Path(journal.path).parent / "keepers")


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
            settler(node, journal).fill(state, transfer)


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


def assert_one_deposit(state):
    (deposit,) = deposits(state)
    quote = deposit.operation.quote
    assert quote.source == "across_fork_constructed", "a fork deposit signs a constructed quote"
    report = AcrossProjection.report(state)
    assert [t["quote_source"] for t in report["transfers"]] == ["across_fork_constructed"]
    context, _ = AcrossProjection.dashboard(state)
    (row,) = context.datasets["transfers"].rows
    assert row["quote_source"] == "across_fork_constructed"
    assert (deposit.operation.amount.amount, quote.output_amount) == (SENT, OFFERED)


def assert_holding(node, state):
    assert_one_deposit(state)
    (transfer,) = state.transfers
    assert transfer.resolution.received.amount == OFFERED
    assert balances(node, state) == (funded(state) - SENT, OFFERED, 0)
    assert remembered(state) == (SENT, OFFERED, None)


def assert_closed(node, state):
    assert state.mode == "closed" and all(work.closed for work in state.work)
    assert_holding(node, state)


def assert_refunded(node, state):
    assert_one_deposit(state)
    (transfer,) = state.transfers
    assert transfer.unfilled and transfer.resolution.returned.amount == SENT
    assert balances(node, state) == (funded(state), 0, 0)
    assert remembered(state) == (SENT, None, SENT)
