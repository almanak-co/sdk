"""Run the composed bridge-and-lend reference: bridge, measured arrival, supply, withdraw.

No relayer runs on a fork, so this scenario fills the open transfer through the
destination SpokePool's own `fillRelay`. On a fork run the quote the deposit signed is
the fork gateway's constructed one, never the venue's. A production rehearsal signs the
venue's own answer instead; `tests/strategy_fork/bridge_rehearsal.py` says how. Owned
inventory is asserted at every stage: value in flight is in neither account's inventory.
"""

import time

from eth_abi import encode
from eth_utils import keccak

from almanak.transfers import Arrived, Refunded
from tests.strategy_fork import bridge_rehearsal
from tests.strategy_fork.bridge_rehearsal import prepare_rehearsal, settler  # noqa: F401

SENT = 100_000_000
# The constructed fork quote charges one basis point.
OFFERED = SENT - SENT // 10_000
# Comet rounds a supplied balance down by its index, so a full withdrawal can return
# up to this many base units less than was supplied before interest.
ROUNDING = 2
WAIT_SECONDS = 600


def awaited(journal, running, predicate):
    deadline = time.monotonic() + WAIT_SECONDS
    while time.monotonic() < deadline:
        state = journal.load()
        if predicate(state):
            return state
        assert running.poll() is None, "coordinator stopped while a transfer was awaited"
        time.sleep(0.5)
    raise AssertionError("the reference did not reach the awaited transfer state")


def in_flight(state) -> bool:
    return any(t.open for t in state.transfers)


def drive_external(node, journal, running):
    """Fill the transfer once its deposit is mined. A later call finds nothing open."""
    state = awaited(journal, running, lambda s: bool(s.transfers) or s.mode != "running")
    for transfer in state.transfers:
        if transfer.open:
            settler(node, journal, state).fill(state, transfer)


def mined(state, kind) -> list:
    return [
        o
        for o in state.outcomes
        if getattr(getattr(o, "operation", None), "kind", None) == kind and o.success
    ]


def deposits(state) -> list:
    return mined(state, "deposit")


def delivered(state) -> bool:
    return state.delivered == len(state.outcomes)


def remembered(state) -> tuple:
    """What the strategy recorded from its outcomes: sent, received, returned, supplied."""
    values = state.memory.values
    return tuple(values.get(name) for name in ("sent", "received", "returned", "supplied"))


def entered(state) -> bool:
    return (
        delivered(state)
        and len(state.transfers) == 1
        and isinstance(state.transfers[0].resolution, Arrived)
        and remembered(state)[3] is not None
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


def inventory(state) -> tuple[int, int]:
    """Owned cash of the bridged asset: (source account, destination account)."""
    bridge = state.config.target("move")
    amounts = {(b.key.account, b.key.asset): b.amount for b in state.inventory}
    return (
        amounts[(bridge.account, bridge.asset)],
        amounts[(bridge.destination_account, bridge.destination_asset)],
    )


def chain_balances(node, state) -> tuple[int, int, int, int]:
    """(source wallet, destination wallet, SpokePool allowance, Comet balance)."""
    bridge, earn = state.config.target("move"), state.config.target("earn")
    source, destination = bridge.fund, bridge.destination_fund
    read = "balanceOf(address)"
    return (
        token_read(node, source.chain, bridge.asset.reference, read, source.address),
        token_read(
            node,
            destination.chain,
            bridge.destination_asset.reference,
            read,
            destination.address,
        ),
        token_read(
            node,
            source.chain,
            bridge.asset.reference,
            "allowance(address,address)",
            source.address,
            bridge.route.spoke_pool,
        ),
        token_read(node, destination.chain, earn.reserve.market.comet, read, destination.address),
    )


def offered(state) -> int:
    (deposit,) = deposits(state)
    return bridge_rehearsal.offered(state, deposit, SENT, OFFERED)


def assert_one_deposit(state):
    (deposit,) = deposits(state)
    assert (deposit.operation.amount.amount, deposit.operation.quote.output_amount) == (
        SENT,
        offered(state),
    )


def assert_in_flight(node, state):
    """The deposit is mined: the amount is in neither inventory and in neither wallet."""
    assert_one_deposit(state)
    (transfer,) = state.transfers
    assert (
        transfer.open and transfer.sent.amount == SENT and transfer.offered.amount == offered(state)
    )
    assert inventory(state) == (0, 0), "value in flight entered an inventory"
    assert chain_balances(node, state) == (0, 0, 0, 0)
    assert not mined(state, "supply"), "the offer was supplied before it arrived"


def assert_arrived(node, state):
    """The arrival is measured and credited; nothing is supplied yet."""
    (transfer,) = state.transfers
    assert transfer.resolution.received.amount == offered(state)
    assert inventory(state) == (0, offered(state))
    assert not mined(state, "supply")


def assert_supplied_what_arrived(state):
    (transfer,) = state.transfers
    assert transfer.resolution.received.amount == offered(state)
    (supply,) = mined(state, "supply")
    assert supply.operation.amount.amount == offered(state), "the supply is exactly what arrived"
    assert remembered(state) == (SENT, offered(state), None, offered(state))


def assert_lent_on_chain(node, state):
    source, destination, allowance, lent = chain_balances(node, state)
    assert (source, destination, allowance) == (0, 0, 0)
    assert offered(state) - ROUNDING <= lent, "the supplied amount is not in the market"


def assert_holding(node, state):
    assert_one_deposit(state)
    assert_supplied_what_arrived(state)
    assert inventory(state) == (0, 0)
    assert_lent_on_chain(node, state)


def assert_closed(node, state):
    assert state.mode == "closed" and all(work.closed for work in state.work)
    assert_one_deposit(state)
    assert len(mined(state, "supply")) == 1 and not in_flight(state)
    owned_source, owned_destination = inventory(state)
    source, destination, allowance, lent = chain_balances(node, state)
    # The funds stay on the destination chain: nothing bridges back.
    assert (owned_source, source, allowance, lent) == (0, 0, 0, 0)
    assert owned_destination == destination >= offered(state) - ROUNDING


def assert_refund_recorded(state):
    (transfer,) = state.transfers
    assert transfer.unfilled and transfer.resolution.returned.amount == SENT
    assert not mined(state, "supply")
    assert remembered(state) == (SENT, None, SENT, None)


def assert_refunded(node, state):
    assert_one_deposit(state)
    assert_refund_recorded(state)
    assert inventory(state) == (SENT, 0), "the refund makes the source inventory whole"
    assert chain_balances(node, state) == (SENT, 0, 0, 0)
