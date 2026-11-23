"""Across settlement against the real SpokePools on two identified forks.

Component evidence only. The deposit, the fill and the refund leaf run through the
deployed contract code, and the SDK's own pages read them back. The quote is
constructed, the deposit is sent from an impersonated fund rather than through the
gateway, and the refund is a fork-constructed bundle: an impersonated admin relays a
root of one leaf the fixture wrote, which no dataworker proposed and no HubPool
validated. Nothing here is promotion or Fork Qualification evidence.
"""

from contextlib import ExitStack
from types import SimpleNamespace

import pytest

from almanak.connectors.across.domain import spoke_pool
from almanak.connectors.across.fork_settler import FORK_CONSTRUCTED, Settler, sent, timestamp
from almanak.connectors.across.models import Config
from almanak.connectors.across.operations import DepositOperation, plan_call
from almanak.connectors.across.receipts import movement, opened_transfer
from almanak.connectors.across.settlement_reader import fill_status
from almanak.connectors.support import ConnectorChainSupport, EconomicAction, RunMode
from almanak.gateway.environments import ForkBinding
from almanak.gateway.ethereum import EthereumReader
from almanak.gateway.fork_config import ForkTarget
from almanak.gateway.fork_fixture import Fork
from almanak.gateway.fork_funding import fund_erc20
from almanak.gateway.providers import ALCHEMY_NETWORKS, AlchemyProvider
from almanak.gateway.read_endpoints import mainnet_reader, read_endpoint
from almanak.models import ZERO_ADDRESS, Chain, StrategyAccounts
from almanak.triggers import IntervalTrigger
from tests.across_settlement_support import settle
from tests.across_support import (
    AMOUNT,
    ARBITRUM,
    BASE,
    POOL,
    RECIPIENT,
    SENDER,
    allocation,
    eoa,
    quote,
    route,
)
from tests.test_across_records import checkpoint

pytestmark = pytest.mark.fork

OFFERED = AMOUNT - 150_000


class Forks:
    def __init__(self, nodes):
        self.nodes = nodes

    def rpc(self, chain):
        return self.nodes[chain].rpc

    def origin(self, chain):
        return self.nodes[chain].origin


def started(stack, env_file, chain):
    provider = AlchemyProvider(
        kind="alchemy", network=ALCHEMY_NETWORKS[chain.chain_id], api_key_env="ALCHEMY_API_KEY"
    )
    with mainnet_reader(chain.chain_id, read_endpoint(chain.chain_id, env_file)) as reader:
        origin = reader.pin_block("finalized")
    target = ForkTarget(chain=chain, provider=provider, block=origin)
    node = stack.enter_context(Fork(env_file, origin, target=target))
    instance = node.metadata["instanceId"]
    fork = Chain(chain_id=chain.chain_id, environment="anvil:" + instance)
    reader = stack.enter_context(EthereumReader(fork, node.rpc.endpoint, local_transport=True))
    return fork, node, reader, ForkBinding(chain=fork, origin=origin, instance=instance)


@pytest.fixture
def forks(request, monkeypatch):
    env_file = request.config.getoption("--fork-env")
    fork_only = tuple(
        ConnectorChainSupport(
            chain=chain.chain_id,
            modes=(RunMode.FORK,),
            actions=(EconomicAction.BRIDGE, EconomicAction.REVOKE_BRIDGE),
            account_profiles=("eoa",),
            restrictions=("test fixture, not support",),
        )
        for chain in (ARBITRUM, BASE)
    )
    monkeypatch.setattr("almanak.connectors.across.connector.candidate_support", lambda: fork_only)
    with ExitStack() as stack:
        here, there = (started(stack, env_file, chain) for chain in (ARBITRUM, BASE))
        config = Config(
            decision_trigger=IntervalTrigger(kind="interval", seconds=60),
            accounts=StrategyAccounts(
                bindings=(eoa("arb", here[0], SENDER), eoa("base", there[0], RECIPIENT))
            ),
            allocations=(allocation(route=route(chain=here[0])),),
            fork_bindings=(here[3], there[3]),
        )
        yield SimpleNamespace(config=config, here=here, there=there)


def deposited(forks):
    """One real `deposit` from the impersonated fund, with a constructed quote."""
    config, (_, node, _, _) = forks.config, forks.here
    target, now = config.target("move"), timestamp(node.rpc)
    offer = quote(
        outputAmount=str(OFFERED),
        timestamp=str(now),
        fillDeadline=str(now + 7200),
        exclusiveRelayer=ZERO_ADDRESS,
        exclusivityDeadline=0,
    )
    operation = DepositOperation(
        target=target,
        amount=config.bridge("move", AMOUNT).amount,
        quote=offer,
        quote_time_buffer=POOL.quote_time_buffer,
    )
    mined = sent(
        node.rpc, SENDER, target.route.spoke_pool, bytes.fromhex(plan_call(operation).data[2:])
    )
    at = timestamp(node.rpc, mined["blockNumber"])
    _, deposit = movement(mined, operation, at)
    transfer = opened_transfer(operation, deposit)
    block = SimpleNamespace(timestamp=at)
    outcome = SimpleNamespace(
        transfer=transfer, operation=operation, receipt=mined, after=SimpleNamespace(block=block)
    )
    return transfer, outcome


def settled(state, config, reader):
    """Tick one chain until a page records an outcome; a scan may need several pages."""
    recorded = len(state.outcomes)
    for _ in range(6):
        state = settle(state, config, reader)
        if len(state.outcomes) > recorded:
            return state
    raise AssertionError("no settlement was recorded")


def ticked(state, config, reader):
    """A few ticks of one chain, each committing at most one page."""
    for _ in range(4):
        state = settle(state, config, reader)
    return state


def balance(rpc, token, holder):
    data = "0x70a08231" + holder[2:].rjust(64, "0")
    return int(rpc("eth_call", [{"to": token, "data": data}, "latest"]), 16)


@pytest.fixture
def run(forks, tmp_path):
    """Two real deposits of one funded account, and the state that holds their transfers."""
    config, target = forks.config, forks.config.target("move")
    (here, source, _, _), (there, destination, _, _) = forks.here, forks.there
    token, pool = target.asset.reference, target.route.spoke_pool
    fund_erc20(source, SENDER, target.asset, 2 * AMOUNT)
    approve = "095ea7b3" + pool[2:].rjust(64, "0") + hex(2 * AMOUNT)[2:].rjust(64, "0")
    sent(source.rpc, SENDER, token, bytes.fromhex(approve))
    (filled, first), (refunded, second) = deposited(forks), deposited(forks)
    state = checkpoint(config, transfers=(filled, refunded))
    works = tuple(work.model_copy(update={"initialized": True}) for work in state.work)
    return SimpleNamespace(
        config=config,
        target=target,
        state=state.model_copy(update={"work": works}),
        retained=SimpleNamespace(outcomes=(first, second), transfers=state.transfers),
        settler=Settler(Forks({here: source, there: destination}), tmp_path / "keepers"),
        source=source,
        destination=destination,
        readers=(forks.here[2], forks.there[2]),
    )


def arrived(run):
    """Fill the first transfer on the destination fork and read the arrival back."""
    filled, target, reader = run.state.transfers[0], run.target, run.readers[1]
    received = target.destination_asset.reference
    held = balance(run.destination.rpc, received, RECIPIENT)
    row = run.settler.fill(run.retained, filled)
    state = settled(run.state, run.config, reader)
    (arrival,) = state.outcomes
    assert (arrival.kind, arrival.transfer, arrival.fill_type, arrival.tx_hash) == (
        "across_arrival",
        filled.reference,
        "fast",
        row["tx_hash"],
    )
    paid = balance(run.destination.rpc, received, RECIPIENT) - held
    assert (arrival.received.amount, paid) == (OFFERED, OFFERED)
    # The status the destination pool keeps under each relay hash agrees with the page.
    pool = spoke_pool(BASE.chain_id).address
    hashes = [transfer.reference.split(":")[1] for transfer in state.transfers]
    assert [fill_status(reader, pool, h, arrival.block) for h in hashes] == [2, 0]
    return state.model_copy(update={"delivered": len(state.outcomes)})


def returned(run, state):
    """Refund the second transfer through a fork-constructed bundle and read it back."""
    refunded, reader = state.transfers[1], run.readers[0]
    token = run.target.asset.reference
    before = settle(state, run.config, reader)
    row = run.settler.refund(run.retained, refunded)
    # The refund is on chain, yet nothing is read before the destination's own proof.
    unchanged = (before, settle(state, run.config, reader)) == (state, state)
    state = ticked(state, run.config, run.readers[1])
    proven = state.transfers[1].unfilled
    state = settled(state, run.config, reader)
    refund = state.outcomes[-1]
    assert (unchanged, proven, row["evidence"]) == (True, True, FORK_CONSTRUCTED)
    assert (refund.kind, refund.transfers, refund.tx_hash, refund.returned.amount) == (
        "across_refund",
        (refunded.reference,),
        row["tx_hash"],
        AMOUNT,
    )
    assert balance(run.source.rpc, token, SENDER) == AMOUNT
    return state


def test_a_real_fill_and_a_fork_constructed_refund_settle_two_transfers(run):
    state = returned(run, arrived(run))
    assert [t.resolution.kind for t in state.transfers] == ["arrived", "refunded"]
