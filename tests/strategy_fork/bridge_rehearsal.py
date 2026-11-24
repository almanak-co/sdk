"""What the bridge references share between a fork run and a production rehearsal.

A fork run names each node as its own environment, and its gateway constructs the
quote. A production rehearsal runs the production gateway against the same kind of
node under the origin's chain identity. That gateway declares the venue's own API as
its quote transport, so the rehearsed deposit signs a quote the venue answered, under
the source `across_api`. The venue times its quotes in the present, so the rehearsal
moves the forks' clocks there first. In both, no relayer runs: the fill is the
scenario's own call of the destination SpokePool.
"""

import time
from pathlib import Path

from almanak.connectors.across.fork_settler import Settler
from almanak.connectors.across.quote import FORK_SOURCE, SOURCE, fee_ceiling
from almanak.gateway.environments import is_fork


class Nodes:
    """The run's fork nodes as a fork keeper addresses them: by the chain they fork."""

    def __init__(self, handles):
        # A supervised run names its nodes as handles, a rehearsal as a set by chain.
        self.handles = getattr(handles, "handles", None) or tuple(handles.nodes.values())

    def handle(self, chain_id):
        return next(h for h in self.handles if h.origin.chain.chain_id == chain_id)

    def rpc(self, chain_id):
        return self.handle(chain_id).rpc

    def origin(self, chain_id):
        return self.handle(chain_id).origin


class RehearsalSettler(Settler):
    """Fills on a rehearsal's forks, which carry their origins' chain identity."""

    def fork(self, chain):
        return self.forks.rpc(chain.chain_id), self.forks.origin(chain.chain_id)


def rehearsed(state) -> bool:
    """Whether the run is a production rehearsal: its accounts name no fork node."""
    return not is_fork(state.config.target("move").fund.chain)


def settler(node, journal, state=None) -> Settler:
    kind = RehearsalSettler if state is not None and rehearsed(state) else Settler
    return kind(Nodes(node), Path(journal.path).parent / "keepers")


def prepare_rehearsal(nodes) -> None:
    """Move every fork's clock to the present, where the venue times its quotes."""
    now = int(time.time())
    for node in nodes.nodes.values():
        node.rpc("evm_setNextBlockTimestamp", [now])
        node.rpc("evm_mine", [])


def quote_source(state) -> str:
    return SOURCE if rehearsed(state) else FORK_SOURCE


def offered(state, deposit, sent: int, constructed: int) -> int:
    """What the deposit's quote offered: the constructed fee on a fork, the venue's own
    in a rehearsal. Either lies under the author's ceiling."""
    quote = deposit.operation.quote
    assert quote.source == quote_source(state), (
        "a fork signs a constructed quote, a rehearsal the venue's"
    )
    if not rehearsed(state):
        assert quote.output_amount == constructed
    ceiling = fee_ceiling(sent, deposit.operation.target.route.max_fee_bps)
    assert 0 < sent - quote.output_amount <= ceiling
    return quote.output_amount
