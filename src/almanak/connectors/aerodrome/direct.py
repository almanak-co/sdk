"""Aerodrome direct operator actions: an exact-input swap in one named stable or
volatile pool.

The deployment holds one trading allocation, with liquidity disabled, whose capital is
the amount sold, in the asset sold, offered whole as one entry. The gateway then sizes
its router quote at exactly the stated amount and admits the swap only along that
quote, and the inventory attributed to the deployment is that amount and no more of
the wallet. The intent and the position report are the V3 pool family's: the swap
follows the entry quote of the observation it is admitted with, and the report lists
the deployment's inventory and the nonzero allowances of the last observed block.

The steps are the approval of exactly the stated amount to the router, then the swap,
which spends that approval whole: a completed swap leaves the router no allowance. An
allowance of another size that the wallet already gave the router for the sold asset
is reset to zero first. A swap that is refused or reverts after its approval was mined
leaves that approval standing. Nothing here revokes it; the result lists it, and a
later swap of the same amount spends it. An allowance the wallet gave the router for
the other currency or for the pool's shares is never changed, and is listed.

A trading allocation owns no pool shares. A wallet that holds shares or stored fee
claims of the pool is refused whenever the deployment observes it before a step is
sent: those claims are account-wide and cannot be kept apart from the deployment.
Nothing is then planned, signed or sent. A step that was already signed and not yet
sent stays recorded, and `almanak ax recover` refuses to send it for as long as the
wallet holds the shares. Shares that reach the wallet once a step is sent cannot stop
it. The step is reconciled from its own effects, and its outcome lists the shares as
an accounting issue; they are never attributed to the deployment, and the wallet's
next command is refused until they are gone. Shares that sit in the pool's own balance
do not matter to a swap.

The heavy imports stay inside the functions: the command line imports each connector's
`direct` module to build its choices.
"""

from almanak.connectors import v3_pool_direct
from almanak.connectors.direct import ActionRequest, DirectActions, Wallet


def positions(reader, block, resolved, wallet: Wallet, request: ActionRequest) -> dict:
    """One trading allocation in the resolved pool, funded with exactly the amount sold.

    The request checks and the allocation are the V3 pool family's. A classic pool has
    no range to leave unset: its allocation states instead that it has no liquidity
    authority, so no pool shares are minted, burned or approved.
    """
    document = v3_pool_direct.positions(reader, block, resolved, wallet, request)
    (allocation,) = document["allocations"]
    stated = {name: value for name, value in allocation.items() if name != "range"}
    return {"allocations": [{**stated, "liquidity_enabled": False}]}


DIRECT = DirectActions(
    actions=("swap",),
    positions=positions,
    intent=v3_pool_direct.intent,
    position=v3_pool_direct.position,
)
