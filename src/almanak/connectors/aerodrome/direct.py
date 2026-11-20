"""Aerodrome direct operator actions in one named stable or volatile pool: an
exact-input swap, and the liquidity commands of `liquidity_direct`.

A swap deployment holds one trading allocation, with liquidity disabled, whose capital is
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
signed: those claims are account-wide and cannot be kept apart from the deployment.
Nothing is then planned or signed. Shares that reach the wallet once a step is signed
cannot stop it, whether it was sent yet or not: anyone can send them, signed bytes
cannot be recalled, and an approval carries no deadline to release it by. The step is
sent, by the command or by `almanak ax recover`, and reconciled from its own effects,
and its outcome lists the shares as an accounting issue. They are never attributed to
the deployment. A swap whose approval was signed before the shares arrived ends with
that approval sent and the swap itself refused, unsigned. A strategy's allocation
follows a narrower rule (`validation.sent_past_shares`). The wallet's next command
is refused until the shares are gone. Shares that sit in the pool's own balance do not
matter to a swap.

The heavy imports stay inside the functions: the command line imports each connector's
`direct` module to build its choices.
"""

from almanak.connectors import v3_pool_direct
from almanak.connectors.direct import ActionRequest, DirectActions, Wallet


def positions(reader, block, resolved, wallet: Wallet, request: ActionRequest) -> dict:
    """The deployment's one allocation in the resolved pool, as the command states it."""
    if request.action != "swap":
        from almanak.connectors.aerodrome import liquidity_direct

        return liquidity_direct.positions(reader, block, resolved, wallet, request)
    return swap_positions(resolved, wallet, request)


def swap_positions(resolved, wallet: Wallet, request: ActionRequest) -> dict:
    """One trading allocation in the resolved pool, funded with exactly the amount sold.

    The request checks and the allocation are the V3 pool family's. A classic pool has
    no range to leave unset: its allocation states instead that it has no liquidity
    authority, so no pool shares are minted, burned or approved.
    """
    document = v3_pool_direct.swap_positions(resolved, wallet, request)
    (allocation,) = document["allocations"]
    stated = {name: value for name, value in allocation.items() if name != "range"}
    return {"allocations": [{**stated, "liquidity_enabled": False}]}


def intent(config, observed, request: ActionRequest):
    """The command's intent, built from the observation it is admitted with."""
    if request.action != "swap":
        from almanak.connectors.aerodrome import liquidity_direct

        return liquidity_direct.intent(config, observed, request)
    return v3_pool_direct.swap_intent(config, observed, request)


def position(state) -> dict:
    """What the deployment owns, for the command's result.

    A swap deployment reports its inventory and the allowances last observed, as the V3
    pool family's does. A liquidity deployment also reports its shares and what its
    steps moved.
    """
    if state.owned[0].target.liquidity_enabled:
        from almanak.connectors.aerodrome import liquidity_direct

        return liquidity_direct.position(state)
    return v3_pool_direct.position(state)


def liquidity_flags(command: str):
    from almanak.connectors.aerodrome.liquidity_direct import FLAGS

    return FLAGS[command]


DIRECT = DirectActions(
    actions=(
        "swap",
        "open-liquidity",
        "increase-liquidity",
        "decrease-liquidity",
        "collect-liquidity-fees",
        "close-liquidity",
        "revoke-exchange",
    ),
    positions=positions,
    intent=intent,
    position=position,
    liquidity_flags=liquidity_flags,
)
