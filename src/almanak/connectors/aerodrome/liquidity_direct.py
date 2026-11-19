"""Direct liquidity actions for Aerodrome stable and volatile pools: the wallet's shares
of one named pool.

A position is the wallet's balance of the pool's own share token, with the fee claims
the pool stores for the wallet. Nothing else names it: no tick range and no NFT. Each
command is one operator deployment with one allocation that has liquidity authority,
adopts (`Allocation.adopt`) and swaps nothing: its entry share is zero and its
observations carry no quote, so the gateway has no quote to admit a swap along.

The deployment owns the wallet's whole position in the pool as its first observation
reads it: every share, with the stored fee claims and indices. Shares are fungible and
their fee claims are the account's, so the deployment cannot own a part of the balance.
A wallet that holds none starts without a position. `open-liquidity` is the first
deposit and is refused for a wallet that already holds shares or fee claims;
`increase-liquidity` adds to them. Shares that reach the wallet from elsewhere before a
step is sent refuse it, because the observed position is then not the adopted one. Once
a step is sent they cannot stop it: the step is reconciled from its own effects, the
position as the pool stores it stays owned whole, and the step's outcome lists the
difference as an accounting issue.

A deposit states the most of each currency it may use. Those two amounts are the
deployment's whole inventory, attributed only when the wallet covers both. The router
takes the two currencies in the pool's own proportion, so it uses all of one amount and
at most the other, and both must be above zero. It approves the router for exactly each
stated amount; what the deposit does not use stays approved.

`decrease-liquidity` burns the stated number of shares and `close-liquidity` all of
them, each after approving the router for exactly those shares, which the burn spends
whole. A burn pays principal only. `collect-liquidity-fees` claims the fees, and a close
claims what is left to claim once the shares are gone. A deposit or a burn is refused
while the pool's own balance holds shares another transfer left there: the pool's burn
would consume them.

Nothing here revokes an allowance. `revoke-exchange` is its own command: it sets every
allowance the wallet gave the router, in the two pool currencies and in the pool's
shares, to zero and does nothing else.

The heavy imports stay inside the functions: the command line imports each connector's
`direct` module to build its choices.
"""

from almanak.connectors.direct import ALLOCATION, ActionRequest, Wallet
from almanak.connectors.v3_pool_liquidity_direct import ADDITIONS, ADMISSION_SECONDS, flag

ACTIONS = (
    "open-liquidity",
    "increase-liquidity",
    "decrease-liquidity",
    "collect-liquidity-fees",
    "close-liquidity",
    "revoke-exchange",
)
# What each command must state, as `ActionRequest` field names. A deposit's two amounts
# are checked apart: the command line states an absent one as zero.
REQUIRED = {
    "open-liquidity": ("slippage_bps", "deadline_seconds"),
    "increase-liquidity": ("slippage_bps", "deadline_seconds"),
    "decrease-liquidity": ("liquidity", "slippage_bps", "deadline_seconds"),
    "collect-liquidity-fees": (),
    "close-liquidity": ("slippage_bps", "deadline_seconds"),
    "revoke-exchange": (),
}
# The position flags each command takes, as `ActionRequest` field names: those it
# requires, then those it also takes (`DirectActions.liquidity_flags`). A deposit states
# both amounts; a decrease states the shares to burn.
FLAGS = {
    "open-liquidity": (("amount0", "amount1"), ()),
    "increase-liquidity": (("amount0", "amount1"), ()),
    "decrease-liquidity": (("liquidity",), ()),
    "collect-liquidity-fees": ((), ()),
    "close-liquidity": ((), ()),
    "revoke-exchange": ((), ()),
}
# How other connectors name a position or shape a deposit and a burn: a tick range, an
# NFT, an amount per coin, a payout choice. None of these fits a balance of shares of a
# pool that takes and pays its two currencies in its own proportion.
FOREIGN = ("tick_lower", "tick_upper", "token_id", "amounts", "receive")


def positions(reader, block, resolved, wallet: Wallet, request: ActionRequest) -> dict:
    """One adopting allocation with liquidity authority in the resolved pool."""
    from almanak.chains import evm_network

    require_stated(request)
    amount0, amount1 = stated_amounts(request)
    pool = resolved.pool
    return {
        "allocations": [
            {
                "name": ALLOCATION,
                "account": wallet.account,
                "pool": pool.model_dump(mode="json"),
                "contracts": resolved.contracts.model_dump(mode="json"),
                "liquidity_enabled": True,
                "capital": {"asset": str(pool.currency0), "amount": amount0},
                "adopt": {"counter": amount1},
                "entry_swap_bps": 0,
                # A fee claim and a revocation have no price bound.
                "slippage_bps": request.slippage_bps or 0,
                "deadline_seconds": request.deadline_seconds or ADMISSION_SECONDS,
                "native_asset": str(evm_network(pool.chain.chain_id).native_asset),
                "minimum_native_balance": 0,
            }
        ]
    }


def require_stated(request: ActionRequest) -> None:
    from almanak.ax_liquidity import flags

    foreign = [name for name in FOREIGN if getattr(request, name) is not None]
    if foreign:
        raise ValueError(
            "an Aerodrome position is the wallet's balance of the pool's own share token: "
            f"{request.action} takes no {flags(foreign)}"
        )
    missing = [flag(name) for name in REQUIRED[request.action] if getattr(request, name) is None]
    if missing:
        raise ValueError(f"{request.action} requires {', '.join(missing)}")


def stated_amounts(request: ActionRequest) -> tuple[int, int]:
    """Base units of currency0 and currency1 the command may put into the pool."""
    if request.action not in ADDITIONS:
        return 0, 0
    amounts = request.amount0 or 0, request.amount1 or 0
    if not all(amounts):
        raise ValueError(
            f"{request.action} requires --amount0 and --amount1 above zero: a deposit "
            "takes the pool's two currencies in the pool's own proportion"
        )
    return amounts


def intent(config, observed, request: ActionRequest):
    """The command's one intent, built from the observation it is admitted with.

    A deposit offers the whole attributed inventory, which is the stated amounts. A
    burn's minimum outputs are the shares' part of the pool's balances at the observed
    block less the stated slippage. Each deadline is the observed block time plus the
    allocation's seconds.
    """
    from almanak.connectors.composition import connector_for

    allocation = config.allocation(ALLOCATION)
    (seen,) = observed
    return BUILDERS[request.action](connector_for(config).codec.actions, allocation, seen, request)


def open_liquidity(actions, allocation, seen, request):
    if seen.owned.position is not None:
        raise ValueError(
            "the wallet already holds shares or fee claims of this pool; "
            "increase-liquidity adds to them"
        )
    return actions.authoring.open_liquidity(allocation, seen)


def increase_liquidity(actions, allocation, seen, request):
    if seen.owned.position is None:
        raise ValueError(
            "the wallet holds no shares of this pool; open-liquidity makes the first deposit"
        )
    return actions.authoring.increase_liquidity(allocation, seen)


def held_shares(seen, request) -> int:
    """The shares the deployment owns; a wallet without any is refused by name."""
    position = seen.owned.position
    if position is None or not position.shares.amount:
        claims = "" if position is None else "; collect-liquidity-fees claims its fees"
        raise ValueError(
            f"the wallet holds no shares of this pool, so {request.action} has nothing "
            f"to burn{claims}"
        )
    return position.shares.amount


def decrease_liquidity(actions, allocation, seen, request):
    held = held_shares(seen, request)
    if request.liquidity >= held:
        raise ValueError(
            f"--liquidity must be below the {held} shares the wallet holds; "
            "close-liquidity burns all of them"
        )
    return actions.authoring.decrease_liquidity(allocation, seen, request.liquidity)


def close_liquidity(actions, allocation, seen, request):
    held_shares(seen, request)
    return actions.authoring.close_liquidity(allocation, seen)


def collect_liquidity_fees(actions, allocation, seen, request):
    """A claim of what the pool would pay the wallet at the observed block.

    Refused when that is nothing: the claim would be a transaction that moves nothing.
    """
    from almanak.connectors.aerodrome.ownership import at_indices

    claimable = at_indices(seen.owned.position, seen.pool)
    if claimable is None or not (claimable.fees0.amount or claimable.fees1.amount):
        raise ValueError("the wallet has no fees to claim from this pool at the observed block")
    return actions.collect_intent(
        target=seen.target,
        deadline=int(seen.effective_at.timestamp()) + allocation.deadline_seconds,
    )


def revoke_exchange(actions, allocation, seen, request):
    if not seen.needs_cleanup:
        raise ValueError(
            "the wallet has given the pool's router no allowance in either pool currency "
            "or in the pool's shares"
        )
    return actions.revoke_intent(target=seen.target)


BUILDERS = {
    "open-liquidity": open_liquidity,
    "increase-liquidity": increase_liquidity,
    "decrease-liquidity": decrease_liquidity,
    "collect-liquidity-fees": collect_liquidity_fees,
    "close-liquidity": close_liquidity,
    "revoke-exchange": revoke_exchange,
}


def position(state) -> dict:
    """The shares the deployment owns, what its steps moved, its inventory and the
    allowances the wallet has given the router.

    `position` is None when the deployment owns no shares and no fee claims: before a
    first deposit, and after a close. Its `fees_recorded` is what the pool stored for
    the wallet at the position's last change, not the fees accrued since. `moved` is the
    sum of what the deployment's mined steps moved in each pool currency, in base units:
    negative left the wallet for the pool, positive came back. It does not split fees
    from principal. Inventory is the deployment's own, never the wallet's whole balance.
    Allowances are those of the last block the deployment observed, in the two pool
    currencies and in the pool's shares; only nonzero ones are listed.
    """
    from almanak.connectors.v3_pool_direct import last_observed

    owned = state.owned[0]
    seen = last_observed(state)
    assets = owned.target.assets
    return {
        "position": held_position(owned),
        "moved": listed(zip(assets, moved(state), strict=True)),
        "inventory": listed((v.asset, v.amount) for v in (owned.inventory0, owned.inventory1)),
        "allowances": [
            {"asset": str(item.asset), "spender": item.spender, "amount": str(item.amount)}
            for item in (() if seen is None else seen.allowances)
            if item.amount
        ],
    }


def listed(amounts) -> list[dict]:
    return [{"asset": str(asset), "amount": str(amount)} for asset, amount in amounts]


def moved(state) -> tuple[int, int]:
    mined = [outcome for outcome in state.outcomes if outcome.success]
    return sum(o.delta0 for o in mined), sum(o.delta1 for o in mined)


def held_position(owned) -> dict | None:
    held = owned.position
    if held is None:
        return None
    return {
        "shares": {"asset": str(held.shares.asset), "amount": str(held.shares.amount)},
        "fees_recorded": listed((fee.asset, fee.amount) for fee in (held.fees0, held.fees1)),
    }
