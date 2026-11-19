"""Direct liquidity actions for the V3 pool family: one position in one named pool.

Each command is one operator deployment with one allocation that adopts what the
command states (`Allocation.adopt`) and swaps nothing: its entry share is zero and its
observations carry no quote, so the gateway has no quote to admit a swap along.

`open-liquidity` states a fixed tick range and the most of each currency the mint may
use. Those two amounts are the deployment's whole inventory: the capital is the stated
amount of currency0 and `adopt.counter` the stated amount of currency1, attributed only
when the wallet covers both. The gateway admits the mint only into the stated range and
only up to that inventory.

The other position commands name an NFT by its token ID. The command reads the NFT at
its pinned block and refuses one the wallet does not own or one of another pool. The
NFT's own range becomes the allocation's range and its identity `adopt.nft`. The
deployment owns the position only once its first observation has read it again and
verified owner, pool and range; the gateway repeats that read. Every later step acts on
that one retained NFT.

A position without liquidity can only be increased. The family's collection is
reconciled against the pool's own fee update, which the position manager makes only for
a position that holds liquidity, so collecting from or closing an emptied position is
refused here before anything is signed.

A mint or an increase approves the position manager for exactly the stated amount of
each currency and uses at most that. What it does not use stays approved. Nothing here
revokes it: `revoke-exchange` is its own command, which sets every allowance the wallet
gave the pool's router and position manager in the two pool currencies to zero and does
nothing else.

The heavy imports stay inside the functions: the command line imports each connector's
`direct` module to build its choices.
"""

from almanak.connectors.direct import ALLOCATION, ActionRequest, Wallet

# Commands that act on an existing position, named by its token ID.
NFT_ACTIONS = (
    "increase-liquidity",
    "decrease-liquidity",
    "collect-liquidity-fees",
    "close-liquidity",
)
# Commands that put the stated amounts into a position.
ADDITIONS = ("open-liquidity", "increase-liquidity")
ACTIONS = ("open-liquidity", *NFT_ACTIONS, "revoke-exchange")
# What each command must state besides its range or its NFT, as `ActionRequest` field names.
REQUIRED = {
    "open-liquidity": ("tick_lower", "tick_upper", "slippage_bps", "deadline_seconds"),
    "increase-liquidity": ("slippage_bps", "deadline_seconds"),
    "decrease-liquidity": ("liquidity", "slippage_bps", "deadline_seconds"),
    "collect-liquidity-fees": (),
    "close-liquidity": ("slippage_bps", "deadline_seconds"),
    "revoke-exchange": (),
}
# Seconds after the observed block within which the gateway still plans and signs a
# collection or a revocation. Neither call carries a deadline on-chain.
ADMISSION_SECONDS = 300


def positions(reader, block, resolved, wallet: Wallet, request: ActionRequest) -> dict:
    """One adopting allocation in the resolved pool, for the range or NFT the command names."""
    from almanak.chains import evm_network

    require_stated(request)
    pool = resolved.pool
    selected, nft = None, None
    if request.action == "open-liquidity":
        selected = stated_range(pool, request)
    elif request.action in NFT_ACTIONS:
        selected, nft = named_position(reader, block, resolved, wallet, request)
    amount0, amount1 = stated_amounts(request)
    return {
        "allocations": [
            {
                "name": ALLOCATION,
                "account": wallet.account,
                "pool": pool.model_dump(mode="json"),
                "contracts": resolved.contracts.model_dump(mode="json"),
                "range": selected,
                "capital": {"asset": str(pool.currency0), "amount": amount0},
                "adopt": {"counter": amount1, "nft": nft},
                "entry_swap_bps": 0,
                # A collection and a revocation have no price bound.
                "slippage_bps": request.slippage_bps or 0,
                "deadline_seconds": request.deadline_seconds or ADMISSION_SECONDS,
                "native_asset": str(evm_network(pool.chain.chain_id).native_asset),
                "minimum_native_balance": 0,
            }
        ]
    }


def flag(name: str) -> str:
    return "--" + name.replace("_", "-")


def require_stated(request: ActionRequest) -> None:
    missing = [flag(name) for name in REQUIRED[request.action] if getattr(request, name) is None]
    if missing:
        raise ValueError(f"{request.action} requires {', '.join(missing)}")


def stated_amounts(request: ActionRequest) -> tuple[int, int]:
    """Base units of currency0 and currency1 the command may put into the position."""
    if request.action not in ADDITIONS:
        return 0, 0
    amounts = request.amount0 or 0, request.amount1 or 0
    if not any(amounts):
        raise ValueError(f"{request.action} requires --amount0 or --amount1 above zero")
    return amounts


def stated_range(pool, request: ActionRequest) -> dict:
    spacing = pool.tick_spacing
    if request.tick_lower % spacing or request.tick_upper % spacing:
        raise ValueError(
            f"--tick-lower and --tick-upper must be multiples of the pool's tick spacing, {spacing}"
        )
    return {"lower": request.tick_lower, "upper": request.tick_upper}


def named_position(reader, block, resolved, wallet: Wallet, request: ActionRequest):
    """The range and CAIP-19 identity of the NFT the command names.

    The token ID is operator input, so each way it can be wrong is refused by name:
    an NFT that does not exist, one another account owns, one of another pool, and one
    without liquidity for a command that removes or collects.
    """
    token = request.token_id
    if token is None:
        raise ValueError(f"{request.action} requires --token-id")
    owner, record = named_record(reader, block, resolved, token)
    if owner != wallet.address:
        raise ValueError(f"NFT {token} belongs to {owner}, not to the wallet {wallet.address}")
    if not record[7] and request.action != "increase-liquidity":
        raise ValueError(
            f"NFT {token} holds no liquidity; {request.action} is not served for an "
            "emptied position"
        )
    nft = f"{resolved.pool.chain.chain_id}/erc721:{resolved.contracts.position_manager}/{token}"
    return {"lower": record[5], "upper": record[6]}, nft


def named_record(reader, block, resolved, token: int):
    """The owner and the manager's record of an NFT of the resolved pool.

    Refused by name: a token the position manager does not hold, and a position of
    another pool. The owner is returned unchecked.
    """
    from almanak.connectors import v3_pool_reader as reads
    from almanak.gateway.ethereum import Reverted

    pool, manager = resolved.pool, resolved.contracts.position_manager
    key_type, key = reads.position_key(pool)
    try:
        owner = reads.nft_owner(reader, manager, token, block)
        record = reads.nft_record(reader, manager, token, block, key_type)
    except Reverted:
        raise ValueError(f"the position manager {manager} holds no NFT {token}") from None
    if record[2:5] != (pool.currency0.reference, pool.currency1.reference, key):
        raise ValueError(f"NFT {token} is a position of another pool, not of {pool.pool}")
    return owner, record


def intent(config, observed, request: ActionRequest):
    """The command's one intent, built from the observation it is admitted with.

    An addition offers the whole attributed inventory, which is the stated amounts. A
    removal's minimum outputs are the principal at the observed price less the stated
    slippage. Each deadline is the observed block time plus the allocation's seconds.
    """
    from almanak.connectors.composition import connector_for

    allocation = config.allocation(ALLOCATION)
    (seen,) = observed
    require_named(allocation, seen, request)
    return BUILDERS[request.action](connector_for(config).codec.actions, allocation, seen, request)


def require_named(allocation, seen, request: ActionRequest) -> None:
    """The observation owns exactly the NFT the command names, or none for an open."""
    position = seen.owned.position
    named = None if request.action not in NFT_ACTIONS else request.token_id
    held = None if position is None else position.token_id
    if named != held or (named is not None and allocation.adopt.nft != position.nft):
        raise ValueError("the deployment does not own the position the command names")


def open_liquidity(actions, allocation, seen, request):
    require_liquidity(seen)
    return actions.authoring.open_liquidity(allocation, seen)


def increase_liquidity(actions, allocation, seen, request):
    require_liquidity(seen)
    return actions.authoring.increase_liquidity(allocation, seen)


def require_liquidity(seen) -> None:
    """Refuse amounts that add nothing at the observed price, naming what the range takes."""
    from almanak.connectors.concentrated_math import liquidity_for
    from almanak.connectors.v3_pool_planning import concrete_range, sqrt_bounds

    owned, tick = seen.owned, seen.pool.tick
    bounds = sqrt_bounds(seen.target, position=owned.position)
    held = owned.inventory0.amount, owned.inventory1.amount
    if liquidity_for(*held, seen.pool.sqrt_price_x96, *bounds):
        return
    selected = concrete_range(seen.target, position=owned.position)
    if tick < selected.lower:
        takes = "only currency0"
    elif tick >= selected.upper:
        takes = "only currency1"
    else:
        takes = "both currencies"
    raise ValueError(
        f"the stated amounts add no liquidity: at the pool's tick {tick} the range "
        f"{selected.lower} to {selected.upper} takes {takes}"
    )


def decrease_liquidity(actions, allocation, seen, request):
    held = seen.owned.position.liquidity
    if request.liquidity >= held:
        raise ValueError(
            f"--liquidity must be below the position's {held}; close-liquidity removes all of it"
        )
    return actions.authoring.decrease_liquidity(allocation, seen, request.liquidity)


def collect_liquidity_fees(actions, allocation, seen, request):
    require_held_liquidity(seen, request)
    return actions.collect_intent(
        target=seen.target,
        deadline=int(seen.effective_at.timestamp()) + allocation.deadline_seconds,
    )


def close_liquidity(actions, allocation, seen, request):
    require_held_liquidity(seen, request)
    return actions.authoring.close_liquidity(allocation, seen)


def require_held_liquidity(seen, request) -> None:
    if not seen.owned.position.liquidity:
        raise ValueError(
            f"the position holds no liquidity; {request.action} is not served for an "
            "emptied position"
        )


def revoke_exchange(actions, allocation, seen, request):
    if not seen.needs_cleanup:
        raise ValueError(
            "the wallet has given the pool's router and position manager no allowance "
            "in either pool currency"
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
    """The NFT the deployment owns and what its steps moved in the wallet.

    `position` is None when the deployment owns no NFT: before a mint, and after a
    close. Its `owed_recorded` is what the position manager recorded at the position's
    last change, not fees accrued since. `moved` is the sum of what the deployment's
    mined steps moved in each pool currency, in base units: negative left the wallet
    for the pool, positive came back. It does not split fees from principal.
    """
    owned = state.owned[0]
    deltas = [0, 0]
    for outcome in state.outcomes:
        if outcome.success:
            deltas[0] += outcome.delta0
            deltas[1] += outcome.delta1
    return {
        "position": held_position(owned),
        "moved": [
            {"asset": str(asset), "amount": str(delta)}
            for asset, delta in zip(owned.target.assets, deltas, strict=True)
        ],
    }


def held_position(owned) -> dict | None:
    held = owned.position
    if held is None:
        return None
    selected = held.range or owned.target.range
    return {
        "nft": str(held.nft),
        "token_id": str(held.token_id),
        "range": {"lower": selected.lower, "upper": selected.upper},
        "liquidity": str(held.liquidity),
        "owed_recorded": [
            {"asset": str(asset), "amount": str(amount)}
            for asset, amount in zip(owned.target.assets, (held.owed0, held.owed1), strict=True)
        ],
    }
