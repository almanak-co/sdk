"""Direct liquidity actions for Uniswap V4, and the revocation of what a direct action
leaves approved: one position in one pool named by its key.

Each command is one operator deployment with one allocation that adopts what the
command states (`Allocation.adopt`) and swaps nothing: its entry share is zero and its
observations carry no quote, so the gateway has no quote to admit a swap along.

`open-liquidity` states a fixed tick range and the most of each held token the mint may
use. Those two amounts are the deployment's whole inventory: the capital is the stated
amount of the pool's first token and `adopt.counter` the stated amount of its second,
attributed only when the wallet covers both. The gateway admits the mint only into the
stated range and only up to that inventory.

`collect-liquidity-fees` and `close-liquidity` name an NFT by its token ID. The command
reads the NFT at its pinned block and refuses one the wallet does not own, one of
another pool key and one without liquidity. The NFT's own range becomes the allocation's
range and its identity `adopt.nft`. The deployment owns the position only once its
first observation has read it again and verified owner, pool key and range; the gateway
repeats that read. The position's fee checkpoint is the PoolManager's own, so what it
has earned and not collected is measured from it. The connector has no intent that
adds to or removes part of a position, so neither is served.

A pool key may name the chain's native coin. The wallet holds and provides the wrapped
token, and receives the wrapped token back: the PositionManager unwraps and wraps
inside the call, and no call carries value. `--amount0` and `--amount1` follow the
key's order, with the wrapped token in the native coin's place.

A mint is for a fixed liquidity: the most the stated amounts cover at every price
within the stated slippage of the price of the observation its step is planned with,
which is what they cover at the edge of the slippage where each token is needed most.
The most it may take of each token is what that liquidity takes at that edge, never
more than the stated amount (`planning.bounded_mint`, which only an operator request's
planning applies). The PositionManager reverts a mint that needs more than that. So a
mint mined at a price within the stated slippage of the price it was planned at fits
in both directions, and one mined further away reverts, instead of spending the amount
stated beyond the range's ratio. The liquidity is less than the stated amounts cover
at the planned price: by about half the slippage in a range much wider than the
slippage, and by more the narrower the range is against it, down to what one stated
amount covers of the whole range. What the mint does not take stays in the wallet.

The range bounds what the slippage cannot: where an edge of the slippage lies at or
beyond a bound of the range, the mint may take all the range holds of that token and no
price in that direction reverts it, and a range wholly beside the whole slippage takes
one token only, the same amount at every price on that side. Where the slippage reaches
into a range the price lies beside, the range takes both tokens there, and a command
that states only one is refused: it states both, or a slippage that stays outside the
range. The gateway plans the mint only while the price is within the stated slippage of
the price the command decided at, and does not send it once the price has left the
slippage around the planned price; those two are its own checks, not the chain's.

A token the mint may take any of is approved for its stated amount, whatever the price
of the step's observation: Permit2 by the token, and the PositionManager by a Permit2
allowance that expires at the mint's deadline. No observation's most exceeds the stated
amount, so a price that moves between the steps plans no approval again: a wallet with
no allowance sends two approvals per token, then the mint. A token the mint may take
none of is not approved. An approval the wallet already gave that covers the stated
amount is used as it stands. The mint takes at most its own most, and what it does not
take stays approved: the Permit2 allowance until it expires, the ERC-20 approval to
Permit2 without limit of time. After a request that ended before its mint, that is the
whole stated amount, to the same two spenders and no other. No mint revokes either;
the result lists both. A collection and a close approve nothing.

`revoke-exchange` is its own command, for a swap's allowances as well as a mint's. Its
allocation states no range, no inventory and no NFT, and its steps are the connector's
own allowance cleanup, one transaction each: for each held token, the ERC-20 approval
to Permit2 is set to zero, then the Permit2 allowance to the router and the one to the
PositionManager are set to zero and expired. What it clears is what the connector's
own cleanup counts (`Allowance.needs_cleanup`): an allowance that holds an amount, and
a Permit2 allowance that holds none and has not expired, which a completed swap leaves
until its deadline and on which a strategy's start in the wallet is refused. With
neither, the command is refused. The ERC-20 approval to Permit2 is not this venue's
alone: every application that spends the token through Permit2 relies on it, and needs
it approved again afterwards; the result's `note` says so before anything is signed.
Permit2 allowances to any other spender are neither read nor changed. The cleanup
takes the allowances in order, so a token whose approval of zero reverts stops it
there.

The heavy imports stay inside the functions: the command line imports each connector's
`direct` module to build its choices.
"""

from almanak.connectors.direct import ALLOCATION, ActionRequest, Wallet

# Commands that act on an existing position, named by its token ID.
NFT_ACTIONS = ("collect-liquidity-fees", "close-liquidity")
ACTIONS = ("open-liquidity", *NFT_ACTIONS, "revoke-exchange")
# What each command must state besides its range or its NFT, as `ActionRequest` field names.
REQUIRED = {
    "open-liquidity": ("tick_lower", "tick_upper", "slippage_bps", "deadline_seconds"),
    "collect-liquidity-fees": (),
    "close-liquidity": ("slippage_bps", "deadline_seconds"),
    "revoke-exchange": (),
}
# Seconds after the observed block within which a collection is still planned and
# signed, and mined: its call carries that deadline. A revocation's allocation states
# them only because the field is required: its calls carry no deadline, and nothing
# holds its steps to this bound.
ADMISSION_SECONDS = 300


def positions(reader, block, resolved, wallet: Wallet, request: ActionRequest) -> dict:
    """One adopting allocation in the resolved pool, for the range or NFT the command names."""
    from almanak.chains import evm_network
    from almanak.connectors.uniswap_v4.custody import custody_assets

    require_stated(request)
    pool = resolved.pool
    selected, nft = None, None
    amount0, amount1 = 0, 0
    if request.action == "open-liquidity":
        selected = stated_range(pool, request)
        amount0, amount1 = stated_amounts(request)
    elif request.action in NFT_ACTIONS:
        selected, nft = named_position(reader, block, resolved, wallet, request)
    return {
        "allocations": [
            {
                "name": ALLOCATION,
                "account": wallet.account,
                "pool": pool.model_dump(mode="json"),
                "contracts": resolved.contracts.model_dump(mode="json"),
                "range": selected,
                "capital": {"asset": str(custody_assets(pool)[0]), "amount": amount0},
                "adopt": {"counter": amount1, "nft": nft},
                # Stated, since the default offers the whole capital as an entry swap.
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
    """Base units of the pool's first and second held token the mint may use."""
    amounts = request.amount0 or 0, request.amount1 or 0
    if not any(amounts):
        raise ValueError(f"{request.action} requires --amount0 or --amount1 above zero")
    return amounts


def stated_range(pool, request: ActionRequest) -> dict:
    spacing = pool.tick_spacing
    lower, upper = request.tick_lower or 0, request.tick_upper or 0
    if lower % spacing or upper % spacing:
        raise ValueError(
            f"--tick-lower and --tick-upper must be multiples of the pool's tick spacing, {spacing}"
        )
    if lower >= upper:
        raise ValueError("--tick-lower must be below --tick-upper")
    return {"lower": lower, "upper": upper}


def named_position(reader, block, resolved, wallet: Wallet, request: ActionRequest):
    """The range and CAIP-19 identity of the NFT the command names.

    The token ID is operator input, so each way it can be wrong is refused by name: an
    NFT that does not exist, one another account owns, one of another pool key, and
    one without liquidity.
    """
    token = request.token_id
    if token is None:
        raise ValueError(f"{request.action} requires --token-id")
    owner, ticks, liquidity = named_record(reader, block, resolved, token)
    if owner != wallet.address:
        raise ValueError(f"NFT {token} belongs to {owner}, not to the wallet {wallet.address}")
    if not liquidity:
        raise ValueError(
            f"NFT {token} holds no liquidity; {request.action} is not served for an "
            "emptied position"
        )
    nft = f"{resolved.pool.chain.chain_id}/erc721:{resolved.contracts.position_manager}/{token}"
    return {"lower": ticks[0], "upper": ticks[1]}, nft


def named_record(reader, block, resolved, token: int):
    """The owner, the ticks and the liquidity of an NFT of the resolved pool key.

    Refused by name: a token the PositionManager has not minted or has burned, and a
    position of another pool key. The owner is returned unchecked.
    """
    from almanak.connectors.uniswap_v4 import abi
    from almanak.connectors.uniswap_v4.reader import signed_tick
    from almanak.gateway.ethereum import Reverted

    manager = resolved.contracts.position_manager

    def read(signature, outputs):
        return reader.call(
            manager, signature, block, inputs=("uint256",), args=(token,), outputs=outputs
        )

    try:
        (owner,) = read("ownerOf(uint256)", ("address",))
    except Reverted:
        raise ValueError(f"the PositionManager {manager} holds no NFT {token}") from None
    key, info = read("getPoolAndPositionInfo(uint256)", (abi.POOL_KEY, "uint256"))
    if key != abi.pool_key(resolved.pool):
        raise ValueError(f"NFT {token} is a position of another pool key, not of the one named")
    (liquidity,) = read("getPositionLiquidity(uint256)", ("uint128",))
    return owner, (signed_tick(info >> 8), signed_tick(info >> 32)), liquidity


def intent(config, observed, request: ActionRequest):
    """The command's one intent, built from the observation it is admitted with.

    A mint states the whole attributed inventory, which is the stated amounts; the
    planner sizes the mint to them and bounds what it may take. A close's minimum outputs are
    the principal at the observed price less the stated slippage. Each deadline is the
    observed block time plus the allocation's seconds.
    """
    allocation = config.allocation(ALLOCATION)
    (seen,) = observed
    require_named(allocation, seen, request)
    return BUILDERS[request.action](allocation, seen)


def require_named(allocation, seen, request: ActionRequest) -> None:
    """The observation owns exactly the NFT the command names, or none for an open."""
    position = seen.owned.position
    named = None if request.action not in NFT_ACTIONS else request.token_id
    held = None if position is None else position.token_id
    if named != held or (named is not None and allocation.adopt.nft != position.nft):
        raise ValueError("the deployment does not own the position the command names")


def open_liquidity(allocation, seen):
    from almanak.connectors.uniswap_v4.authoring import open_liquidity as build

    require_liquidity(seen, allocation.slippage_bps)
    return build(allocation, seen)


def require_liquidity(seen, slippage_bps: int) -> None:
    """Refuse amounts that add nothing, naming what the range takes.

    Refused are amounts that cover no liquidity at the observed price, and amounts
    that cover some there and none at an edge of the slippage, which is what the
    planner sizes the mint for.
    """
    from almanak.connectors.uniswap_v4.math import liquidity_for
    from almanak.connectors.uniswap_v4.planning import tolerant_liquidity
    from almanak.connectors.uniswap_v4.reader import sqrt_bounds

    owned, tick, selected = seen.owned, seen.pool.tick, seen.target.range
    held = owned.inventory0.amount, owned.inventory1.amount
    price, (lower, upper) = seen.pool.sqrt_price_x96, sqrt_bounds(seen.target)
    if liquidity_for(*held, price, lower, upper):
        if tolerant_liquidity(held[0], held[1], price, lower, upper, slippage_bps):
            return
        raise ValueError(
            f"the stated amounts add no liquidity at every price within {slippage_bps} "
            f"basis points of the pool's price: inside that slippage the range "
            f"{selected.lower} to {selected.upper} takes both tokens. State both amounts, "
            "or a --slippage-bps that stays outside the range"
        )
    if tick < selected.lower:
        takes = "only the first token"
    elif tick >= selected.upper:
        takes = "only the second token"
    else:
        takes = "both tokens"
    raise ValueError(
        f"the stated amounts add no liquidity: at the pool's tick {tick} the range "
        f"{selected.lower} to {selected.upper} takes {takes}"
    )


def collect_liquidity_fees(allocation, seen):
    from almanak.connectors.uniswap_v4.models import Target
    from almanak.dex_intents import CollectLiquidityFees

    return CollectLiquidityFees[Target](
        target=seen.target,
        deadline=int(seen.effective_at.timestamp()) + allocation.deadline_seconds,
    )


def close_liquidity(allocation, seen):
    from almanak.connectors.uniswap_v4.authoring import close_liquidity as build

    return build(allocation, seen)


def revoke_exchange(allocation, seen):
    """The connector's allowance cleanup, refused when it would have nothing to clear."""
    from almanak.connectors.uniswap_v4.models import Target
    from almanak.dex_intents import RevokeExchange

    if not seen.needs_cleanup:
        raise ValueError(
            "nothing to revoke: no approval of a pool token to Permit2 holds an amount, "
            "and no Permit2 allowance to the pool's router or position manager holds an "
            "amount or has yet to expire"
        )
    return RevokeExchange[Target](target=seen.target)


def note(config, observed, request: ActionRequest) -> str | None:
    """What a revocation does beyond this venue: the approvals to Permit2 it zeroes."""
    if request.action != "revoke-exchange":
        return None
    (seen,) = observed
    shared = [str(a.asset) for a in seen.allowances if a.expiration is None and a.amount]
    if not shared:
        return None
    return (
        f"revoke-exchange sets the wallet's ERC-20 approval to Permit2 to zero for "
        f"{' and '.join(shared)}. Permit2 is shared: every other application that spends "
        "these tokens from this wallet through Permit2 needs them approved again afterwards"
    )


BUILDERS = {
    "open-liquidity": open_liquidity,
    "collect-liquidity-fees": collect_liquidity_fees,
    "close-liquidity": close_liquidity,
    "revoke-exchange": revoke_exchange,
}


def position(state) -> dict:
    """The NFT the deployment owns and what its steps moved in the wallet.

    `position` is None when the deployment owns no NFT: before a mint, and after a
    close. `moved` is the sum of what the deployment's mined steps moved in each held
    token, in base units: negative left the wallet for the pool, positive came back.
    It does not split fees from principal. Uniswap V4 records no amount owed to a
    position, so none is reported: `ax liquidity-position` computes what a collection
    would pay.
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
    }
