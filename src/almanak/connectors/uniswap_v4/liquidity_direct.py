"""Direct liquidity actions for Uniswap V4: one position in one pool named by its key.

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

A mint is for a fixed liquidity, the most the stated amounts cover at the observed
price. Each amount the mint may take is the stated one, trimmed to what that liquidity
takes at the edge of the stated slippage around the observed price, so a price moved
further than that in either direction after signing reverts the mint instead of
spending the amount stated beyond the range's ratio (`bounded`).

A mint approves Permit2 for exactly that most of each token and gives the
PositionManager a Permit2 allowance of it that expires at the mint's deadline. The
mint takes at most that, and what it does not take stays approved: the Permit2
allowance until it expires, the ERC-20 approval to Permit2 without limit of time. An
approval the wallet already gave that covers the amount is used as it stands. Nothing
here revokes either; the result lists both. A collection and a close approve nothing.

The heavy imports stay inside the functions: the command line imports each connector's
`direct` module to build its choices.
"""

from almanak.connectors.direct import ALLOCATION, ActionRequest, Wallet

# Commands that act on an existing position, named by its token ID.
NFT_ACTIONS = ("collect-liquidity-fees", "close-liquidity")
ACTIONS = ("open-liquidity", *NFT_ACTIONS)
# What each command must state besides its range or its NFT, as `ActionRequest` field names.
REQUIRED = {
    "open-liquidity": ("tick_lower", "tick_upper", "slippage_bps", "deadline_seconds"),
    "collect-liquidity-fees": (),
    "close-liquidity": ("slippage_bps", "deadline_seconds"),
}
# Seconds after the observed block within which a collection is still planned and
# signed, and mined: its call carries that deadline.
ADMISSION_SECONDS = 300


def positions(reader, block, resolved, wallet: Wallet, request: ActionRequest) -> dict:
    """One adopting allocation in the resolved pool, for the range or NFT the command names."""
    from almanak.chains import evm_network
    from almanak.connectors.uniswap_v4.custody import custody_assets

    require_stated(request)
    pool = resolved.pool
    nft = None
    if request.action == "open-liquidity":
        selected = stated_range(pool, request)
        amount0, amount1 = stated_amounts(request)
    else:
        selected, nft = named_position(reader, block, resolved, wallet, request)
        amount0, amount1 = 0, 0
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
                # A collection has no price bound.
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

    A mint offers the whole attributed inventory, which is the stated amounts. A
    close's minimum outputs are the principal at the observed price less the stated
    slippage. Each deadline is the observed block time plus the allocation's seconds.
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

    require_liquidity(seen)
    return bounded(allocation, seen, build(allocation, seen))


def bounded(allocation, seen, intent):
    """The mint with each maximum trimmed to what its liquidity takes inside the price
    tolerance, so the stated slippage bounds the mined price in both directions.

    A mint is for a fixed liquidity, the most the stated amounts cover at the observed
    price, and the PositionManager only refuses one that needs more than a maximum. The
    amount stated beyond the range's ratio would otherwise be a maximum far above what
    that liquidity takes: a price pushed after signing could make the mint take up to
    all of it at the pushed price. Each maximum is therefore at most what the liquidity
    takes at the edge of the tolerance window where it takes the most of that token:
    the lowest price for the first token, the highest for the second. The token that
    limits the liquidity keeps its stated amount, which already reverts the mint when
    the price moves against it. The planner derives the same liquidity from the trimmed
    maximums, since each still covers what that liquidity takes at the observed price.
    """
    from math import isqrt

    from almanak.connectors.uniswap_v4.math import amounts, liquidity_for
    from almanak.connectors.uniswap_v4.reader import sqrt_bounds

    lower, upper = sqrt_bounds(seen.target)
    price = seen.pool.sqrt_price_x96
    stated = intent.maximum0, intent.maximum1
    liquidity = liquidity_for(stated[0].amount, stated[1].amount, price, lower, upper)
    # Square-root prices at the observed price less and plus the tolerance.
    low, high = (
        isqrt(price * price * (10000 + sign * allocation.slippage_bps) // 10000) for sign in (-1, 1)
    )
    most = (
        amounts(liquidity, low, lower, upper, up=True)[0],
        amounts(liquidity, high, lower, upper, up=True)[1],
    )
    trimmed = [
        value.model_copy(update={"amount": min(value.amount, limit)})
        for value, limit in zip(stated, most, strict=True)
    ]
    return intent.model_copy(update={"maximum0": trimmed[0], "maximum1": trimmed[1]})


def require_liquidity(seen) -> None:
    """Refuse amounts that add nothing at the observed price, naming what the range takes."""
    from almanak.connectors.uniswap_v4.math import liquidity_for
    from almanak.connectors.uniswap_v4.reader import sqrt_bounds

    owned, tick, selected = seen.owned, seen.pool.tick, seen.target.range
    held = owned.inventory0.amount, owned.inventory1.amount
    if liquidity_for(*held, seen.pool.sqrt_price_x96, *sqrt_bounds(seen.target)):
        return
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


BUILDERS = {
    "open-liquidity": open_liquidity,
    "collect-liquidity-fees": collect_liquidity_fees,
    "close-liquidity": close_liquidity,
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
