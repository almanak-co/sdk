"""Direct liquidity actions in one named Curve pool: deposits for, and burns of, its LP token.

A Curve position is the wallet's balance of the pool's LP token. It has no range and no
NFT, and the balance is fungible. Each command is one operator deployment with one
adopting allocation (`Allocation.adopt`) that has liquidity authority and swaps nothing:
its entry share is zero and its observations carry no quote, so the gateway has no quote
to admit a swap along.

Adoption. At its first observation the deployment takes the wallet's whole balance of
the pool's LP token as its position, whoever minted or sent those shares: there is no
part of a fungible balance the wallet does not own. `open-liquidity` is for a wallet
that holds none and `increase-liquidity` for one that holds some; the other commands
burn from what the wallet holds. The connector keeps no cost basis, entry price or fee
baseline for shares, so shares minted elsewhere are burned and reconciled exactly like
its own. LP tokens that reach or leave the wallet between that first observation and a
signature refuse the step, and the command is stated again. After a signature they
stop nothing: the step is reconciled from its own receipt and the difference is an
accounting issue on its outcome.

Deposits. A deposit states base units per named coin and uses exactly those amounts:
a Curve pool takes what it is given and mints for it. The stated amounts are the
deployment's whole inventory, attributed only when the wallet covers them. The fewest
LP shares the deposit may mint is what the pool's own formula gives for those amounts
at the observed block, less the stated slippage; the pool enforces it.

Burns. `decrease-liquidity` burns a stated number of LP shares, below the balance, and
`close-liquidity` burns the whole balance. The operator states how either pays out:
`balanced` pays every coin in the pool's current proportions, and one named coin pays
that coin alone, which trades against the pool and pays its fee. The fewest of each
coin the burn may pay is the pool's formula at the observed block less the stated
slippage; the pool enforces it.

Allowances. A deposit approves the pool itself for exactly the stated amount of each
coin, one step per coin, and the pool takes each whole: a completed deposit leaves no
allowance. An allowance of another size that the wallet already gave the pool for a
deposited coin is reset to zero first. A deposit that is refused or interrupted after
some approvals were mined leaves those standing, and the result lists them. A burn
needs no approval. `revoke-exchange` sets every allowance the wallet gave the pool in
any of its coins to zero, one step per coin, and does nothing else.

Time. No Curve call carries a deadline. `deadline_seconds` bounds only when a step may
be signed; a signed step stays valid until it is mined or the wallet's nonce is used.

The heavy imports stay inside the functions: the command line imports each connector's
`direct` module to build its choices.
"""

from almanak.connectors.direct import ALLOCATION, ActionRequest, Wallet

# Commands that put the stated amounts into the pool, and commands that burn LP shares.
ADDITIONS = ("open-liquidity", "increase-liquidity")
REMOVALS = ("decrease-liquidity", "close-liquidity")
ACTIONS = (*ADDITIONS, *REMOVALS, "revoke-exchange")
# The position flags each command takes for this connector, as `ActionRequest` field
# names: those it requires, then those it also takes.
FLAGS = {
    "open-liquidity": (("amounts",), ()),
    "increase-liquidity": (("amounts",), ()),
    "decrease-liquidity": (("liquidity", "receive"), ()),
    "close-liquidity": (("receive",), ()),
    "revoke-exchange": ((), ()),
}
# What each priced command must also state.
BOUNDS = ("slippage_bps", "deadline_seconds")
# The payout of a burn that pays every coin in the pool's current proportions.
BALANCED = "balanced"
# Seconds after the observed block within which the gateway still plans and signs a
# revocation, which states no deadline of its own.
ADMISSION_SECONDS = 300


def positions(reader, block, resolved, wallet: Wallet, request: ActionRequest) -> dict:
    """One adopting liquidity allocation in the resolved pool, as the command states it."""
    from almanak.chains import evm_network

    require_stated(request)
    pool = resolved.pool
    coins = tuple(str(coin) for coin in pool.coins)
    capital = stated_amounts(request, coins)
    payout = stated_payout(request, coins)
    require_shares(request, held_shares(reader, block, pool, wallet))
    return {
        "allocations": [
            {
                "name": ALLOCATION,
                "account": wallet.account,
                "pool": pool.model_dump(mode="json"),
                # A liquidity deployment trades no pair; the target only has to name one.
                "funding_asset": coins[0],
                "trading_asset": coins[1],
                "liquidity_enabled": True,
                "single_coin_exit": payout,
                "capital": [{"asset": coin, "amount": capital[coin]} for coin in coins],
                "adopt": True,
                "entry_swap_bps": 0,
                # A revocation has no price bound.
                "slippage_bps": request.slippage_bps or 0,
                "deadline_seconds": request.deadline_seconds or ADMISSION_SECONDS,
                "native_asset": str(evm_network(pool.chain.chain_id).native_asset),
                "minimum_native_balance": 0,
            }
        ]
    }


def flag(name: str) -> str:
    return "--amount" if name == "amounts" else "--" + name.replace("_", "-")


def require_stated(request: ActionRequest) -> None:
    names = FLAGS[request.action][0] + BOUNDS * (request.action != "revoke-exchange")
    missing = [flag(name) for name in names if getattr(request, name) is None]
    if missing:
        raise ValueError(f"{request.action} requires {', '.join(missing)}")


def stated_amounts(request: ActionRequest, coins: tuple[str, ...]) -> dict[str, int]:
    """Base units of each pool coin the command deposits; none for a command that deposits
    nothing."""
    stated = dict.fromkeys(coins, 0)
    if request.action not in ADDITIONS:
        return stated
    for coin, amount in request.amounts or ():
        if coin not in coins:
            raise ValueError(
                f"--amount names {coin}, which is not a coin of the pool: {', '.join(coins)}; "
                "the native coin is deposited as its wrapped token"
            )
        if stated[coin]:
            raise ValueError(f"--amount names {coin} more than once")
        stated[coin] = amount
    if not any(stated.values()):
        raise ValueError(f"{request.action} requires an --amount above zero")
    return stated


def stated_payout(request: ActionRequest, coins: tuple[str, ...]) -> str | None:
    """The one coin a burn pays, or None when it pays every coin in proportion."""
    if request.action not in REMOVALS or request.receive == BALANCED:
        return None
    if request.receive not in coins:
        raise ValueError(
            f"--receive must be `{BALANCED}` or one of the pool's coins: {', '.join(coins)}"
        )
    return request.receive


def held_shares(reader, block, pool, wallet: Wallet) -> int:
    """The wallet's balance of the pool's LP token at the block the pool resolved at."""
    from almanak.caip import AccountReference
    from almanak.connectors.curve.reader import fund_shares
    from almanak.models import Account

    fund = Account(chain=pool.chain, address=AccountReference(wallet.address))
    return fund_shares(reader, pool, fund, block)


def require_shares(request: ActionRequest, held: int) -> None:
    """Refuse a command the wallet's LP balance cannot serve, before a deployment exists."""
    action = request.action
    if action == "open-liquidity" and held:
        raise ValueError(
            f"the wallet already holds {held} base units of this pool's LP token; "
            "increase-liquidity adds to them"
        )
    if action in ("increase-liquidity", *REMOVALS) and not held:
        raise ValueError(
            f"the wallet holds none of this pool's LP token; {action} acts on shares it holds"
        )
    if action == "decrease-liquidity" and (request.liquidity or 0) >= held:
        raise ValueError(below_balance(held))


def below_balance(held: int) -> str:
    return (
        f"--liquidity must be below the wallet's {held} base units of the pool's LP token; "
        "close-liquidity burns all of them"
    )


def intent(config, observed, request: ActionRequest):
    """The command's one intent, built from the observation it is admitted with.

    A deposit offers the whole attributed inventory, which is the stated amounts. A
    burn's minimum outputs are what the pool's formula pays for the shares at the
    observed block, less the stated slippage. The intent's deadline is the observed
    block time plus the allocation's seconds; it bounds planning and signing only,
    because no Curve call carries it.
    """
    allocation = config.allocation(ALLOCATION)
    (seen,) = observed
    require_shares(request, held_now(seen))
    return BUILDERS[request.action](allocation, seen, request)


def held_now(seen) -> int:
    position = seen.owned.position
    return 0 if position is None else position.shares.amount


def open_liquidity(allocation, seen, request):
    from almanak.connectors.curve import authoring

    return authoring.open_liquidity(allocation, seen)


def increase_liquidity(allocation, seen, request):
    from almanak.connectors.curve import authoring

    return authoring.increase_liquidity(allocation, seen)


def decrease_liquidity(allocation, seen, request):
    from almanak.connectors.curve import authoring

    return authoring.decrease_liquidity(allocation, seen, request.liquidity)


def close_liquidity(allocation, seen, request):
    from almanak.connectors.curve import authoring

    return authoring.close_liquidity(allocation, seen)


def revoke_exchange(allocation, seen, request):
    from almanak.connectors.curve.models import Target
    from almanak.dex_intents import RevokeExchange

    if not seen.needs_cleanup:
        raise ValueError("the wallet has given the pool no allowance in any of its coins")
    return RevokeExchange[Target](target=seen.target)


BUILDERS = {
    "open-liquidity": open_liquidity,
    "increase-liquidity": increase_liquidity,
    "decrease-liquidity": decrease_liquidity,
    "close-liquidity": close_liquidity,
    "revoke-exchange": revoke_exchange,
}


def position(state) -> dict:
    """The LP shares the deployment owns and what its mined steps moved in the wallet.

    `shares` is the deployment's position in base units of the pool's LP token: the
    balance it adopted, plus what its deposits minted, less what its burns burned.
    `moved` sums what its mined steps moved per pool coin and in the LP token, in base
    units: negative left the wallet, positive came to it. LP fees stay inside the share
    value, so nothing here splits fees from principal.
    """
    owned = state.owned[0]
    target = owned.target
    assets = (*target.assets, target.pool.shares_asset)
    deltas = [0] * len(assets)
    for outcome in state.outcomes:
        if outcome.success:
            changes = (*outcome.asset_changes, outcome.share_change)
            deltas = [total + change.amount for total, change in zip(deltas, changes, strict=True)]
    held = 0 if owned.position is None else owned.position.shares.amount
    return {
        "shares": {"asset": str(target.pool.shares_asset), "amount": str(held)},
        "moved": [
            {"asset": str(asset), "amount": str(delta)}
            for asset, delta in zip(assets, deltas, strict=True)
        ],
    }
