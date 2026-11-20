"""Direct actions in one named Curve pool: one command, one deployment.

This module holds the exact-input swap and the report every command shares. The
liquidity commands and the revocation are `curve/liquidity_direct.py`.

A Curve pool can hold more than two coins, so the request names both the coin sold and
the coin bought. The deployment holds one trading allocation over the pool: the sold
coin funds it, its capital is the stated amount of that coin and nothing of any other,
and the whole of it is offered as one entry. The gateway then sizes its entry quote at
exactly the stated amount, into the coin bought, and admits the swap only along that
quote. The inventory attributed to the deployment is that amount and no more of the
wallet, so nothing in the deployment can sell more than the operator stated.

The allocation has no liquidity authority and owns no LP shares. A wallet that holds
the pool's LP token is refused while nothing is signed: here, before a deployment
exists, and at every later observation up to the signature. LP tokens that reach the
wallet after a signature stop nothing, since anyone can send them: the signed step is
still sent and reconciled from its own receipt, and its outcome lists the shares as an
accounting issue. The shares themselves are never moved.

The steps are the approval of exactly the stated amount to the pool itself, then the
pool's `exchange`, which spends that approval whole: a completed swap leaves the pool
no allowance of the sold coin. An allowance of another size that the wallet already
gave the pool for that coin is reset to zero first. A swap that is refused or reverts
after its approval was mined leaves that approval standing. Nothing here revokes it;
the result lists it, and a later swap of the same amount spends it. Allowances of the
pool's other coins are never touched, and the result lists the nonzero ones.

Time. No Curve call carries a deadline. `deadline_seconds` bounds only when the swap
may be signed: the gateway refuses to plan or sign it later than that many seconds
after the block the request was observed at. Once signed, the bytes stay valid until
they are mined or the wallet's nonce is used by another transaction. However long ago
they were signed, `almanak ax recover` reconciles their receipt or, when no provider
holds a copy and the swap still simulates at or above its minimum output, sends the
same bytes again. When the pool would pay less than the minimum, a swap that may have
been sent is neither sent again nor released: recovery reports
`signed_attempt_unsendable`, and the record keeps the wallet's claim until the swap
passes again, or until another transaction of the wallet that used its nonce is final,
which releases it. A swap the gateway retains as never sent is released instead. The one
bound a mined swap always meets is that minimum output, which the pool enforces.

The heavy imports stay inside the functions: the command line imports each connector's
`direct` module to build its choices.
"""

from almanak.connectors.direct import ALLOCATION, ActionRequest, DirectActions, Wallet


def positions(reader, block, resolved, wallet: Wallet, request: ActionRequest) -> dict:
    """The deployment's one allocation in the resolved pool, as the command states it."""
    if request.action != "swap":
        from almanak.connectors.curve import liquidity_direct as liquidity

        return liquidity.positions(reader, block, resolved, wallet, request)
    return swap_positions(reader, block, resolved, wallet, request)


def swap_positions(reader, block, resolved, wallet: Wallet, request: ActionRequest) -> dict:
    """One trading allocation in the resolved pool, funded with exactly the amount sold."""
    from almanak.chains import evm_network

    pool = resolved.pool
    coins = tuple(str(coin) for coin in pool.coins)
    require_pair(request, coins)
    if None in (request.amount, request.slippage_bps, request.deadline_seconds):
        raise ValueError("swap requires --amount, --slippage-bps and --deadline-seconds")
    require_no_shares(reader, block, pool, wallet)
    return {
        "allocations": [
            {
                "name": ALLOCATION,
                "account": wallet.account,
                "pool": pool.model_dump(mode="json"),
                "funding_asset": request.sell,
                "trading_asset": request.buy,
                "liquidity_enabled": False,
                "capital": [
                    {"asset": coin, "amount": request.amount if coin == request.sell else 0}
                    for coin in coins
                ],
                # The whole capital is one entry, so the entry quote is the stated swap.
                "entry_swap_bps": 10000,
                "slippage_bps": request.slippage_bps,
                "deadline_seconds": request.deadline_seconds,
                "native_asset": str(evm_network(pool.chain.chain_id).native_asset),
                "minimum_native_balance": 0,
            }
        ]
    }


def require_pair(request: ActionRequest, coins: tuple[str, ...]) -> None:
    listed = ", ".join(coins)
    if request.buy is None:
        raise ValueError(f"--buy must name the coin bought, one of the pool's coins: {listed}")
    for flag, stated in (("--sell", request.sell), ("--buy", request.buy)):
        if stated not in coins:
            raise ValueError(
                f"{flag} must name one of the pool's coins: {listed}; "
                "the native coin is traded as its wrapped token"
            )
    if request.sell == request.buy:
        raise ValueError("--sell and --buy must name two different coins of the pool")


def require_no_shares(reader, block, pool, wallet: Wallet) -> None:
    """Refuse a wallet that holds the pool's LP token, at the block the pool resolved at."""
    from almanak.caip import AccountReference
    from almanak.connectors.curve.ownership import unowned_shares
    from almanak.connectors.curve.reader import fund_shares
    from almanak.models import Account, AssetAmount

    fund = Account(chain=pool.chain, address=AccountReference(wallet.address))
    held = fund_shares(reader, pool, fund, block)
    if held:
        raise unowned_shares(AssetAmount(asset=pool.shares_asset, amount=held))


def intent(config, observed, request: ActionRequest):
    """The command's intent, built from the observation it is admitted with."""
    if request.action != "swap":
        from almanak.connectors.curve import liquidity_direct as liquidity

        return liquidity.intent(config, observed, request)
    return swap_intent(config, observed, request)


def swap_intent(config, observed, request: ActionRequest):
    """The swap along the entry quote of the observation it is admitted with.

    The amount is the quote's input, the coin bought is the quote's output, and the
    minimum output is the quoted output less the stated slippage. A quote of any other
    size or pair than the stated one is refused. The intent's deadline is the observed
    block time plus the stated seconds; it bounds planning and signing only, because no
    Curve call carries it.
    """
    from almanak.connectors.curve import authoring

    allocation = config.allocation(ALLOCATION)
    (seen,) = observed
    quote = seen.entry_quote
    if quote is None:
        raise ValueError(
            seen.entry_quote_error or "the pool quotes no output for the stated amount"
        )
    sold, bought = quote.amount_in, quote.amount_out
    if (str(sold.asset), sold.amount, str(bought.asset)) != (
        request.sell,
        request.amount,
        request.buy,
    ):
        raise ValueError("the observed quote is not for the stated amount and pair of coins")
    return authoring.swap(allocation, seen, quote)


def position(state) -> dict:
    """What the deployment holds of each pool coin, and what the wallet lets the pool spend.

    Inventory is the deployment's own, in the pool's coin order: what the command
    stated before its steps and what they left after, never the wallet's whole balance.
    Allowances are those of the last block the deployment observed, so a signed step
    still unresolved is not in them; only nonzero ones are listed. A liquidity
    deployment also reports the LP shares it owns and what its steps moved.
    """
    from almanak.connectors.v3_pool_direct import last_observed

    owned = state.owned[0]
    seen = last_observed(state)
    liquidity: dict = {}
    if owned.target.liquidity_enabled:
        from almanak.connectors.curve.liquidity_direct import position as held

        liquidity = held(state)
    return {
        **liquidity,
        "inventory": [
            {"asset": str(value.asset), "amount": str(value.amount)}
            for value in owned.inventory.root
        ],
        "allowances": [
            {"asset": str(item.asset), "spender": item.spender, "amount": str(item.amount)}
            for item in (() if seen is None else seen.allowances)
            if item.amount
        ],
    }


def liquidity_flags(command: str):
    from almanak.connectors.curve.liquidity_direct import FLAGS

    return FLAGS[command]


DIRECT = DirectActions(
    actions=(
        "swap",
        "open-liquidity",
        "increase-liquidity",
        "decrease-liquidity",
        "close-liquidity",
        "revoke-exchange",
    ),
    positions=positions,
    intent=intent,
    position=position,
    names_bought=True,
    liquidity_flags=liquidity_flags,
)
