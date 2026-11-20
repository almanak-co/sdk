"""Uniswap V4 direct operator actions in one pool named by its key.

This module holds the exact-input swap and the report every command shares. The
liquidity commands and `revoke-exchange` are `liquidity_direct`.

The deployment holds one trading allocation whose capital is the amount sold, in the
asset sold, offered whole as one entry. The gateway then sizes its entry quote at
exactly the stated amount and admits the swap only along that quote, and the inventory
attributed to the deployment is that amount and no more of the wallet.

A pool key may name the chain's native coin. The wallet never sends native value: it
holds and sells the wrapped token, and receives the wrapped token when the pool pays
the native coin; the router unwraps and wraps inside the swap. `--sell` therefore names
one of the two tokens the wallet holds for the pool, never the native coin.

The steps are an ERC-20 approval of exactly the stated amount to Permit2, a Permit2
allowance of that amount to the router that expires at the swap's deadline, then the
swap, which spends both whole: Permit2 then holds only the expiration, with no amount.
An allowance the wallet already gave that covers the amount is used as it stands, a
Permit2 one only when it outlasts the swap's deadline, and what the swap does not spend
of it remains. Permit2 leaves its own unlimited allowance whole; a token leaves an
unlimited approval to Permit2 whole or lowers it by the amount sold, as that token is
written.

A swap that is refused or reverts after its approvals were mined leaves them standing:
the Permit2 allowance lapses at its expiration, the ERC-20 approval to Permit2 does not.
A signed swap whose deadline passes unmined is released, and leaves them the same way.
No swap revokes either; the result lists both, and `revoke-exchange` sets them to zero.

The heavy imports stay inside the functions: the command line imports each connector's
`direct` module to build its choices.
"""

from almanak.connectors.direct import ALLOCATION, ActionRequest, DirectActions, Wallet


def positions(reader, block, resolved, wallet: Wallet, request: ActionRequest) -> dict:
    """The deployment's one allocation in the resolved pool, as the command states it."""
    if request.action != "swap":
        from almanak.connectors.uniswap_v4 import liquidity_direct as liquidity

        return liquidity.positions(reader, block, resolved, wallet, request)
    return swap_positions(resolved, wallet, request)


def swap_positions(resolved, wallet: Wallet, request: ActionRequest) -> dict:
    """One trading allocation in the resolved pool, funded with exactly the amount sold."""
    from almanak.chains import evm_network
    from almanak.connectors.uniswap_v4.custody import custody_assets

    pool = resolved.pool
    if None in (request.amount, request.slippage_bps, request.deadline_seconds):
        raise ValueError("swap requires --amount, --slippage-bps and --deadline-seconds")
    held = tuple(str(asset) for asset in custody_assets(pool))
    if request.sell not in held:
        raise ValueError(sell_refusal(pool, held, request.sell))
    return {
        "allocations": [
            {
                "name": ALLOCATION,
                "account": wallet.account,
                "pool": pool.model_dump(mode="json"),
                "contracts": resolved.contracts.model_dump(mode="json"),
                "capital": {"asset": request.sell, "amount": request.amount},
                # The whole capital is one entry, so the entry quote is the stated swap.
                "entry_swap_bps": 10000,
                "slippage_bps": request.slippage_bps,
                "deadline_seconds": request.deadline_seconds,
                "native_asset": str(evm_network(pool.chain.chain_id).native_asset),
                "minimum_native_balance": 0,
            }
        ]
    }


def sell_refusal(pool, held: tuple[str, ...], sold) -> str:
    """Why an asset cannot be sold in a pool, naming what the wallet holds for it."""
    choice = f"{held[0]} or {held[1]}"
    if sold in (str(pool.currency0), str(pool.currency1)):
        # Only a native currency is a pool currency that the wallet does not hold.
        return (
            f"--sell names the pool's native coin, which the wallet holds and sells as its "
            f"wrapped token: name {choice}"
        )
    return f"--sell must name one of the two tokens the wallet holds for this pool, {choice}"


def intent(config, observed, request: ActionRequest):
    """The command's intent, built from the observation it is admitted with."""
    if request.action != "swap":
        from almanak.connectors.uniswap_v4 import liquidity_direct as liquidity

        return liquidity.intent(config, observed, request)
    return swap_intent(config, observed, request)


def swap_intent(config, observed, request: ActionRequest):
    """The swap along the entry quote of the observation it is admitted with.

    The amount is the quote's input, the minimum output is the quoted output less the
    stated slippage, and the deadline is the observed block time plus the stated
    seconds. A quote of any other size than the stated amount is refused.
    """
    from almanak.connectors.uniswap_v4.authoring import swap

    allocation = config.allocation(ALLOCATION)
    (seen,) = observed
    quote = seen.entry_quote
    if quote is None:
        raise ValueError(
            seen.entry_quote_error or "the pool quotes no output for the stated amount"
        )
    if (str(quote.amount.asset), quote.amount.amount) != (request.sell, request.amount):
        raise ValueError("the observed quote is not for the stated amount of the sold asset")
    return swap(allocation, seen, quote)


def position(state) -> dict:
    """What the deployment holds of the pool's two tokens, and what the wallet lets
    Permit2 and, through Permit2, the router and position manager spend.

    Inventory is the deployment's own: the amount to sell before the swap, the amount
    received after it, never the wallet's whole balance. Allowances are those of the
    last block the deployment observed, so a signed step still unresolved is not in
    them. Only nonzero amounts are listed. A Permit2 allowance carries its expiration
    in Unix seconds, after which it grants nothing; an ERC-20 approval to Permit2 has
    none. `permit2_unexpired`, present only when there is one, lists each Permit2
    allowance that holds no amount and had not expired at that block: it grants
    nothing, `revoke-exchange` expires it, and a strategy's start in the wallet is
    refused while it stands. A liquidity deployment also reports the NFT it owns and
    what its steps moved.
    """
    owned = state.owned[0]
    seen = last_observed(state)
    liquidity: dict = {}
    if owned.target.liquidity_enabled:
        from almanak.connectors.uniswap_v4.liquidity_direct import position as held

        liquidity = held(state)
    return {
        **liquidity,
        "inventory": [
            {"asset": str(value.asset), "amount": str(value.amount)}
            for value in (owned.inventory0, owned.inventory1)
        ],
        **standing(seen),
    }


def standing(seen) -> dict:
    """The allowances of one observation as a result lists them."""
    if seen is None:
        return {"allowances": []}
    now = seen.block.timestamp
    empty = [
        {"asset": str(item.asset), "spender": item.spender, "expiration": item.expiration}
        for item in seen.allowances
        if not item.amount and item.needs_cleanup(now)
    ]
    listed = {"allowances": [allowance(item) for item in seen.allowances if item.amount]}
    return {**listed, "permit2_unexpired": empty} if empty else listed


def allowance(item) -> dict:
    stated = {"asset": str(item.asset), "spender": item.spender, "amount": str(item.amount)}
    if item.expiration is not None:
        stated["expiration"] = item.expiration
    return stated


def note(config, observed, request: ActionRequest) -> str | None:
    from almanak.connectors.uniswap_v4 import liquidity_direct as liquidity

    return liquidity.note(config, observed, request)


def last_observed(state):
    """The latest observation the deployment retains: after its last step, else its start."""
    if state.outcomes:
        return state.outcomes[-1].after
    return state.baseline[0] if state.baseline else None


DIRECT = DirectActions(
    actions=(
        "swap",
        "open-liquidity",
        "collect-liquidity-fees",
        "close-liquidity",
        "revoke-exchange",
    ),
    positions=positions,
    intent=intent,
    position=position,
    note=note,
)
