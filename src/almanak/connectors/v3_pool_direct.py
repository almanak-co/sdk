"""Direct exact-input swaps for the V3 pool family: one stated amount in one named pool.

The deployment holds one trading allocation whose capital is the amount sold, in the
asset sold, offered whole as one entry. The gateway then sizes its entry quote at
exactly the stated amount and admits the swap only along that quote, and the inventory
attributed to the deployment is that amount and no more of the wallet. Nothing in the
deployment can sell more than the operator stated.

The steps are the approval of exactly the stated amount to the pool's router, then the
swap, which spends that approval whole: a completed swap leaves the router no
allowance. An allowance of another size that the wallet already gave the router is
reset to zero first, so it does not survive the command either. A swap that is refused
or reverts after its approval was mined leaves that approval standing. Nothing here
revokes it; the result lists it, and a later swap of the same amount spends it.

The heavy imports stay inside the functions: the command line imports each connector's
`direct` module to build its choices.
"""

from almanak.connectors.direct import ALLOCATION, ActionRequest, DirectActions, Wallet


def positions(reader, block, resolved, wallet: Wallet, request: ActionRequest) -> dict:
    """One trading allocation in the resolved pool, funded with exactly the amount sold."""
    from almanak.chains import evm_network

    pool = resolved.pool
    currencies = (str(pool.currency0), str(pool.currency1))
    if request.sell not in currencies:
        raise ValueError(
            f"--sell must name one of the pool's currencies, {currencies[0]} or "
            f"{currencies[1]}; the native coin is sold as its wrapped token"
        )
    if None in (request.amount, request.slippage_bps, request.deadline_seconds):
        raise ValueError("swap requires --amount, --slippage-bps and --deadline-seconds")
    return {
        "allocations": [
            {
                "name": ALLOCATION,
                "account": wallet.account,
                "pool": pool.model_dump(mode="json"),
                "contracts": resolved.contracts.model_dump(mode="json"),
                "range": None,
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


def intent(config, observed, request: ActionRequest):
    """The swap along the entry quote of the observation it is admitted with.

    The amount is the quote's input, the minimum output is the quoted output less the
    stated slippage, and the deadline is the observed block time plus the stated
    seconds. A quote of any other size than the stated amount is refused.
    """
    from almanak.connectors.composition import connector_for

    allocation = config.allocation(ALLOCATION)
    (seen,) = observed
    quote = seen.entry_quote
    if quote is None:
        raise ValueError(
            seen.entry_quote_error or "the pool quotes no output for the stated amount"
        )
    sold = quote.amount_in
    if (str(sold.asset), sold.amount) != (request.sell, request.amount):
        raise ValueError("the observed quote is not for the stated amount of the sold asset")
    return connector_for(config).codec.actions.authoring.swap(allocation, seen, quote)


def position(state) -> dict:
    """What the deployment holds of the pool's two currencies, and what the wallet lets
    the pool's router and position manager spend.

    Inventory is the deployment's own: the amount to sell before the swap, the amount
    received after it, never the wallet's whole balance. Allowances are those of the
    last block the deployment observed, so a signed step still unresolved is not in
    them; only nonzero ones are listed.
    """
    owned = state.owned[0]
    seen = last_observed(state)
    return {
        "inventory": [
            {"asset": str(value.asset), "amount": str(value.amount)}
            for value in (owned.inventory0, owned.inventory1)
        ],
        "allowances": [
            {"asset": str(item.asset), "spender": item.spender, "amount": str(item.amount)}
            for item in (() if seen is None else seen.allowances)
            if item.amount
        ],
    }


def last_observed(state):
    """The latest observation the deployment retains: after its last step, else its start."""
    if state.outcomes:
        return state.outcomes[-1].after
    return state.baseline[0] if state.baseline else None


DIRECT = DirectActions(actions=("swap",), positions=positions, intent=intent, position=position)
