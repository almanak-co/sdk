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
claims of the pool is refused when the deployment first observes it, before anything
is planned, because those claims are account-wide and cannot be kept apart from the
deployment. Shares that sit in the pool's own balance do not matter to a swap.

The heavy imports stay inside the functions: the command line imports each connector's
`direct` module to build its choices.
"""

from almanak.connectors.direct import ALLOCATION, ActionRequest, DirectActions, Wallet
from almanak.connectors.v3_pool_direct import intent, position


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
                # No pool shares are minted, burned or approved.
                "liquidity_enabled": False,
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


DIRECT = DirectActions(actions=("swap",), positions=positions, intent=intent, position=position)
