"""The read behind `ax liquidity-position` for Aerodrome: one account's shares of one
pool at a pinned block.

A position is an account's balance of the pool's own share token. The request names a
chain, a pool and the account. The pool is resolved by the connector's own resolver, as
`ax pool` resolves it. Nothing is required of the account: one that holds no shares and
no fee claims reads as zero everywhere.
"""

from typing import Final

from pydantic import model_validator

from almanak.caip import ChainId
from almanak.models import ZERO_ADDRESS, Address, Block, Model

CONNECTOR: Final = "aerodrome"


class PositionRequest(Model):
    chain: ChainId
    pool: Address
    account: Address

    @model_validator(mode="after")
    def scope(self):
        if self.chain.namespace != "eip155" or ZERO_ADDRESS in (self.pool, self.account):
            raise ValueError(
                "position request requires an EVM chain, a nonzero pool and a nonzero account"
            )
        return self


def read_liquidity_position(reader, request: PositionRequest, block: Block) -> dict:
    from almanak.connectors.aerodrome import resolution
    from almanak.connectors.aerodrome.observations import stored_position
    from almanak.connectors.aerodrome.ownership import at_indices
    from almanak.connectors.aerodrome.reader import read_pool
    from almanak.connectors.v3_pool_request import side, support_level

    if request.chain != reader.chain.chain_id:
        raise ValueError("request chain differs from the reader chain")
    level = support_level(CONNECTOR, request.chain)
    selection = resolution.MarketSelection(
        connector=CONNECTOR, chain=request.chain, pool=request.pool
    )
    resolved = resolution.resolve_market(reader, selection, block)
    pool = read_pool(reader, resolved.pool, block)
    recorded = stored_position(reader, holder(resolved, request.account), pool)
    claimable = at_indices(recorded, pool)
    shares = recorded.shares.amount
    currencies = pool.currency0, pool.currency1
    return {
        "product": "liquidity_position",
        "request": {**request.model_dump(mode="json"), "connector": CONNECTOR},
        "support": level,
        "resolution": resolved.summary(),
        "block": block.model_dump(mode="json"),
        "owner": request.account,
        "position": {
            "shares": {"asset": str(recorded.shares.asset), "amount": str(shares)},
            # The same number under the name `ax decrease-liquidity --liquidity` takes.
            "liquidity": str(shares),
        },
        "pool": {
            "stable": resolved.pool.stable,
            "total_supply": str(pool.total_supply),
            "balances": [
                side(asset, value)
                for asset, value in zip(currencies, (pool.balance0, pool.balance1), strict=True)
            ],
        },
        # What burning every share would release at the pool's balances at this block.
        "principal": [
            side(asset, value)
            for asset, value in zip(currencies, principal(pool, shares), strict=True)
        ],
        # What a claim would pay at this block: `owed_recorded` plus the fees the shares
        # earned since the pool last updated the account.
        "owed": [
            side(asset, fee.amount)
            for asset, fee in zip(currencies, (claimable.fees0, claimable.fees1), strict=True)
        ],
        # What the pool stored for the account at the position's last change.
        "owed_recorded": [
            side(asset, fee.amount)
            for asset, fee in zip(currencies, (recorded.fees0, recorded.fees1), strict=True)
        ],
    }


def holder(resolved, account):
    """The resolved pool as the target of an account's own shares, for the position read."""
    from almanak.connectors.aerodrome.models import Target
    from almanak.models import Account

    pool = resolved.pool
    return Target(
        allocation="position",
        account="owner",
        fund=Account(chain=pool.chain, address=account),
        pool=pool,
        contracts=resolved.contracts,
        funding_asset=pool.currency0,
        liquidity_enabled=True,
    )


def principal(pool, shares: int) -> tuple[int, int]:
    """The shares' part of the pool's own balances, rounded down as a burn rounds."""
    if not pool.total_supply:
        return 0, 0
    return (
        shares * pool.balance0 // pool.total_supply,
        shares * pool.balance1 // pool.total_supply,
    )
