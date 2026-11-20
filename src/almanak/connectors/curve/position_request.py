"""The read behind `ax liquidity-position` for Curve: one account's LP shares in one pool.

A Curve position is an account's balance of the pool's LP token, so the request names a
chain, a pool and an account. The pool is resolved by the connector's own resolver, as
`ax pool` resolves it. Nothing is required of the account: an account that holds no
shares is reported with none.
"""

from pydantic import model_validator

from almanak.caip import ChainId
from almanak.models import ZERO_ADDRESS, Address, Block, Model


class PositionRequest(Model):
    chain: ChainId
    pool: Address
    account: Address

    @model_validator(mode="after")
    def scope(self):
        if self.chain.namespace != "eip155" or ZERO_ADDRESS in (self.pool, self.account):
            raise ValueError("position request requires an EVM chain, a pool and an account")
        return self


def read_liquidity_position(reader, request: PositionRequest, block: Block) -> dict:
    from almanak.caip import AccountReference
    from almanak.connectors.curve import liquidity_quotes
    from almanak.connectors.curve import reader as reads
    from almanak.connectors.curve.resolution import MarketSelection, resolve_market
    from almanak.connectors.v3_pool_request import support_level
    from almanak.models import Account

    if request.chain != reader.chain.chain_id:
        raise ValueError("request chain differs from the reader chain")
    level = support_level("curve", request.chain)
    selection = MarketSelection(connector="curve", chain=request.chain, pool=request.pool)
    resolved = resolve_market(reader, selection, block)
    pool = resolved.pool
    owner = Account(chain=pool.chain, address=AccountReference(request.account))
    held = reads.fund_shares(reader, pool, owner, block)
    observation = reads.read_pool(reader, pool, block)
    paid = liquidity_quotes.removed(observation, held, None) if held else (0,) * len(pool.coins)
    return {
        "product": "liquidity_position",
        "request": {**request.model_dump(mode="json"), "connector": "curve"},
        "support": level,
        "resolution": resolved.summary(),
        "block": block.model_dump(mode="json"),
        "owner": request.account,
        "position": {
            "shares": side(resolved.share, held),
            "total_supply": side(resolved.share, observation.total_supply),
        },
        # What burning all the shares for every coin in the pool's proportions would pay
        # at this block, by the pool's own formula. LP fees are inside the share value,
        # so nothing here splits fees from principal.
        "principal": [
            side(asset, value) for asset, value in zip(resolved.coins, paid, strict=True)
        ],
    }


def side(asset, value: int) -> dict:
    return {"asset": str(asset.identifier), "amount": str(value), "decimals": asset.decimals}
