"""The read behind `ax liquidity-position` for Uniswap V4: one PositionManager NFT.

The request names a chain, a pool key and a token ID. The pool is resolved by the
connector's own resolver, and the NFT is read from that chain's reviewed
PositionManager. An NFT that does not exist or belongs to another pool key is refused.
Nothing is required of the owner: the report names it.

Amounts are in the tokens a wallet holds for the pool: where the key names the native
coin, the wrapped token stands in its place, as it does in every action.
"""

import json
from typing import Annotated

from pydantic import Field

from almanak.caip import AssetType, ChainId
from almanak.connectors.uniswap_v4.domain import PoolFee
from almanak.models import ZERO_ADDRESS, Address, Block, Model, UInt


class PositionRequest(Model):
    chain: ChainId
    currency0: AssetType
    currency1: AssetType
    fee: PoolFee
    tick_spacing: Annotated[int, Field(strict=True, ge=1, le=32767)]
    hooks: Address = ZERO_ADDRESS
    token_id: Annotated[UInt, Field(gt=0, lt=2**256)]

    def selection(self):
        """The pool key as the connector's own market selection, which validates it."""
        from almanak.connectors.uniswap_v4.resolution import MarketSelection

        key = self.model_dump(mode="json", exclude={"token_id"})
        return MarketSelection.model_validate_json(json.dumps({"connector": "uniswap_v4", **key}))


def read_liquidity_position(reader, request: PositionRequest, block: Block) -> dict:
    from almanak.connectors.uniswap_v4.liquidity_direct import named_record
    from almanak.connectors.uniswap_v4.math import amounts, sqrt_at_tick
    from almanak.connectors.uniswap_v4.pool import read_pool
    from almanak.connectors.uniswap_v4.resolution import resolve_market
    from almanak.connectors.v3_pool_request import side, support_level

    if request.chain != reader.chain.chain_id:
        raise ValueError("request chain differs from the reader chain")
    level = support_level("uniswap_v4", request.chain)
    resolved = resolve_market(reader, request.selection(), block)
    owner, ticks, liquidity = named_record(reader, block, resolved, request.token_id)
    observation = read_pool(reader, resolved.pool, block)
    principal = amounts(
        liquidity, observation.sqrt_price_x96, sqrt_at_tick(ticks[0]), sqrt_at_tick(ticks[1])
    )
    owed = accrued_fees(reader, resolved, owner, ticks, liquidity, request.token_id, block)
    manager = resolved.contracts.position_manager
    return {
        "product": "liquidity_position",
        "request": {**request.model_dump(mode="json"), "connector": "uniswap_v4"},
        "support": level,
        "resolution": resolved.summary(),
        "block": block.model_dump(mode="json"),
        "owner": owner,
        "position": {
            "nft": f"{request.chain}/erc721:{manager}/{request.token_id}",
            "token_id": str(request.token_id),
            "range": {"lower": ticks[0], "upper": ticks[1]},
            "liquidity": str(liquidity),
        },
        "pool": {
            "sqrt_price_x96": str(observation.sqrt_price_x96),
            "tick": observation.tick,
            # Liquidity earns fees only while the pool's tick is inside the range.
            "in_range": ticks[0] <= observation.tick < ticks[1],
        },
        # What removing all liquidity would release at the pool's price at this block.
        "principal": [
            side(asset, value) for asset, value in zip(resolved.assets, principal, strict=True)
        ],
        # What a collection would pay at this block: the fees the position's liquidity
        # earned since the PoolManager last moved its fee checkpoint. Uniswap V4 records
        # no other amount owed to a position.
        "owed": [side(asset, value) for asset, value in zip(resolved.assets, owed, strict=True)],
    }


def accrued_fees(reader, resolved, owner, ticks, liquidity, token_id, block) -> tuple[int, int]:
    """Fees the position's liquidity earned since its checkpoint in the PoolManager.

    The pool's fee growth inside the range, less the checkpoint the position holds,
    times its liquidity. A position without liquidity earns nothing.
    """
    from almanak.connectors.uniswap_v4 import reader as reads
    from almanak.connectors.uniswap_v4.math import accrued
    from almanak.connectors.uniswap_v4.models import Range, Target
    from almanak.models import Account

    if not liquidity:
        return 0, 0
    target = Target(
        allocation="position",
        account="owner",
        fund=Account(chain=resolved.pool.chain, address=owner),
        pool=resolved.pool,
        contracts=resolved.contracts,
        range=Range(lower=ticks[0], upper=ticks[1]),
    )
    current = reads.fee_growth(reader, target, block)
    _, checkpoint = reads.position_growth(reader, target, token_id, block)
    earned = tuple(
        accrued(liquidity, previous, now) for previous, now in zip(checkpoint, current, strict=True)
    )
    return earned[0], earned[1]
