"""The read behind `ax liquidity-position`: one position-manager NFT at a pinned block.

The request names a chain, a pool and a token ID. The pool is resolved by its
connector's own resolver, as `ax pool` resolves it, and the NFT is read from that
pool's reviewed position manager. An NFT that does not exist or belongs to another
pool is refused. Nothing is required of the owner: the report names it.
"""

from typing import Annotated

from pydantic import Field, model_validator

from almanak.caip import ChainId
from almanak.models import ZERO_ADDRESS, Address, Block, Model, UInt


class PositionRequest(Model):
    chain: ChainId
    pool: Address
    token_id: Annotated[UInt, Field(gt=0, lt=2**256)]

    @model_validator(mode="after")
    def scope(self):
        if self.chain.namespace != "eip155" or self.pool == ZERO_ADDRESS:
            raise ValueError("position request requires an EVM chain and a nonzero pool address")
        return self


def read_liquidity_position(reader, connector: str, request: PositionRequest, block: Block) -> dict:
    from almanak.connectors.concentrated_math import amounts, sqrt_at_tick
    from almanak.connectors.v3_pool_liquidity_direct import named_record
    from almanak.connectors.v3_pool_request import family, side, support_level

    if request.chain != reader.chain.chain_id:
        raise ValueError("request chain differs from the reader chain")
    level = support_level(connector, request.chain)
    selected = family(connector)
    selection = selected.resolution.MarketSelection(
        connector=connector, chain=request.chain, pool=request.pool
    )
    resolved = selected.resolution.resolve_market(reader, selection, block)
    owner, record = named_record(reader, block, resolved, request.token_id)
    position = verified_position(reader, connector, resolved, owner, record, request, block)
    lower, upper = record[5], record[6]
    observation = selected.read_pool(reader, resolved.pool, block)
    principal = amounts(
        position.liquidity, observation.sqrt_price_x96, sqrt_at_tick(lower), sqrt_at_tick(upper)
    )
    recorded = position.owed0, position.owed1
    accrued = accrued_fees(reader, selected, observation, block, position, (lower, upper))
    currencies = observation.currency0, observation.currency1
    return {
        "product": "liquidity_position",
        "request": {**request.model_dump(mode="json"), "connector": connector},
        "support": level,
        "resolution": resolved.summary(),
        "block": block.model_dump(mode="json"),
        "owner": owner,
        "position": {
            "nft": str(position.nft),
            "token_id": str(request.token_id),
            "range": {"lower": lower, "upper": upper},
            "liquidity": str(position.liquidity),
        },
        "pool": {
            "sqrt_price_x96": str(observation.sqrt_price_x96),
            "tick": observation.tick,
            # Liquidity earns fees only while the pool's tick is inside the range.
            "in_range": lower <= observation.tick < upper,
        },
        # What removing all liquidity would release at the pool's price at this block.
        "principal": [
            side(asset, value) for asset, value in zip(currencies, principal, strict=True)
        ],
        # What a collection would pay at this block: `owed_recorded` plus the fees the
        # position's liquidity earned since the position manager last updated it.
        "owed": [
            side(asset, old + new)
            for asset, old, new in zip(currencies, recorded, accrued, strict=True)
        ],
        # What the position manager recorded at the position's last change. It can
        # include principal a decrease released and nobody collected.
        "owed_recorded": [
            side(asset, value) for asset, value in zip(currencies, recorded, strict=True)
        ],
    }


def verified_position(reader, connector, resolved, owner, record, request, block):
    """The position as the connector's own reader verifies it against the resolved pool."""
    from importlib import import_module

    from almanak.caip import AssetId
    from almanak.connectors.v3_pool_domain import TickRange
    from almanak.models import Account

    package = f"almanak.connectors.{connector}"
    pool = resolved.pool
    target = import_module(f"{package}.models").Target(
        allocation="position",
        account="owner",
        fund=Account(chain=pool.chain, address=owner),
        pool=pool,
        contracts=resolved.contracts,
        range=TickRange(lower=record[5], upper=record[6]),
        funding_asset=pool.currency0,
    )
    nft = AssetId(
        f"{pool.chain.chain_id}/erc721:{resolved.contracts.position_manager}/{request.token_id}"
    )
    return import_module(f"{package}.reader").nft_position(reader, target, nft, block)


def accrued_fees(reader, selected, observation, block, position, bounds) -> tuple[int, int]:
    """Fees the position's liquidity earned since the manager last recorded its growth.

    The pool's fee growth inside the range, less the checkpoint the position holds,
    times its liquidity: the amounts the position manager adds to what it owes when the
    position is next updated. A position without liquidity earns nothing.
    """
    from almanak.connectors.v3_fee_growth import accrued, inside

    if not position.liquidity:
        return 0, 0
    pool = observation.source
    total = tuple(
        reader.call(pool.pool, f"feeGrowthGlobal{index}X128()", block)[0] for index in (0, 1)
    )
    lower, upper = (selected.read_tick(reader, pool, block, tick) for tick in bounds)
    current = inside(observation.tick, bounds[0], bounds[1], total, lower[2:4], upper[2:4])
    earned = tuple(
        accrued(position.liquidity, previous, now)
        for previous, now in zip(position.fee_growth, current, strict=True)
    )
    return earned[0], earned[1]
