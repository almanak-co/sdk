"""Pinned Pancake observations retain the configured pool, currencies and Quoter."""

from almanak.connectors.pancakeswap_v3.deployments import verify_periphery, verify_pool
from almanak.connectors.pancakeswap_v3.domain import PoolObservation
from almanak.connectors.v3_pool_reader import allowance as allowance
from almanak.connectors.v3_pool_reader import consumed_quote
from almanak.connectors.v3_pool_reader import nft_position as nft_position
from almanak.connectors.v3_pool_reader import wallet_balance as wallet_balance


def read_pool(reader, source, block):
    verify_pool(reader, source, block)
    slot = reader.call(
        source.pool,
        "slot0()",
        block,
        outputs=("uint160", "int24", "uint16", "uint16", "uint16", "uint32", "bool"),
    )
    liquidity = reader.call(source.pool, "liquidity()", block, outputs=("uint128",))[0]
    callback = reader.call(source.pool, "lmPool()", block, outputs=("address",))[0]
    return PoolObservation(
        source=source,
        block=block,
        currency0=reader.asset(source.currency0, block),
        currency1=reader.asset(source.currency1, block),
        sqrt_price_x96=slot[0],
        tick=slot[1],
        liquidity=liquidity,
        protocol_fee=slot[5],
        unlocked=slot[6],
        lm_pool=callback,
    )


def quote(reader, source, contracts, block, amount, output_asset):
    if (
        amount.amount <= 0
        or amount.asset == output_asset
        or {amount.asset, output_asset} != {source.currency0, source.currency1}
    ):
        raise ValueError("Pancake quote requires positive input and the other pool currency")
    verify_pool(reader, source, block)
    verify_periphery(reader, source, contracts, block)
    result = reader.call(
        contracts.quoter,
        "quoteExactInputSingle((address,address,uint256,uint24,uint160))",
        block,
        inputs=("(address,address,uint256,uint24,uint160)",),
        args=((amount.asset.reference, output_asset.reference, amount.amount, source.fee, 0),),
        outputs=("uint256", "uint160", "uint32", "uint256"),
    )
    return consumed_quote(result, source, amount, "Pancake")
