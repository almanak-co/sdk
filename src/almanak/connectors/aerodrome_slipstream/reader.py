"""Pinned Slipstream observations retain mutable fees and exact periphery tuples."""

from almanak.connectors.aerodrome_slipstream.deployments import (
    fee_modules,
    require_no_discount,
    verify_periphery,
    verify_pool,
)
from almanak.connectors.aerodrome_slipstream.domain import PoolObservation
from almanak.connectors.v3_pool_reader import allowance as allowance
from almanak.connectors.v3_pool_reader import consumed_quote
from almanak.connectors.v3_pool_reader import wallet_balance as wallet_balance
from almanak.gateway.ethereum import DataError


def read_pool(reader, source, block):
    verify_pool(reader, source, block)
    require_no_discount(reader, source, block)
    slot = reader.call(
        source.pool,
        "slot0()",
        block,
        outputs=("uint160", "int24", "uint16", "uint16", "uint16", "bool"),
    )
    return PoolObservation(
        source=source,
        block=block,
        currency0=reader.asset(source.currency0, block),
        currency1=reader.asset(source.currency1, block),
        sqrt_price_x96=slot[0],
        tick=slot[1],
        liquidity=reader.call(source.pool, "liquidity()", block, outputs=("uint128",))[0],
        unlocked=slot[5],
        swap_fee=reader.call(source.pool, "fee()", block, outputs=("uint24",))[0],
        unstaked_fee=reader.call(source.pool, "unstakedFee()", block, outputs=("uint24",))[0],
        staked_liquidity=reader.call(source.pool, "stakedLiquidity()", block, outputs=("uint128",))[
            0
        ],
        gauge=reader.call(source.pool, "gauge()", block, outputs=("address",))[0],
        nft_manager=reader.call(source.pool, "nft()", block, outputs=("address",))[0],
        **fee_modules(reader, source, block),
    )


def quote(reader, source, contracts, block, amount, output_asset):
    if (
        amount.amount <= 0
        or amount.asset == output_asset
        or {amount.asset, output_asset} != {source.currency0, source.currency1}
    ):
        raise ValueError("Slipstream quote requires positive input and the other pool currency")
    verify_pool(reader, source, block)
    verify_periphery(reader, source, contracts, block)
    require_no_discount(reader, source, block)
    result = reader.call(
        contracts.quoter,
        "quoteExactInputSingle((address,address,uint256,int24,uint160))",
        block,
        inputs=("(address,address,uint256,int24,uint160)",),
        args=(
            (amount.asset.reference, output_asset.reference, amount.amount, source.tick_spacing, 0),
        ),
        outputs=("uint256", "uint160", "uint32", "uint256"),
    )
    return consumed_quote(result, source, amount, "Slipstream")


def nft_position(reader, target, nft, block, *, expected=None):
    from almanak.connectors.v3_pool_reader import keyed_nft_position

    return keyed_nft_position(
        reader, target, nft, block, "int24", target.pool.tick_spacing, expected=expected
    )


def read_tick(reader, source, block, tick):
    result = reader.call(
        source.pool,
        "ticks(int24)",
        block,
        inputs=("int24",),
        args=(tick,),
        outputs=(
            "uint128",
            "int128",
            "int128",
            "uint256",
            "uint256",
            "uint256",
            "int56",
            "uint160",
            "uint32",
            "bool",
        ),
    )
    if bool(result[0]) != result[9]:
        raise DataError(
            "inconsistent", "Slipstream tick initialization differs from gross liquidity"
        )
    # Historical unstaked fee growth is already net of the pool's staking split.
    return result[0], result[1], result[3], result[4]
