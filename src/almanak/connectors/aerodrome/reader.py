"""Canonical pool balances, separate fee claims and explicit single-pool quotes."""

from almanak.connectors.aerodrome.deployments import verify_pool, verify_router
from almanak.connectors.aerodrome.domain import PoolObservation
from almanak.connectors.aerodrome.math import accrued
from almanak.gateway.ethereum import DataError


def read_pool(reader, source, block):
    *_, fees = verify_pool(reader, source, block)
    currency0 = reader.asset(source.currency0, block)
    currency1 = reader.asset(source.currency1, block)
    metadata = reader.call(
        source.pool,
        "metadata()",
        block,
        outputs=("uint256", "uint256", "uint256", "uint256", "bool", "address", "address"),
    )
    expected = (
        10**currency0.decimals,
        10**currency1.decimals,
        source.stable,
        source.currency0.reference,
        source.currency1.reference,
    )
    if (*metadata[:2], *metadata[4:]) != expected:
        raise DataError(
            "identity", "Aerodrome stored token metadata differs from configured assets"
        )
    return PoolObservation(
        source=source,
        block=block,
        currency0=currency0,
        currency1=currency1,
        shares=reader.asset(source.shares_asset, block),
        reserve0=metadata[2],
        reserve1=metadata[3],
        balance0=balance(reader, source.currency0, source.pool, block),
        balance1=balance(reader, source.currency1, source.pool, block),
        deposited_shares=balance(reader, source.shares_asset, source.pool, block),
        total_supply=reader.call(source.pool, "totalSupply()", block)[0],
        fee_bps=reader.call(
            source.factory,
            "getFee(address,bool)",
            block,
            inputs=("address", "bool"),
            args=(source.pool, source.stable),
        )[0],
        index0=reader.call(source.pool, "index0()", block)[0],
        index1=reader.call(source.pool, "index1()", block)[0],
        pool_fees=fees.address,
        paused=reader.call(source.factory, "isPaused()", block, outputs=("bool",))[0],
    )


def balance(reader, asset, owner, block):
    return reader.call(
        asset.reference, "balanceOf(address)", block, inputs=("address",), args=(owner,)
    )[0]


def quote(reader, source, contracts, block, amount, output_asset):
    if (
        amount.amount <= 0
        or amount.asset == output_asset
        or {amount.asset, output_asset} != {source.currency0, source.currency1}
    ):
        raise ValueError("Aerodrome quote requires positive input and the other pool currency")
    verify_pool(reader, source, block)
    verify_router(reader, source, contracts, block)
    route = (amount.asset.reference, output_asset.reference, source.stable, source.factory)
    result = reader.call(
        contracts.router,
        "getAmountsOut(uint256,(address,address,bool,address)[])",
        block,
        inputs=("uint256", "(address,address,bool,address)[]"),
        args=(amount.amount, (route,)),
        outputs=("uint256[]",),
    )[0]
    if len(result) != 2 or result[0] != amount.amount:
        raise DataError("liquidity", "Aerodrome router returned no valid single-pool output")
    if not result[1]:
        # Pool math returns zero for any input when a reserve is empty; with both
        # reserves positive a zero output measures input too small to move them.
        pool = read_pool(reader, source, block)
        if not min(pool.reserve0, pool.reserve1):
            raise DataError("liquidity", "Aerodrome pool has an empty reserve")
    return result[1]


def fund_position(reader, observation, fund):
    source, block = observation.source, observation.block
    if fund.chain != source.chain:
        raise ValueError("Aerodrome LP fund belongs to another execution scope")
    shares = reader.call(
        source.pool, "balanceOf(address)", block, inputs=("address",), args=(fund.address,)
    )[0]
    fees = tuple(
        fund_fee(reader, source.pool, block, fund.address, shares, index, global_index)
        for index, global_index in enumerate((observation.index0, observation.index1))
    )
    return shares, fees


def fund_fee(reader, pool, block, fund, shares, index, global_index):
    supply_index, claimable = fee_checkpoint(reader, pool, block, fund, index)
    return accrued(shares, global_index, supply_index, claimable)


def fee_checkpoint(reader, pool, block, fund, index):
    return tuple(
        reader.call(pool, f"{name}{index}(address)", block, inputs=("address",), args=(fund,))[0]
        for name in ("supplyIndex", "claimable")
    )
