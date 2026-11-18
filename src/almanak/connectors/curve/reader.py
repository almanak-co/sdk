"""Canonical Curve state and quotes retain the configured pool and coin identity."""

from almanak.connectors.curve import crypto_reader, tricrypto_reader
from almanak.connectors.curve.deployment import scalar, verify_pool
from almanak.connectors.curve.domain import Amplification, PoolObservation
from almanak.connectors.curve.pool_abi import layout, signature
from almanak.gateway.ethereum import DataError


def quantities(reader, source, block, name):
    return tuple(
        reader.call(source.pool, name + "(uint256)", block, inputs=("uint256",), args=(index,))[0]
        for index in range(len(source.coins))
    )


def read_pool(reader, source, block):
    if source.family == "tricrypto_ng":
        return tricrypto_reader.read_pool(reader, source, block)
    if source.family == "twocrypto_ng":
        return crypto_reader.read_pool(reader, source, block)
    return read_stable_pool(reader, source, block)


def read_stable_pool(reader, source, block):
    deployment = verify_pool(reader, source, block)
    currencies = tuple(reader.asset(coin, block) for coin in source.coins)
    rates = tuple(scalar(reader, source.pool, "stored_rates()", block, outputs=("uint256[]",)))
    if rates != deployment.rates or rates != tuple(
        10 ** (36 - coin.decimals) for coin in currencies
    ):
        raise DataError(
            "asset_units", "Curve standard-coin rates differ from immutable token precision"
        )
    ramp = Amplification(
        **{
            field: scalar(reader, source.pool, name + "()", block, outputs=("uint256",))
            for field, name in (
                ("initial", "initial_A"),
                ("future", "future_A"),
                ("initial_time", "initial_A_time"),
                ("future_time", "future_A_time"),
            )
        }
    )
    if ramp.at(block.timestamp) != scalar(
        reader, source.pool, "A_precise()", block, outputs=("uint256",)
    ):
        raise DataError(
            "amplification", "Curve measured amplification differs from its retained ramp"
        )
    return PoolObservation(
        source=source,
        block=block,
        currencies=currencies,
        shares=reader.asset(source.shares_asset, block),
        balances=quantities(reader, source, block, "balances"),
        admin_balances=quantities(reader, source, block, "admin_balances"),
        rates=rates,
        asset_types=deployment.asset_types,
        total_supply=scalar(reader, source.pool, "totalSupply()", block, outputs=("uint256",)),
        amplification=ramp,
        fee=scalar(reader, source.pool, "fee()", block, outputs=("uint256",)),
        offpeg_multiplier=scalar(
            reader, source.pool, "offpeg_fee_multiplier()", block, outputs=("uint256",)
        ),
        implementation=deployment.blueprint.address,
        views=deployment.views.address,
        revision=deployment.revision,
    )


def quote(reader, source, block, amount, output_asset):
    pair = amount.asset, output_asset
    if (
        amount.amount <= 0
        or len(set(pair)) != 2
        or any(asset not in source.coins for asset in pair)
    ):
        raise ValueError("Curve quote requires positive input and two configured pool coins")
    verify_pool(reader, source, block)
    first, second = (source.coins.index(asset) for asset in pair)
    index = layout(source).index
    types = (index, index, "uint256")
    output = reader.call(
        source.pool,
        signature("get_dy", types),
        block,
        inputs=types,
        args=(first, second, amount.amount),
    )[0]
    if not output:
        # With both coin balances positive, a zero output measures an input too small
        # to move them; an empty balance is missing liquidity.
        balances = quantities(reader, source, block, "balances")
        if not min(balances[first], balances[second]):
            raise DataError("liquidity", "Curve pool has an empty balance for the configured swap")
    return output


def fund_shares(reader, source, fund, block):
    if fund.chain != source.chain or block.chain != source.chain:
        raise ValueError("Curve LP fund belongs to another execution scope")
    return reader.call(
        source.pool, "balanceOf(address)", block, inputs=("address",), args=(fund.address,)
    )[0]
