"""Retain TriCrypto storage and custody balances at one canonical block."""

from almanak.connectors.curve.crypto_deployment import storage
from almanak.connectors.curve.crypto_reader import scalar
from almanak.connectors.curve.tricrypto_deployment import revision, revision_files, verify_pool
from almanak.connectors.curve.tricrypto_observation import (
    TriCryptoAccounting,
    TriCryptoObservation,
    TriCryptoRamp,
)
from almanak.connectors.curve.tricrypto_token_accounting import TokenAccounting
from almanak.gateway.ethereum import DataError


def ramp(reader, source, block):
    initial = scalar(reader, source, block, "initial_A_gamma")
    future = scalar(reader, source, block, "future_A_gamma")
    result = TriCryptoRamp(
        initial_amplification=initial >> 128,
        initial_gamma=initial & (2**128 - 1),
        future_amplification=future >> 128,
        future_gamma=future & (2**128 - 1),
        initial_time=scalar(reader, source, block, "initial_A_gamma_time"),
        future_time=scalar(reader, source, block, "future_A_gamma_time"),
    )
    measured = tuple(scalar(reader, source, block, name) for name in ("A", "gamma"))
    if result.at(block.timestamp) != measured:
        raise DataError("amplification", "TriCrypto parameters differ from their retained ramp")
    return result


def stored_prices(reader, source, block, name):
    slot = revision(revision_files(source.implementation_revision)[0])["pool_storage_layout"][name][
        "slot"
    ]
    value = storage(reader, source.pool, slot, block)
    return value & (2**128 - 1), value >> 128


def accounting(reader, source, block):
    return TriCryptoAccounting(
        virtual_price=scalar(reader, source, block, "virtual_price"),
        profit=scalar(reader, source, block, "xcp_profit"),
        claimed_profit=scalar(reader, source, block, "xcp_profit_a"),
        receiver=reader.call(source.factory, "fee_receiver()", block, outputs=("address",))[0],
    )


def indexed(reader, source, block, name, count):
    return tuple(
        reader.call(source.pool, name + "(uint256)", block, inputs=("uint256",), args=(index,))[0]
        for index in range(count)
    )


def custody_balances(reader, source, block, wrapped):
    result = []
    for coin in source.coins:
        if coin.reference == wrapped:
            value = int(
                reader._rpc(
                    "eth_getBalance",
                    [source.pool, {"blockHash": block.hash, "requireCanonical": True}],
                ),
                16,
            )
        else:
            value = reader.call(
                coin.reference,
                "balanceOf(address)",
                block,
                inputs=("address",),
                args=(source.pool,),
            )[0]
        result.append(value)
    return tuple(result)


def read_pool(reader, source, block):
    deployment = verify_pool(reader, source, block)
    timestamps, tokens = token_accounting(reader, source, block)
    return TriCryptoObservation(
        source=source,
        block=block,
        currencies=tuple(reader.asset(coin, block) for coin in source.coins),
        shares=reader.asset(source.shares_asset, block),
        balances=indexed(reader, source, block, "balances", 3),
        actual_balances=custody_balances(reader, source, block, deployment.wrapped_native),
        precisions=deployment.precisions,
        total_supply=scalar(reader, source, block, "totalSupply"),
        ramp=ramp(reader, source, block),
        invariant=scalar(reader, source, block, "D"),
        packed_fees=scalar(reader, source, block, "packed_fee_params"),
        packed_rebalancing=scalar(reader, source, block, "packed_rebalancing_params"),
        price_scales=indexed(reader, source, block, "price_scale", 2),
        cached_price_oracle=stored_prices(reader, source, block, "price_oracle_packed"),
        price_oracle=indexed(reader, source, block, "price_oracle", 2),
        last_prices=indexed(reader, source, block, "last_prices", 2),
        last_timestamp=timestamps,
        accounting=accounting(reader, source, block),
        implementation=deployment.pool.address,
        math=deployment.math.address,
        views=deployment.views.address,
        revision=source.implementation_revision or "tricrypto-ng-v2.0.0",
        token_accounting=tokens,
    )


def token_accounting(reader, source, block):
    if source.implementation_revision != "tricrypto-ng-2024-01-04":
        return scalar(reader, source, block, "last_prices_timestamp"), None
    packed = scalar(reader, source, block, "last_timestamp")
    retained = TokenAccounting(
        virtual_admin_shares=token_storage(reader, source, block, "admin_lp_virtual_balance"),
        last_claim_time=token_storage(reader, source, block, "last_admin_fee_claim_timestamp"),
        cached_xcp_oracle=token_storage(reader, source, block, "cached_xcp_oracle"),
        last_xcp=scalar(reader, source, block, "last_xcp"),
        xcp_timestamp=packed >> 128,
        xcp_ma_time=scalar(reader, source, block, "xcp_ma_time"),
    )
    if retained.oracle(block.timestamp) != scalar(reader, source, block, "xcp_oracle"):
        raise DataError("oracle", "TriCrypto XCP oracle differs from retained EMA inputs")
    return packed & (2**128 - 1), retained


def token_storage(reader, source, block, name):
    slot = revision("tricrypto-2024-01-04.json")["pool_storage_layout"][name]["slot"]
    return storage(reader, source.pool, slot, block)
