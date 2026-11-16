"""Read Crypto state only after verifying the revision that owns its storage."""

from almanak.connectors.curve.crypto_deployment import revision, storage, verify_pool
from almanak.connectors.curve.crypto_observation import (
    CryptoAccounting,
    CryptoObservation,
    CryptoRamp,
)
from almanak.gateway.ethereum import DataError


def scalar(reader, source, block, name):
    return reader.call(source.pool, name + "()", block)[0]


def ramp(reader, source, block):
    initial = scalar(reader, source, block, "initial_A_gamma")
    future = scalar(reader, source, block, "future_A_gamma")
    result = CryptoRamp(
        initial_amplification=initial >> 128,
        initial_gamma=initial & (2**128 - 1),
        future_amplification=future >> 128,
        future_gamma=future & (2**128 - 1),
        initial_time=scalar(reader, source, block, "initial_A_gamma_time"),
        future_time=scalar(reader, source, block, "future_A_gamma_time"),
    )
    measured = tuple(scalar(reader, source, block, name) for name in ("A", "gamma"))
    if result.at(block.timestamp) != measured:
        raise DataError("amplification", "TwoCrypto parameters differ from their retained ramp")
    return result


def stored(reader, source, block, name):
    slot = revision()["pool_storage_layout"][name]["slot"]
    return storage(reader, source.pool, slot, block)


def accounting(reader, source, block):
    return CryptoAccounting(
        virtual_price=scalar(reader, source, block, "virtual_price"),
        profit=scalar(reader, source, block, "xcp_profit"),
        claimed_profit=scalar(reader, source, block, "xcp_profit_a"),
        virtual_shares=stored(reader, source, block, "admin_lp_virtual_balance"),
        last_claim_time=stored(reader, source, block, "last_admin_fee_claim_timestamp"),
        receiver=reader.call(source.factory, "fee_receiver()", block, outputs=("address",))[0],
    )


def read_pool(reader, source, block):
    deployment = verify_pool(reader, source, block)
    return CryptoObservation(
        source=source,
        block=block,
        currencies=tuple(reader.asset(coin, block) for coin in source.coins),
        shares=reader.asset(source.shares_asset, block),
        balances=tuple(
            reader.call(
                source.pool, "balances(uint256)", block, inputs=("uint256",), args=(index,)
            )[0]
            for index in range(2)
        ),
        precisions=deployment.precisions,
        total_supply=scalar(reader, source, block, "totalSupply"),
        ramp=ramp(reader, source, block),
        invariant=scalar(reader, source, block, "D"),
        packed_fees=scalar(reader, source, block, "packed_fee_params"),
        packed_rebalancing=scalar(reader, source, block, "packed_rebalancing_params"),
        price_scale=scalar(reader, source, block, "price_scale"),
        cached_price_oracle=stored(reader, source, block, "cached_price_oracle"),
        price_oracle=scalar(reader, source, block, "price_oracle"),
        last_prices=scalar(reader, source, block, "last_prices"),
        last_timestamp=scalar(reader, source, block, "last_timestamp"),
        accounting=accounting(reader, source, block),
        implementation=deployment.blueprint.address,
        math=deployment.math.address,
        views=deployment.views.address,
    )
