"""Pinned read primitives shared by compatible Aave V3 and SparkLend Pool ABIs."""

from almanak.caip import AssetType
from almanak.connectors.pool_lending_domain import AccountRisk
from almanak.gateway.ethereum import DataError

LEGACY_RESERVE = (
    "((uint256),uint128,uint128,uint128,uint128,uint128,uint40,uint16,"
    "address,address,address,address,uint128,uint128,uint128)"
)


def price_source(reader, source, block, provider) -> tuple[str, str]:
    """The Pool's current oracle and the feed it uses for the reserve asset."""
    oracle = reader.call(provider, "getPriceOracle()", block, outputs=("address",))[0]
    feed = reader.call(
        oracle,
        "getSourceOfAsset(address)",
        block,
        inputs=("address",),
        args=(source.asset.reference,),
        outputs=("address",),
    )[0]
    return oracle, feed


def oracle_values(reader, source, block, provider):
    oracle, feed = price_source(reader, source, block, provider)
    currency = reader.call(oracle, "BASE_CURRENCY()", block, outputs=("address",))[0]
    unit = reader.call(oracle, "BASE_CURRENCY_UNIT()", block)[0]
    price = reader.call(
        oracle,
        "getAssetPrice(address)",
        block,
        inputs=("address",),
        args=(source.asset.reference,),
    )[0]
    return dict(
        block=block,
        asset=source.asset,
        oracle=oracle,
        feed=feed,
        base_currency=currency,
        base_currency_unit=unit,
        price=price,
    )


def balance(reader, asset: AssetType, account, block, *, scaled=False):
    return reader.call(
        asset.reference,
        "scaledBalanceOf(address)" if scaled else "balanceOf(address)",
        block,
        inputs=("address",),
        args=(account,),
    )[0]


def account_risk(reader, source, fund, block):
    values = reader.call(
        source.pool,
        "getUserAccountData(address)",
        block,
        inputs=("address",),
        args=(fund.address,),
        outputs=("uint256",) * 6,
    )
    return AccountRisk(
        total_collateral_base=values[0],
        total_debt_base=values[1],
        available_borrows_base=values[2],
        liquidation_threshold_bps=values[3],
        ltv_bps=values[4],
        health_factor_wad=values[5],
        emode_category=reader.call(
            source.pool,
            "getUserEMode(address)",
            block,
            inputs=("address",),
            args=(fund.address,),
        )[0],
    )


def optional_evidence(name, read):
    # Missing valuation must remain visible without hiding independently observed
    # balances and debt that can still support repayment or collateral release.
    # Only a genuinely absent value (the provider returns no data) or invalid
    # protocol data degrades to None here. A transport or provider rejection
    # (DataError of any other kind) is retryable and must surface as such, not be
    # masked as absent data and later misreported as an invalid input.
    try:
        return {name: read()}
    except DataError as exc:
        if exc.kind != "unavailable":
            raise
        return {name: None, name + "_error": exc.kind}
    except ValueError:
        return {name: None, name + "_error": "invalid_protocol_data"}
