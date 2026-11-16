"""Pinned Comet accounting, complete collateral scope and explicit price evidence."""

from almanak.caip import AssetType
from almanak.connectors.compound_v3.accounting import validate_account
from almanak.connectors.compound_v3.deployment import read_deployment
from almanak.connectors.compound_v3.domain import (
    Accounting,
    AccountObservation,
    CollateralBalance,
    CollateralObservation,
    ExtendedControls,
    Market,
    MarketObservation,
    OracleQuote,
    RateCurve,
    Revision,
)
from almanak.gateway.ethereum import DataError

ASSET_INFO = "(uint8,address,address,uint64,uint64,uint64,uint64,uint128)"
TOTALS = ("uint64", "uint64", "uint64", "uint64", "uint104", "uint104", "uint40", "uint8")
USER = ("int104", "uint64", "uint64", "uint16", "uint8")


def price_evidence(reader, market, block, asset, feed):
    try:
        value = reader.call(
            market.comet, "getPrice(address)", block, inputs=("address",), args=(feed,)
        )[0]
        quote = OracleQuote(block=block, asset=asset, comet=market.comet, feed=feed, price=value)
        return {"price": quote, "price_error": None}
    except (DataError, ValueError) as error:
        return {"price": None, "price_error": str(error)}


def curve(configuration, side):
    return RateCurve(
        kink=configuration[side + "Kink"],
        base_per_second=configuration[side + "PerSecondInterestRateBase"],
        slope_low_per_second=configuration[side + "PerSecondInterestRateSlopeLow"],
        slope_high_per_second=configuration[side + "PerSecondInterestRateSlopeHigh"],
    )


def accounting(reader, market, block):
    values = reader.call(market.comet, "totalsBasic()", block, outputs=TOTALS)
    return Accounting(
        supply_index=values[0],
        borrow_index=values[1],
        tracking_supply_index=values[2],
        tracking_borrow_index=values[3],
        total_supply_principal=values[4],
        total_borrow_principal=values[5],
        last_accrual_time=values[6],
        pause_flags=values[7],
    )


def extended_controls(reader, market, block, revision):
    if revision is Revision.STANDARD:
        return None
    fields = {
        "pause_flags": "extendedPauseFlags",
        "collateral_supply_flags": "collateralsSupplyPauseFlags",
        "collateral_withdraw_flags": "collateralsWithdrawPauseFlags",
        "collateral_transfer_flags": "collateralsTransferPauseFlags",
        "deactivated_collaterals": "deactivatedCollaterals",
        "maximum_borrow_utilization": "MAX_SUPPORTED_UTILIZATION",
    }
    return ExtendedControls(
        **{
            field: reader.call(market.comet, name + "()", block)[0]
            for field, name in fields.items()
        }
    )


def collateral(reader, market, block, index):
    info = reader.call(
        market.comet,
        "getAssetInfo(uint8)",
        block,
        inputs=("uint8",),
        args=(index,),
        outputs=(ASSET_INFO,),
    )[0]
    asset = AssetType(f"{market.chain.chain_id}/erc20:{info[1]}")
    total = reader.call(
        market.comet,
        "totalsCollateral(address)",
        block,
        inputs=("address",),
        args=(info[1],),
        outputs=("uint128", "uint128"),
    )[0]
    return CollateralObservation(
        asset=reader.asset(asset, block),
        offset=info[0],
        price_feed=info[2],
        scale=info[3],
        borrow_factor=info[4],
        liquidate_factor=info[5],
        liquidation_factor=info[6],
        supply_cap=info[7],
        total_supply=total,
        **price_evidence(reader, market, block, asset, info[2]),
    )


def read_market(reader, market: Market, block) -> MarketObservation:
    deployment = read_deployment(reader, market, block)
    config = deployment.configuration
    base = reader.asset(market.base_asset, block)
    if base.decimals != config["decimals"]:
        raise DataError("market", "Comet base token decimals differ from the implementation")
    price = price_evidence(reader, market, block, market.base_asset, market.base_price_feed)
    return MarketObservation(
        source=market,
        block=block,
        revision=deployment.revision,
        contracts=deployment.contracts,
        base_asset=base,
        accounting=accounting(reader, market, block),
        controls=extended_controls(reader, market, block, deployment.revision),
        supply_curve=curve(config, "supply"),
        borrow_curve=curve(config, "borrow"),
        base_borrow_minimum=config["baseBorrowMin"],
        base_available=reader.call(
            market.base_asset.reference,
            "balanceOf(address)",
            block,
            inputs=("address",),
            args=(market.comet,),
        )[0],
        base_price=price["price"],
        base_price_error=price["price_error"],
        collaterals=tuple(collateral(reader, market, block, i) for i in range(config["numAssets"])),
    )


def read_account(reader, market: MarketObservation, fund) -> AccountObservation:
    source, block = market.source, market.block
    if reader._pinned != block or reader.chain != block.chain or fund.chain != block.chain:
        raise DataError("block_mismatch", "Comet fund observation requires the same pinned market")
    user = reader.call(
        source.comet,
        "userBasic(address)",
        block,
        inputs=("address",),
        args=(fund.address,),
        outputs=USER,
    )
    balances = tuple(
        CollateralBalance(
            asset=row.asset_type,
            balance=reader.call(
                source.comet,
                "collateralBalanceOf(address,address)",
                block,
                inputs=("address", "address"),
                args=(fund.address, row.asset_type.reference),
            )[0],
        )
        for row in market.collaterals
    )
    return validate_account(
        AccountObservation(
            market=market,
            fund=fund,
            principal=user[0],
            tracking_index=user[1],
            tracking_accrued=user[2],
            assets_in=user[3],
            extended_assets_in=user[4],
            supplied_base=reader.call(
                source.comet,
                "balanceOf(address)",
                block,
                inputs=("address",),
                args=(fund.address,),
            )[0],
            borrowed_base=reader.call(
                source.comet,
                "borrowBalanceOf(address)",
                block,
                inputs=("address",),
                args=(fund.address,),
            )[0],
            collaterals=balances,
        )
    )
