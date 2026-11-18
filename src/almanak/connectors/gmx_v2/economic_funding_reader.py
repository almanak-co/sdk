"""Read funding controls and collateral-specific open interest at a verified market block."""

from almanak.connectors.gmx_v2.economic_funding import (
    CollateralValues,
    FundingBounds,
    FundingParameters,
    FundingState,
)
from almanak.connectors.gmx_v2.economic_spot_reader import market_uint
from almanak.connectors.gmx_v2.oracle_reader import stored
from almanak.connectors.gmx_v2.position_reader import verify_market


def collateral_interest(reader, market, block, is_long, name):
    divisor = 2 if market.long_asset == market.short_asset else 1
    values = tuple(
        stored(
            reader,
            block,
            market.contracts.data_store,
            "Uint",
            name,
            ("address", "address", "bool"),
            (market.share_asset.reference, asset.reference, is_long),
        )
        // divisor
        for asset in (market.long_asset, market.short_asset)
    )
    return CollateralValues(long=values[0], short=values[1])


FUNDING_KEYS = {
    "factor": "FUNDING_FACTOR",
    "exponent": "FUNDING_EXPONENT_FACTOR",
    "increase_per_second": "FUNDING_INCREASE_FACTOR_PER_SECOND",
    "minimum_increase_per_second": "MIN_FUNDING_INCREASE_RATE_PER_SECOND",
    "decrease_per_second": "FUNDING_DECREASE_FACTOR_PER_SECOND",
    "stable_threshold": "THRESHOLD_FOR_STABLE_FUNDING",
    "decrease_threshold": "THRESHOLD_FOR_DECREASE_FUNDING",
}
BOUND_KEYS = {
    "minimum_per_second": "MIN_FUNDING_FACTOR_PER_SECOND",
    "maximum_per_second": "MAX_FUNDING_FACTOR_PER_SECOND",
}


def funding_bounds(reader, market, block, longs_pay):
    return FundingBounds(
        **{
            field: market_uint(reader, market, block, key, positive=longs_pay)
            for field, key in BOUND_KEYS.items()
        }
    )


def funding_parameters(reader, market, block):
    return FundingParameters(
        **{field: market_uint(reader, market, block, key) for field, key in FUNDING_KEYS.items()},
        longs_pay=funding_bounds(reader, market, block, True),
        shorts_pay=funding_bounds(reader, market, block, False),
    )


def prepared_funding(reader, market, block, prices):
    verify_market(reader, market, block)
    by_asset = {price.asset: price for price in prices}
    if set(by_asset) != {market.index_asset, market.long_asset, market.short_asset} or len(
        by_asset
    ) != len(prices):
        raise ValueError("GMX funding requires the market's exact unique oracle prices")
    index = by_asset[market.index_asset]
    in_tokens = stored(
        reader,
        block,
        market.contracts.data_store,
        "Bool",
        "USE_OPEN_INTEREST_IN_TOKENS_FOR_BALANCE",
    )
    long = collateral_interest(reader, market, block, True, "OPEN_INTEREST")
    short = collateral_interest(reader, market, block, False, "OPEN_INTEREST")
    balance = (
        tuple(
            collateral_interest(reader, market, block, side, "OPEN_INTEREST_IN_TOKENS").total
            * ((index.minimum + index.maximum) // 2)
            for side in (True, False)
        )
        if in_tokens
        else (long.total, short.total)
    )
    return FundingState(
        market=market,
        block=block,
        long_price=by_asset[market.long_asset],
        short_price=by_asset[market.short_asset],
        parameters=funding_parameters(reader, market, block),
        long_interest=long,
        short_interest=short,
        long_balance_usd_e30=balance[0],
        short_balance_usd_e30=balance[1],
        updated_at=market_uint(reader, market, block, "FUNDING_UPDATED_AT"),
        saved_per_second=stored(
            reader,
            block,
            market.contracts.data_store,
            "Int",
            "SAVED_FUNDING_FACTOR_PER_SECOND",
            ("address",),
            (market.share_asset.reference,),
        ),
    )


def funding_indexes(reader, market, block):
    from almanak.connectors.gmx_v2.economic_funding import FundingIndexes

    values = {}
    for side, is_long in (("long", True), ("short", False)):
        for field, key in (
            ("payments", "FUNDING_FEE_AMOUNT_PER_SIZE"),
            ("claims", "CLAIMABLE_FUNDING_AMOUNT_PER_SIZE"),
        ):
            amounts = tuple(
                stored(
                    reader,
                    block,
                    market.contracts.data_store,
                    "Uint",
                    key,
                    ("address", "address", "bool"),
                    (market.share_asset.reference, asset.reference, is_long),
                )
                for asset in (market.long_asset, market.short_asset)
            )
            values[f"{side}_{field}"] = CollateralValues(long=amounts[0], short=amounts[1])
    return FundingIndexes(**values)
