"""Acquire the exact DataStore inputs used by the verified market's spot Reader."""

from almanak.connectors.gmx_v2.economic_spot_types import (
    Currency,
    FeeParameters,
    ImpactParameters,
    SpotState,
)
from almanak.connectors.gmx_v2.oracle_reader import stored
from almanak.connectors.gmx_v2.position_reader import verify_market


def market_uint(reader, market, block, name, *, positive=None):
    types = ("address",) if positive is None else ("address", "bool")
    values = (
        (market.share_asset.reference,)
        if positive is None
        else (market.share_asset.reference, positive)
    )
    return stored(reader, block, market.contracts.data_store, "Uint", name, types, values)


def currency(reader, market, block, price):
    types, values = ("address", "address"), (market.share_asset.reference, price.asset.reference)
    amounts = tuple(
        stored(reader, block, market.contracts.data_store, "Uint", name, types, values)
        for name in ("POOL_AMOUNT", "SWAP_IMPACT_POOL_AMOUNT")
    )
    divisor = 2 if market.long_asset == market.short_asset else 1
    return Currency(price=price, pool_amount=amounts[0] // divisor, impact_pool_amount=amounts[1])


def virtual_inventory(reader, market, block):
    identifier = stored(
        reader,
        block,
        market.contracts.data_store,
        "Bytes32",
        "VIRTUAL_MARKET_ID",
        ("address",),
        (market.share_asset.reference,),
    )
    if identifier == bytes(32):
        return None
    return tuple(
        stored(
            reader,
            block,
            market.contracts.data_store,
            "Uint",
            "VIRTUAL_INVENTORY_FOR_SWAPS",
            ("bytes32", "bool"),
            (identifier, side),
        )
        for side in (True, False)
    )


def prepared_spot(reader, market, block, prices, *, pricing_type, ui_factor):
    verify_market(reader, market, block)
    if pricing_type not in ("SWAP", "DEPOSIT", "WITHDRAWAL"):
        raise ValueError("GMX spot acquisition requires its exact pricing action")
    by_asset = {price.asset: price for price in prices}
    if len(by_asset) != len(prices):
        raise ValueError("GMX prepared prices must identify unique market assets")
    return SpotState(
        pricing_type=pricing_type,
        market=market,
        block=block,
        long=currency(reader, market, block, by_asset[market.long_asset]),
        short=currency(reader, market, block, by_asset[market.short_asset]),
        impact=ImpactParameters(
            positive_factor=market_uint(reader, market, block, "SWAP_IMPACT_FACTOR", positive=True),
            negative_factor=market_uint(
                reader, market, block, "SWAP_IMPACT_FACTOR", positive=False
            ),
            exponent=market_uint(reader, market, block, "SWAP_IMPACT_EXPONENT_FACTOR"),
        ),
        fees=FeeParameters(
            positive_factor=market_uint(
                reader, market, block, pricing_type + "_FEE_FACTOR", positive=True
            ),
            negative_factor=market_uint(
                reader, market, block, pricing_type + "_FEE_FACTOR", positive=False
            ),
            receiver_factor=stored(
                reader, block, market.contracts.data_store, "Uint", "SWAP_FEE_RECEIVER_FACTOR"
            ),
            ui_factor=ui_factor,
        ),
        virtual_inventory=virtual_inventory(reader, market, block),
    )
