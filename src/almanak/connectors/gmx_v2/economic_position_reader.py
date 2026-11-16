"""Acquire position impact state and perpetual controls from the verified market."""

from almanak.connectors.gmx_v2.deployments import records
from almanak.connectors.gmx_v2.economic_liquidity_risk import RequestControls
from almanak.connectors.gmx_v2.economic_perpetual_risk import (
    PerpetualRiskState,
    PerpetualSideRisk,
)
from almanak.connectors.gmx_v2.economic_position_types import (
    ImpactSettlementState,
    PositionImpactParameters,
    PositionPricingState,
)
from almanak.connectors.gmx_v2.economic_spot_reader import market_uint
from almanak.connectors.gmx_v2.oracle_reader import stored
from almanak.connectors.gmx_v2.position_reader import verify_market
from almanak.gateway.ethereum import DataError
from almanak.models import ZERO_ADDRESS

# Deployed Order.OrderType indexes: the model settles market increases and decreases
# through the OrderHandler and observes liquidations through the LiquidationHandler.
MARKET_INCREASE = 2
MARKET_DECREASE = 4
LIQUIDATION = 7
SIDE_RISK_KEYS = {
    "maximum_open_interest_usd_e30": "MAX_OPEN_INTEREST",
    "reserve_factor": "RESERVE_FACTOR",
    "open_interest_reserve_factor": "OPEN_INTEREST_RESERVE_FACTOR",
    "collateral_open_interest_multiplier": "MIN_COLLATERAL_FACTOR_FOR_OPEN_INTEREST_MULTIPLIER",
}


def open_interest(reader, market, block, is_long, *, in_tokens):
    divisor = 2 if market.long_asset == market.short_asset else 1
    name = "OPEN_INTEREST_IN_TOKENS" if in_tokens else "OPEN_INTEREST"
    return sum(
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


def virtual_position_inventory(reader, market, block, *, in_tokens):
    identifier = stored(
        reader,
        block,
        market.contracts.data_store,
        "Bytes32",
        "VIRTUAL_TOKEN_ID",
        ("address",),
        (market.index_asset.reference,),
    )
    if identifier == bytes(32):
        return None
    name = (
        "VIRTUAL_INVENTORY_FOR_POSITIONS_IN_TOKENS"
        if in_tokens
        else "VIRTUAL_INVENTORY_FOR_POSITIONS"
    )
    return stored(
        reader, block, market.contracts.data_store, "Int", name, ("bytes32",), (identifier,)
    )


def prepared_position_pricing(reader, market, block, index_price):
    verify_market(reader, market, block)
    in_tokens = stored(
        reader,
        block,
        market.contracts.data_store,
        "Bool",
        "USE_OPEN_INTEREST_IN_TOKENS_FOR_BALANCE",
    )
    values = {}
    for prefix, positive in (("positive", True), ("negative", False)):
        values[prefix + "_factor"] = market_uint(
            reader, market, block, "POSITION_IMPACT_FACTOR", positive=positive
        )
        values[prefix + "_exponent"] = market_uint(
            reader, market, block, "POSITION_IMPACT_EXPONENT_FACTOR", positive=positive
        )
    values["maximum_positive_factor"] = market_uint(
        reader, market, block, "MAX_POSITION_IMPACT_FACTOR", positive=True
    )
    return PositionPricingState(
        market=market,
        block=block,
        index_price=index_price,
        impact=PositionImpactParameters(**values),
        use_open_interest_in_tokens=in_tokens,
        long_open_interest=open_interest(reader, market, block, True, in_tokens=in_tokens),
        short_open_interest=open_interest(reader, market, block, False, in_tokens=in_tokens),
        virtual_inventory=virtual_position_inventory(reader, market, block, in_tokens=in_tokens),
    )


def prepared_impact_settlement(reader, pricing, prices):
    market, block = pricing.market, pricing.block
    by_asset = {price.asset: price for price in prices}
    if len(by_asset) != len(prices) or by_asset[market.index_asset] != pricing.index_price:
        raise ValueError("GMX impact settlement requires the same unique oracle prices")
    divisor = 2 if market.long_asset == market.short_asset else 1
    pool_usd = sum(
        stored(
            reader,
            block,
            market.contracts.data_store,
            "Uint",
            "POOL_AMOUNT",
            ("address", "address"),
            (market.share_asset.reference, asset.reference),
        )
        // divisor
        * by_asset[asset].minimum
        for asset in (market.long_asset, market.short_asset)
    )
    return ImpactSettlementState(
        pricing=pricing,
        pool_usd_e30=pool_usd,
        maximum_negative_factor=market_uint(
            reader, market, block, "MAX_POSITION_IMPACT_FACTOR", positive=False
        ),
        impact_pool_amount=market_uint(reader, market, block, "POSITION_IMPACT_POOL_AMOUNT"),
        maximum_lendable_factor=market_uint(reader, market, block, "MAX_LENDABLE_IMPACT_FACTOR"),
        maximum_lendable_usd_e30=market_uint(reader, market, block, "MAX_LENDABLE_IMPACT_USD"),
        lent_impact_amount=market_uint(reader, market, block, "LENT_POSITION_IMPACT_POOL_AMOUNT"),
    )


def liquidation_handler(market):
    matches = [
        address
        for address, record in records(market).items()
        if record["contract"] == "LiquidationHandler"
    ]
    if len(matches) != 1:
        raise DataError("revision", "GMX requires one verified LiquidationHandler deployment")
    return matches[0]


def order_switch(reader, market, block, prefix, module, order_type):
    return stored(
        reader,
        block,
        market.contracts.data_store,
        "Bool",
        prefix + "_ORDER_FEATURE_DISABLED",
        ("address", "uint256"),
        (module, order_type),
    )


def request_controls(reader, market, block, family, types, values):
    """Read the CREATE/EXECUTE/CANCEL feature switches of one request family."""
    return RequestControls(
        **{
            field: stored(
                reader,
                block,
                market.contracts.data_store,
                "Bool",
                prefix + "_" + family + "_FEATURE_DISABLED",
                types,
                values,
            )
            for field, prefix in (
                ("creation_disabled", "CREATE"),
                ("execution_disabled", "EXECUTE"),
                ("cancellation_disabled", "CANCEL"),
            )
        }
    )


def order_controls(reader, market, block, order_type):
    return request_controls(
        reader,
        market,
        block,
        "ORDER",
        ("address", "uint256"),
        (market.contracts.order_handler, order_type),
    )


def perpetual_side_risk(reader, market, block, is_long):
    return PerpetualSideRisk(
        **{
            field: market_uint(reader, market, block, name, positive=is_long)
            for field, name in SIDE_RISK_KEYS.items()
        }
    )


def perpetual_risk(reader, market, block):
    store = market.contracts.data_store
    return PerpetualRiskState(
        market=market,
        block=block,
        increases=order_controls(reader, market, block, MARKET_INCREASE),
        decreases=order_controls(reader, market, block, MARKET_DECREASE),
        market_disabled=stored(
            reader,
            block,
            store,
            "Bool",
            "IS_MARKET_DISABLED",
            ("address",),
            (market.share_asset.reference,),
        ),
        # The ExchangeRouter is the module the deployed FeeUtils claim check names.
        funding_claim_disabled=stored(
            reader,
            block,
            store,
            "Bool",
            "CLAIM_FUNDING_FEES_FEATURE_DISABLED",
            ("address",),
            (market.contracts.exchange_router,),
        ),
        liquidation_disabled=order_switch(
            reader, market, block, "EXECUTE", liquidation_handler(market), LIQUIDATION
        ),
        holding_address_available=stored(reader, block, store, "Address", "HOLDING_ADDRESS")
        != ZERO_ADDRESS,
        request_expiration_seconds=stored(reader, block, store, "Uint", "REQUEST_EXPIRATION_TIME"),
        maximum_oracle_age_seconds=stored(reader, block, store, "Uint", "MAX_ORACLE_PRICE_AGE"),
        minimum_position_usd_e30=stored(reader, block, store, "Uint", "MIN_POSITION_SIZE_USD"),
        minimum_collateral_usd_e30=stored(reader, block, store, "Uint", "MIN_COLLATERAL_USD"),
        minimum_collateral_factor=market_uint(reader, market, block, "MIN_COLLATERAL_FACTOR"),
        liquidation_collateral_factor=market_uint(
            reader, market, block, "MIN_COLLATERAL_FACTOR_FOR_LIQUIDATION"
        ),
        maximum_liquidation_impact_factor=market_uint(
            reader, market, block, "MAX_POSITION_IMPACT_FACTOR_FOR_LIQUIDATIONS"
        ),
        long=perpetual_side_risk(reader, market, block, True),
        short=perpetual_side_risk(reader, market, block, False),
    )
