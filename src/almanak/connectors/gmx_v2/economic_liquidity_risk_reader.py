"""Acquire separate GM request switches and deposit/withdrawal-specific pool bounds."""

from almanak.connectors.gmx_v2.economic_data import accounting_market
from almanak.connectors.gmx_v2.economic_liquidity_risk import LiquidityRiskState
from almanak.connectors.gmx_v2.economic_position_reader import request_controls
from almanak.connectors.gmx_v2.economic_spot_reader import market_uint
from almanak.connectors.gmx_v2.economic_swap_risk_reader import side_risk
from almanak.connectors.gmx_v2.oracle_reader import stored


def controls(reader, market, block, kind):
    handler = getattr(market.contracts, kind.lower() + "_handler")
    return request_controls(reader, market, block, kind, ("address",), (handler,))


def liquidity_risk(reader, market, block):
    maxima = {
        side: stored(
            reader,
            block,
            market.contracts.data_store,
            "Uint",
            "MAX_POOL_USD_FOR_DEPOSIT",
            ("address", "address"),
            (market.share_asset.reference, getattr(market, side + "_asset").reference),
        )
        for side in ("long", "short")
    }
    return LiquidityRiskState(
        market=accounting_market(market),
        block=block,
        long=side_risk(reader, market, block, True),
        short=side_risk(reader, market, block, False),
        deposits=controls(reader, market, block, "DEPOSIT"),
        withdrawals=controls(reader, market, block, "WITHDRAWAL"),
        market_disabled=stored(
            reader,
            block,
            market.contracts.data_store,
            "Bool",
            "IS_MARKET_DISABLED",
            ("address",),
            (market.share_asset.reference,),
        ),
        request_expiration_seconds=stored(
            reader, block, market.contracts.data_store, "Uint", "REQUEST_EXPIRATION_TIME"
        ),
        maximum_oracle_age_seconds=stored(
            reader, block, market.contracts.data_store, "Uint", "MAX_ORACLE_PRICE_AGE"
        ),
        maximum_long_deposit_usd_e30=maxima["long"],
        maximum_short_deposit_usd_e30=maxima["short"],
        minimum_first_deposit_shares=market_uint(
            reader, market, block, "MIN_MARKET_TOKENS_FOR_FIRST_DEPOSIT"
        ),
        maximum_lent_impact_withdrawal_factor=market_uint(
            reader, market, block, "MAX_LENDABLE_IMPACT_FACTOR_FOR_WITHDRAWALS"
        ),
    )
