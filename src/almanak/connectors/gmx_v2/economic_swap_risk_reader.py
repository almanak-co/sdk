"""Capture market switches and reserve limits independently of accounting sampling."""

from almanak.connectors.gmx_v2.economic_position_reader import open_interest, order_controls
from almanak.connectors.gmx_v2.economic_spot_reader import market_uint
from almanak.connectors.gmx_v2.economic_swap_risk import SwapRiskState, SwapSideRisk
from almanak.connectors.gmx_v2.oracle_reader import key, stored


def maximum_pnl_factor(reader, market, block, is_long, action):
    return stored(
        reader,
        block,
        market.contracts.data_store,
        "Uint",
        "MAX_PNL_FACTOR",
        ("bytes32", "address", "bool"),
        (key("MAX_PNL_FACTOR_FOR_" + action), market.share_asset.reference, is_long),
    )


def side_risk(reader, market, block, is_long):
    asset = market.long_asset if is_long else market.short_asset
    return SwapSideRisk(
        maximum_pool_amount=stored(
            reader,
            block,
            market.contracts.data_store,
            "Uint",
            "MAX_POOL_AMOUNT",
            ("address", "address"),
            (market.share_asset.reference, asset.reference),
        ),
        reserve_factor=market_uint(reader, market, block, "RESERVE_FACTOR", positive=is_long),
        maximum_deposit_pnl_factor=maximum_pnl_factor(reader, market, block, is_long, "DEPOSITS"),
        maximum_withdrawal_pnl_factor=maximum_pnl_factor(
            reader, market, block, is_long, "WITHDRAWALS"
        ),
        open_interest_usd_e30=open_interest(reader, market, block, is_long, in_tokens=False),
        open_interest_tokens=open_interest(reader, market, block, is_long, in_tokens=True),
    )


def swap_risk(reader, market, block):
    # Market swaps are Order.OrderType 0 on the same OrderHandler switches.
    switches = order_controls(reader, market, block, 0)
    return SwapRiskState(
        market=market,
        block=block,
        long=side_risk(reader, market, block, True),
        short=side_risk(reader, market, block, False),
        market_disabled=stored(
            reader,
            block,
            market.contracts.data_store,
            "Bool",
            "IS_MARKET_DISABLED",
            ("address",),
            (market.share_asset.reference,),
        ),
        creation_disabled=switches.creation_disabled,
        execution_disabled=switches.execution_disabled,
        cancellation_disabled=switches.cancellation_disabled,
        request_expiration_seconds=stored(
            reader, block, market.contracts.data_store, "Uint", "REQUEST_EXPIRATION_TIME"
        ),
        maximum_oracle_age_seconds=stored(
            reader, block, market.contracts.data_store, "Uint", "MAX_ORACLE_PRICE_AGE"
        ),
    )
