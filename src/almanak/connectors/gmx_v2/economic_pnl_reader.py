"""Capture independent pool profit caps and open interest for position realization."""

from almanak.connectors.gmx_v2.economic_pnl import PnlSide, PnlState
from almanak.connectors.gmx_v2.economic_position_reader import open_interest
from almanak.connectors.gmx_v2.economic_spot_reader import currency
from almanak.connectors.gmx_v2.economic_swap_risk_reader import maximum_pnl_factor
from almanak.connectors.gmx_v2.position_reader import verify_market


def prepared_pnl(reader, market, block, prices):
    verify_market(reader, market, block)
    by_asset = {price.asset: price for price in prices}
    if len(by_asset) != len(prices):
        raise ValueError("GMX position PnL prices must identify distinct assets")
    sides = tuple(
        PnlSide(
            open_interest_usd_e30=open_interest(reader, market, block, is_long, in_tokens=False),
            open_interest_tokens=open_interest(reader, market, block, is_long, in_tokens=True),
            pool_amount=currency(reader, market, block, by_asset[asset]).pool_amount,
            pool_price=by_asset[asset],
            maximum_trader_pnl_factor=maximum_pnl_factor(reader, market, block, is_long, "TRADERS"),
        )
        for is_long, asset in ((True, market.long_asset), (False, market.short_asset))
    )
    return PnlState(
        market=market,
        block=block,
        index_price=by_asset[market.index_asset],
        long=sides[0],
        short=sides[1],
    )
