"""Build eligible GM publications from raw action-specific accounting and risk state."""

from almanak.connectors.gmx_v2.economic_data import LiquidityOpportunity, accounting_market
from almanak.connectors.gmx_v2.economic_solvency_reader import market_solvency
from almanak.connectors.gmx_v2.economic_spot_reader import prepared_spot
from almanak.connectors.gmx_v2.economic_valuation_reader import prepared_valuation


def opportunity(reader, market, published, transaction_index, risk):
    market = accounting_market(market)
    index = next(price for price in published.prices if price.asset == market.index_asset)
    states = tuple(
        prepared_valuation(
            reader,
            prepared_spot(
                reader, market, risk.block, published.prices, pricing_type=action, ui_factor=0
            ),
            index,
        )
        for action in ("DEPOSIT", "WITHDRAWAL")
    )
    return LiquidityOpportunity(
        publication=published,
        transaction_index=transaction_index,
        deposit=states[0],
        withdrawal=states[1],
        risk=risk,
        solvency=market_solvency(reader, market, risk.block),
    )
