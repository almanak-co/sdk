"""V3 economic risk over hypothetical scaled holdings and retained market evidence."""

from almanak.connectors.aave_v3.math import mul_div, uint
from almanak.connectors.aave_v3.risk import RiskProjection, account_totals


def require_group(positions, markets):
    if not positions or len(positions) != len(markets):
        raise ValueError("economic Pool risk requires every owned reserve")
    target = positions[0].target
    scope = target.fund, target.reserve.pool
    if len({p.target.reserve for p in positions}) != len(positions):
        raise ValueError("economic Pool risk cannot count a reserve twice")
    for p, market in zip(positions, markets, strict=True):
        if (
            p.target.fund,
            p.target.reserve.pool,
        ) != scope or market.reserve.source != p.target.reserve:
            raise ValueError("economic risk cannot combine different funds, Pools or reserves")
        if market.time != markets[0].time:
            raise ValueError("economic risk inputs require one simulation time")


def prices(positions, markets):
    selected = []
    for position, market in zip(positions, markets, strict=True):
        if not (position.collateral_enabled or position.debt_scaled):
            continue
        quote = market.reserve.oracle
        if quote is None:
            raise ValueError("economic Pool risk requires the exposed reserve's oracle evidence")
        selected.append(quote)
    if len({(p.oracle, p.base_currency, p.base_currency_unit) for p in selected}) > 1:
        raise ValueError("economic Pool risk cannot combine oracle sources or quote units")
    return tuple(selected)


def risk(positions, markets):
    require_group(positions, markets)
    evidence = prices(positions, markets)
    collateral, debt, weighted_ltv, threshold = 0, 0, 0, 0
    zero_ltv = False
    for p, market in zip(positions, markets, strict=True):
        if not (p.collateral_enabled or p.debt_scaled):
            continue
        quote = market.reserve.oracle
        unit = 10**market.reserve.asset.decimals
        value = (
            mul_div(market.supplied(p.supply_scaled), quote.price, unit)
            if p.collateral_enabled
            else 0
        )
        borrowed = mul_div(market.borrowed(p.debt_scaled), quote.price, unit, ceiling=True)
        config = market.reserve.configuration
        collateral, debt = uint(collateral + value), uint(debt + borrowed)
        weighted_ltv = uint(weighted_ltv + value * config.ltv_bps)
        threshold = uint(threshold + value * config.liquidation_threshold_bps)
        zero_ltv |= p.collateral_enabled and config.ltv_bps == 0
    return RiskProjection(
        risk=account_totals(collateral, debt, weighted_ltv, threshold),
        has_zero_ltv_collateral=zero_ltv,
        prices=evidence,
    )


def validate_sources(config, positions, markets):
    for p, market in zip(positions, markets, strict=True):
        if not (p.collateral_enabled or p.debt_scaled):
            continue
        allocation = config.allocation(p.target.allocation)
        price = market.reserve.oracle
        if price is None or (price.oracle, price.feed) != (
            allocation.oracle,
            allocation.price_source,
        ):
            raise ValueError("economic risk price differs from its configured oracle and feed")
