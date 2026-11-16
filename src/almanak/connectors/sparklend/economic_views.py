"""Hypothetical position values retain the original historical reserve evidence."""

from almanak.connectors.sparklend.domain import PositionObservation


def bitmap(positions, markets):
    return sum(
        (int(bool(p.debt_scaled)) + 2 * int(p.collateral_enabled)) << (2 * m.reserve.reserve_id)
        for p, m in zip(positions, markets, strict=True)
    )


def position_view(position, market, user_configuration, account_risk):
    return PositionObservation(
        reserve=market.reserve,
        fund=position.target.fund,
        supplied_assets=market.supplied(position.supply_scaled),
        supplied_scaled=position.supply_scaled,
        debt_assets=market.borrowed(position.debt_scaled),
        debt_scaled=position.debt_scaled,
        stable_debt_assets=0,
        collateral_enabled=position.collateral_enabled,
        user_configuration=user_configuration,
        risk=account_risk,
    )
