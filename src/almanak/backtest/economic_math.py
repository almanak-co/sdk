"""V4 debt and account-risk calculations over modeled indices."""

from typing import Annotated

from pydantic import Field, model_validator

from almanak.backtest.economic_data import (
    BPS,
    RAY,
    WAD,
    OraclePrice,
    RiskParameters,
    VendorRiskPrice,
)
from almanak.backtest.economic_state import EconomicPosition, RiskView
from almanak.connectors.aave_v4.history import ceiling
from almanak.models import Model, UInt


class MarketView(Model):
    supply_index: Annotated[UInt, Field(gt=0)]
    borrow_index: Annotated[UInt, Field(ge=RAY)]
    risk: RiskParameters
    price: OraclePrice | VendorRiskPrice

    @model_validator(mode="after")
    def source(self):
        if self.risk.reserve.source != self.price.source:
            raise ValueError("risk and oracle inputs refer to different reserves")
        return self

    def supplied(self, shares: int, *, up=False) -> int:
        amount = shares * self.supply_index
        return ceiling(amount, RAY) if up else amount // RAY

    def supply_shares(self, assets: int, *, up=False) -> int:
        shares = assets * RAY
        return ceiling(shares, self.supply_index) if up else shares // self.supply_index

    def value(self, amount: int) -> int:
        return amount * self.price.price * 10 ** (18 - self.risk.reserve.asset.decimals)


def premium_ray(position: EconomicPosition, index: int) -> int:
    result = position.premium_shares * index - position.premium_offset_ray
    if result < 0:
        raise ValueError("premium accounting produced negative debt")
    return result


def debts(position: EconomicPosition, index: int) -> tuple[int, int]:
    return ceiling(position.drawn_shares * index, RAY), ceiling(premium_ray(position, index), RAY)


def reset_premium(position, index, risk, *, taken=0, restored=0):
    remaining = premium_ray(position, index) - restored
    if not 0 <= taken <= position.drawn_shares or restored < 0 or remaining < 0:
        raise ValueError("premium refresh cannot restore more debt than the position owns")
    shares = ceiling((position.drawn_shares - taken) * risk, BPS)
    return EconomicPosition.model_validate(
        position.model_dump()
        | {
            "drawn_shares": position.drawn_shares - taken,
            "premium_shares": shares,
            "premium_offset_ray": shares * index - remaining,
            "risk_premium_bps": risk,
        }
    )


def repay(position, market: MarketView, amount: int):
    if amount < 0 or position.target.reserve != market.risk.reserve.source:
        raise ValueError("repayment requires a nonnegative amount and the position's own market")
    drawn, premium = debts(position, market.borrow_index)
    premium_paid = min(amount, premium)
    restored = min(premium_paid * RAY, premium_ray(position, market.borrow_index))
    drawn_paid = min(max(0, amount - premium_paid), drawn)
    taken = min(position.drawn_shares, drawn_paid * RAY // market.borrow_index)
    after = reset_premium(
        position, market.borrow_index, position.risk_premium_bps, taken=taken, restored=restored
    )
    return after, drawn_paid + ceiling(restored, RAY), taken


def risk_view(positions: tuple[EconomicPosition, ...], markets: tuple[MarketView, ...]) -> RiskView:
    fund, spoke = require_risk_scope(positions, markets)
    collateral, weighted, debt, count = 0, 0, 0, 0
    collateral_risks = []
    for position, market in zip(positions, markets, strict=True):
        factor = market.risk.config(position.dynamic_config_key).collateral_factor_bps
        if position.collateral and factor > 0 and position.shares > 0:
            value = market.value(market.supplied(position.shares))
            collateral += value
            weighted += value * factor
            collateral_risks.append((market.risk.reserve.collateral_risk_bps, value))
        if position.drawn_shares:
            debt += market.value(
                position.drawn_shares * market.borrow_index
                + premium_ray(position, market.borrow_index)
            )
            count += 1
    health = weighted * (WAD // BPS) * RAY // debt if debt else None
    risk = weighted_risk(collateral_risks, ceiling(debt, RAY))
    return RiskView(
        fund=fund,
        spoke=spoke,
        collateral_value=collateral,
        weighted_collateral_value=weighted,
        debt_value_ray=debt,
        health_factor_wad=health,
        risk_premium_bps=risk,
        active_collateral_count=len(collateral_risks),
        borrow_count=count,
    )


def require_risk_scope(positions, markets):
    if not positions or len(positions) != len(markets):
        raise ValueError("account risk requires each declared position and its market")
    scopes = {(p.target.fund, p.target.reserve.spoke) for p in positions}
    if len(scopes) != 1 or len({p.target.identity for p in positions}) != len(positions):
        raise ValueError("account risk cannot mix funds, Spokes or duplicate reserve ownership")
    for position, market in zip(positions, markets, strict=True):
        if position.target.reserve != market.risk.reserve.source:
            raise ValueError("risk market differs from its reserve position")
    return next(iter(scopes))


def weighted_risk(collateral, debt):
    left, risk = debt, 0
    for rate, value in sorted(collateral, key=lambda pair: (pair[0], -pair[1])):
        used = min(value, left)
        risk += used * rate
        left -= used
    covered = debt - left
    return ceiling(risk, covered) if covered else 0


def require_healthy(risk: RiskView):
    if risk.health_factor_wad is not None and risk.health_factor_wad < WAD:
        raise ValueError("action would leave the Spoke health factor below one")
