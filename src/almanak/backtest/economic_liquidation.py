"""V4 liquidation amounts under the declared funded-liquidator assumption.

Rounding follows LiquidationLogic at 4d86c2d391b039bf2d3909fb2299b69998d0b720.
Historical liquidity and transaction permissions are excluded by this model.
"""

from almanak.backtest.economic_data import BPS, RAY, WAD
from almanak.backtest.economic_math import MarketView, premium_ray, require_risk_scope
from almanak.backtest.economic_state import EconomicPosition, LiquidationEffect, RiskView
from almanak.connectors.aave_v4.history import ceiling
from almanak.models import Model, UInt

DUST_VALUE = 1000 * 10**26


class LiquidationAmounts(Model):
    collateral_shares: UInt
    fee_shares: UInt
    drawn_shares: UInt
    premium_ray: UInt
    bonus_bps: UInt


class LiquidationPair(Model):
    collateral: EconomicPosition
    debt: EconomicPosition
    collateral_market: MarketView
    debt_market: MarketView
    risk: RiskView

    def require_eligible(self):
        c, d = self.collateral, self.debt
        cm, dm = self.collateral_market, self.debt_market
        if c.target == d.target:
            if c != d or cm != dm:
                raise ValueError("same-reserve liquidation requires one authoritative position")
            fund, spoke = require_risk_scope((c,), (cm,))
        else:
            fund, spoke = require_risk_scope((c, d), (cm, dm))
        if (fund, spoke) != (self.risk.fund, self.risk.spoke):
            raise ValueError("liquidation risk has a different fund or Spoke")
        if not c.collateral or not c.shares or not d.drawn_shares:
            raise ValueError("liquidation requires active collateral and debt")
        if cm.risk.config(c.dynamic_config_key).collateral_factor_bps == 0:
            raise ValueError("liquidation collateral has no collateral factor")
        if self.risk.health_factor_wad is None or self.risk.health_factor_wad >= WAD:
            raise ValueError("liquidation requires a health factor below one")

    def bonus(self) -> int:
        config = self.collateral_market.risk
        maximum = config.config(self.collateral.dynamic_config_key).max_liquidation_bonus_bps
        health = self.risk.health_factor_wad
        if health is None or health >= WAD:
            raise ValueError("liquidation bonus requires unhealthy debt")
        threshold = config.liquidation.health_factor_for_max_bonus
        if health <= threshold:
            return maximum
        minimum = BPS + (maximum - BPS) * config.liquidation.bonus_factor_bps // BPS
        return minimum + (maximum - minimum) * (WAD - health) // (WAD - threshold)

    def debt_to_target(self, bonus: int) -> int:
        cm, dm = self.collateral_market, self.debt_market
        target = cm.risk.liquidation.target_health_factor
        factor = cm.risk.config(self.collateral.dynamic_config_key).collateral_factor_bps
        penalty = bonus * factor * (WAD // (BPS * BPS))
        health = self.risk.health_factor_wad
        if health is None or health >= target:
            raise ValueError("liquidation target must exceed current health")
        return ceiling(
            self.risk.debt_value_ray * 10**dm.risk.reserve.asset.decimals * (target - health),
            (target - penalty) * dm.price.price * WAD,
        )

    def initial_debt(self, bonus: int) -> tuple[int, int]:
        target = self.debt_to_target(bonus)
        dm, debt = self.debt_market, self.debt
        premium = premium_ray(debt, dm.borrow_index)
        restored = min(ceiling(target, RAY) * RAY, premium)
        drawn = min(debt.drawn_shares, ceiling(max(0, target - restored), dm.borrow_index))
        remaining = (debt.drawn_shares - drawn) * dm.borrow_index + premium - restored
        if drawn < debt.drawn_shares and dm.value(remaining) < DUST_VALUE * RAY:
            return debt.drawn_shares, premium
        return drawn, restored

    def collateral_shares(self, drawn: int, restored: int, bonus: int) -> int:
        cm, dm = self.collateral_market, self.debt_market
        debt_ray = drawn * dm.borrow_index + restored
        assets = (
            debt_ray
            * dm.price.price
            * 10**cm.risk.reserve.asset.decimals
            * bonus
            // (10**dm.risk.reserve.asset.decimals * cm.price.price * BPS * RAY)
        )
        return cm.supply_shares(assets)

    def collateral_limited(self, bonus: int) -> tuple[int, int, int]:
        cm, dm = self.collateral_market, self.debt_market
        shares = self.collateral.shares
        debt_ray = ceiling(
            cm.supplied(shares, up=True)
            * cm.price.price
            * 10**dm.risk.reserve.asset.decimals
            * BPS
            * RAY,
            dm.price.price * 10**cm.risk.reserve.asset.decimals * bonus,
        )
        premium = premium_ray(self.debt, dm.borrow_index)
        if debt_ray <= premium:
            return shares, 0, min(ceiling(debt_ray, RAY) * RAY, premium)
        drawn = ceiling(debt_ray - premium, dm.borrow_index)
        if drawn > self.debt.drawn_shares:
            drawn = self.debt.drawn_shares
            shares = min(shares, self.collateral_shares(drawn, premium, bonus))
        return shares, drawn, premium


def liquidation_amounts(pair: LiquidationPair) -> LiquidationAmounts:
    pair.require_eligible()
    bonus = pair.bonus()
    drawn, restored = pair.initial_debt(bonus)
    shares = pair.collateral_shares(drawn, restored, bonus)
    cm, collateral = pair.collateral_market, pair.collateral
    dust = shares < collateral.shares and (
        cm.value(cm.supplied(collateral.shares - shares)) < DUST_VALUE
    )
    if shares > collateral.shares or (dust and drawn < pair.debt.drawn_shares):
        shares, drawn, restored = pair.collateral_limited(bonus)
    fee = cm.risk.config(collateral.dynamic_config_key).liquidation_fee_bps
    return LiquidationAmounts(
        collateral_shares=shares,
        fee_shares=ceiling(shares * fee * (bonus - BPS), bonus * BPS),
        drawn_shares=drawn,
        premium_ray=restored,
        bonus_bps=bonus,
    )


def settle_liquidations(positions, markets):
    """Return new positions and movements without touching hypothetical wallet cash."""
    from almanak.backtest.economic_actions import refresh_positions
    from almanak.backtest.economic_math import risk_view

    require_risk_scope(positions, markets)
    movements: list[LiquidationEffect] = []
    for _ in range(1000):
        risk = risk_view(positions, markets)
        if risk.health_factor_wad is None or risk.health_factor_wad >= WAD:
            return positions, tuple(movements)
        collateral, debt = select_pair(positions, markets)
        if collateral is None:
            # V4 reports deficits as a consequence of liquidation. A configuration
            # change alone cannot forgive an otherwise unliquidatable debt.
            remaining = record_deficits(positions, markets) if movements else positions
            return remaining, tuple(movements)
        pair = LiquidationPair(
            collateral=positions[collateral],
            debt=positions[debt],
            collateral_market=markets[collateral],
            debt_market=markets[debt],
            risk=risk,
        )
        amounts = liquidation_amounts(pair)
        if amounts.collateral_shares == 0 or not (amounts.drawn_shares or amounts.premium_ray):
            raise ValueError("modeled liquidation cannot make progress at the available precision")
        positions, movement = apply_liquidation(positions, markets, collateral, debt, amounts)
        movements.append(movement)
        positions = refresh_positions(positions, markets)
    raise ValueError("modeled liquidation did not converge within its explicit work limit")


def select_pair(positions, markets):
    collateral = []
    debt = []
    for index, (p, m) in enumerate(zip(positions, markets, strict=True)):
        identity = p.target.reserve.reserve_id
        if p.collateral and p.shares and m.risk.config(p.dynamic_config_key).collateral_factor_bps:
            collateral.append((-m.value(m.supplied(p.shares)), identity, index))
        if p.drawn_shares:
            owed = p.drawn_shares * m.borrow_index + premium_ray(p, m.borrow_index)
            debt.append((-m.value(owed), identity, index))
    if not debt:
        raise ValueError("unhealthy account has no declared drawn debt")
    return min(collateral)[2] if collateral else None, min(debt)[2]


def apply_liquidation(positions, markets, collateral, debt, amounts):
    from almanak.backtest.economic_math import reset_premium
    from almanak.backtest.economic_state import LiquidationEffect
    from almanak.models import AssetAmount

    changed = list(positions)
    c, d = positions[collateral], positions[debt]
    cm, dm = markets[collateral], markets[debt]
    restored = ceiling(amounts.drawn_shares * dm.borrow_index, RAY) + ceiling(
        amounts.premium_ray, RAY
    )
    removed = cm.supplied(amounts.collateral_shares)
    changed[collateral] = c.model_copy(
        update={
            "shares": c.shares - amounts.collateral_shares,
            "liquidated_collateral": c.liquidated_collateral + removed,
        }
    )
    after = reset_premium(
        changed[debt],
        dm.borrow_index,
        d.risk_premium_bps,
        taken=amounts.drawn_shares,
        restored=amounts.premium_ray,
    )
    changed[debt] = after.model_copy(update={"liquidated_debt": d.liquidated_debt + restored})
    return tuple(changed), LiquidationEffect(
        collateral=c.target,
        debt=d.target,
        collateral_removed=AssetAmount(asset=c.target.asset, amount=removed),
        debt_restored=AssetAmount(asset=d.target.asset, amount=restored),
        fee_shares=amounts.fee_shares,
    )


def record_deficits(positions, markets):
    from almanak.backtest.economic_math import debts, reset_premium

    changed = []
    for p, m in zip(positions, markets, strict=True):
        total = sum(debts(p, m.borrow_index))
        after = reset_premium(
            p,
            m.borrow_index,
            0,
            taken=p.drawn_shares,
            restored=premium_ray(p, m.borrow_index),
        )
        changed.append(after.model_copy(update={"deficit": p.deficit + total}))
    return tuple(changed)
