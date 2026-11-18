"""Offline GM share quotes use captured action-specific valuation and external inventory."""

from typing import Literal

from pydantic import model_validator

from almanak.connectors.gmx_v2.economic_spot_math import fees_for, impact_amount, spot_impact
from almanak.connectors.gmx_v2.economic_spot_types import Fees, SpotState
from almanak.models import Model, UInt


class LiquidityState(Model):
    action: Literal["DEPOSIT", "WITHDRAWAL"]
    spot: SpotState
    pool_value_usd_e30: UInt
    share_supply: UInt

    @model_validator(mode="after")
    def value_for_supply(self):
        if self.action != self.spot.pricing_type:
            raise ValueError("GMX liquidity valuation and fee actions differ")
        if self.share_supply and not self.pool_value_usd_e30:
            raise ValueError("GMX outstanding shares require positive pool valuation")
        return self


class DepositCalculation(Model):
    long_amount: UInt
    short_amount: UInt
    shares: UInt
    price_impact_usd_e30: int
    long_fees: Fees
    short_fees: Fees


class WithdrawalCalculation(Model):
    shares: UInt
    long_amount: UInt
    short_amount: UInt
    long_fees: Fees
    short_fees: Fees


def shares_for_usd(state, usd):
    if not state.share_supply:
        # The verified protocol initializes GM at one USD, including pre-existing pool value.
        return (usd + state.pool_value_usd_e30) // 10**12
    return state.share_supply * usd // state.pool_value_usd_e30


def signed_proportion(value, numerator, denominator):
    magnitude = abs(value) * numerator // denominator
    return magnitude if value >= 0 else -magnitude


def deposit_currency(state, incoming, outgoing, amount, impact_usd, improved):
    fees = fees_for(amount, state.spot.fees, improved)
    net = fees.after_fees
    minted = 0
    if impact_usd > 0 and state.share_supply:
        bonus, leftover = impact_amount(outgoing, impact_usd)
        minted = shares_for_usd(state, bonus * outgoing.price.maximum)
        extra, _ = impact_amount(incoming, leftover)
        net += extra
    elif impact_usd < 0:
        penalty, _ = impact_amount(incoming, impact_usd)
        net += penalty
    if net < 0:
        raise ValueError("GMX deposit impact exceeds its currency amount after fees")
    return minted + shares_for_usd(state, net * incoming.price.minimum), fees


def quote_deposit(state, long_amount, short_amount):
    if state.action != "DEPOSIT":
        raise ValueError("GMX deposit requires deposit-specific valuation and fees")
    if min(long_amount, short_amount) < 0 or long_amount + short_amount <= 0:
        raise ValueError("GMX deposit requires nonnegative amounts and a positive total")
    spot = state.spot
    usd = (long_amount * spot.long.price.midpoint, short_amount * spot.short.price.midpoint)
    impact = spot_impact(spot, usd)
    amounts = (long_amount, short_amount)
    sides = ((spot.long, spot.short), (spot.short, spot.long))
    results = tuple(
        deposit_currency(
            state,
            incoming,
            outgoing,
            amount,
            signed_proportion(impact.usd_e30, value, sum(usd)),
            impact.balance_improved,
        )
        for (incoming, outgoing), amount, value in zip(sides, amounts, usd, strict=True)
    )
    return DepositCalculation(
        long_amount=long_amount,
        short_amount=short_amount,
        shares=sum(r[0] for r in results),
        price_impact_usd_e30=impact.usd_e30,
        long_fees=results[0][1],
        short_fees=results[1][1],
    )


def quote_withdrawal(state, shares):
    if state.action != "WITHDRAWAL":
        raise ValueError("GMX withdrawal requires withdrawal-specific valuation and fees")
    if shares <= 0 or not state.share_supply or shares > state.share_supply:
        raise ValueError("GMX withdrawal requires positive shares within the captured supply")
    spot = state.spot
    sides = (spot.long, spot.short)
    inventory_usd = tuple(side.pool_amount * side.price.maximum for side in sides)
    total = sum(inventory_usd)
    if not total or not state.pool_value_usd_e30:
        raise ValueError("GMX withdrawal requires positive inventory and pool valuation")
    value = state.pool_value_usd_e30 * shares // state.share_supply
    amounts = tuple(
        value * usd // total // side.price.maximum
        for usd, side in zip(inventory_usd, sides, strict=True)
    )
    fees = tuple(fees_for(amount, spot.fees, False) for amount in amounts)
    return WithdrawalCalculation(
        shares=shares,
        long_amount=fees[0].after_fees,
        short_amount=fees[1].after_fees,
        long_fees=fees[0],
        short_fees=fees[1],
    )
