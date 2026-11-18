"""Accrue GMX funding from canonical skew, rate controls and collateral-specific interest."""

from pydantic import model_validator

from almanak.connectors.gmx_v2.domain import Market
from almanak.connectors.gmx_v2.economic_precision import (
    FLOAT,
    apply_exponent,
    apply_factor,
    ceil_div,
)
from almanak.connectors.gmx_v2.economic_spot_types import Price
from almanak.models import Block, Model, UInt


class CollateralValues(Model):
    long: UInt
    short: UInt

    @property
    def total(self):
        return self.long + self.short


class FundingBounds(Model):
    minimum_per_second: UInt
    maximum_per_second: UInt

    @model_validator(mode="after")
    def ordered(self):
        if self.minimum_per_second > self.maximum_per_second:
            raise ValueError("GMX funding minimum exceeds its maximum rate")
        return self


class FundingParameters(Model):
    factor: UInt
    exponent: UInt
    increase_per_second: UInt
    # Floor of an adaptive increase while open interest is skewed.
    minimum_increase_per_second: UInt
    decrease_per_second: UInt
    # The deployed market keys its rate bounds by the side that pays.
    longs_pay: FundingBounds
    shorts_pay: FundingBounds
    stable_threshold: UInt
    decrease_threshold: UInt

    def bounds(self, longs_pay: bool) -> FundingBounds:
        return self.longs_pay if longs_pay else self.shorts_pay


class FundingState(Model):
    market: Market
    block: Block
    long_price: Price
    short_price: Price
    parameters: FundingParameters
    long_interest: CollateralValues
    short_interest: CollateralValues
    long_balance_usd_e30: UInt
    short_balance_usd_e30: UInt
    updated_at: UInt
    saved_per_second: int

    @model_validator(mode="after")
    def scope(self):
        if (
            self.block.chain != self.market.chain
            or self.long_price.asset != self.market.long_asset
            or self.short_price.asset != self.market.short_asset
            or self.updated_at > self.block.timestamp
        ):
            raise ValueError("GMX funding requires canonical market prices and a past update")
        return self


class FundingRate(Model):
    longs_pay_shorts: bool
    per_second: UInt
    next_saved_per_second: int


class FundingAccrual(Model):
    rate: FundingRate
    long_payments: CollateralValues
    short_payments: CollateralValues
    long_claims: CollateralValues
    short_claims: CollateralValues


def magnitude_bound(value, minimum, maximum):
    return min(max(abs(value), minimum), maximum) * (-1 if value < 0 else 1)


def adaptive_saved_rate(state, skew, duration):
    policy, saved = state.parameters, state.saved_per_second
    long, short = state.long_balance_usd_e30, state.short_balance_usd_e30
    same_direction = (saved > 0 and long > short) or (saved < 0 and short > long)
    if not same_direction or skew > policy.stable_threshold:
        per_second = apply_factor(skew, policy.increase_per_second)
        if skew:
            per_second = max(per_second, policy.minimum_increase_per_second)
        change = per_second * duration
        return saved + (-change if long < short else change)
    if skew < policy.decrease_threshold and saved:
        remaining = max(1, abs(saved) - policy.decrease_per_second * duration)
        return remaining * (-1 if saved < 0 else 1)
    return saved


def next_rate(state):
    policy = state.parameters
    long, short = state.long_balance_usd_e30, state.short_balance_usd_e30
    difference = abs(long - short)
    if difference == 0 and policy.increase_per_second == 0:
        return FundingRate(longs_pay_shorts=True, per_second=0, next_saved_per_second=0)
    if long + short == 0:
        raise ValueError("GMX funding rate cannot divide by empty open interest")
    skew = apply_exponent(difference, policy.exponent) * FLOAT // (long + short)
    if policy.increase_per_second == 0:
        maximum = policy.bounds(long > short).maximum_per_second
        return FundingRate(
            longs_pay_shorts=long > short,
            per_second=min(apply_factor(skew, policy.factor), maximum),
            next_saved_per_second=0,
        )
    duration = state.block.timestamp - state.updated_at if state.updated_at else 0
    saved = adaptive_saved_rate(state, skew, duration)
    if saved == 0:
        # No side pays, so neither side's minimum applies.
        return FundingRate(longs_pay_shorts=True, per_second=0, next_saved_per_second=0)
    bounds = policy.bounds(saved > 0)
    saved = magnitude_bound(saved, 0, bounds.maximum_per_second)
    rate = magnitude_bound(saved, bounds.minimum_per_second, bounds.maximum_per_second)
    return FundingRate(longs_pay_shorts=rate > 0, per_second=abs(rate), next_saved_per_second=saved)


def amount_per_size(funding_usd, open_interest, token_price, *, payment):
    if not funding_usd or not open_interest:
        return 0
    if payment:
        return ceil_div(ceil_div(funding_usd * FLOAT * 10**15, open_interest), token_price)
    return (funding_usd * FLOAT * 10**15 // open_interest) // token_price


def funding_deltas(state, rate):
    paying, receiving = (
        (state.long_interest, state.short_interest)
        if rate.longs_pay_shorts
        else (state.short_interest, state.long_interest)
    )
    duration = state.block.timestamp - state.updated_at if state.updated_at else 0
    divisor = 2 if state.market.long_asset == state.market.short_asset else 1
    funding_usd = apply_factor(paying.total, duration * rate.per_second) // divisor
    payments, claims = {}, {}
    for side, price in (("long", state.long_price), ("short", state.short_price)):
        interest = getattr(paying, side)
        split = funding_usd * interest // paying.total
        payments[side] = amount_per_size(split, interest, price.maximum, payment=True)
        claims[side] = amount_per_size(split, receiving.total, price.maximum, payment=False)
    return CollateralValues(**payments), CollateralValues(**claims)


def accrue_funding(state):
    zero = CollateralValues(long=0, short=0)
    if not state.long_interest.total or not state.short_interest.total:
        return FundingAccrual(
            rate=FundingRate(longs_pay_shorts=False, per_second=0, next_saved_per_second=0),
            long_payments=zero,
            short_payments=zero,
            long_claims=zero,
            short_claims=zero,
        )
    rate = next_rate(state)
    payments, claims = funding_deltas(state, rate)
    return FundingAccrual(
        rate=rate,
        long_payments=payments if rate.longs_pay_shorts else zero,
        short_payments=zero if rate.longs_pay_shorts else payments,
        long_claims=zero if rate.longs_pay_shorts else claims,
        short_claims=claims if rate.longs_pay_shorts else zero,
    )


class FundingIndexes(Model):
    long_payments: CollateralValues
    short_payments: CollateralValues
    long_claims: CollateralValues
    short_claims: CollateralValues


def accumulated_funding(state, stored):
    accrued = accrue_funding(state)
    multiplier = 2 if state.market.long_asset == state.market.short_asset else 1
    values = {}
    for field in FundingIndexes.model_fields:
        before, delta = getattr(stored, field), getattr(accrued, field)
        values[field] = CollateralValues(
            long=before.long + multiplier * delta.long,
            short=before.short + multiplier * delta.short,
        )
    return FundingIndexes(**values)
