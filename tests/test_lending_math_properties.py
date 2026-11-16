"""Generated amounts check Aave ray, wad and share rounding against exact fractions.

Each conversion must favor the pool: shares minted for a supply and assets paid on
a withdrawal round down, shares burned for a withdrawal and debt owed round up.
Captured revision vectors stay in test_aave_v3_math.py.
"""

from decimal import Decimal, localcontext
from fractions import Fraction
from math import ceil, floor

from hypothesis import given
from hypothesis import strategies as st

from almanak.backtest.economic_math import MarketView
from almanak.connectors.aave_v3 import math
from almanak.connectors.aave_v3.risk import BPS, WAD, account_totals
from almanak.connectors.aave_v4.history import VIRTUAL, Market
from almanak.connectors.pool_lending_math import RAY, SECONDS_PER_YEAR, linear_interest, ray_mul

words = st.integers(0, 2**128 - 1)
amounts = st.integers(0, 2**96)
# Liquidity and borrow indices start at one ray and only grow.
indices = st.integers(RAY, 10**4 * RAY)
rates = st.integers(0, 10 * RAY)
elapsed = st.integers(0, 10 * SECONDS_PER_YEAR)


@given(words, words)
def test_ray_multiplication_rounds_half_up(a, b):
    exact = Fraction(a * b, RAY)
    assert ray_mul(a, b) == floor(exact + Fraction(1, 2))


@given(words, words, st.integers(1, 2**128))
def test_mul_div_floors_or_ceils_the_exact_quotient(a, b, denominator):
    exact = Fraction(a * b, denominator)
    assert math.mul_div(a, b, denominator) == floor(exact)
    assert math.mul_div(a, b, denominator, ceiling=True) == ceil(exact)


@given(amounts, indices)
def test_v3_supply_and_withdraw_shares_never_overpay_the_supplier(amount, index):
    # A supply credits no more than it paid; a withdrawal burns enough for its amount.
    assert math.supplied_assets(math.supply_shares(amount, index), index) <= amount
    assert math.supplied_assets(math.withdraw_shares(amount, index), index) >= amount
    assert math.supply_shares(amount, index) <= math.withdraw_shares(amount, index)


@given(amounts, indices)
def test_v3_borrow_and_repay_shares_never_undercount_debt(amount, index):
    # A borrow owes at least what it received; a repayment clears at most what it paid.
    assert math.debt_assets(math.borrow_shares(amount, index), index) >= amount
    assert math.debt_assets(math.repay_shares(amount, index), index) <= amount
    assert math.repay_shares(amount, index) <= math.borrow_shares(amount, index)


@given(rates, elapsed, elapsed)
def test_interest_accrues_monotonically_and_compounds_above_linear(rate, first, second):
    shorter, longer = sorted((first, second))
    assert RAY <= linear_interest(rate, shorter) <= linear_interest(rate, longer)
    assert math.compounded_interest(rate, shorter) <= math.compounded_interest(rate, longer)
    assert linear_interest(rate, longer) <= math.compounded_interest(rate, longer)


@given(rates, elapsed)
def test_compounded_interest_stays_at_or_below_continuous_compounding(rate, seconds):
    with localcontext() as context:
        context.prec = 80
        growth = Decimal(rate * seconds // SECONDS_PER_YEAR) / RAY
        continuous = growth.exp() * RAY
    # Three Taylor terms of exp, each floored, plus one unit of ray rounding.
    assert math.compounded_interest(rate, seconds) <= continuous + 1


@given(st.integers(1, 2**128), st.integers(0, 2**128), st.integers(0, 10**4))
def test_health_factor_is_the_wad_quotient_of_weighted_collateral_and_debt(debt, value, threshold):
    ltv = value * min(threshold, 9500)
    risk = account_totals(value, debt, ltv, value * threshold)
    exact = Fraction(value * threshold * WAD, debt * BPS)
    assert abs(risk.health_factor_wad - exact) <= 1
    worse = account_totals(value, debt + 1, ltv, value * threshold)
    assert worse.health_factor_wad <= risk.health_factor_wad


def v4_market(added_assets, added_shares):
    # Share conversions read only these totals; the observation fields are irrelevant.
    return Market.model_construct(added_assets=added_assets, added_shares=added_shares)


@given(st.integers(0, 2**112), st.integers(0, 2**112), amounts)
def test_v4_hub_shares_round_against_the_supplier(assets, shares, amount):
    market = v4_market(assets, shares)
    exact = Fraction(amount * (shares + VIRTUAL), assets + VIRTUAL)
    assert market.supplied_shares(amount) == floor(exact)
    assert market.burned_shares(amount) == ceil(exact)
    assert market.supplied_assets(market.supplied_shares(amount)) <= amount
    assert market.supplied_assets(market.burned_shares(amount)) >= amount
    # Cap exposure values shares at their asset ceiling.
    assert market.cap_exposure(amount) == ceil(
        Fraction(amount * (assets + VIRTUAL), shares + VIRTUAL)
    )


@given(indices, amounts)
def test_v4_economic_supply_index_rounds_both_directions_by_one_unit(index, amount):
    market = MarketView.model_construct(supply_index=index)
    for up in (False, True):
        shares = market.supply_shares(amount, up=up)
        assert shares == (ceil if up else floor)(Fraction(amount * RAY, index))
        assert market.supplied(shares, up=up) == (ceil if up else floor)(
            Fraction(shares * index, RAY)
        )
    assert market.supplied(market.supply_shares(amount)) <= amount
    assert market.supplied(market.supply_shares(amount, up=True)) >= amount
