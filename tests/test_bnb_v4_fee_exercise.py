"""BNB V4 fee exercise sizing over the retained fresh and pinned pool states."""

from types import SimpleNamespace

import pytest

from almanak.connectors.uniswap_v4.economic_data import Tick, Word
from tests.strategy_fork.lp_fee_exercise import fee_exercise_plan
from tests.strategy_fork.lp_minima import fee_currency

LOWER = -66850
UPPER = -65790
EXERCISE = 5 * 10**18
MAXIMUM = 100 * 10**18
USDT = "eip155:56/erc20:0x55d398326f99059ff775485246999027b3197955"
WBNB = "eip155:56/erc20:0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
# Immutable retained inputs:
# fresh checkpoint f4fea4895d4ecd122a87c0e69e9b535fdf1e3237e3e34433710894889c01663b
# pinned checkpoint f2e22401279fb3cbf449bec34e53159510c7db57f3dff5968672b65e84294855
# boundary selection 641ef92f142e401875d921a0cbfa863485726a21ff64578fc86c4836130f2c2d


def target():
    return SimpleNamespace(
        range=SimpleNamespace(lower=LOWER, upper=UPPER),
        funding_currency=USDT,
        assets=(USDT, WBNB),
    )


def sample(tick, sqrt_price_x96, liquidity):
    compressed = UPPER // 10
    words = (
        Word(index=-27, bitmap=0),
        Word(index=-26, bitmap=1 << (compressed & 255)),
    )
    return SimpleNamespace(
        pool=SimpleNamespace(
            tick=tick,
            sqrt_price_x96=sqrt_price_x96,
            liquidity=liquidity,
            protocol_fee=512125,
            lp_fee=500,
            source=SimpleNamespace(tick_spacing=10),
        ),
        lower=LOWER,
        upper=-65720,
        words=words,
        ticks=(Tick(index=UPPER, gross=100177955947605802, net=-100177955947605802),),
        hook=None,
    )


def test_retained_fresh_pool_trade_enters_and_exercises_authored_range():
    retained = sample(
        tick=-65735,
        sqrt_price_x96=2961706894782269168715806667,
        liquidity=1206277642310824332926,
    )
    plan = fee_exercise_plan(retained, target(), EXERCISE, MAXIMUM)
    assert plan.entry_amount == 89547673498531930983
    assert plan.amount <= MAXIMUM
    assert LOWER <= plan.end_tick < UPPER
    assert plan.start_tick == -65735


def test_retained_fresh_pool_refuses_an_insufficient_fixture_budget():
    retained = sample(
        tick=-65735,
        sqrt_price_x96=2961706894782269168715806667,
        liquidity=1206277642310824332926,
    )
    with pytest.raises(ValueError, match="fixture budget cannot reach the authored range"):
        fee_exercise_plan(retained, target(), EXERCISE, 50 * 10**18)


def test_zero_fee_growth_has_an_explicit_failure_instead_of_stop_iteration():
    with pytest.raises(AssertionError, match="produced no positive position fee growth"):
        fee_currency((0, 0))


def test_retained_pinned_pool_uses_only_the_exercise_amount_when_already_in_range():
    retained = sample(
        tick=-66293,
        sqrt_price_x96=2880239612059260558951671455,
        liquidity=149448935108167099987,
    )
    plan = fee_exercise_plan(retained, target(), EXERCISE, MAXIMUM)
    assert plan.entry_amount == 0
    assert plan.amount == EXERCISE
    assert LOWER <= plan.end_tick < UPPER
