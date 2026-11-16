"""Funding follows the deployed GMX rate rules, side by side, without retained evidence."""

import pytest

from almanak.connectors.gmx_v2.economic_funding import FundingBounds, next_rate
from almanak.connectors.gmx_v2.economic_precision import FLOAT
from tests import gmx_synthetic_frames as synthetic

# The synthetic state was last updated 30 seconds before its block.
DURATION = 30


@pytest.fixture
def state():
    market = synthetic.perpetual_target().market
    return synthetic.funding(market, synthetic.block(market))


def adaptive(state, *, saved=0, long=200, short=100, **parameters):
    policy = {"increase_per_second": 1000, **parameters}
    return state.model_copy(
        update={
            "parameters": state.parameters.model_copy(update=policy),
            "saved_per_second": saved,
            "long_balance_usd_e30": long * FLOAT,
            "short_balance_usd_e30": short * FLOAT,
        }
    )


def rate(state):
    result = next_rate(state)
    return result.longs_pay_shorts, result.per_second, result.next_saved_per_second


def test_static_rate_is_capped_by_the_larger_sides_maximum(state):
    capped = FundingBounds(minimum_per_second=0, maximum_per_second=7)
    longs = state.model_copy(
        update={
            "parameters": state.parameters.model_copy(update={"longs_pay": capped}),
            "long_balance_usd_e30": 200 * FLOAT,
            "short_balance_usd_e30": 100 * FLOAT,
        }
    )
    assert rate(longs) == (True, 7, 0)
    shorts = longs.model_copy(
        update={"long_balance_usd_e30": 100 * FLOAT, "short_balance_usd_e30": 200 * FLOAT}
    )
    assert rate(shorts) == (False, (FLOAT // 3) * 10**24 // FLOAT, 0)


def test_balanced_static_market_pays_nothing(state):
    balanced = state.model_copy(update={"short_balance_usd_e30": state.long_balance_usd_e30})
    assert rate(balanced) == (True, 0, 0)


def test_adaptive_rate_refuses_empty_open_interest(state):
    empty = adaptive(state, long=0, short=0)
    with pytest.raises(ValueError, match="empty open interest"):
        next_rate(empty)


def test_adaptive_increase_follows_the_skew_and_the_paying_sides_bounds(state):
    # Skew FLOAT / 3 at 1000 per second for 30 seconds moves the saved rate by 9990.
    assert rate(adaptive(state, saved=100)) == (True, 10090, 10090)
    narrow = FundingBounds(minimum_per_second=2000, maximum_per_second=2500)
    assert rate(adaptive(state, saved=-100, long=100, short=200, shorts_pay=narrow)) == (
        False,
        2500,
        -2500,
    )
    # The narrow bounds apply only while shorts pay.
    assert rate(adaptive(state, saved=100, shorts_pay=narrow)) == (True, 10090, 10090)


def test_minimum_increase_rate_floors_only_a_skewed_increase(state):
    floored = adaptive(state, increase_per_second=1, minimum_increase_per_second=50)
    assert rate(floored) == (True, 50 * DURATION, 50 * DURATION)
    balanced = adaptive(
        state, long=100, short=100, increase_per_second=1, minimum_increase_per_second=50
    )
    assert rate(balanced) == (True, 0, 0)


def test_zero_next_saved_rate_pays_nothing_despite_a_minimum(state):
    floor = FundingBounds(minimum_per_second=1000, maximum_per_second=10**25)
    balanced = adaptive(state, long=100, short=100, longs_pay=floor, shorts_pay=floor)
    assert rate(balanced) == (True, 0, 0)


@pytest.mark.parametrize(
    "saved,expected", [(1000, 1000 - 10 * DURATION), (100, 1), (-1000, -700), (-100, -1)]
)
def test_aligned_small_skew_decreases_toward_but_never_through_zero(state, saved, expected):
    # Skew 1 / 201 is under the decrease threshold; the decrease is 10 per second.
    long, short = (101, 100) if saved > 0 else (100, 101)
    floor = FundingBounds(minimum_per_second=0, maximum_per_second=10**25)
    result = rate(adaptive(state, saved=saved, long=long, short=short, longs_pay=floor))
    assert result == (expected > 0, abs(expected), expected)


def test_aligned_moderate_skew_keeps_the_saved_rate(state):
    # Skew FLOAT / 5 lies between the decrease (FLOAT / 10) and stable (FLOAT / 4) thresholds.
    assert rate(adaptive(state, saved=500, long=150, short=100)) == (True, 500, 500)
    assert rate(adaptive(state, saved=-500, long=100, short=150)) == (False, 500, -500)


def test_bounds_refuse_a_minimum_above_the_maximum():
    with pytest.raises(ValueError, match="minimum exceeds its maximum"):
        FundingBounds(minimum_per_second=2, maximum_per_second=1)
