"""Generated cases check the shared V3/V4 integer math against exact rational bounds.

Every pool-facing rounding must favor the pool: an input the pool takes rounds up,
an output it pays rounds down, and a price moved by an input moves no further than
exact arithmetic would. Contract vectors stay in test_uniswap_v3_math.py; these
properties cover the domain between them.
"""

from decimal import Decimal, localcontext
from fractions import Fraction
from math import ceil, floor, isqrt

import pytest
from hypothesis import given
from hypothesis import strategies as st

from almanak.connectors import concentrated_math as math
from almanak.connectors.concentrated_swap import v3_step
from almanak.connectors.uniswap_v4.swap_math import PIPS, swap_fee
from almanak.connectors.uniswap_v4.swap_math import swap_step as v4_step

MIN_TICK, MAX_TICK = -887272, 887272
MIN_SQRT, MAX_SQRT = math.sqrt_at_tick(MIN_TICK), math.sqrt_at_tick(MAX_TICK)

ticks = st.integers(MIN_TICK, MAX_TICK)
prices = st.integers(MIN_SQRT, MAX_SQRT - 1)
liquidity = st.integers(1, 2**128 - 1)
inputs = st.integers(0, 2**127 - 1)
v3_fees = st.integers(0, PIPS - 1)
steps = pytest.mark.parametrize(
    ("step", "maximum_fee"), [(v3_step, PIPS - 1), (v4_step, PIPS)], ids=["v3", "v4"]
)


@st.composite
def intervals(draw):
    """A strictly ordered sqrt-price interval taken from two distinct ticks."""
    lower, upper = sorted(draw(st.lists(ticks, min_size=2, max_size=2, unique=True)))
    return math.sqrt_at_tick(lower), math.sqrt_at_tick(upper)


def exact_amount0(liquidity_value, lower, upper):
    return Fraction(liquidity_value * math.Q96 * (upper - lower), lower * upper)


def exact_amount1(liquidity_value, lower, upper):
    return Fraction(liquidity_value * (upper - lower), math.Q96)


@given(st.integers(MIN_TICK, MAX_TICK - 1))
def test_sqrt_price_strictly_increases_with_tick(tick):
    assert MIN_SQRT <= math.sqrt_at_tick(tick) < math.sqrt_at_tick(tick + 1) <= MAX_SQRT


@given(ticks)
def test_sqrt_price_tracks_the_exact_tick_power(tick):
    with localcontext() as context:
        context.prec = 90
        exact = (Decimal("1.0001") ** tick).sqrt() * (1 << 96)
    # TickMath multiplies 128-bit reciprocal factors and inverts them for positive
    # ticks, so near the top tick only about 64 bits of the reciprocal remain. The
    # measured worst case is 5.3e-20 relative beyond the final round-up unit.
    assert abs(Decimal(math.sqrt_at_tick(tick)) - exact) <= 1 + exact / Decimal(10) ** 19


@given(ticks)
def test_tick_inverse_returns_the_tick_of_its_own_price(tick):
    assert math.tick_at_price(math.sqrt_at_tick(tick)) == tick


@given(prices)
def test_tick_inverse_brackets_every_price(price):
    tick = math.tick_at_price(price)
    assert math.sqrt_at_tick(tick) <= price < math.sqrt_at_tick(tick + 1)


@given(liquidity, prices, intervals())
def test_position_amounts_round_up_only_by_one_unit(size, price, interval):
    lower, upper = interval
    down = math.amounts(size, price, lower, upper)
    up = math.amounts(size, price, lower, upper, up=True)
    clipped = max(lower, min(price, upper))
    exact = exact_amount0(size, clipped, upper), exact_amount1(size, lower, clipped)
    for low, high, value in zip(down, up, exact, strict=True):
        assert (low, high) == (floor(value), ceil(value))


@given(st.integers(0, 2**128 - 1), st.integers(0, 2**128 - 1), prices, intervals())
def test_liquidity_from_inventory_never_requires_more_than_supplied(
    amount0, amount1, price, interval
):
    lower, upper = interval
    size = math.liquidity_for(amount0, amount1, price, lower, upper)
    required = math.amounts(size, price, lower, upper, up=True)
    assert required[0] <= amount0 and required[1] <= amount1


@given(liquidity, liquidity, prices, intervals())
def test_position_amounts_grow_with_liquidity(first, second, price, interval):
    smaller, larger = sorted((first, second))
    for up in (False, True):
        low = math.amounts(smaller, price, *interval, up=up)
        high = math.amounts(larger, price, *interval, up=up)
        assert low[0] <= high[0] and low[1] <= high[1]


@given(
    st.integers(1, 2**128 - 1),
    st.integers(1, 2**128 - 1),
    prices,
    intervals(),
    st.integers(0, 500),
    st.integers(0, 2**32),
)
def test_addition_minimums_hold_across_the_admitted_price_window(
    amount0, amount1, price, interval, tolerance, position
):
    lower, upper = interval
    floor0, floor1, floor_liquidity = math.addition_minimums(
        amount0, amount1, price, lower, upper, tolerance
    )
    low = Fraction(price * price * (10000 - tolerance), 10000)
    high = Fraction(price * price * (10000 + tolerance), 10000)
    # Any execution sqrt price whose square lies inside the admitted window.
    first, last = isqrt(ceil(low) - 1) + 1, isqrt(floor(high))
    execution = first + position % (last - first + 1)
    size = math.liquidity_for(amount0, amount1, execution, lower, upper)
    pulled = math.amounts(size, execution, lower, upper, up=True)
    assert floor0 <= pulled[0] and floor1 <= pulled[1] and floor_liquidity <= size


@given(st.integers(0, 2**128 - 1), st.integers(0, 2**256 - 1), st.integers(0, 2**256 - 1))
def test_fee_growth_accrual_wraps_like_uint256(size, previous, delta):
    current = (previous + delta) % math.UINT256
    assert math.accrued(size, previous, current) == size * delta // math.Q128


@given(prices, prices, liquidity)
def test_amount_delta_is_symmetric_and_rounds_by_at_most_one(price, target, size):
    for currency0 in (True, False):
        down = math.amount_delta(price, target, size, currency0, up=False)
        up = math.amount_delta(price, target, size, currency0, up=True)
        assert down == math.amount_delta(target, price, size, currency0, up=False)
        assert down <= up <= down + 1


@given(prices, liquidity, inputs, st.booleans())
def test_input_moves_price_no_further_than_exact_arithmetic(price, size, amount, zero_for_one):
    try:
        moved = math.next_price(price, size, amount, zero_for_one)
    except ValueError:
        # Only a currency1 input can push the price past the uint160 domain.
        assert not zero_for_one
        return
    assert moved <= price if zero_for_one else moved >= price
    # The input the pool requires to reach the rounded price fits inside what was paid.
    assert math.amount_delta(price, moved, size, zero_for_one, up=True) <= amount


@steps
@given(prices, prices, liquidity, inputs, st.integers(0, PIPS))
def test_exact_input_step_conserves_input_and_stays_inside_its_target(
    step, maximum_fee, price, target, size, remaining, fee
):
    fee = min(fee, maximum_fee)
    updated, used, output, paid = step(price, target, size, remaining, fee)
    zero_for_one = price >= target
    assert min(price, target) <= updated <= max(price, target)
    assert used >= 0 and paid >= 0 and used + paid <= remaining
    if updated != target:
        assert used + paid == remaining
    assert output == math.amount_delta(price, updated, size, not zero_for_one, up=False)
    # The input the pool takes for its price move rounds up.
    assert used >= math.amount_delta(price, updated, size, zero_for_one, up=True)
    # The fee comes off the input first and is at least its exact share of what was used.
    assert used <= remaining * (PIPS - fee) // PIPS
    if fee < PIPS:
        assert paid >= ceil(Fraction(used * fee, PIPS - fee))


@steps
@given(prices, prices, liquidity, inputs, v3_fees)
def test_a_swap_and_its_reverse_never_return_more_than_was_paid(
    step, maximum_fee, price, target, size, amount, fee
):
    del maximum_fee
    moved, used, output, _ = step(price, target, size, amount, fee)
    # The reverse runs inside the same liquidity range, back toward the start price.
    back, _, returned, _ = step(moved, price, size, output, fee)
    assert min(price, moved) <= back <= max(price, moved)
    assert returned <= used


def scanned_tick(bitmap, tick, spacing, zero_for_one):
    """The next initialized tick within one bitmap word, by scanning every bit."""
    compressed = tick // spacing + (0 if zero_for_one else 1)
    word, bit = compressed >> 8, compressed & 255
    positions = range(bit, -1, -1) if zero_for_one else range(bit, 256)
    found = next((p for p in positions if bitmap >> p & 1), None)
    edge = (0 if zero_for_one else 255) if found is None else found
    result = ((word << 8) + edge) * spacing
    return max(MIN_TICK, min(MAX_TICK, result)), found is not None


@given(st.integers(0, 2**256 - 1), ticks, st.integers(1, 32767), st.booleans())
def test_next_tick_matches_a_scan_of_the_bitmap_word(bitmap, tick, spacing, zero_for_one):
    compressed = tick // spacing + (0 if zero_for_one else 1)
    words = {compressed >> 8: bitmap}
    assert math.next_tick(words, tick, spacing, zero_for_one) == scanned_tick(
        bitmap, tick, spacing, zero_for_one
    )


@given(st.integers(0, PIPS), st.integers(0, 1000), st.integers(0, 1000), st.booleans())
def test_v4_swap_fee_composes_within_both_component_fees(lp_fee, forward, reverse, zero_for_one):
    combined = swap_fee(lp_fee, forward | reverse << 12, zero_for_one)
    protocol = forward if zero_for_one else reverse
    assert max(lp_fee, protocol) <= combined <= min(PIPS, lp_fee + protocol)
