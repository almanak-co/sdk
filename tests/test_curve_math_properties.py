"""Generated pools check that the Curve integer kernels never pay out value the pool lacks.

Exact contract equality is retained in the captured comparisons of
test_curve_stable_math.py, test_curve_twocrypto_math.py and
test_curve_tricrypto_math.py. These properties cover generated pools between those
captures: a swap pays less than the output reserve, keeps or grows the invariant,
cannot be reversed at a profit and pays more for more input.

Sizes stay above dust. The contracts subtract one rounding unit from every output
and revert when that underflows, so a one-unit burn or swap refuses. The crypto
invariants converge only to one part in 10**14, so a swap of a few base units can
return a few tens of base units more than it paid; above about D / 10**12 the fee
dominates. Both are protocol arithmetic the SDK reproduces, not modeling errors.
Crypto bounds below are measured: over 4000 generated pools the output solver
missed its own balance by at most 1.5e-11 of D and a round trip gained at most
6.2e-12 of D, whatever the trade size.
"""

from dataclasses import replace

from hypothesis import given
from hypothesis import strategies as st

from almanak.connectors.curve import (
    tricrypto_math,
    tricrypto_quotes,
    twocrypto_math,
    twocrypto_quotes,
)
from almanak.connectors.curve.stable_math import (
    DECEMBER,
    NOVEMBER,
    StableState,
    addition,
    removal_one,
    removal_proportional,
    swap,
)

decimals = st.sampled_from((6, 8, 18))


def pair(draw, count):
    first = draw(st.integers(0, count - 1))
    return first, (first + draw(st.integers(1, count - 1))) % count


@st.composite
def stable_states(draw):
    count = draw(st.integers(2, 4))
    places = draw(st.lists(decimals, min_size=count, max_size=count))
    depth = draw(st.integers(10**21, 10**28))
    normalized = [draw(st.integers(depth // 10, depth)) for _ in range(count)]
    return StableState(
        balances=tuple(v // 10 ** (18 - d) for v, d in zip(normalized, places, strict=True)),
        rates=tuple(10 ** (36 - d) for d in places),
        amplification=draw(st.integers(100, 10**6)),
        fee=draw(st.integers(0, 10**8)),
        offpeg_multiplier=draw(st.integers(10**10, 10**11)),
        total_supply=sum(normalized),
        revision=draw(st.sampled_from((NOVEMBER, DECEMBER))),
    )


@st.composite
def stable_swaps(draw):
    state = draw(stable_states())
    first, second = pair(draw, len(state.balances))
    return state, first, second, draw(st.integers(1, state.balances[first]))


def after_stable_swap(state, first, second, amount, result):
    balances = list(state.balances)
    balances[first] += amount
    # The admin share leaves the pool's accounted balances; the LP share stays.
    balances[second] -= result.output + result.admin_fee
    return replace(state, balances=tuple(balances))


@given(stable_swaps())
def test_stable_swap_pays_less_than_the_reserve_and_keeps_the_invariant(case):
    state, first, second, amount = case
    result = swap(state, first, second, amount)
    assert 0 <= result.admin_fee <= result.fee
    assert result.output + result.fee < state.balances[second]
    after = after_stable_swap(state, first, second, amount, result)
    # The invariant's Newton iteration stops within one unit, so a zero-fee swap
    # may leave D one unit lower; any loss beyond that is value paid out of the pool.
    assert after.invariant(after.normalized) >= state.invariant(state.normalized) - 1


@given(stable_swaps())
def test_stable_swap_cannot_be_reversed_at_a_profit(case):
    state, first, second, amount = case
    result = swap(state, first, second, amount)
    after = after_stable_swap(state, first, second, amount, result)
    if result.output:
        returned = swap(after, second, first, result.output).output
        # Normalized units: the invariant and output solver each stop within one unit.
        rate = state.rates[first]
        assert returned * rate // 10**18 <= amount * rate // 10**18 + 2


@given(stable_swaps(), st.data())
def test_stable_swap_output_grows_with_input(case, data):
    state, first, second, amount = case
    extra = data.draw(st.integers(1, state.balances[first]))
    larger = swap(state, first, second, amount + extra).output
    assert swap(state, first, second, amount).output <= larger


@given(stable_states(), st.integers(1, 10**6))
def test_proportional_addition_then_removal_returns_at_most_the_deposit(state, share):
    amounts = tuple(balance * share // 10**6 for balance in state.balances)
    result = addition(state, amounts)
    grown = replace(
        state,
        balances=tuple(
            b + a - f for b, a, f in zip(state.balances, amounts, result.admin_fees, strict=True)
        ),
        total_supply=state.total_supply + result.minted,
    )
    returned = removal_proportional(grown, result.minted)
    assert all(r <= a for r, a in zip(returned, amounts, strict=True))


@given(stable_states(), st.data())
def test_single_coin_removal_pays_less_than_the_reserve_and_grows_with_shares(state, data):
    index = data.draw(st.integers(0, len(state.balances) - 1))
    burns = st.integers(max(1, state.total_supply // 10**6), state.total_supply - 1)
    shares = sorted(data.draw(st.lists(burns, min_size=2, max_size=2)))
    smaller, larger = (removal_one(state, index, s) for s in shares)
    assert 0 <= smaller.admin_fee <= smaller.fee
    assert smaller.output <= larger.output < state.balances[index]


def packed_fees(draw):
    mid = draw(st.integers(10**5, 10**8))
    out = draw(st.integers(mid, 10**9))
    return mid << 128 | out << 64 | draw(st.integers(10**13, 10**18))


@st.composite
def two_states(draw):
    places = draw(st.lists(decimals, min_size=2, max_size=2))
    depth = draw(st.integers(10**20, 10**30))
    scale = draw(st.integers(10**12, 10**24))
    normalized = depth, draw(st.integers(depth // 2, depth * 2))
    precisions = tuple(10 ** (18 - d) for d in places)
    balances = normalized[0] // precisions[0], normalized[1] * 10**18 // scale // precisions[1]
    amplification = draw(st.integers(4000, 4 * 10**7))
    gamma = draw(st.integers(10**10, 2 * 10**16))
    state = twocrypto_quotes.QuoteState(
        balances, precisions, scale, amplification, gamma, 1, packed_fees(draw)
    )
    invariant = twocrypto_math.invariant(amplification, gamma, state.normalized(balances))
    return replace(state, invariant=invariant)


@st.composite
def three_states(draw):
    places = draw(st.lists(decimals, min_size=3, max_size=3))
    depth = draw(st.integers(10**20, 10**30))
    scales = tuple(draw(st.lists(st.integers(10**12, 10**24), min_size=2, max_size=2)))
    precisions = tuple(10 ** (18 - d) for d in places)
    normalized = depth, *(draw(st.integers(depth // 2, depth * 2)) for _ in scales)
    balances = (
        normalized[0] // precisions[0],
        *(
            v * 10**18 // s // p
            for v, s, p in zip(normalized[1:], scales, precisions[1:], strict=True)
        ),
    )
    amplification = draw(st.integers(2700, 27 * 10**7))
    gamma = draw(st.integers(10**10, 5 * 10**16))
    state = tricrypto_quotes.QuoteState(
        balances, precisions, scales, amplification, gamma, 1, packed_fees(draw)
    )
    invariant = tricrypto_math.invariant(amplification, gamma, state.normalized(balances))
    return replace(state, invariant=invariant)


CRYPTO = (
    (two_states, twocrypto_quotes, twocrypto_math),
    (three_states, tricrypto_quotes, tricrypto_math),
)


@st.composite
def crypto_swaps(draw):
    states, quotes, model = draw(st.sampled_from(CRYPTO))
    state = draw(states())
    first, second = pair(draw, len(state.balances))
    balance = state.balances[first]
    amount = draw(st.integers(max(1, balance // 10**8), max(1, balance // 5)))
    return quotes, model, state, first, second, amount


@given(crypto_swaps())
def test_crypto_swap_pays_less_than_the_reserve_and_grows_with_input(case):
    quotes, _, state, first, second, amount = case
    output, _, after = quotes.swap_details(state, first, second, amount)
    assert output < state.balances[second]
    assert after[first] == state.balances[first] + amount
    assert after[second] == state.balances[second] - output
    assert quotes.swap(state, first, second, amount + amount // 3 + 1) >= output


def value(state, index, amount):
    """An amount of one coin in the pool's normalized, price-scaled units."""
    amounts = [0] * len(state.balances)
    amounts[index] = amount
    return state.normalized(tuple(amounts))[index]


@given(crypto_swaps())
def test_crypto_swap_cannot_be_reversed_beyond_the_invariant_tolerance(case):
    quotes, model, state, first, second, amount = case
    output, ratio, after = quotes.swap_details(state, first, second, amount)
    if not output:
        return
    # The contract recomputes the invariant from the new balances, starting from the
    # output solver's price ratio, before the next quote.
    invariant = model.invariant(state.amplification, state.gamma, state.normalized(after), ratio)
    moved = replace(state, balances=after, invariant=invariant)
    returned = quotes.swap(moved, second, first, output)
    gain = value(state, first, returned) - value(state, first, amount)
    assert gain <= state.invariant // 10**11


@st.composite
def crypto_states(draw):
    states, _, model = draw(st.sampled_from(CRYPTO))
    return model, draw(states())


@given(crypto_states(), st.data())
def test_crypto_output_solver_recovers_each_balance_at_the_pool_invariant(case, data):
    model, state = case
    values = state.normalized(state.balances)
    index = data.draw(st.integers(0, len(values) - 1))
    solved, _ = model.solve_balance(
        state.amplification, state.gamma, values, state.invariant, index
    )
    assert abs(solved - values[index]) <= state.invariant // 10**10
