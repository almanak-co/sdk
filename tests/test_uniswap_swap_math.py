"""Integer edge cases supplement the independent deployed Quoter comparisons."""

from fractions import Fraction

import pytest

from almanak.connectors.uniswap_v4.economic_data import PoolSample, Tick, Word
from almanak.connectors.uniswap_v4.math import Q96, UINT256, divide, sqrt_at_tick
from almanak.connectors.uniswap_v4.swap_math import (
    amount_delta,
    exact_input,
    next_price,
    next_tick,
    swap_fee,
    swap_step,
    tick_at_price,
)
from tests.uniswap_economic_support import prepared


@pytest.fixture
def sample(tmp_path):
    manifest, _, model = prepared(tmp_path)
    try:
        target = manifest.config.target("trading")
        payload = model.book.at(target, manifest.settings.schedule.start).payload
        return payload.model_copy(
            update={
                "pool": payload.pool.model_copy(
                    update={
                        "sqrt_price_x96": Q96,
                        "tick": 0,
                        "liquidity": 1_000_000,
                        "lp_fee": 0,
                        "protocol_fee": 0,
                    }
                ),
                "lower": -1000,
                "upper": 1000,
                "words": (Word(index=-1, bitmap=0), Word(index=0, bitmap=0)),
                "ticks": (),
            }
        )
    finally:
        model.book.close()


def with_ticks(sample, values):
    words = {-1: 0, 0: 0}
    ticks = []
    for index, net in sorted(values):
        compressed = index // sample.pool.source.tick_spacing
        words[compressed >> 8] |= 1 << (compressed & 255)
        ticks.append(Tick(index=index, gross=abs(net), net=net))
    updated = sample.model_copy(
        update={
            "words": tuple(Word(index=k, bitmap=v) for k, v in words.items()),
            "ticks": tuple(ticks),
        }
    )
    return PoolSample.model_validate_json(updated.model_dump_json())


@pytest.mark.parametrize("direction", [True, False])
def test_constant_liquidity_matches_independent_constant_product_invariant(sample, direction):
    sample = PoolSample.model_validate_json(sample.model_dump_json())
    amount, liquidity = 1000, sample.pool.liquidity
    calculated = exact_input(sample, amount, direction)
    expected = Fraction(liquidity * amount, liquidity + amount)
    assert calculated.output == expected.numerator // expected.denominator
    assert not calculated.crossed_ticks


@pytest.mark.parametrize("direction", [True, False])
def test_zero_liquidity_gaps_are_traversed_using_prepared_ticks(sample, direction):
    values = [(-20, -1_000_000), (-10, 1_000_000), (10, -1_000_000), (20, 1_000_000)]
    sample = with_ticks(sample, values)
    result = exact_input(sample, 2000, direction)
    assert result.crossed_ticks == ((-10, -20) if direction else (10, 20))
    assert result.liquidity == 1_000_000 and result.output > 0


def test_bitmap_boundaries_and_negative_tick_compression():
    words = {-2: 0, -1: 1 << 255, 0: 1 << 1, 1: 0}
    assert next_tick(words, -1, 10, True) == (-10, True)
    assert next_tick(words, -11, 10, True) == (-2560, False)
    assert next_tick(words, -1, 10, False) == (10, True)
    assert next_tick(words, 10, 10, False) == (2550, False)
    with pytest.raises(ValueError, match="unprepared tick bitmap"):
        next_tick({}, 0, 10, True)


def test_directional_protocol_fee_composition_and_invalid_encodings():
    packed = 125 | (700 << 12)
    assert swap_fee(500, packed, True) == 625
    assert swap_fee(500, packed, False) == 1200
    assert swap_fee(3000, 1000 | (1000 << 12), True) == 3997
    assert swap_fee(1_000_000, packed, True) == 1_000_000
    for lp, protocol in [
        (-1, 0),
        (1_000_001, 0),
        (500, -1),
        (500, 2**24),
        (0, 1001),
        (0, 1001 << 12),
    ]:
        with pytest.raises(ValueError):
            swap_fee(lp, protocol, True)


def test_solidity_uint256_overflow_branch_and_exact_rounding():
    price, liquidity, amount = sqrt_at_tick(800000), 2**120, 2**126
    assert amount * price >= UINT256
    expected = divide(liquidity * Q96, liquidity * Q96 // price + amount, True)
    assert next_price(price, liquidity, amount, True) == expected
    assert next_price(Q96, 1_000_000, 0, True) == Q96
    assert next_price(Q96, 1_000_000, 1, False) == Q96 + Q96 // 1_000_000
    with pytest.raises(ValueError, match="uint160"):
        next_price(2**160 - 1, 1, 1, False)
    with pytest.raises(ValueError, match="positive"):
        next_price(Q96, 0, 1, True)


def test_full_fee_consumes_remaining_without_inventing_output():
    price, used, output, paid = swap_step(Q96, sqrt_at_tick(-10), 1_000_000, 1000, 1_000_000)
    assert (price, used, output, paid) == (Q96, 0, 0, 1000)
    assert amount_delta(Q96, Q96 * 2, 3, True, up=True) == 2
    assert amount_delta(Q96, Q96 * 2, 3, True, up=False) == 1


def test_initialized_bitmap_requires_matching_liquidity_and_explicit_coverage(sample):
    bad = sample.model_copy(update={"words": (Word(index=-1, bitmap=0), Word(index=0, bitmap=2))})
    with pytest.raises(ValueError, match="does not match"):
        PoolSample.model_validate_json(bad.model_dump_json())
    with pytest.raises(ValueError, match="unprepared initialized tick"):
        exact_input(bad, 1000, False)
    with pytest.raises(ValueError, match="missing or unordered"):
        PoolSample.model_validate_json(sample.model_copy(update={"words": ()}).model_dump_json())
    with pytest.raises(ValueError, match="outside prepared tick coverage"):
        exact_input(sample, 1_000_000, True)
    for amount in (0, -1, True, 2**127):
        with pytest.raises(ValueError, match="positive supported int128"):
            exact_input(sample, amount, True)


@pytest.mark.parametrize("tick", [-887272, -123451, -1, 0, 1, 543211, 887271])
def test_tick_inverse_respects_exact_boundaries(tick):
    assert tick_at_price(sqrt_at_tick(tick)) == tick
    assert tick_at_price(sqrt_at_tick(tick + 1) - 1) == tick
