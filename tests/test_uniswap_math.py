"""Independent integer invariants for principal and fee accounting."""

import pytest

from almanak.connectors.uniswap_v4.math import (
    Q96,
    Q128,
    UINT256,
    accrued,
    amounts,
    entry_split,
    liquidity_for,
    sqrt_at_tick,
)


@pytest.mark.parametrize(
    "tick,expected",
    [
        (-887272, 4295128739),
        (0, Q96),
        (887272, 1461446703485210103287273052203988822378723970342),
    ],
)
def test_tick_math_boundary_vectors(tick, expected):
    assert sqrt_at_tick(tick) == expected


@pytest.mark.parametrize("tick", [-887273, 887273, True])
def test_invalid_tick_is_refused(tick):
    with pytest.raises(ValueError):
        sqrt_at_tick(tick)


@pytest.mark.parametrize("tick", [-200, -100, 0, 100, 200])
def test_deposit_and_withdraw_conserve_inventory_with_integer_rounding(tick):
    lower, upper = sqrt_at_tick(-100), sqrt_at_tick(100)
    price = sqrt_at_tick(tick)
    liquidity = liquidity_for(1_000_000, 1_000_000, price, lower, upper)
    deposited = amounts(liquidity, price, lower, upper, up=True)
    withdrawn = amounts(liquidity, price, lower, upper)
    assert all(
        0 <= out <= paid <= 1_000_000 for paid, out in zip(deposited, withdrawn, strict=True)
    )
    assert all(paid - out <= 1 for paid, out in zip(deposited, withdrawn, strict=True))


def test_one_sided_positions_use_only_the_active_currency():
    assert amounts(100, Q96 // 2, Q96, Q96 * 2) == (50, 0)
    assert amounts(100, Q96 * 3, Q96, Q96 * 2) == (0, 100)
    assert liquidity_for(50, 0, Q96 // 2, Q96, Q96 * 2) == 100
    assert liquidity_for(0, 100, Q96 * 3, Q96, Q96 * 2) == 100


def test_growth_wraparound_and_collection_checkpoint():
    previous, current = UINT256 - Q128, Q128
    assert accrued(100, previous, current) == 200
    assert accrued(100, current, current) == 0
    assert accrued(100, current, current + Q128) == 100
    assert accrued(200, previous, current) == 400


def test_balancing_search_uses_quotes_and_handles_one_sided_ranges():
    assert entry_split(100, Q96, Q96 // 2, Q96 * 2, lambda amount: amount) == 50
    assert entry_split(100, Q96 // 4, Q96 // 2, Q96 * 2, lambda _: 0) == 0
    assert entry_split(100, Q96 * 3, Q96 // 2, Q96 * 2, lambda _: 0) == 100


@pytest.mark.parametrize(
    "arguments", [(-1, Q96, Q96, Q96 * 2), (1, 0, Q96, Q96 * 2), (1, Q96, Q96, Q96)]
)
def test_invalid_principal_inputs(arguments):
    with pytest.raises(ValueError):
        amounts(*arguments)


def test_invalid_liquidity_and_growth_inputs():
    with pytest.raises(ValueError):
        liquidity_for(-1, 1, Q96, Q96 // 2, Q96 * 2)
    with pytest.raises(ValueError):
        accrued(1, UINT256, 0)
    with pytest.raises(ValueError):
        entry_split(0, Q96, Q96 // 2, Q96 * 2, lambda _: 0)
