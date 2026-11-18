"""Exact vectors from compiled upstream libraries, separate from pool lifecycle proofs."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.connectors.aave_v3 import math

VECTORS = json.loads(
    (Path(__file__).parent / "fixtures/aave-v3-revision11-math-vectors.json").read_text()
)


@pytest.mark.parametrize("vector", VECTORS["rounding"])
def test_scaled_balances_and_transfers_match_compiled_upstream_rounding(vector):
    functions = (
        math.supply_shares,
        math.withdraw_shares,
        math.supplied_assets,
        math.borrow_shares,
        math.repay_shares,
        math.debt_assets,
    )
    actual = [function(vector["value"], vector["index"]) for function in functions]
    assert actual == vector["result"]


@pytest.mark.parametrize("vector", VECTORS["interest"])
def test_elapsed_interest_matches_compiled_revision_11_libraries(vector):
    assert [
        math.linear_interest(vector["rate"], vector["elapsed"]),
        math.compounded_interest(vector["rate"], vector["elapsed"]),
    ] == vector["result"]


@pytest.mark.parametrize("amount,index", [(-1, math.RAY), (1, 0), (2**256 - 1, math.RAY)])
def test_scaled_arithmetic_refuses_unsigned_overflow_and_zero_divisors(amount, index):
    with pytest.raises(ValueError):
        math.supply_shares(amount, index)


@pytest.mark.parametrize("rate,elapsed", [(2**128, 1), (1, -1), (1, 2**40)])
def test_interest_refuses_rates_and_times_outside_protocol_representation(rate, elapsed):
    with pytest.raises(ValueError):
        math.compounded_interest(rate, elapsed)


def test_round_half_up_checks_the_added_rounding_term_for_overflow():
    with pytest.raises(ValueError):
        math.ray_mul(math.UINT256_MAX, 1)


def test_first_borrow_uses_stored_index_even_when_normalized_view_has_accrued():
    reserve = SimpleNamespace(
        liquidity_index_ray=math.RAY,
        variable_borrow_index_ray=math.RAY,
        liquidity_rate_ray=0,
        variable_borrow_rate_ray=math.RAY * math.SECONDS_PER_YEAR // 100,
        last_update_timestamp=100,
        total_scaled_debt=0,
        accrued_to_treasury_scaled=7,
        configuration=SimpleNamespace(reserve_factor_bps=500),
    )
    assert math.normalized_indices(reserve, 101)[1] > math.RAY
    assert math.transition_indices(reserve, 101) == (math.RAY, math.RAY)
    assert math.treasury_shares(reserve, 101) == 7
    reserve.total_scaled_debt = 1000
    assert math.transition_indices(reserve, 101) == math.normalized_indices(reserve, 101)
    # Ten debt units accrue after flooring; the 5% reserve factor rounds half up.
    assert math.treasury_shares(reserve, 101) == 8
    reserve.configuration.reserve_factor_bps = 0
    assert math.treasury_shares(reserve, 101) == 7
