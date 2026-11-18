"""SparkLend arithmetic compared with compiled verified revision-4 library sources."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.connectors.aave_v3 import math as aave
from almanak.connectors.sparklend import math

VECTORS = json.loads(
    (Path(__file__).parent / "fixtures/sparklend-revision4-math-vectors.json").read_text()
)


@pytest.mark.parametrize("vector", VECTORS["rounding"])
def test_token_rounding_matches_compiled_verified_sources(vector):
    functions = (
        math.supply_shares,
        math.withdraw_shares,
        math.supplied_assets,
        math.borrow_shares,
        math.repay_shares,
        math.debt_assets,
    )
    assert [f(vector["value"], vector["index"]) for f in functions] == vector["result"]


@pytest.mark.parametrize("vector", VECTORS["interest"])
def test_elapsed_interest_matches_compiled_verified_sources(vector):
    assert [
        math.linear_interest(vector["rate"], vector["elapsed"]),
        math.compounded_interest(vector["rate"], vector["elapsed"]),
    ] == vector["result"]


@pytest.mark.parametrize("amount,index", [(-1, math.RAY), (1, 0), (math.UINT256_MAX, 1)])
def test_scaled_arithmetic_refuses_unsigned_overflow_and_zero_divisors(amount, index):
    with pytest.raises(ValueError):
        math.supply_shares(amount, index)


@pytest.mark.parametrize("rate,elapsed", [(2**128, 1), (1, -1), (1, 2**40)])
def test_interest_requires_protocol_rate_and_time_bounds(rate, elapsed):
    with pytest.raises(ValueError):
        math.compounded_interest(rate, elapsed)


def test_revision_differences_remain_explicit():
    assert math.supply_shares(1, math.RAY + 1) == 1
    assert aave.supply_shares(1, math.RAY + 1) == 0
    rate, elapsed = math.RAY // 10, math.SECONDS_PER_YEAR
    assert math.compounded_interest(rate, elapsed) != aave.compounded_interest(rate, elapsed)


def test_treasury_accrual_requires_zero_stable_debt_and_retains_empty_borrow_index():
    reserve = SimpleNamespace(
        liquidity_index_ray=math.RAY,
        variable_borrow_index_ray=math.RAY,
        liquidity_rate_ray=0,
        variable_borrow_rate_ray=math.RAY * math.SECONDS_PER_YEAR // 100,
        last_update_timestamp=100,
        total_scaled_debt=0,
        total_stable_debt=0,
        accrued_to_treasury_scaled=7,
        configuration=SimpleNamespace(reserve_factor_bps=500),
    )
    assert math.normalized_indices(reserve, 101)[1] > math.RAY
    assert math.transition_indices(reserve, 101) == (math.RAY, math.RAY)
    assert math.treasury_shares(reserve, 101) == 7
    reserve.total_scaled_debt = 1000
    assert math.transition_indices(reserve, 101) == math.normalized_indices(reserve, 101)
    assert math.treasury_shares(reserve, 101) == 8
    reserve.total_stable_debt = 1
    with pytest.raises(ValueError, match="stable-rate debt"):
        math.treasury_shares(reserve, 101)
