"""Captured Curve contract views independently constrain the integer calculations."""

import gzip
import json
from dataclasses import replace
from pathlib import Path

import pytest

from almanak.connectors.curve.stable_math import (
    StableState,
    addition,
    dynamic_fee,
    invariant,
    removal_one,
    removal_proportional,
    swap,
)

EVIDENCE = (
    Path(__file__).parents[1]
    / "docs/internal/reports/evidence/2026-09-07-rollout/curve-initial-discovery"
)


def captured(name):
    return json.loads(gzip.decompress((EVIDENCE / name).read_bytes()))


@pytest.fixture
def state():
    row = captured("curve-stable-pools.json.gz")["pools"][0]
    return StableState(
        balances=tuple(row["balances"]),
        rates=tuple(row["stored_rates()"]),
        amplification=row["A_precise()"],
        fee=row["fee()"],
        offpeg_multiplier=row["offpeg_fee_multiplier()"],
        total_supply=row["totalSupply()"],
    )


@pytest.mark.parametrize("case", captured("curve-math-comparison.json.gz")["cases"])
def test_swap_arithmetic_matches_captured_pool_quotes_exactly(state, case):
    result = swap(state, case["input_index"], case["output_index"], case["input_base_units"])
    assert result.output == case["contract_output"]
    assert 0 <= result.admin_fee <= result.fee


@pytest.mark.parametrize("case", captured("curve-lp-comparison.json.gz")["cases"])
def test_liquidity_arithmetic_matches_captured_pool_views_exactly(state, case):
    if case["kind"] == "add":
        result = addition(state, tuple(case["amounts"]))
        assert result.minted == case["contract_output"]
        assert len(result.fees) == len(state.balances)
    else:
        result = removal_one(state, case["output_index"], case["burn"])
        assert result.output == case["contract_output"]
        assert 0 <= result.admin_fee <= result.fee


def test_proportional_burn_conserves_each_coin_and_full_burn_exhausts_supply(state):
    burn = state.total_supply // 3
    outputs = removal_proportional(state, burn)
    for balance, output in zip(state.balances, outputs, strict=True):
        assert output * state.total_supply <= balance * burn < (output + 1) * state.total_supply
    assert removal_proportional(state, state.total_supply) == state.balances


def test_three_coin_initial_liquidity_preserves_order_without_pair_fields():
    state = StableState((0, 0, 0), (10**18, 10**30, 10**30), 10000, 1000000, 10**10, 0)
    result = addition(state, (10**18, 10**6, 10**6))
    assert result.minted == 3 * 10**18 and result.fees == result.admin_fees == (0, 0, 0)
    with pytest.raises(ValueError, match="every coin"):
        addition(state, (10**18, 0, 10**6))
    assert invariant((0, 0, 0), state.amplification) == 0
    assert dynamic_fee(state, 1, 1, state.fee) == state.fee


@pytest.mark.parametrize("change", [{"rates": (0, 1)}, {"amplification": 0}, {"fee": True}])
def test_invalid_financial_inputs_are_not_replaced_with_usable_values(state, change):
    with pytest.raises(ValueError):
        replace(state, **change)


def test_missing_liquidity_invalid_burns_and_overflow_refuse_calculation(state):
    with pytest.raises(ValueError, match="funded reserves"):
        swap(replace(state, balances=(0, 1)), 0, 1, 10**18)
    with pytest.raises(ValueError, match="different coins"):
        swap(state, 0, 0, 10**18)
    with pytest.raises(ValueError, match="coin index"):
        swap(state, 0, 2, 10**18)
    with pytest.raises(ValueError, match="uint256"):
        swap(state, 0, 1, 2**256 - 1)
    with pytest.raises(ValueError, match="entire LP supply"):
        removal_one(state, 0, state.total_supply)
    with pytest.raises(ValueError, match="pool supply"):
        removal_proportional(state, state.total_supply + 1)
    with pytest.raises(ValueError, match="positive inventory"):
        addition(state, (0, 0))
