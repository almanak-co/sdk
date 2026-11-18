"""Pinned deployed v2.1.0 contracts constrain every expected integer result."""

import gzip
import json

import pytest

from almanak.connectors.curve.crypto_integer import cube_root, divide
from almanak.connectors.curve.twocrypto_math import (
    dynamic_fee,
    invariant,
    newton_balance,
    solve_balance,
)
from almanak.connectors.curve.twocrypto_quotes import QuoteState, swap
from tests.retained_fixtures import retained

EVIDENCE = retained("2026-09-07-rollout/curve-crypto-discovery")
CAPTURE = json.loads(gzip.decompress((EVIDENCE / "curve-two-math-comparison.json.gz").read_bytes()))
FALLBACK = json.loads(
    gzip.decompress((EVIDENCE / "curve-two-fallback-comparison.json.gz").read_bytes())
)


@pytest.fixture
def state():
    pool = CAPTURE["pool"]
    return QuoteState(
        tuple(pool["balances"]),
        tuple(pool["precisions()"]),
        pool["price_scale"][0],
        pool["A()"],
        pool["gamma()"],
        pool["D()"],
        pool["packed_fee_params()"],
    )


@pytest.mark.parametrize("case", CAPTURE["cases"])
def test_exact_pinned_contract_results(state, case):
    if case["kind"] == "swap":
        result = swap(state, case["i"], case["j"], case["dx"])
    elif case["kind"] == "invariant":
        result = invariant(case["ann"], case["gamma"], case["x"])
    else:
        result = list(solve_balance(case["ann"], case["gamma"], case["x"], case["D"], case["i"]))
    assert result == case["expected"]


@pytest.mark.parametrize("case", FALLBACK["cases"])
def test_nonpositive_cubic_discriminant_uses_the_contract_newton_result(case):
    if case["method"] == "newton_y":
        assert newton_balance(*case["arguments"]) == case["expected"]
    else:
        assert list(solve_balance(*case["arguments"])) == case["expected"]
        assert case["expected"][1] == 0


@pytest.mark.parametrize("numerator,denominator,expected", [(-7, 3, -2), (7, -3, -2), (-7, -3, 2)])
def test_signed_division_preserves_evm_truncation(numerator, denominator, expected):
    assert divide(numerator, denominator) == expected


def test_non_evm_numbers_and_zero_divisor_are_rejected():
    for numerator, denominator in ((2**255, 1), (1, 0), (True, 1), (-(2**255), -1)):
        with pytest.raises(ValueError):
            divide(numerator, denominator)
    assert cube_root(0) == 0
    assert cube_root(1) == 10**12


@pytest.mark.parametrize(
    "input_index,output_index,amount",
    [(0, 0, 1), (0, 2, 1), (True, 0, 1), (0, 1, 0), (0, 1, 2**256)],
)
def test_invalid_swap_inputs_cannot_produce_quotes(state, input_index, output_index, amount):
    with pytest.raises(ValueError):
        swap(state, input_index, output_index, amount)


@pytest.mark.parametrize(
    "ann,gamma,values",
    [
        (3999, 10**14, (10**18, 10**18)),
        (4000, 0, (10**18, 10**18)),
        (4000, 10**14, (0, 10**18)),
        (4000, 10**14, (10**18,)),
        (4000, 10**14, (10**34, 10**18)),
    ],
)
def test_unsafe_invariant_inputs_refuse_calculation(ann, gamma, values):
    with pytest.raises(ValueError):
        invariant(ann, gamma, values)


def test_dynamic_fee_is_symmetric_and_increases_with_imbalance(state):
    balanced = dynamic_fee((10**18, 10**18), state.packed_fees)
    imbalanced = dynamic_fee((10**18, 10**20), state.packed_fees)
    assert balanced < imbalanced
    assert imbalanced == dynamic_fee((10**20, 10**18), state.packed_fees)
    with pytest.raises(ValueError):
        dynamic_fee((10**18, 10**18), 0)
