"""Pinned three-coin contract results, including its Newton fallback branch."""

import gzip
import json

import pytest

from almanak.connectors.curve import tricrypto_math as model
from almanak.connectors.curve.tricrypto_quotes import QuoteState, swap
from tests.retained_fixtures import retained

EVIDENCE = retained("2026-09-07-rollout/curve-tricrypto-math")
CAPTURE = json.loads(
    gzip.decompress((EVIDENCE / "curve-three-math-comparison.json.gz").read_bytes())
)


@pytest.fixture
def state():
    pool = CAPTURE["pool"]
    return QuoteState(
        tuple(pool["balances"]),
        tuple(pool["precisions()"]),
        tuple(pool["price_scale"]),
        pool["A()"],
        pool["gamma()"],
        pool["D()"],
        pool["packed_fee_params()"],
    )


def calculated(state, case):
    arguments = case["arguments"]
    if case["kind"] == "swap":
        return swap(state, *arguments)
    if case["kind"] == "prices":
        values, invariant, parameters = arguments
        return model.marginal_prices(values, invariant, *parameters)
    functions = {
        "invariant": model.invariant,
        "get_y": model.solve_balance,
        "cube_root": model.cube_root,
        "geometric_mean": model.geometric_mean,
        "reduction": model.reduction_coefficient,
    }
    return functions[case["kind"]](*arguments)


@pytest.mark.parametrize("case", CAPTURE["cases"])
def test_exact_pinned_contract_results(state, case):
    result = calculated(state, case)
    assert (list(result) if isinstance(result, tuple) else result) == case["expected"]


def test_fallback_cases_retain_contract_newton_results():
    cases = [
        case for case in CAPTURE["cases"] if case["kind"] == "get_y" and case["expected"][1] == 0
    ]
    assert len(cases) == 9
    for case in cases:
        assert model.newton_balance(*case["arguments"]) == case["expected"][0]


@pytest.mark.parametrize(
    "ann,gamma,values",
    [
        (2699, 10**14, (10**18,) * 3),
        (270000001, 10**14, (10**18,) * 3),
        (2700, 0, (10**18,) * 3),
        (2700, 5 * 10**16 + 1, (10**18,) * 3),
        (2700, 10**14, (10**18,) * 2),
        (2700, 10**14, (0, 10**18, 10**18)),
        (True, 10**14, (10**18,) * 3),
        (2700, 10**14, (2**256 - 1, 10**18, 10**18)),
    ],
)
def test_invalid_invariant_domain_is_refused(ann, gamma, values):
    with pytest.raises(ValueError):
        model.invariant(ann, gamma, values)


@pytest.mark.parametrize(
    "index,quantity", [(0, 10**17 - 1), (0, 10**33 + 1), (3, 10**18), (True, 10**18)]
)
def test_invalid_output_balance_domain_is_refused(index, quantity):
    with pytest.raises(ValueError):
        model.solve_balance(2700, 10**14, (10**18,) * 3, quantity, index)


@pytest.mark.parametrize(
    "input_index,output_index,amount", [(0, 0, 1), (0, 3, 1), (True, 2, 1), (0, 1, 0)]
)
def test_invalid_swap_is_refused(state, input_index, output_index, amount):
    with pytest.raises(ValueError):
        swap(state, input_index, output_index, amount)


def test_fee_and_ratio_domain_is_refused():
    with pytest.raises(ValueError):
        model.dynamic_fee((10**18,) * 3, 0)
    with pytest.raises(ValueError):
        model.balance_ratio(1, 10**33)
    with pytest.raises(ValueError):
        model.geometric_mean((1, 2))
