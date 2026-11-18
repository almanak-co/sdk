"""Contract comparisons include exponential boundaries and capped oracle decay."""

import pytest

from almanak.connectors.curve.twocrypto_prices import exponential, marginal_price, oracle
from tests.test_curve_crypto_revisions import captured

CAPTURE = captured("curve-two-price-comparison.json")


@pytest.mark.parametrize("case", CAPTURE["cases"])
def test_exact_deployed_math_and_pool_oracle(case):
    if case["kind"] == "exp":
        result = exponential(case["value"])
    elif case["kind"] == "price":
        values, invariant, parameters = case["arguments"]
        result = marginal_price(values, invariant, *parameters)
    else:
        result = oracle(**case["arguments"])
    assert result == case["expected"]


@pytest.mark.parametrize("value", [True, 1.0, -(2**255) - 1, 2**255, 135305999368893231589])
def test_exponential_rejects_noncontract_domain(value):
    with pytest.raises(ValueError, match="int256"):
        exponential(value)


@pytest.mark.parametrize("value", [10**17 - 1, 10**33 + 1])
def test_price_rejects_invariant_outside_the_verified_math_domain(value):
    with pytest.raises(ValueError, match="invariant"):
        marginal_price((10**18, 10**18), value, 4000, 10**10)


@pytest.mark.parametrize(
    "change,message", [({"duration": 0}, "positive"), ({"timestamp": 0}, "future")]
)
def test_oracle_requires_known_duration_and_nonfuture_inputs(change, message):
    inputs = dict(
        cached=10**18, last_price=10**18, scale=10**18, last_timestamp=1, timestamp=2, duration=1
    )
    with pytest.raises(ValueError, match=message):
        oracle(**dict(inputs, **change))
