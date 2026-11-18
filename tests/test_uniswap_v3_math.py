"""Independent V3 Solidity outputs constrain shared math and connector-specific steps."""

import gzip
import json
from pathlib import Path

import pytest

from almanak.connectors import concentrated_math as math
from almanak.connectors.uniswap_v3.swap_math import protocol_fee_divisor, swap_step
from almanak.connectors.uniswap_v4.swap_math import swap_step as v4_step

FIXTURE = Path(__file__).with_name("fixtures") / "uniswap-v3-math-vectors.json.gz"
CAPTURE = json.loads(gzip.decompress(FIXTURE.read_bytes()))


def evaluate(row):
    args = row["args"]
    method = row["method"]
    if method == "amounts":
        return math.amounts(*args[:4], up=args[4])
    if method == "nextTick":
        word, bitmap, tick, spacing, direction = args
        return math.next_tick({word: bitmap}, tick, spacing, direction)
    function = {
        "sqrtAtTick": math.sqrt_at_tick,
        "tickAtPrice": math.tick_at_price,
        "liquidity": math.liquidity_for,
        "nextPrice": math.next_price,
        "swapStep": swap_step,
    }[method]
    value = function(*args)
    return value if isinstance(value, tuple) else (value,)


@pytest.mark.parametrize("method", sorted({r["method"] for r in CAPTURE["records"]}))
def test_integer_outputs_match_v3_core_and_periphery(method):
    rows = [r for r in CAPTURE["records"] if r["method"] == method]
    assert rows
    for row in rows:
        assert evaluate(row) == tuple(row["result"]), row


def test_partial_steps_preserve_v3_input_and_fee_rounding():
    differences = CAPTURE["v3_v4_step_differences"]
    assert len(differences) == 30
    for row in differences:
        assert swap_step(*row["args"]) == tuple(row["v3"])
        assert v4_step(*row["args"]) == tuple(row["v4"])
        assert row["v3"][0] == row["v4"][0] and row["v3"][2] == row["v4"][2]
        assert sum(row["v3"][i] for i in (1, 3)) == sum(row["v4"][i] for i in (1, 3))
    zero_fee = next(r for r in differences if r["args"][-1] == 0)
    assert zero_fee["v3"][3] > 0 and zero_fee["v4"][3] == 0


@pytest.mark.parametrize("fee", [-1, 1_000_000])
def test_step_refuses_invalid_v3_pool_fee(fee):
    with pytest.raises(ValueError, match="valid fee"):
        swap_step(math.Q96, math.sqrt_at_tick(-1), 1000, 1, fee)


@pytest.mark.parametrize("direction,expected", [(True, 4), (False, 10)])
def test_v3_protocol_fee_is_a_directional_divisor(direction, expected):
    assert protocol_fee_divisor(4 | (10 << 4), direction) == expected
    assert protocol_fee_divisor(0, direction) == 0
    for invalid in (-1, 256, 1, 3 << 4, 11):
        with pytest.raises(ValueError, match="protocol fee"):
            protocol_fee_divisor(invalid, direction)
