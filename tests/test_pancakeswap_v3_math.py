"""Independent Pancake Solidity outputs constrain shared math and connector-specific steps."""

import gzip
import json
from pathlib import Path

import pytest

from almanak.connectors import concentrated_math as math
from almanak.connectors.pancakeswap_v3.swap_math import (
    protocol_fee_amount,
    protocol_fee_bps,
    swap_step,
)

FIXTURE = Path(__file__).with_name("fixtures") / "pancakeswap-v3-math-vectors.json.gz"
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
def test_integer_outputs_match_pancake_core_and_periphery(method):
    rows = [r for r in CAPTURE["records"] if r["method"] == method]
    assert rows
    for row in rows:
        assert evaluate(row) == tuple(row["result"]), row


@pytest.mark.parametrize("direction,expected", [(True, 3200), (False, 3300)])
def test_protocol_fees_are_directional_basis_point_fractions(direction, expected):
    encoded = 3200 | (3300 << 16)
    assert protocol_fee_bps(encoded, direction) == expected
    assert protocol_fee_amount(101, encoded, direction) == 101 * expected // 10000
    assert protocol_fee_amount(101, 0, direction) == 0
    for invalid in (-1, 2**32, 999, 4001 << 16, 4 | (10 << 4)):
        with pytest.raises(ValueError, match="protocol fee"):
            protocol_fee_bps(invalid, direction)


def test_protocol_fee_multiplication_preserves_checked_core_overflow():
    with pytest.raises(ValueError, match="overflows"):
        protocol_fee_amount(2**256 - 1, 4000, True)
    for invalid in (-1, 2**256, True):
        with pytest.raises(ValueError, match="uint256"):
            protocol_fee_amount(invalid, 4000, True)
