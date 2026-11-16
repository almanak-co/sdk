"""Comet index and principal arithmetic compared with retained mined transitions."""

import gzip
import json
from pathlib import Path

import pytest

from almanak.connectors.compound_v3 import math

ROOT = Path(__file__).parents[1]
EVIDENCE = ROOT / "docs/internal/reports/evidence/2026-09-07-rollout/compound-v3-discovery"


def captured(network, name):
    return json.loads(gzip.decompress((EVIDENCE / f"{network}{name}.json.gz").read_bytes()))


def rate(values, side, utilization):
    return math.kink_rate(
        utilization,
        values[side + "Kink"],
        values[side + "PerSecondInterestRateBase"],
        values[side + "PerSecondInterestRateSlopeLow"],
        values[side + "PerSecondInterestRateSlopeHigh"],
    )


@pytest.mark.parametrize("network", ["mainnet", "base"])
def test_indices_and_signed_principal_match_mined_roundtrip(network):
    values = captured(network, "")["values"]
    operations = captured(network, "-operations")
    for step in operations["steps"]:
        before, after = step["before"], step["after"]
        original, actual = before["totals"], after["totals"]
        name, kind = step["account"], step["kind"]
        old = before["accounts"][name]["user"][0]
        indices = tuple(original[:2])
        accrues = (
            kind
            in {
                "base_supply",
                "borrow",
                "partial_repay",
                "partial_base_withdraw",
                "full_repay",
                "full_base_withdraw",
            }
            or network == "base"
            and kind
            in {
                "collateral_supply",
                "partial_collateral_release",
                "full_collateral_release",
            }
        )
        if accrues:
            utilization = math.utilization(original[4], original[5], indices)
            elapsed = after["block"]["timestamp"] - original[6]
            indices = tuple(
                math.accrue_index(index, rate(values, side, utilization), elapsed)
                for index, side in zip(indices, ("supply", "borrow"), strict=True)
            )
        assert actual[:2] == list(indices)
        cash = after["accounts"][name]["cash"] - before["accounts"][name]["cash"]
        expected = (
            math.principal_value(math.present_value(old, *indices) - cash, *indices)
            if cash
            else old
        )
        assert after["accounts"][name]["user"][0] == expected
        repaid, supplied = math.repay_and_supply(old, expected)
        withdrawn, borrowed = math.withdraw_and_borrow(old, expected)
        assert actual[4] == original[4] + supplied - withdrawn
        assert actual[5] == original[5] + borrowed - repaid


def test_principal_sign_and_rounding_do_not_reuse_aave_debt_rounding():
    index = 15 * 10**14
    assert math.present_value(1, index, index) == 1
    assert math.present_value(-1, index, index) == -1
    assert math.principal_value(2, index, index) == 1
    assert math.principal_value(-2, index, index) == -2
    assert math.repay_and_supply(-7, 3) == (7, 3)
    assert math.withdraw_and_borrow(7, -3) == (7, 3)
    assert math.repay_and_supply(7, 3) == (0, 0)
    assert math.withdraw_and_borrow(-7, -3) == (0, 0)
    assert math.utilization(0, 0, (index, index)) == 0
    assert math.signed_price(-3, 5, 2) == -7
    assert math.signed_price(3, 5, 2) == 7


@pytest.mark.parametrize(
    "operation",
    [
        lambda: math.uint(True),
        lambda: math.uint(-1),
        lambda: math.signed(2**103, 104),
        lambda: math.mul_div(2**255, 2, 1),
        lambda: math.mul_div(1, 1, 0),
        lambda: math.present_value(-(2**103), math.INDEX_SCALE, math.INDEX_SCALE),
        lambda: math.principal_value(-1, math.INDEX_SCALE, 0),
        lambda: math.principal_value(2**255, math.INDEX_SCALE, math.INDEX_SCALE),
        lambda: math.accrue_index(2**64 - 1, math.FACTOR_SCALE, 1),
        lambda: math.signed_price(2**254, 2, 1),
        lambda: math.signed_price(1, 1, 0),
    ],
)
def test_checked_arithmetic_refuses_solidity_overflow_and_invalid_units(operation):
    with pytest.raises(ValueError):
        operation()
