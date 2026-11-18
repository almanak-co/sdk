"""Offline integer power equality against the exact deployed compiler-source harness."""

import json
from pathlib import Path

import pytest

from almanak.connectors.gmx_v2.economic_precision import (
    FLOAT,
    MAX_UINT,
    SCALE,
    apply_exponent,
    ceil_div,
    exponential2,
    logarithm2,
    multiply_fixed,
    uint,
)

EVIDENCE = Path(__file__).parents[1] / (
    "docs/internal/reports/evidence/2026-09-09-gmx-prepared/precision-contract-comparison.json"
)
SAMPLES = json.loads(EVIDENCE.read_text())["samples"]


@pytest.mark.parametrize("sample", SAMPLES)
def test_fixed_point_power_matches_actual_contract_result(sample):
    assert apply_exponent(sample["value"], sample["exponent"]) == sample["contract_result"]


@pytest.mark.parametrize("value", [-1, MAX_UINT + 1])
def test_unsigned_calculation_bounds(value):
    with pytest.raises(ValueError, match="uint256"):
        uint(value)


def test_prb_domain_and_half_up_rounding_are_preserved():
    with pytest.raises(ValueError, match="at least one"):
        logarithm2(SCALE - 1)
    with pytest.raises(ValueError, match="supported range"):
        exponential2(192 * SCALE)
    with pytest.raises(ValueError, match="overflows"):
        multiply_fixed(MAX_UINT, MAX_UINT)
    assert multiply_fixed(1, SCALE // 2 - 1) == 0
    assert multiply_fixed(1, SCALE // 2) == 1
    assert apply_exponent(FLOAT - 1, 0) == 0


def test_unsigned_ceiling_division_keeps_exact_and_nonexact_amounts():
    assert ceil_div(0, 7) == 0
    assert ceil_div(14, 7) == 2
    assert ceil_div(15, 7) == 3
    with pytest.raises(ValueError, match="positive divisor"):
        ceil_div(1, 0)
