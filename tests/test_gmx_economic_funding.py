"""Funding rate transitions preserve paying-side rounding and zero-side behavior."""

import json
from pathlib import Path

import pytest

from almanak.connectors.gmx_v2.economic_funding import (
    CollateralValues,
    FundingBounds,
    FundingParameters,
    FundingState,
    accrue_funding,
    amount_per_size,
    next_rate,
)
from almanak.connectors.gmx_v2.economic_precision import FLOAT
from tests.test_gmx_economic_valuation import valuation as valuation

# Captured against the current closure's Reader at the publication block of the
# 2026-10-20 recaptured range dataset (funding-capture.py.txt in that directory).
EVIDENCE = Path("docs/internal/reports/evidence/2026-09-09-gmx-finish-2026-10-20")


@pytest.mark.parametrize("chain", ["arbitrum", "avalanche"])
def test_funding_matches_canonical_reader(chain):
    path = EVIDENCE / f"{chain}-funding/result.json"
    captured = json.loads(path.read_text())
    state = FundingState.model_validate_json(json.dumps(captured["state"]))
    result = accrue_funding(state)
    assert result.model_dump(mode="json") == captured["calculation"]
    actual = captured["contract_funding"]
    assert actual[:3] == [
        result.rate.longs_pay_shorts,
        result.rate.per_second,
        result.rate.next_saved_per_second,
    ]
    assert actual[3:] == [
        [
            [result.long_payments.long, result.long_payments.short],
            [result.short_payments.long, result.short_payments.short],
        ],
        [
            [result.long_claims.long, result.long_claims.short],
            [result.short_claims.long, result.short_claims.short],
        ],
    ]
    assert captured["provider_attempts"] == captured["replacement_attempts"]


@pytest.fixture
def funding(valuation):
    return FundingState(
        market=valuation.spot.market,
        block=valuation.spot.block,
        long_price=valuation.spot.long.price,
        short_price=valuation.spot.short.price,
        parameters=FundingParameters(
            factor=10**24,
            exponent=FLOAT,
            increase_per_second=0,
            minimum_increase_per_second=0,
            decrease_per_second=10,
            longs_pay=FundingBounds(minimum_per_second=0, maximum_per_second=10**25),
            shorts_pay=FundingBounds(minimum_per_second=0, maximum_per_second=10**25),
            stable_threshold=FLOAT // 4,
            decrease_threshold=FLOAT // 10,
        ),
        long_interest=CollateralValues(long=100 * FLOAT, short=100 * FLOAT),
        short_interest=CollateralValues(long=50 * FLOAT, short=50 * FLOAT),
        long_balance_usd_e30=200 * FLOAT,
        short_balance_usd_e30=100 * FLOAT,
        updated_at=valuation.spot.block.timestamp - 10,
        saved_per_second=0,
    )


def test_static_funding_uses_skew_and_only_paying_side_collateral(funding):
    result = accrue_funding(funding)
    assert result.rate.per_second == (FLOAT // 3) * 10**24 // FLOAT
    assert result.rate.longs_pay_shorts
    assert result.long_payments.total > 0 and result.short_claims.total > 0
    assert result.short_payments.total == result.long_claims.total == 0
    assert result.rate.next_saved_per_second == 0


@pytest.mark.parametrize(
    "saved,long,short,expected",
    [
        (100, 200, 100, 3430),
        (-100, 200, 100, 3230),
        (100, 100, 200, -3230),
        (-100, 100, 200, -3430),
    ],
)
def test_adaptive_funding_can_change_payment_direction(funding, saved, long, short, expected):
    state = funding.model_copy(
        update={
            "parameters": funding.parameters.model_copy(update={"increase_per_second": 1000}),
            "saved_per_second": saved,
            "long_balance_usd_e30": long * FLOAT,
            "short_balance_usd_e30": short * FLOAT,
        }
    )
    result = next_rate(state)
    assert result.next_saved_per_second == expected
    assert result.longs_pay_shorts == (expected > 0)


@pytest.mark.parametrize("saved,expected", [(1000, 900), (10, 1), (-1000, -900), (-10, -1)])
def test_decreasing_funding_preserves_sign_and_saved_rate_below_floor(funding, saved, expected):
    state = funding.model_copy(
        update={
            "parameters": funding.parameters.model_copy(
                update={
                    "increase_per_second": 1000,
                    "longs_pay": FundingBounds(minimum_per_second=1000, maximum_per_second=10**25),
                    "shorts_pay": FundingBounds(minimum_per_second=1000, maximum_per_second=10**25),
                }
            ),
            "saved_per_second": saved,
            "long_balance_usd_e30": (101 if saved > 0 else 100) * FLOAT,
            "short_balance_usd_e30": (100 if saved > 0 else 101) * FLOAT,
        }
    )
    result = next_rate(state)
    assert result.next_saved_per_second == expected and result.per_second == 1000


def test_empty_receiving_side_does_not_accrue_or_update_saved_rate(funding):
    state = funding.model_copy(update={"short_interest": CollateralValues(long=0, short=0)})
    result = accrue_funding(state)
    assert result.rate.per_second == result.rate.next_saved_per_second == 0
    assert not result.rate.longs_pay_shorts
    assert result.long_payments.total == result.short_claims.total == 0


def test_funding_payment_rounds_twice_and_claims_round_down():
    assert amount_per_size(1, 3 * FLOAT * 10**15, 2, payment=True) == 1
    assert amount_per_size(1, 3 * FLOAT * 10**15, 2, payment=False) == 0
    assert amount_per_size(0, 3, 2, payment=True) == 0
    assert amount_per_size(1, 0, 2, payment=True) == 0
