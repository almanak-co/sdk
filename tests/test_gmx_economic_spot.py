"""Prepared integer swap outputs match actual pinned Reader calls with explicit price inputs."""

import json
from pathlib import Path

import pytest

from almanak.connectors.gmx_v2.economic_precision import FLOAT
from almanak.connectors.gmx_v2.economic_spot_math import (
    fees_for,
    imbalance_impact,
    impact_amount,
    quote_swap,
    spot_impact,
)
from almanak.connectors.gmx_v2.economic_spot_types import (
    FeeParameters,
    ImpactParameters,
    SpotState,
    SwapCalculation,
)

ROOT = Path(__file__).parents[1] / "docs/internal/reports/evidence/2026-09-09-gmx-prepared"


@pytest.fixture(params=["arbitrum", "avalanche"])
def proof(request):
    return json.loads((ROOT / (request.param + "-spot-reader-comparison.json")).read_text())


@pytest.mark.parametrize("index", range(8))
def test_swap_calculation_matches_entire_actual_reader_result(proof, index):
    state = SpotState.model_validate_json(json.dumps(proof["state"]))
    row = proof["cases"][index]
    retained = SwapCalculation.model_validate_json(json.dumps(row["calculation"]))
    result = quote_swap(state, retained.asset_in, retained.amount_in)
    assert result == retained
    assert (result.amount_out, result.impact_amount) == tuple(row["contract_result"][:2])
    assert (result.fees.receiver_amount, result.fees.pool_amount, result.fees.after_fees) == tuple(
        row["contract_result"][2][:3]
    )


def test_balance_crossover_cannot_turn_asymmetric_impact_into_an_arbitrage():
    parameters = ImpactParameters(
        positive_factor=10**24, negative_factor=2 * 10**24, exponent=FLOAT
    )
    before = (2000 * FLOAT, 1000 * FLOAT)
    after = (2000 * FLOAT, 2999 * FLOAT)
    result = imbalance_impact(before, after, parameters)
    assert result.balance_improved
    assert result.usd_e30 == -998 * 10**24
    with pytest.raises(ValueError, match="available pool value"):
        imbalance_impact(before, (-1, 1), parameters)


def test_positive_factor_is_capped_by_negative_factor():
    parameters = ImpactParameters(
        positive_factor=3 * 10**24, negative_factor=10**24, exponent=FLOAT
    )
    result = imbalance_impact(
        (2000 * FLOAT, 1000 * FLOAT), (1500 * FLOAT, 1000 * FLOAT), parameters
    )
    assert result.usd_e30 == 500 * 10**24


def test_positive_swap_impact_cap_and_negative_ceiling_have_distinct_rounding(proof):
    state = SpotState.model_validate_json(json.dumps(proof["state"]))
    currency = state.long.model_copy(update={"impact_pool_amount": 2})
    impact, remaining = impact_amount(currency, 5 * currency.price.maximum + 1)
    assert impact == 2 and remaining == 3 * currency.price.maximum
    impact, remaining = impact_amount(currency, -currency.price.minimum - 1)
    assert impact == -2 and remaining == 0


def test_fee_exhaustion_and_invalid_input_are_refused(proof):
    state = SpotState.model_validate_json(json.dumps(proof["state"]))
    fees = FeeParameters(
        positive_factor=FLOAT, negative_factor=FLOAT, receiver_factor=0, ui_factor=FLOAT
    )
    with pytest.raises(ValueError, match="gross input"):
        fees_for(100, fees, True)
    with pytest.raises(ValueError, match="positive amount"):
        quote_swap(state, state.market.long_asset, 0)
    with pytest.raises(ValueError, match="configured market currency"):
        quote_swap(state, state.market.share_asset, 1)


def test_nonnegative_actual_impact_does_not_apply_virtual_penalty(proof):
    state = SpotState.model_validate_json(json.dumps(proof["state"]))
    state = state.model_copy(update={"virtual_inventory": (0, 0)})
    initial = (
        state.long.pool_amount * state.long.price.midpoint
        - state.short.pool_amount * state.short.price.midpoint
    )
    delta = abs(initial) // 4
    changes = (-delta, delta) if initial > 0 else (delta, -delta)
    actual = spot_impact(state, changes)
    assert actual.usd_e30 >= 0
