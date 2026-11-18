"""Liquidation guards ignore positive impact and cap adverse exit impact."""

import pytest

from almanak.connectors.gmx_v2.economic_position_risk import (
    PositionRiskInput,
    position_risk,
    risk_impact,
)
from almanak.connectors.gmx_v2.economic_precision import FLOAT
from tests.test_gmx_economic_valuation import valuation as valuation


@pytest.fixture
def risk(valuation):
    price = valuation.spot.long.price.model_copy(update={"minimum": 2, "maximum": 3})
    return PositionRiskInput(
        size_usd_e30=1000,
        collateral_amount=100,
        collateral_price=price,
        index_price=price,
        capped_pnl_usd_e30=0,
        close_impact_usd_e30=0,
        pending_impact_amount=0,
        maximum_positive_impact_factor=FLOAT // 10,
        maximum_liquidation_impact_factor=FLOAT // 20,
        close_cost_amount=10,
        minimum_collateral_usd_e30=20,
        minimum_collateral_factor=FLOAT // 10,
        validate_minimum_collateral=True,
    )


@pytest.mark.parametrize(
    "impact,pending,expected",
    [(10000, 0, 0), (-10000, 0, -50), (10000, -40, -20), (-40, 10, -20)],
)
def test_liquidation_impact_caps_before_pending_and_ignores_positive(
    risk, impact, pending, expected
):
    state = risk.model_copy(
        update={"close_impact_usd_e30": impact, "pending_impact_amount": pending}
    )
    assert risk_impact(state) == expected


@pytest.mark.parametrize(
    "pnl,validate,reason",
    [
        (-161, True, "min collateral"),
        (-180, False, "< 0"),
        (-81, True, "min collateral for leverage"),
        (-80, True, None),
    ],
)
def test_liquidation_thresholds_preserve_native_strictness_and_reason(risk, pnl, validate, reason):
    result = position_risk(
        risk.model_copy(update={"capped_pnl_usd_e30": pnl, "validate_minimum_collateral": validate})
    )
    assert result.reason == reason
    assert result.liquidatable == (reason is not None)
    assert result.remaining_collateral_usd_e30 == 180 + pnl
    assert result.minimum_collateral_usd_e30 == (20 if validate else 0)
