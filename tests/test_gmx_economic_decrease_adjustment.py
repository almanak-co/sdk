"""Decreases can remove withdrawal and expand to full close under native bounds."""

import pytest

from almanak.connectors.gmx_v2.economic_decrease_adjustment import (
    DecreaseAdjustmentInput,
    adjust_decrease,
)
from almanak.connectors.gmx_v2.economic_precision import FLOAT
from almanak.connectors.gmx_v2.economic_spot_types import SwapConstraint


@pytest.fixture
def adjustment():
    return DecreaseAdjustmentInput(
        position_usd_e30=1000,
        collateral_amount=100,
        collateral_minimum_price=2,
        full_pnl_usd_e30=0,
        requested_size_usd_e30=100,
        requested_withdrawal=0,
        next_minimum_collateral_factor=FLOAT // 10,
        minimum_collateral_usd_e30=20,
        minimum_position_usd_e30=50,
    )


def test_partial_decrease_suppresses_leverage_breaking_withdrawal(adjustment):
    result = adjust_decrease(adjustment.model_copy(update={"requested_withdrawal": 75}))
    assert result.size_usd_e30 == 100 and result.withdrawal == 0


def test_collateral_only_withdrawal_refuses_unhealthy_remainder(adjustment):
    with pytest.raises(SwapConstraint, match="insufficient collateral"):
        adjust_decrease(
            adjustment.model_copy(update={"requested_size_usd_e30": 0, "requested_withdrawal": 75})
        )


def test_low_equity_remainder_expands_to_full_close(adjustment):
    result = adjust_decrease(adjustment.model_copy(update={"full_pnl_usd_e30": -190}))
    assert result.size_usd_e30 == 1000 and result.withdrawal == 0


def test_subminimum_size_remainder_expands_to_full_close(adjustment):
    result = adjust_decrease(adjustment.model_copy(update={"requested_size_usd_e30": 951}))
    assert result.size_usd_e30 == 1000
    exact = adjust_decrease(adjustment.model_copy(update={"requested_size_usd_e30": 950}))
    assert exact.size_usd_e30 == 950


def test_market_close_caps_stale_notional_and_drops_explicit_withdrawal(adjustment):
    result = adjust_decrease(
        adjustment.model_copy(update={"requested_size_usd_e30": 1001, "requested_withdrawal": 1001})
    )
    assert result.size_usd_e30 == 1000 and result.withdrawal == 0
