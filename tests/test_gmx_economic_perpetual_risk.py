"""Perpetual increases bound post-action open interest and distinct reserve ceilings."""

from types import SimpleNamespace

import pytest

from almanak.connectors.gmx_v2.economic_perpetual_risk import (
    PerpetualSideRisk,
    validate_increase_reserves,
)
from almanak.connectors.gmx_v2.economic_precision import FLOAT
from almanak.connectors.gmx_v2.economic_spot_types import SwapConstraint
from tests.test_gmx_economic_valuation import valuation as valuation


@pytest.fixture
def reserve(valuation):
    price = valuation.index_price.model_copy(update={"minimum": 2, "maximum": 3})
    currency = valuation.spot.long.model_copy(update={"pool_amount": 1000, "price": price})
    state = valuation.model_copy(
        update={
            "index_price": price,
            "spot": valuation.spot.model_copy(update={"long": currency}),
            "long": valuation.long.model_copy(
                update={"open_interest_usd_e30": 100, "open_interest_tokens": 100}
            ),
        }
    )
    side = PerpetualSideRisk(
        maximum_open_interest_usd_e30=1000,
        reserve_factor=FLOAT,
        open_interest_reserve_factor=FLOAT // 2,
        collateral_open_interest_multiplier=FLOAT * FLOAT // 1000,
    )
    risk = SimpleNamespace(
        market=state.spot.market,
        block=state.spot.block,
        long=side,
        short=side,
        minimum_collateral_factor=FLOAT // 10,
    )
    return risk, state


def test_increase_leverage_uses_post_action_open_interest(reserve):
    risk, state = reserve
    assert validate_increase_reserves(risk, state, 100, 100, True) == FLOAT // 5


def test_increase_bounds_usd_notional_separately_from_index_token_reserve(reserve):
    risk, state = reserve
    with pytest.raises(SwapConstraint, match="maximum open interest"):
        validate_increase_reserves(risk, state, 901, 1, True)
    with pytest.raises(SwapConstraint, match="position reserve"):
        validate_increase_reserves(risk, state, 100, 600, True)
    with pytest.raises(SwapConstraint, match="open-interest reserve"):
        validate_increase_reserves(risk, state, 100, 234, True)
    assert validate_increase_reserves(risk, state, 100, 233, True) == FLOAT // 5
