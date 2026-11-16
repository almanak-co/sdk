"""Post-swap limits follow token direction and conservative Oracle price bounds."""

import json
from pathlib import Path

import pytest

from almanak.connectors.gmx_v2.economic_precision import FLOAT
from almanak.connectors.gmx_v2.economic_spot_math import quote_swap
from almanak.connectors.gmx_v2.economic_spot_types import SpotState
from almanak.connectors.gmx_v2.economic_swap_risk import (
    SwapRiskState,
    SwapSideRisk,
    maximum_side_pnl,
    pool_after_swap,
    validate_pnl,
    validate_swap_risk,
    validate_swap_side,
)

ROOT = Path(__file__).parents[1] / "docs/internal/reports/evidence/2026-09-09-gmx-prepared"


@pytest.fixture(params=["arbitrum", "avalanche"])
def spot(request):
    proof = json.loads((ROOT / (request.param + "-spot-reader-comparison.json")).read_text())
    return SpotState.model_validate_json(json.dumps(proof["state"]))


def side(**changes):
    values = dict(
        maximum_pool_amount=10**50,
        reserve_factor=FLOAT,
        maximum_deposit_pnl_factor=FLOAT,
        maximum_withdrawal_pnl_factor=FLOAT,
        open_interest_usd_e30=0,
        open_interest_tokens=0,
    )
    return SwapSideRisk(**(values | changes))


def risk(spot, **changes):
    values = dict(
        market=spot.market,
        block=spot.block,
        long=side(),
        short=side(),
        market_disabled=False,
        creation_disabled=False,
        execution_disabled=False,
        cancellation_disabled=False,
        request_expiration_seconds=300,
        maximum_oracle_age_seconds=60,
    )
    return SwapRiskState(**(values | changes))


@pytest.mark.parametrize("forward", [True, False])
def test_pool_limit_checks_input_and_reserve_checks_output(spot, forward):
    incoming = spot.long if forward else spot.short
    result = quote_swap(spot, incoming.price.asset, incoming.pool_amount // 10000)
    amounts = pool_after_swap(spot, result)
    accepted = risk(spot)
    validate_swap_risk(spot, accepted, spot.long.price, result)
    incoming_side, outgoing_side = ("long", "short") if forward else ("short", "long")
    input_amount = amounts[0 if forward else 1]
    blocked = accepted.model_copy(
        update={incoming_side: side(maximum_pool_amount=input_amount - 1)}
    )
    with pytest.raises(ValueError, match="maximum pool amount"):
        validate_swap_risk(spot, blocked, spot.long.price, result)
    # An existing output-side pool above its cap does not block a reducing swap.
    accepted = accepted.model_copy(update={outgoing_side: side(maximum_pool_amount=0)})
    validate_swap_risk(spot, accepted, spot.long.price, result)
    blocked = accepted.model_copy(
        update={outgoing_side: side(open_interest_usd_e30=10**55, open_interest_tokens=10**40)}
    )
    with pytest.raises(ValueError, match="position reserve"):
        validate_swap_risk(spot, blocked, spot.long.price, result)


@pytest.mark.parametrize("field", ["market_disabled", "execution_disabled"])
def test_disabled_execution_is_refused_at_fill(spot, field):
    result = quote_swap(spot, spot.long.price.asset, spot.long.pool_amount // 10000)
    with pytest.raises(ValueError, match="disabled"):
        validate_swap_risk(spot, risk(spot, **{field: True}), spot.long.price, result)


def test_fill_requires_exact_historical_state_and_inventory(spot):
    result = quote_swap(spot, spot.long.price.asset, spot.long.pool_amount // 10000)
    stale = risk(spot).model_copy(
        update={"block": spot.block.model_copy(update={"number": spot.block.number + 1})}
    )
    with pytest.raises(ValueError, match="same canonical"):
        validate_swap_risk(spot, stale, spot.long.price, result)
    with pytest.raises(ValueError, match="positive output"):
        validate_swap_risk(
            spot, risk(spot), spot.long.price, result.model_copy(update={"amount_out": 0})
        )
    with pytest.raises(ValueError, match="available pool inventory"):
        validate_swap_risk(
            spot,
            risk(spot),
            spot.long.price,
            result.model_copy(update={"pool_amount_out": spot.short.pool_amount + 1}),
        )


@pytest.mark.parametrize("is_long", [True, False])
@pytest.mark.parametrize("usd,tokens", [(0, 100), (100, 0), (0, 0)])
def test_zero_open_interest_component_produces_zero_pnl(spot, is_long, usd, tokens):
    assert (
        maximum_side_pnl(
            side(open_interest_usd_e30=usd, open_interest_tokens=tokens), spot.long.price, is_long
        )
        == 0
    )


def test_pnl_uses_directional_price_bound_and_action_specific_limit(spot):
    price = spot.long.price.model_copy(update={"minimum": 9, "maximum": 11})
    limits = side(
        open_interest_usd_e30=100,
        open_interest_tokens=10,
        maximum_deposit_pnl_factor=FLOAT // 10,
        maximum_withdrawal_pnl_factor=FLOAT // 20,
    )
    assert maximum_side_pnl(limits, price, True) == 10
    assert maximum_side_pnl(limits, price, False) == 10
    validate_pnl(limits, price, 100, True, incoming=True)
    with pytest.raises(ValueError, match="trader-PnL"):
        validate_pnl(limits, price, 100, True, incoming=False)
    validate_pnl(limits, price, 0, True, incoming=False)
    losing = limits.model_copy(update={"open_interest_usd_e30": 200})
    validate_pnl(losing, price, 100, True, incoming=False)


def test_reserve_equality_is_allowed_and_long_uses_maximum_index_price(spot):
    price = spot.long.price.model_copy(update={"minimum": 9, "maximum": 11})
    limits = side(open_interest_usd_e30=100, open_interest_tokens=10)
    validate_swap_side(limits, price, 10, 110, True, incoming=False)
    with pytest.raises(ValueError, match="position reserve"):
        validate_swap_side(limits, price, 10, 109, True, incoming=False)
    validate_swap_side(limits, price, 10, 100, False, incoming=False)
    with pytest.raises(ValueError, match="position reserve"):
        validate_swap_side(limits, price, 10, 99, False, incoming=False)
