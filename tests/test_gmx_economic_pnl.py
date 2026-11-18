"""Profit sharing and fee precision retain deployed side and payment rounding rules."""

import json
from pathlib import Path

import pytest

from almanak.connectors.gmx_v2.economic_pnl import (
    PnlSide,
    PnlState,
    borrowing_fee_usd,
    funding_amount,
    position_pnl,
)
from almanak.connectors.gmx_v2.economic_precision import FLOAT
from almanak.connectors.gmx_v2.economic_spot_types import SpotState


@pytest.mark.parametrize("chain", ["arbitrum", "avalanche"])
@pytest.mark.parametrize("index", range(24))
def test_actual_reader_position_pnl(chain, index):
    path = Path("docs/internal/reports/evidence/2026-09-09-gmx-position-pnl-proof")
    captured = json.loads((path / f"{chain}.json").read_text())
    state = PnlState.model_validate_json(json.dumps(captured["state"]))
    case = captured["cases"][index]
    result = position_pnl(
        state,
        case["size_usd_e30"],
        case["size_tokens"],
        case["size_delta_usd_e30"],
        case["is_long"],
    )
    assert list(result.model_dump().values()) == case["contract_result"]
    assert result.model_dump(mode="json") == case["calculation"]


@pytest.fixture
def pnl_state():
    path = Path(
        "docs/internal/reports/evidence/2026-09-09-gmx-prepared/arbitrum-spot-reader-comparison.json"
    )
    spot = SpotState.model_validate_json(json.dumps(json.loads(path.read_text())["state"]))
    index = spot.long.price.model_copy(update={"minimum": 9, "maximum": 11})
    sides = tuple(
        PnlSide(
            open_interest_usd_e30=1,
            open_interest_tokens=100,
            pool_amount=10,
            pool_price=side.price.model_copy(update={"minimum": 2, "maximum": 3}),
            maximum_trader_pnl_factor=FLOAT,
        )
        for side in (spot.long, spot.short)
    )
    return PnlState(
        market=spot.market, block=spot.block, index_price=index, long=sides[0], short=sides[1]
    )


@pytest.mark.parametrize("is_long,tokens,pnl", [(True, 4, -4), (False, 3, -3)])
def test_partial_loss_uses_side_specific_size_rounding_and_signed_truncation(
    pnl_state, is_long, tokens, pnl
):
    result = position_pnl(pnl_state, 100, 10, 34, is_long)
    assert result.size_delta_tokens == tokens
    assert result.pnl_usd_e30 == result.uncapped_pnl_usd_e30 == pnl


def test_profitable_long_scales_by_capped_aggregate_market_profit(pnl_state):
    result = position_pnl(pnl_state, 100, 20, 100, True)
    assert result.uncapped_pnl_usd_e30 == 80
    assert result.pnl_usd_e30 == 1
    assert result.size_delta_tokens == 20
    zero_cap = pnl_state.model_copy(
        update={"long": pnl_state.long.model_copy(update={"maximum_trader_pnl_factor": 0})}
    )
    assert position_pnl(zero_cap, 100, 20, 100, True).pnl_usd_e30 == 80


def test_empty_market_open_interest_does_not_invent_a_profit_cap(pnl_state):
    state = pnl_state.model_copy(
        update={"long": pnl_state.long.model_copy(update={"open_interest_usd_e30": 0})}
    )
    assert position_pnl(state, 100, 20, 100, True).pnl_usd_e30 == 80


@pytest.mark.parametrize("size,tokens,delta", [(0, 1, 0), (1, 0, 0), (1, 1, -1), (1, 1, 2)])
def test_invalid_position_or_size_change_fails_closed(pnl_state, size, tokens, delta):
    with pytest.raises(ValueError, match="nonempty position"):
        position_pnl(pnl_state, size, tokens, delta, True)


@pytest.mark.parametrize(
    "difference,size,pay,claim",
    [(0, FLOAT, 0, 0), (1, 1, 1, 0), (10**15, FLOAT, 1, 1), (10**15 + 1, FLOAT, 2, 1)],
)
def test_funding_payment_rounds_up_but_claim_rounds_down(difference, size, pay, claim):
    assert funding_amount(10 + difference, 10, size, payment=True) == pay
    assert funding_amount(10 + difference, 10, size, payment=False) == claim


def test_borrowing_uses_whole_position_size_and_factor_precision():
    assert borrowing_fee_usd(11, 10, 2 * FLOAT + 1) == 2


@pytest.mark.parametrize("latest,entry,size", [(1, 2, 1), (1, -1, 1), (1, 0, -1)])
def test_fee_accumulators_cannot_decrease_or_use_negative_units(latest, entry, size):
    with pytest.raises(ValueError, match="cannot decrease"):
        funding_amount(latest, entry, size, payment=True)
    with pytest.raises(ValueError, match="cannot decrease"):
        borrowing_fee_usd(latest, entry, size)
