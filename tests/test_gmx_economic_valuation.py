"""GM valuation includes borrowing, capped trader PnL and impact distribution in order."""

import json
from pathlib import Path

import pytest

from almanak.connectors.gmx_v2.economic_precision import FLOAT
from almanak.connectors.gmx_v2.economic_spot_types import SpotState
from almanak.connectors.gmx_v2.economic_valuation import (
    borrowing_rate,
    kink_borrowing_rate,
    market_token_price,
    next_borrowing_factor,
    next_impact_pool,
    pending_borrowing_fees,
    pool_value,
)
from almanak.connectors.gmx_v2.economic_valuation_types import (
    BorrowingSide,
    ImpactDistribution,
    ValuationState,
)


@pytest.mark.parametrize("index", range(4))
def test_full_pool_valuation_matches_pinned_arbitrum_reader(index):
    path = Path(
        "docs/internal/reports/evidence/2026-09-09-gmx-arbitrum-valuation-proof/arbitrum.json"
    )
    case = json.loads(path.read_text())["cases"][index]
    state = ValuationState.model_validate_json(json.dumps(case["state"]))
    result = pool_value(state, maximize=case["maximize"])
    assert result.model_dump(mode="json") == case["calculation"]
    assert list(result.reader_fields()) == case["contract_result"][1]
    assert market_token_price(state, result) == case["contract_result"][0]


@pytest.fixture
def valuation():
    path = Path(
        "docs/internal/reports/evidence/2026-09-09-gmx-prepared/arbitrum-liquidity-reader-comparison.json"
    )
    raw = json.loads(path.read_text())["states"]["DEPOSIT"]["state"]["spot"]
    spot = SpotState.model_validate_json(json.dumps(raw))
    side = BorrowingSide(
        open_interest_usd_e30=0,
        open_interest_tokens=0,
        cumulative_factor=0,
        updated_at=0,
        total_borrowing_usd_e30=0,
        optimal_usage_factor=FLOAT * 4 // 5,
        base_factor=10**20,
        above_optimal_factor=10**21,
        factor=10**20,
        exponent=FLOAT,
        reserve_factor=FLOAT,
        maximum_pnl_factor=FLOAT,
    )
    return ValuationState(
        spot=spot,
        index_price=spot.long.price,
        long=side,
        short=side,
        skip_borrowing_for_smaller_side=False,
        use_open_interest_tokens_for_balance=False,
        borrowing_fee_receiver_factor=FLOAT * 37 // 100,
        impact=ImpactDistribution(
            pool_amount=0, minimum_amount=0, rate=0, distributed_at=0, lent_amount=0
        ),
        share_supply=10**18,
    )


@pytest.mark.parametrize(
    "reserved,expected", [(500, 5 * 10**19), (1000, 10**21), (1500, 33 * 10**20)]
)
def test_kink_rates_preserve_below_and_above_optimal_integer_usage(valuation, reserved, expected):
    assert kink_borrowing_rate(valuation.long, reserved, 1000) == expected


def test_kink_requires_nonzero_reserve_capacity(valuation):
    with pytest.raises(ValueError, match="reserve capacity"):
        kink_borrowing_rate(valuation.long.model_copy(update={"reserve_factor": 0}), 1, 1000)


def test_pool_value_is_full_inventory_when_external_positions_and_impact_are_empty(valuation):
    value = pool_value(valuation, maximize=True)
    expected = sum(
        s.pool_amount * s.price.maximum for s in (valuation.spot.long, valuation.spot.short)
    )
    assert value.pool_usd_e30 == expected
    assert value.borrowing_fees_usd_e30 == value.net_pnl_usd_e30 == 0
    assert market_token_price(valuation, value) == expected
    assert market_token_price(valuation.model_copy(update={"share_supply": 0}), value) == FLOAT


def test_pending_impact_distribution_stops_at_its_explicit_minimum(valuation):
    impact = ImpactDistribution(
        pool_amount=100,
        minimum_amount=60,
        rate=7 * FLOAT,
        distributed_at=valuation.spot.block.timestamp - 10,
        lent_amount=0,
    )
    state = valuation.model_copy(update={"impact": impact})
    assert next_impact_pool(state) == 60
    assert (
        next_impact_pool(
            state.model_copy(update={"impact": impact.model_copy(update={"distributed_at": 0})})
        )
        == 100
    )
    assert (
        next_impact_pool(
            state.model_copy(update={"impact": impact.model_copy(update={"minimum_amount": 100})})
        )
        == 100
    )


def test_impact_and_lent_amounts_move_pool_value_in_opposite_directions(valuation):
    base = pool_value(valuation, maximize=True)
    state = valuation.model_copy(
        update={"impact": valuation.impact.model_copy(update={"pool_amount": 9, "lent_amount": 4})}
    )
    changed = pool_value(state, maximize=True)
    assert (
        changed.pool_usd_e30
        == base.pool_usd_e30 - 9 * valuation.index_price.minimum + 4 * valuation.index_price.maximum
    )


def test_borrowing_skip_uses_strictly_smaller_side_and_absolute_accumulators(valuation):
    long = valuation.long.model_copy(
        update={
            "open_interest_tokens": 1,
            "open_interest_usd_e30": FLOAT,
            "cumulative_factor": FLOAT,
            "updated_at": valuation.spot.block.timestamp - 10,
        }
    )
    short = valuation.short.model_copy(update={"open_interest_usd_e30": 2 * FLOAT})
    state = valuation.model_copy(
        update={"long": long, "short": short, "skip_borrowing_for_smaller_side": True}
    )
    assert borrowing_rate(state, True) == 0
    assert next_borrowing_factor(state, True) == FLOAT
    assert pending_borrowing_fees(state, True) == FLOAT
    invalid = state.model_copy(
        update={"long": long.model_copy(update={"total_borrowing_usd_e30": 2 * FLOAT})}
    )
    with pytest.raises(ValueError, match="aggregate borrowing"):
        pending_borrowing_fees(invalid, True)


def flat_deposit_state(valuation):
    currencies = {
        name: getattr(valuation.spot, name).model_copy(
            update={
                "price": getattr(valuation.spot, name).price.model_copy(
                    update={"minimum": FLOAT, "maximum": FLOAT}
                ),
                "pool_amount": 100,
                "impact_pool_amount": 0,
            }
        )
        for name in ("long", "short")
    }
    spot = valuation.spot.model_copy(
        update={
            **currencies,
            "fees": valuation.spot.fees.model_copy(
                update={"positive_factor": 0, "negative_factor": 0, "ui_factor": 0}
            ),
            "impact": valuation.spot.impact.model_copy(
                update={"positive_factor": 0, "negative_factor": 0}
            ),
            "virtual_inventory": None,
        }
    )
    return valuation.model_copy(
        update={"spot": spot, "index_price": spot.long.price, "share_supply": 200 * 10**18}
    )


def test_deposit_executor_updates_pool_and_share_supply_between_nonempty_legs(valuation):
    from almanak.connectors.gmx_v2.economic_deposit_execution import execute_deposit

    state = flat_deposit_state(valuation)
    result = execute_deposit(state, 10, 20)
    assert result.shares == 30 * 10**18
    assert result.legs[0].after.share_supply == 210 * 10**18
    assert result.after.share_supply == 230 * 10**18
    assert (result.after.spot.long.pool_amount, result.after.spot.short.pool_amount) == (110, 120)
    assert state.share_supply == 200 * 10**18
    assert len(execute_deposit(state, 10, 0).legs) == 1


def test_executor_does_not_use_reader_positive_impact_fallback(valuation):
    from almanak.connectors.gmx_v2.economic_deposit_execution import deposit_leg

    state = flat_deposit_state(valuation)
    state = state.model_copy(
        update={
            "spot": state.spot.model_copy(
                update={"long": state.spot.long.model_copy(update={"impact_pool_amount": 100})}
            )
        }
    )
    result = deposit_leg(state, True, 1, 10 * FLOAT, True)
    assert result.minted == 10**18
    assert result.output_impact_amount == 0
    assert result.after.spot.long.impact_pool_amount == 100
    assert result.after.spot.long.pool_amount == 101


def test_executor_positive_impact_draws_only_available_opposite_inventory(valuation):
    from almanak.connectors.gmx_v2.economic_deposit_execution import deposit_leg

    state = flat_deposit_state(valuation)
    state = state.model_copy(
        update={
            "spot": state.spot.model_copy(
                update={"short": state.spot.short.model_copy(update={"impact_pool_amount": 3})}
            )
        }
    )
    result = deposit_leg(state, True, 1, 10 * FLOAT, True)
    assert result.minted == 4 * 10**18
    assert result.after.spot.short.impact_pool_amount == 0
    assert result.after.spot.short.pool_amount == 103
    assert result.after.spot.long.pool_amount == 101


def test_deposit_freezes_elapsed_borrowing_before_pool_utilization_changes():
    from almanak.connectors.gmx_v2.economic_deposit_execution import execute_deposit

    path = Path(
        "docs/internal/reports/evidence/2026-09-09-gmx-arbitrum-valuation-proof/arbitrum.json"
    )
    state = ValuationState.model_validate_json(
        json.dumps(json.loads(path.read_text())["cases"][0]["state"])
    )
    expected = tuple(pending_borrowing_fees(state, side) for side in (True, False))
    result = execute_deposit(state, 10**18, 2000 * 10**6)
    assert len(result.legs) == 2
    for leg in result.legs:
        assert tuple(pending_borrowing_fees(leg.after, side) for side in (True, False)) == expected
