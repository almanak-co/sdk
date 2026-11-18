"""GM execution enforces its own switches and pool limits in deployed operation order."""

import pytest

from almanak.connectors.gmx_v2.economic_deposit_execution import execute_deposit
from almanak.connectors.gmx_v2.economic_liquidity import LiquidityState, quote_withdrawal
from almanak.connectors.gmx_v2.economic_liquidity_risk import (
    LiquidityRiskState,
    RequestControls,
    validate_deposit,
    validate_withdrawal,
)
from almanak.connectors.gmx_v2.economic_precision import FLOAT
from almanak.connectors.gmx_v2.economic_spot_types import SwapConstraint
from almanak.connectors.gmx_v2.economic_swap_risk import SwapSideRisk
from tests.test_gmx_economic_valuation import flat_deposit_state
from tests.test_gmx_economic_valuation import valuation as valuation


def permissive_risk(state):
    side = SwapSideRisk(
        maximum_pool_amount=1000,
        reserve_factor=FLOAT,
        maximum_deposit_pnl_factor=FLOAT,
        maximum_withdrawal_pnl_factor=FLOAT,
        open_interest_usd_e30=0,
        open_interest_tokens=0,
    )
    controls = RequestControls(
        creation_disabled=False, execution_disabled=False, cancellation_disabled=False
    )
    return LiquidityRiskState(
        market=state.spot.market,
        block=state.spot.block,
        long=side,
        short=side,
        deposits=controls,
        withdrawals=controls,
        market_disabled=False,
        request_expiration_seconds=300,
        maximum_oracle_age_seconds=60,
        maximum_long_deposit_usd_e30=1000 * FLOAT,
        maximum_short_deposit_usd_e30=1000 * FLOAT,
        minimum_first_deposit_shares=0,
        maximum_lent_impact_withdrawal_factor=FLOAT,
    )


def test_deposit_checks_incoming_usd_cap_and_changed_currency_amount(valuation):
    state = flat_deposit_state(valuation)
    risk = permissive_risk(state)
    result = execute_deposit(state, 10, 0)
    validate_deposit(state, risk, result)
    with pytest.raises(SwapConstraint, match="USD cap"):
        validate_deposit(
            state, risk.model_copy(update={"maximum_long_deposit_usd_e30": 109 * FLOAT}), result
        )
    with pytest.raises(SwapConstraint, match="pool amount"):
        validate_deposit(
            state,
            risk.model_copy(
                update={"long": risk.long.model_copy(update={"maximum_pool_amount": 109})}
            ),
            result,
        )
    # An unchanged opposite pool can remain above its deposit cap while this leg rebalances.
    validate_deposit(
        state,
        risk.model_copy(
            update={"short": risk.short.model_copy(update={"maximum_pool_amount": 99})}
        ),
        result,
    )


def test_deposit_pnl_is_checked_before_the_incoming_inventory(valuation):
    state = flat_deposit_state(valuation)
    risk = permissive_risk(state)
    risk = risk.model_copy(
        update={
            "long": risk.long.model_copy(
                update={"open_interest_tokens": 300, "open_interest_usd_e30": 100 * FLOAT}
            )
        }
    )
    with pytest.raises(SwapConstraint, match="trader-PnL"):
        validate_deposit(state, risk, execute_deposit(state, 200, 0))


def test_first_deposit_requires_locked_receiver_when_configured(valuation):
    state = flat_deposit_state(valuation).model_copy(update={"share_supply": 0})
    risk = permissive_risk(state).model_copy(update={"minimum_first_deposit_shares": 1})
    with pytest.raises(SwapConstraint, match="locked-share receiver"):
        validate_deposit(state, risk, execute_deposit(state, 1, 0))


def withdrawal_case(valuation):
    state = flat_deposit_state(valuation)
    state = state.model_copy(
        update={"spot": state.spot.model_copy(update={"pricing_type": "WITHDRAWAL"})}
    )
    calculation = quote_withdrawal(
        LiquidityState(
            action="WITHDRAWAL",
            spot=state.spot,
            pool_value_usd_e30=200 * FLOAT,
            share_supply=200 * 10**18,
        ),
        20 * 10**18,
    )
    return state, permissive_risk(state), calculation


def test_withdrawal_checks_post_exit_lent_impact_and_both_position_reserves(valuation):
    state, risk, calculation = withdrawal_case(valuation)
    after = validate_withdrawal(state, risk, calculation)
    assert (after.spot.long.pool_amount, after.spot.short.pool_amount) == (90, 90)
    debt = state.model_copy(update={"impact": state.impact.model_copy(update={"lent_amount": 181})})
    with pytest.raises(SwapConstraint, match="lent-impact"):
        validate_withdrawal(debt, risk, calculation)
    for name in ("long", "short"):
        side = getattr(risk, name).model_copy(
            update={"open_interest_tokens": 91, "open_interest_usd_e30": 91 * FLOAT}
        )
        with pytest.raises(SwapConstraint, match="position reserve"):
            validate_withdrawal(state, risk.model_copy(update={name: side}), calculation)


@pytest.mark.parametrize("kind", ["deposits", "withdrawals"])
def test_liquidity_execution_switches_do_not_borrow_order_flags(valuation, kind):
    state, risk, calculation = withdrawal_case(valuation)
    risk = risk.model_copy(
        update={kind: getattr(risk, kind).model_copy(update={"execution_disabled": True})}
    )
    if kind == "withdrawals":
        with pytest.raises(SwapConstraint, match="disabled"):
            validate_withdrawal(state, risk, calculation)
    else:
        validate_withdrawal(state, risk, calculation)
