"""Spark economic effects preserve fund inventory and scoped lending authority."""

from dataclasses import replace

import pytest

from almanak.connectors.sparklend.economic_actions import apply, reserve_inventory
from almanak.connectors.sparklend.economic_data import Market, RiskSample
from almanak.connectors.sparklend.economic_math import risk
from almanak.connectors.sparklend.economic_state import Position
from almanak.connectors.sparklend.math import RAY
from tests.test_sparklend_config import parameters as parameters
from tests.test_sparklend_risk import position


def markets():
    return tuple(
        Market(
            sample=RiskSample(
                reserve=position(i).reserve,
                oracle_sentinel="0x" + "00" * 20,
                block_group=(position(i).reserve.block,),
                following_block=position(i).reserve.block.model_copy(
                    update={
                        "number": position(i).reserve.block.number + 1,
                        "timestamp": position(i).reserve.block.timestamp + 12,
                    }
                ),
            ),
            supply_index=RAY,
            debt_index=RAY,
            time=position(i).reserve.block.timestamp,
        )
        for i in range(2)
    )


def initial(parameters):
    return tuple(Position(target=parameters.target(a.name)) for a in parameters.allocations)


def transition(config, intent, positions, market, inventory, others=()):
    return apply(config, intent, positions, market, inventory, (*positions, *others))


def test_supply_borrow_partial_and_full_exits_retain_exact_inventory(parameters):
    market = markets()
    positions = initial(parameters)
    balances = {
        parameters.target("collateral").asset: 10**18,
        parameters.target("loan").asset: 100 * 10**6,
    }
    actions = (
        parameters.supply("collateral", 10**18),
        parameters.borrow("loan", 1000 * 10**6),
        parameters.repay_amount("loan", 400 * 10**6),
        parameters.withdraw_amount("collateral", 10**17),
        parameters.repay("loan"),
        parameters.set_collateral("collateral", False),
        parameters.set_collateral("collateral", True),
        parameters.withdraw("collateral"),
    )
    for intent in actions:
        changed = transition(parameters, intent, positions, market, balances[intent.target.asset])
        balances[intent.target.asset] += changed.inventory_delta
        positions = changed.positions
    assert balances == {
        parameters.target("collateral").asset: 10**18,
        parameters.target("loan").asset: 100 * 10**6,
    }
    assert not any(p.supply_scaled or p.debt_scaled or p.collateral_enabled for p in positions)
    assert positions[0].supplied == positions[0].withdrawn == 10**18
    assert positions[1].borrowed == positions[1].repaid == 1000 * 10**6
    assert all(reserve_inventory(positions, a.reserve) == 0 for a in parameters.allocations)


def borrowed(parameters):
    market = markets()
    supplied = transition(
        parameters, parameters.supply("collateral", 10**18), initial(parameters), market, 10**18
    )
    debt = transition(
        parameters, parameters.borrow("loan", 1000 * 10**6), supplied.positions, market, 0
    )
    return debt.positions, market


def test_accrual_changes_assets_without_fabricating_inventory_or_changing_owned_shares(parameters):
    positions, market = borrowed(parameters)
    later = (
        replace(market[0], supply_index=RAY * 101 // 100),
        replace(market[1], debt_index=RAY * 102 // 100),
    )
    repayment = transition(parameters, parameters.repay("loan"), positions, later, 1100 * 10**6)
    assert repayment.assets == 1020 * 10**6
    assert repayment.positions[1].debt_scaled == 0
    withdrawal = transition(
        parameters, parameters.withdraw("collateral"), repayment.positions, later, 0
    )
    assert withdrawal.assets == 101 * 10**16
    assert withdrawal.positions[0].supply_scaled == 0


@pytest.mark.parametrize(
    "fault", ["inventory", "withdraw", "health", "collateral", "excess", "authority"]
)
def test_economic_steps_cannot_hide_missing_funds_risk_or_authority(parameters, fault):
    positions, market = borrowed(parameters)
    inventory = 1100 * 10**6
    intent = parameters.repay("loan")
    if fault == "inventory":
        inventory = 0
    elif fault == "withdraw":
        intent = parameters.withdraw_amount("collateral", 2 * 10**18)
    elif fault == "health":
        intent = parameters.withdraw("collateral")
    elif fault == "collateral":
        intent = parameters.set_collateral("collateral", False)
    elif fault == "excess":
        intent = parameters.repay_amount("loan", 1100 * 10**6)
    else:
        intent = parameters.borrow("collateral", 1)
    with pytest.raises(ValueError):
        transition(parameters, intent, positions, market, inventory)


def test_repeated_supply_and_borrowing_carry_no_budget(parameters):
    # Capital is what the allocation started with, not a cap: a second supply beyond
    # it and a second borrow are bounded only by inventory, health and the reserve.
    positions, market = borrowed(parameters)
    supplied = transition(
        parameters, parameters.supply("collateral", 10**18), positions, market, 10**18
    )
    assert supplied.positions[0].supplied == 2 * 10**18
    debt = transition(parameters, parameters.borrow("loan", 1), supplied.positions, market, 0)
    assert debt.positions[1].borrowed == 1000 * 10**6 + 1


def test_full_repayment_follows_the_debt_past_the_borrowed_amount(parameters):
    # Doubling the debt index takes the debt to twice the borrowed amount; with the
    # inventory to cover it, the modeled full repayment needs no configured amount.
    positions, market = borrowed(parameters)
    market = (market[0], replace(market[1], debt_index=2 * RAY))
    repayment = transition(parameters, parameters.repay("loan"), positions, market, 2100 * 10**6)
    assert repayment.positions[1].debt_scaled == 0
    assert repayment.assets > 1000 * 10**6


def test_risk_combines_reserves_and_repayment_can_reduce_risk_without_a_price(parameters):
    positions, market = borrowed(parameters)
    assert risk(positions, market).risk.health_factor_wad == 255 * 10**16
    missing = replace(
        market[0],
        sample=market[0].sample.model_copy(
            update={
                "reserve": market[0].reserve.model_copy(
                    update={"oracle": None, "oracle_error": "unavailable"}
                ),
            }
        ),
    )
    absent = (missing, market[1])
    with pytest.raises(ValueError, match="oracle"):
        risk(positions, absent)
    repaid = transition(parameters, parameters.repay("loan"), positions, absent, 1100 * 10**6)
    assert repaid.positions[1].debt_scaled == 0
    withdrawn = transition(
        parameters, parameters.withdraw("collateral"), repaid.positions, absent, 0
    )
    assert withdrawn.positions[0].supply_scaled == 0


def test_reserve_capacity_counts_another_funds_hypothetical_borrowing(parameters):
    positions, market = borrowed(parameters)
    pool = market[1].reserve
    # The external reserve can settle 1,000 units, but the first hypothetical
    # borrower has already consumed them. A second fund cannot spend them again.
    limited = replace(
        market[1],
        sample=market[1].sample.model_copy(
            update={
                "reserve": pool.model_copy(
                    update={
                        "available_underlying": 1000 * 10**6,
                    }
                ),
            }
        ),
    )
    other = positions[1].model_copy(
        update={
            "target": positions[1].target.model_copy(
                update={
                    "account": "second",
                    "fund": positions[1].target.fund.model_copy(
                        update={"address": "0x" + "12" * 20}
                    ),
                }
            )
        }
    )
    before = (positions[0], initial(parameters)[1])
    with pytest.raises(ValueError, match="reserve inventory"):
        transition(
            parameters, parameters.borrow("loan", 1), before, (market[0], limited), 0, (other,)
        )
