"""Full repayment approves the held balance and repays the debt at inclusion."""

from types import SimpleNamespace

import pytest

from almanak.connectors.aave_v3.math import borrow_shares, debt_assets, transition_indices
from almanak.connectors.aave_v3.operations import (
    ApproveOperation,
    RepayOperation,
    RevokeOperation,
)
from almanak.connectors.aave_v3.planning import exit_operation, repayment_capacity
from almanak.connectors.pool_lending_math import UINT256_MAX
from almanak.connectors.pool_lending_operations import call_arguments
from almanak.gateway.ethereum import DataError
from tests.test_aave_v3_config import parameters as parameters
from tests.test_aave_v3_risk import position

DEBT = 500 * 10**6 + 1


def observed(config, wallet, allowance=0, debt=DEBT):
    current = position(1)
    _, index = transition_indices(current.reserve, current.reserve.block.timestamp)
    scaled = borrow_shares(debt, index)
    return SimpleNamespace(
        target=config.target("loan"),
        wallet_balance=wallet,
        allowance=allowance,
        position=current.model_copy(
            update={"debt_scaled": scaled, "debt_assets": debt_assets(scaled, index)}
        ),
    )


@pytest.mark.parametrize("wallet", [600 * 10**6, 1200 * 10**6])
def test_full_repayment_approves_the_held_balance_and_sends_the_pool_sentinel(parameters, wallet):
    intent = parameters.repay("loan")
    approval = exit_operation(intent, observed(parameters, wallet))
    assert isinstance(approval, ApproveOperation)
    assert approval.amount.amount == wallet
    before = observed(parameters, wallet, wallet)
    repayment = exit_operation(intent, before)
    assert isinstance(repayment, RepayOperation)
    assert repayment.full and repayment.amount.amount == wallet
    repayment_capacity(before, repayment)
    assert call_arguments(repayment)[2][1] == UINT256_MAX


def test_debt_far_above_the_borrowed_amount_still_repays_in_full(parameters):
    # Interest has taken the debt to five times a 1000 USDC borrow; no configured
    # amount bounds the repayment.
    debt = 5000 * 10**6
    before = observed(parameters, 6000 * 10**6, 6000 * 10**6, debt=debt)
    repayment = exit_operation(parameters.repay("loan"), before)
    assert isinstance(repayment, RepayOperation) and repayment.full
    repayment_capacity(before, repayment)


def test_full_repayment_the_fund_cannot_cover_names_its_shortfall(parameters):
    before = observed(parameters, 500 * 10**6)
    shortfall = before.position.debt_assets - 500 * 10**6
    with pytest.raises(DataError, match=f"needs {shortfall} more") as refused:
        exit_operation(parameters.repay("loan"), before)
    assert refused.value.kind == "repayment_funding"


def test_an_approval_that_no_longer_covers_the_debt_is_revoked_first(parameters):
    before = observed(parameters, 600 * 10**6, allowance=100)
    assert isinstance(exit_operation(parameters.repay("loan"), before), RevokeOperation)


def test_partial_repayment_above_the_owned_debt_is_refused(parameters):
    before = observed(parameters, 600 * 10**6)
    with pytest.raises(DataError, match="exceeds the owned debt"):
        exit_operation(parameters.repay_amount("loan", 600 * 10**6), before)
