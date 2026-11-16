"""Component planning preserves decisions, exact approvals and full-repayment ceilings."""

from types import SimpleNamespace

import pytest

from almanak.connectors.compound_v3.authority import declared
from almanak.connectors.compound_v3.domain import CollateralBalance
from almanak.connectors.compound_v3.observations import owned
from almanak.connectors.compound_v3.operations import (
    ApproveOperation,
    RepayOperation,
    RevokeOperation,
)
from almanak.connectors.compound_v3.planning import operation, validate_ownership
from almanak.gateway.ethereum import DataError
from tests.test_compound_v3_config import parameters as parameters
from tests.test_compound_v3_observations import snapshot


def before(parameters, name):
    target = parameters.target(name)
    return SimpleNamespace(
        target=target,
        allowance=0,
        wallet_balance=1000 * 10**6,
        account=SimpleNamespace(
            principal=-100 if name == "loan" else 100,
            supplied_base=100 if name == "earn" else 0,
            borrowed_base=100 if name == "loan" else 0,
            collaterals=(CollateralBalance(asset=target.asset, balance=10**18),),
        ),
    )


def state(parameters, intent):
    return SimpleNamespace(
        capabilities=declared(parameters),
        decision=SimpleNamespace(identity="retained", intent=intent),
        outcomes=(),
    )


def test_full_repayment_approves_the_held_balance_and_survives_wallet_changes(parameters):
    intent = parameters.repay("loan")
    retained, observation = state(parameters, intent), before(parameters, "loan")
    approval = operation(parameters, retained, intent, observation)
    assert isinstance(approval, ApproveOperation)
    assert approval.amount.amount == observation.wallet_balance
    observation.allowance = approval.amount.amount
    observation.wallet_balance -= 1
    repayment = operation(parameters, retained, intent, observation)
    assert isinstance(repayment, RepayOperation) and repayment.full
    assert repayment.amount == approval.amount
    # An approval that no longer covers the debt is revoked and approved afresh.
    observation.allowance = observation.account.borrowed_base - 1
    assert isinstance(operation(parameters, retained, intent, observation), RevokeOperation)


def test_full_repayment_the_fund_cannot_cover_names_its_shortfall(parameters):
    intent = parameters.repay("loan")
    retained, observation = state(parameters, intent), before(parameters, "loan")
    observation.wallet_balance = observation.account.borrowed_base - 1
    with pytest.raises(DataError, match="needs 1 more") as refused:
        operation(parameters, retained, intent, observation)
    assert refused.value.kind == "repayment_funding"


def test_partial_repayment_above_the_debt_is_refused_before_it_becomes_supply(parameters):
    intent = parameters.repay_amount("loan", 101)
    retained, observation = state(parameters, intent), before(parameters, "loan")
    with pytest.raises(DataError, match="exceeds the owned debt"):
        operation(parameters, retained, intent, observation)


def test_completed_economic_effect_can_only_clean_up_its_allowance(parameters):
    intent = parameters.supply("collateral", 10**18)
    retained, observation = state(parameters, intent), before(parameters, "collateral")
    retained.outcomes = (
        SimpleNamespace(
            success=True, decision=retained.decision, operation=SimpleNamespace(kind="supply")
        ),
    )
    observation.allowance = 10
    assert isinstance(operation(parameters, retained, intent, observation), RevokeOperation)
    observation.allowance = 0
    with pytest.raises(DataError, match="again"):
        operation(parameters, retained, intent, observation)


@pytest.mark.parametrize(
    "action,name,amount,kind",
    [
        ("supply", "collateral", 1, "approve"),
        ("borrow", "loan", 1, "borrow"),
        ("withdraw_amount", "collateral", 1, "withdraw"),
        ("repay_amount", "loan", 1, "approve"),
    ],
)
def test_planning_preserves_economic_action_amount_and_target(
    parameters, action, name, amount, kind
):
    intent = getattr(parameters, action)(name, amount)
    result = operation(parameters, state(parameters, intent), intent, before(parameters, name))
    assert result.kind == kind and result.target == intent.target and result.amount == intent.amount


@pytest.mark.parametrize("name,amount", [("earn", 100), ("collateral", 10**18)])
def test_full_withdrawal_keeps_observed_base_or_collateral_units(parameters, name, amount):
    intent = parameters.withdraw(name)
    result = operation(parameters, state(parameters, intent), intent, before(parameters, name))
    assert result.full and result.amount.amount == amount


def test_closed_position_can_only_revoke_remaining_authority(parameters):
    intent = parameters.repay("loan")
    retained, observation = state(parameters, intent), before(parameters, "loan")
    observation.account.principal = 0
    observation.allowance = 10
    assert isinstance(operation(parameters, retained, intent, observation), RevokeOperation)
    observation.allowance = 0
    with pytest.raises(DataError, match="closed"):
        operation(parameters, retained, intent, observation)


def test_planning_refuses_changed_decision_target_capabilities_or_ownership(parameters):
    intent = parameters.supply("collateral", 1)
    retained, observation = state(parameters, intent), before(parameters, "collateral")
    with pytest.raises(ValueError, match="target"):
        operation(parameters, retained, intent, before(parameters, "earn"))
    retained.capabilities = None
    with pytest.raises(ValueError, match="capabilities"):
        operation(parameters, retained, intent, observation)
    retained.capabilities = declared(parameters)
    retained.decision = None
    with pytest.raises(ValueError, match="decision"):
        operation(parameters, retained, intent, observation)
    observed = snapshot()
    ownership = owned(observed)
    validate_ownership(ownership, observed)
    with pytest.raises(DataError, match="ownership"):
        validate_ownership(ownership.model_copy(update={"collateral": 1}), observed)


def test_full_withdrawal_can_recover_owned_base_credit_after_borrower_absorption(parameters):
    intent = parameters.withdraw("loan")
    observation = before(parameters, "loan")
    observation.account.principal = 100
    observation.account.supplied_base = 101
    result = operation(parameters, state(parameters, intent), intent, observation)
    assert result.full and result.amount.amount == 101
    assert result.target == parameters.target("loan")
