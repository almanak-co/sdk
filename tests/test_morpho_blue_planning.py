"""Morpho planning retains shares and funding bounds across intermediate outcomes."""

import json
from types import SimpleNamespace

import pytest

from almanak.connectors.composed.execution import validate_inventory
from almanak.connectors.composed.models import wallet_key
from almanak.connectors.morpho_blue.authority import declared
from almanak.connectors.morpho_blue.capacity import funding_requirement
from almanak.connectors.morpho_blue.capacity import validate as validate_capacity
from almanak.connectors.morpho_blue.domain import AccountObservation, Position
from almanak.connectors.morpho_blue.observations import Snapshot, owned
from almanak.connectors.morpho_blue.operations import (
    ApproveOperation,
    BorrowOperation,
    RepayOperation,
    RevokeOperation,
)
from almanak.connectors.morpho_blue.planning import operation, validate_ownership
from almanak.connectors.morpho_blue.risk import borrowed_assets, health_factor, supplied_assets
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import native_asset
from almanak.models import AssetAmount
from tests.test_morpho_blue_config import parameters as parameters
from tests.test_morpho_blue_reader import recorded


def before(parameters, name):
    raw = recorded(1, True)
    target = parameters.target(name)
    account = next(
        AccountObservation.model_validate_json(json.dumps(row))
        for row in raw["accounts"]
        if row["fund"]["address"] == target.fund.address
    )
    token = "loan" if target.reserve.is_loan else "collateral"
    wallet = raw["steps"][-1]["after"]["accounts"][target.account][token]
    return Snapshot(
        target=target,
        account=account,
        wallet_balance=wallet,
        allowance=0,
        native_balance=AssetAmount(asset=native_asset(target.fund.chain.chain_id), amount=10**18),
        payer=target.fund,
    )


def state(parameters, intent):
    return SimpleNamespace(
        capabilities=declared(parameters),
        decision=SimpleNamespace(identity="retained", intent=intent),
        outcomes=(),
    )


def test_full_repayment_retains_debt_shares_and_approves_the_held_balance(parameters):
    intent = parameters.repay("loan")
    retained, observation = state(parameters, intent), before(parameters, "loan")
    approval = operation(parameters, retained, intent, observation)
    assert isinstance(approval, ApproveOperation)
    assert approval.amount.amount == observation.wallet_balance
    observation = observation.model_copy(update={"allowance": approval.amount.amount})
    repay = operation(parameters, retained, intent, observation)
    assert isinstance(repay, RepayOperation) and repay.full
    assert repay.shares == observation.account.position.borrow_shares
    assert validate_capacity(parameters, observation, repay).position.borrow_shares == 0
    # An approval that no longer covers the debt is revoked and approved afresh.
    stale = observation.model_copy(update={"allowance": 1})
    assert isinstance(operation(parameters, retained, intent, stale), RevokeOperation)
    with pytest.raises(DataError, match="send the shortfall"):
        validate_capacity(parameters, observation.model_copy(update={"wallet_balance": 1}), repay)


def retained_full_repayment(parameters, *, ceiling=200_000, cash=119_950):
    observation = before(parameters, "loan")
    market = observation.account.market
    accounting = market.accounting.model_copy(
        update={
            "borrow_assets": 1_014_485,
            "borrow_shares": 1_009_512_554_507,
            "last_update": market.block.timestamp,
        }
    )
    position = observation.account.position.model_copy(update={"borrow_shares": 99_509_855_682})
    observation = observation.model_copy(
        update={
            "account": observation.account.model_copy(
                update={
                    "market": market.model_copy(update={"accounting": accounting}),
                    "position": position,
                }
            ),
            "wallet_balance": cash,
            "allowance": ceiling,
        }
    )
    operation = RepayOperation(
        target=observation.target,
        amount=AssetAmount(asset=observation.target.asset, amount=ceiling),
        shares=position.borrow_shares,
    )
    inventory = SimpleNamespace(
        inventory=(
            SimpleNamespace(
                key=wallet_key(operation.target, operation.amount.asset),
                amount=cash,
            ),
        )
    )
    return inventory, observation, operation


def test_composed_full_repayment_spends_share_derived_assets_not_allowance_ceiling(parameters):
    inventory, observation, repay = retained_full_repayment(parameters)
    assert repay.amount.amount == 200_000
    assert funding_requirement(observation, repay).amount == 100_001
    validate_inventory(inventory, repay, observation)
    assert validate_capacity(parameters, observation, repay).position.borrow_shares == 0


@pytest.mark.parametrize(
    ("ceiling", "cash", "message"),
    [(200_000, 100_000, "needs 1 more"), (100_000, 119_950, "approved allowance")],
)
def test_composed_full_repayment_rejects_underfunding_and_an_approval_below_the_debt(
    parameters, ceiling, cash, message
):
    inventory, observation, repay = retained_full_repayment(parameters, ceiling=ceiling, cash=cash)
    with pytest.raises(DataError, match=message):
        validate_inventory(inventory, repay, observation)


def test_composed_partial_repayment_still_spends_its_declared_asset_amount(parameters):
    inventory, observation, full = retained_full_repayment(parameters)
    partial = full.model_copy(
        update={
            "amount": full.amount.model_copy(update={"amount": observation.wallet_balance}),
            "shares": 0,
        }
    )
    validate_inventory(inventory, partial, observation)
    partial = full.model_copy(
        update={
            "amount": full.amount.model_copy(update={"amount": observation.wallet_balance + 1}),
            "shares": 0,
        }
    )
    with pytest.raises(DataError, match="retained fund inventory"):
        validate_inventory(inventory, partial, observation)


def test_recorded_effect_can_only_revoke_allowance(parameters):
    intent = parameters.repay("loan")
    retained, observation = state(parameters, intent), before(parameters, "loan")
    retained.outcomes = (
        SimpleNamespace(
            success=True, decision=retained.decision, operation=SimpleNamespace(kind="repay")
        ),
    )
    assert isinstance(
        operation(parameters, retained, intent, observation.model_copy(update={"allowance": 1})),
        RevokeOperation,
    )
    with pytest.raises(DataError, match="again"):
        operation(parameters, retained, intent, observation)


@pytest.mark.parametrize(
    "method,name,quantity,kind",
    [
        ("supply", "collateral", 1, "approve"),
        ("borrow", "loan", 1, "borrow"),
        ("withdraw_amount", "collateral", 1, "withdraw"),
        ("repay_amount", "loan", 1, "approve"),
    ],
)
def test_partial_actions_keep_the_explicit_target_and_amount(
    parameters, method, name, quantity, kind
):
    intent = getattr(parameters, method)(name, quantity)
    result = operation(parameters, state(parameters, intent), intent, before(parameters, name))
    assert result.kind == kind and result.target == intent.target and result.amount == intent.amount


def test_full_exits_retain_actual_owned_shares_or_collateral(parameters):
    for name in ("earn", "collateral"):
        intent, observation = parameters.withdraw(name), before(parameters, name)
        result = operation(parameters, state(parameters, intent), intent, observation)
        assert result.full
        expected = observation.account.position.supply_shares if name == "earn" else 0
        assert result.shares == expected
        assert result.amount.amount == (
            supplied_assets(observation.account)
            if name == "earn"
            else observation.account.position.collateral
        )


def test_repaid_fund_can_only_clean_up_remaining_allowance(parameters):
    intent, observation = parameters.repay("loan"), before(parameters, "loan")
    account = observation.account.model_copy(
        update={"position": Position(supply_shares=1, borrow_shares=0, collateral=10**18)}
    )
    observation = observation.model_copy(update={"account": account, "allowance": 1})
    assert isinstance(
        operation(parameters, state(parameters, intent), intent, observation), RevokeOperation
    )
    with pytest.raises(DataError, match="closed"):
        operation(
            parameters,
            state(parameters, intent),
            intent,
            observation.model_copy(update={"allowance": 0}),
        )


def test_health_requires_its_oracle_only_while_debt_remains(parameters):
    observation = before(parameters, "loan")
    assert borrowed_assets(observation.account) > 200 * 10**6
    assert health_factor(observation.account) > 2 * 10**18
    market = observation.account.market.model_copy(
        update={"price": None, "price_error": "unavailable"}
    )
    account = observation.account.model_copy(update={"market": market})
    with pytest.raises(ValueError, match="oracle"):
        health_factor(account)
    debt_free = account.model_copy(
        update={"position": Position(supply_shares=0, borrow_shares=0, collateral=10**18)}
    )
    assert health_factor(debt_free) is None
    request = BorrowOperation(
        target=observation.target, amount=AssetAmount(asset=observation.target.asset, amount=1)
    )
    with pytest.raises(ValueError, match="oracle"):
        validate_capacity(parameters, observation.model_copy(update={"account": account}), request)


def test_changed_scope_and_ownership_are_refused(parameters):
    intent, observation = parameters.repay("loan"), before(parameters, "loan")
    with pytest.raises(ValueError, match="scope"):
        operation(parameters, state(parameters, intent), intent, before(parameters, "earn"))
    original = owned(observation)
    validate_ownership(original, observation)
    with pytest.raises(DataError, match="ownership"):
        validate_ownership(original.model_copy(update={"borrow_shares": 0}), observation)
    assert not original.empty
