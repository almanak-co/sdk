"""Actual Morpho logs bind transaction effects to market, fund and exact share quantities."""

import copy
from types import SimpleNamespace

import pytest

from almanak.connectors.morpho_blue.receipts import (
    Movement,
    allowance_after,
    movement,
    validate_effects,
)
from almanak.gateway.ethereum import DataError
from tests.test_morpho_blue_math import position, recorded
from tests.test_morpho_blue_operations import captured_operation, target_for


@pytest.mark.parametrize("chain", [1, 8453])
def test_all_native_events_reconcile_owned_shares_and_collateral(chain):
    raw = recorded(f"{chain}-operations")
    for step in raw["steps"]:
        operation = captured_operation(target_for(raw, step), step)
        result = movement(step["receipt"], operation)
        account = step["account"]
        old = position(step["before"]["accounts"][account]["position"])
        new = position(step["after"]["accounts"][account]["position"])
        # Allowance arithmetic is independent of the raw market-state probe.
        # Protocol events and before/after owned balances are actual fork evidence.
        previous_allowance = result.assets
        before = SimpleNamespace(
            account=SimpleNamespace(position=old), allowance=previous_allowance
        )
        after = SimpleNamespace(
            account=SimpleNamespace(position=new),
            allowance=allowance_after(operation, previous_allowance, result.assets),
        )
        deltas = validate_effects(
            SimpleNamespace(before=before, operation=operation), after, result, True
        )
        assert deltas == (
            new.supply_shares - old.supply_shares,
            new.borrow_shares - old.borrow_shares,
            new.collateral - old.collateral,
        )
        token = "loan" if operation.target.reserve.is_loan else "collateral"
        wallet_delta = (
            step["after"]["accounts"][account][token] - step["before"]["accounts"][account][token]
        )
        assert result.assets == abs(wallet_delta)


@pytest.mark.parametrize("fault", ["market", "owner", "caller", "quantity", "duplicate"])
def test_receipts_refuse_cross_market_fund_or_ambiguous_attribution(fault):
    raw = recorded("1-operations")
    step = next(step for step in raw["steps"] if step["kind"] == "borrow")
    operation = captured_operation(target_for(raw, step), step)
    receipt = copy.deepcopy(step["receipt"])
    event = next(
        event
        for event in receipt["logs"]
        if event["address"].lower() == raw["morpho"] and len(event["topics"]) == 4
    )
    if fault == "market":
        event["topics"][1] = "0x" + "00" * 32
    elif fault == "owner":
        event["topics"][2] = "0x" + "00" * 12 + "12" * 20
    elif fault == "caller":
        event["data"] = "0x" + "00" * 12 + "12" * 20 + event["data"][66:]
    elif fault == "quantity":
        event["data"] = event["data"][:66] + (1).to_bytes(32).hex() + event["data"][130:]
    else:
        receipt["logs"].append(copy.deepcopy(event))
    with pytest.raises(DataError):
        movement(receipt, operation)


def test_full_repayment_cannot_change_shares_or_exceed_its_ceiling():
    raw = recorded("1-operations")
    step = next(step for step in raw["steps"] if step["kind"] == "full_repay")
    operation = captured_operation(target_for(raw, step), step)
    with pytest.raises(DataError, match="share count"):
        movement(step["receipt"], operation.model_copy(update={"shares": operation.shares + 1}))
    amount = operation.amount.model_copy(update={"amount": 1})
    with pytest.raises(DataError, match="ceiling"):
        movement(step["receipt"], operation.model_copy(update={"amount": amount}))


def test_failed_receipt_cannot_change_fund_ownership():
    raw = recorded("1-operations")
    step = next(step for step in raw["steps"] if step["kind"] == "borrow")
    operation = captured_operation(target_for(raw, step), step)
    old = position(step["before"]["accounts"]["borrower"]["position"])
    before = SimpleNamespace(account=SimpleNamespace(position=old), allowance=0)
    pending = SimpleNamespace(before=before, operation=operation)
    assert validate_effects(pending, before, Movement(assets=0, shares=0), False) == (0, 0, 0)
    with pytest.raises(DataError, match="failed"):
        validate_effects(pending, before, Movement(assets=1, shares=1), False)
