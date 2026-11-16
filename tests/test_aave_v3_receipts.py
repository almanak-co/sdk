"""Pinned Safe receipts bind economic events to the exact fund and scaled effects."""

from copy import deepcopy

import pytest
from eth_abi import encode

from almanak.connectors.aave_v3.receipts import (
    accounting_issues,
    movement,
    validate_effects,
)
from almanak.gateway.ethereum import DataError
from tests.aave_v3_helpers import recorded_v3 as recorded_v3


def test_recorded_contract_effects_match_their_typed_outcomes(recorded_v3):
    state = recorded_v3
    for outcome in state.outcomes:
        assert movement(outcome.receipt, outcome.operation) == outcome.assets
        assert validate_effects(outcome, outcome.after, outcome.assets, outcome.success) == (
            outcome.supply_delta,
            outcome.debt_delta,
        )
        assert (
            accounting_issues(
                outcome.before,
                outcome.after,
                outcome.operation,
                outcome.assets,
                outcome.gas_paid.amount,
            )
            == ()
        )


@pytest.mark.parametrize("kind", ["approve", "supply", "borrow", "repay", "withdraw", "collateral"])
def test_duplicate_or_foreign_events_cannot_supply_accounting_evidence(recorded_v3, kind):
    outcome = next(o for o in recorded_v3.outcomes if o.operation.kind == kind)
    receipt = deepcopy(outcome.receipt)
    address = (
        outcome.operation.target.asset.reference
        if kind == "approve"
        else outcome.operation.target.reserve.pool
    )
    logs = [log for log in receipt["logs"] if log["address"].lower() == address]
    receipt["logs"].extend(deepcopy(logs))
    with pytest.raises(DataError, match="exactly one"):
        movement(receipt, outcome.operation)
    receipt = deepcopy(outcome.receipt)
    for log in receipt["logs"]:
        if log["address"].lower() == address and len(log["topics"]) > 2:
            log["topics"][2] = "0x" + encode(("address",), ("0x" + "12" * 20,)).hex()
    with pytest.raises(DataError, match="another|differs"):
        movement(receipt, outcome.operation)


@pytest.mark.parametrize("field", ["supply", "debt", "allowance", "collateral"])
def test_receipt_cannot_overwrite_unexplained_position_changes(recorded_v3, field):
    outcome = next(o for o in recorded_v3.outcomes if o.operation.kind == "borrow")
    after = outcome.after
    position = after.position
    if field == "allowance":
        after = after.model_copy(update={"allowance": after.allowance + 1})
    else:
        attribute = {
            "supply": "supplied_scaled",
            "debt": "debt_scaled",
            "collateral": "collateral_enabled",
        }[field]
        value = (
            not position.collateral_enabled
            if field == "collateral"
            else getattr(position, attribute) + 1
        )
        after = after.model_copy(
            update={"position": position.model_copy(update={attribute: value})}
        )
    with pytest.raises(DataError, match="reconcile|collateral flag"):
        validate_effects(outcome, after, outcome.assets, True)


def test_external_fund_gas_and_other_reserve_changes_remain_explicit(recorded_v3):
    outcome = next(o for o in recorded_v3.outcomes if o.operation.kind == "borrow")
    after = outcome.after.model_copy(
        update={
            "wallet_balance": outcome.after.wallet_balance + 1,
            "native_balance": outcome.after.native_balance.model_copy(
                update={"amount": outcome.after.native_balance.amount + 1}
            ),
            "position": outcome.after.position.model_copy(
                update={
                    "user_configuration": outcome.after.position.user_configuration | (1 << 200),
                }
            ),
        }
    )
    issues = accounting_issues(
        outcome.before, after, outcome.operation, outcome.assets, outcome.gas_paid.amount
    )
    assert len(issues) == 3
    assert any("fund token" in issue for issue in issues)
    assert any("gas payer" in issue for issue in issues)
    assert any("outside" in issue for issue in issues)
