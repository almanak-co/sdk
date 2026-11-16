"""Protocol control and risk refusals beside retained successful Comet calls."""

from types import SimpleNamespace

import pytest

from almanak.connectors.compound_v3.capacity import borrow_bounds, paused, validate
from almanak.connectors.compound_v3.operations import (
    ApproveOperation,
    RepayOperation,
    RevokeOperation,
    SupplyOperation,
)
from almanak.connectors.compound_v3.projection import health_factor, projected_account
from almanak.gateway.ethereum import DataError
from tests.test_compound_v3_operations import operation, read
from tests.test_compound_v3_projection import snapshot_input

CONFIG = SimpleNamespace(
    positions=(), allocation=lambda name: SimpleNamespace(minimum_health_factor=2 * 10**18)
)


def examples(network):
    raw, run = read(network), read(network + "-operations")
    allowances = {}
    for step in run["steps"]:
        op = operation(raw, step, allowances)
        identity = step["account"], op.target.asset
        if isinstance(op, ApproveOperation):
            allowances[identity] = op.amount.amount
        elif isinstance(op, RevokeOperation):
            allowances[identity] = 0
        else:
            yield step["kind"], snapshot_input(network, step, op, allowances.get(identity, 0)), op
            if isinstance(op, (SupplyOperation, RepayOperation)):
                field = "cash" if op.target.reserve.is_base else "weth"
                allowances[identity] -= (
                    step["before"]["accounts"][step["account"]][field]
                    - step["after"]["accounts"][step["account"]][field]
                )


def example(kind, network="base"):
    return next((before, op) for name, before, op in examples(network) if name == kind)


def market_change(before, **changes):
    return before.model_copy(
        update={
            "account": before.account.model_copy(
                update={"market": before.account.market.model_copy(update=changes)}
            )
        }
    )


@pytest.mark.parametrize("network", ["mainnet", "base"])
def test_retained_economic_states_pass_with_exact_sdk_allowances_and_risk(network):
    for _, before, op in examples(network):
        if (
            isinstance(op, (SupplyOperation, RepayOperation))
            and before.allowance != op.amount.amount
        ):
            with pytest.raises(DataError, match="allowance"):
                validate(CONFIG, before, op)
            # Direct calibration retained a larger approval across partial repayment.
            # SDK planning instead requires the exact current economic bound.
            before = before.model_copy(update={"allowance": op.amount.amount})
        after = validate(CONFIG, before, op)
        assert after.fund == before.target.fund
        if after.principal < 0:
            assert health_factor(after) >= 2 * 10**18
        else:
            assert health_factor(after) is None


@pytest.mark.parametrize(
    "kind,flag",
    [
        ("base_supply", 0),
        ("collateral_supply", 0),
        ("partial_repay", 0),
        ("borrow", 2),
        ("partial_base_withdraw", 2),
        ("partial_collateral_release", 2),
    ],
)
def test_global_pause_controls_cover_multipurpose_supply_and_withdraw_abi(kind, flag):
    before, op = example(kind)
    after = projected_account(before, op)
    changed = market_change(
        before,
        accounting=before.account.market.accounting.model_copy(update={"pause_flags": 1 << flag}),
    )
    with pytest.raises(DataError, match="paused"):
        paused(changed, op, after)


@pytest.mark.parametrize(
    "kind,flag",
    [
        ("base_supply", 3),
        ("partial_repay", 3),
        ("collateral_supply", 2),
        ("borrow", 1),
        ("partial_base_withdraw", 0),
        ("partial_collateral_release", 7),
    ],
)
def test_extended_pause_controls_keep_lender_borrower_and_collateral_meanings(kind, flag):
    before, op = example(kind)
    after = projected_account(before, op)
    changed = market_change(
        before,
        controls=before.account.market.controls.model_copy(update={"pause_flags": 1 << flag}),
    )
    with pytest.raises(DataError, match="paused"):
        paused(changed, op, after)


@pytest.mark.parametrize(
    "kind,field",
    [
        ("collateral_supply", "collateral_supply_flags"),
        ("partial_collateral_release", "collateral_withdraw_flags"),
    ],
)
def test_per_asset_collateral_pause_is_correlated_to_its_offset(kind, field):
    before, op = example(kind)
    index = next(
        c.offset for c in before.account.market.collaterals if c.asset_type == op.target.asset
    )
    changed = market_change(
        before, controls=before.account.market.controls.model_copy(update={field: 1 << index})
    )
    with pytest.raises(DataError, match="paused"):
        paused(changed, op, projected_account(before, op))


def test_funding_allowance_supply_cap_and_borrow_minimum_refuse():
    before, op = example("collateral_supply")
    with pytest.raises(DataError, match="inventory"):
        validate(CONFIG, before.model_copy(update={"wallet_balance": 0}), op)
    with pytest.raises(DataError, match="allowance"):
        validate(CONFIG, before.model_copy(update={"allowance": 1}), op)
    collaterals = tuple(
        c.model_copy(update={"supply_cap": c.total_supply})
        if c.asset_type == op.target.asset
        else c
        for c in before.account.market.collaterals
    )
    with pytest.raises(DataError, match="cap"):
        validate(CONFIG, market_change(before, collaterals=collaterals), op)
    before, op = example("borrow")
    with pytest.raises(DataError, match="minimum"):
        validate(CONFIG, market_change(before, base_borrow_minimum=op.amount.amount + 1), op)
    strict = SimpleNamespace(
        positions=(), allocation=lambda name: SimpleNamespace(minimum_health_factor=100 * 10**18)
    )
    with pytest.raises(DataError, match="health"):
        validate(strict, before, op)
    after = projected_account(before, op)
    market = after.market.model_copy(
        update={
            "accounting": after.market.accounting.model_copy(update={"total_supply_principal": 1})
        }
    )
    with pytest.raises(DataError, match="utilization"):
        borrow_bounds(before, op, after.model_copy(update={"market": market}))


def test_full_repay_requires_the_actual_accrued_debt_within_its_approval():
    before, op = example("full_repay")
    assert before.account.borrowed_base < op.amount.amount
    funded = before.model_copy(update={"wallet_balance": before.account.borrowed_base})
    assert validate(CONFIG, funded, op).principal == 0
    with pytest.raises(DataError, match="needs 1 more"):
        validate(
            CONFIG, funded.model_copy(update={"wallet_balance": funded.wallet_balance - 1}), op
        )
