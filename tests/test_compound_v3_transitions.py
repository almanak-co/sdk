"""Economic principal transitions reproduce independently executed Comet round trips."""

import pytest

from almanak.connectors.compound_v3.operations import (
    ApproveOperation,
    BorrowOperation,
    RepayOperation,
    RevokeOperation,
    SupplyOperation,
    WithdrawOperation,
)
from almanak.connectors.compound_v3.transitions import transition
from almanak.models import AssetAmount
from tests.test_compound_v3_operations import operation, read, target


@pytest.mark.parametrize("network", ["mainnet", "base"])
def test_principal_and_collateral_transitions_match_all_direct_economic_calls(network):
    raw, run = read(network), read(network + "-operations")
    allowances = {}
    for step in run["steps"]:
        op = operation(raw, step, allowances)
        name = step["account"]
        identity = name, op.target.asset
        if isinstance(op, ApproveOperation):
            allowances[identity] = op.amount.amount
            continue
        if isinstance(op, RevokeOperation):
            allowances[identity] = 0
            continue
        before, after = step["before"]["accounts"][name], step["after"]["accounts"][name]
        result = transition(
            before["user"][0], before["collateral"], op, step["after"]["totals"][:2]
        )
        assert result.principal == (after["user"][0] if op.target.reserve.is_base else 0)
        assert result.collateral == (0 if op.target.reserve.is_base else after["collateral"])
        field = "cash" if op.target.reserve.is_base else "weth"
        assert result.assets.amount == abs(after[field] - before[field])
        if isinstance(op, (SupplyOperation, RepayOperation)):
            allowances[identity] -= result.assets.amount
    assert all(value == 0 for value in allowances.values())


def base_target():
    raw, run = read("base"), read("base-operations")
    return target(raw, next(s for s in run["steps"] if s["kind"] == "borrow"))


@pytest.mark.parametrize(
    "kind,principal,amount,full",
    [
        ("supply", -10, 1, False),
        ("borrow", 10, 1, False),
        ("repay", 0, 1, False),
        ("repay", -10, 11, False),
        ("repay", -10, 9, True),
        ("withdraw", -10, 1, False),
        ("withdraw", 10, 11, False),
    ],
)
def test_economic_intent_cannot_cross_supply_and_debt_or_exceed_ownership(
    kind, principal, amount, full
):
    bound = base_target()
    args = dict(target=bound, amount=AssetAmount(asset=bound.asset, amount=amount))
    cls = {
        "supply": SupplyOperation,
        "borrow": BorrowOperation,
        "repay": RepayOperation,
        "withdraw": WithdrawOperation,
    }[kind]
    if kind in ("repay", "withdraw"):
        args["full"] = full
    with pytest.raises(ValueError):
        transition(principal, 0, cls(**args), (10**15, 10**15))


def test_supply_with_no_principal_effect_refuses_and_full_collateral_must_match():
    bound = base_target()
    op = SupplyOperation(target=bound, amount=AssetAmount(asset=bound.asset, amount=1))
    with pytest.raises(ValueError, match="no principal"):
        transition(0, 0, op, (2 * 10**15, 10**15))
    raw, run = read("base"), read("base-operations")
    bound = target(raw, next(s for s in run["steps"] if s["kind"] == "collateral_supply"))
    op = WithdrawOperation(target=bound, amount=AssetAmount(asset=bound.asset, amount=1), full=True)
    with pytest.raises(ValueError, match="owned"):
        transition(0, 2, op, (10**15, 10**15))
