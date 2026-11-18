"""Typed Morpho call encodings match independently executed native transactions."""

import json

import pytest
from eth_abi import decode, encode
from eth_utils import keccak

from almanak.caip import AssetType
from almanak.connectors.morpho_blue.domain import PARAMS_ABI, Market, MarketId, Reserve, Target
from almanak.connectors.morpho_blue.operations import (
    ApproveOperation,
    BorrowOperation,
    RepayOperation,
    RevokeOperation,
    SupplyOperation,
    WithdrawOperation,
    plan_call,
)
from almanak.models import Account, AssetAmount
from tests.test_morpho_blue_math import recorded


def target_for(raw, step):
    fund = Account.model_validate_json(json.dumps(step["call"]["fund"]))
    params = raw["params"]
    market = Market(
        chain=fund.chain,
        core=raw["morpho"],
        market_id=MarketId("0x" + keccak(encode([PARAMS_ABI], [params])).hex()),
        loan_asset=AssetType(f"{fund.chain.chain_id}/erc20:{params[0]}"),
        collateral_asset=AssetType(f"{fund.chain.chain_id}/erc20:{params[1]}"),
        oracle=params[2],
        irm=params[3],
        lltv=params[4],
    )
    kind = step["kind"]
    collateral = (
        "collateral" in kind or kind in ("approve", "revoke") and step["call"]["to"] == params[1]
    )
    asset = market.collateral_asset if collateral else market.loan_asset
    return Target(
        allocation="position",
        account=step["account"],
        fund=fund,
        reserve=Reserve(market=market, asset=asset),
    )


def captured_quantities(target, step):
    kind = step["kind"]
    callback = kind in ("loan_supply", "collateral_supply", "partial_repay", "full_repay")
    types = (
        [PARAMS_ABI, "uint256"]
        + (["uint256"] if target.reserve.is_loan else [])
        + ["address", "bytes" if callback else "address"]
    )
    args = decode(types, bytes.fromhex(step["call"]["data"][10:]))
    quantity, shares = args[1], args[2] if target.reserve.is_loan else 0
    if not quantity:
        before = step["before"]["accounts"][step["account"]]["loan"]
        after = step["after"]["accounts"][step["account"]]["loan"]
        quantity = abs(after - before)
    return AssetAmount(asset=target.asset, amount=quantity), shares


def captured_operation(target, step):
    kind = step["kind"]
    if kind in ("approve", "revoke"):
        quantity = decode(["address", "uint256"], bytes.fromhex(step["call"]["data"][10:]))[1]
        return (
            ApproveOperation(
                target=target,
                amount=AssetAmount(asset=target.asset, amount=quantity),
                purpose="supply",
            )
            if quantity
            else RevokeOperation(target=target)
        )
    amount, shares = captured_quantities(target, step)
    if kind in ("loan_supply", "collateral_supply"):
        return SupplyOperation(target=target, amount=amount)
    if kind == "borrow":
        return BorrowOperation(target=target, amount=amount)
    if "repay" in kind:
        return RepayOperation(target=target, amount=amount, shares=shares)
    return WithdrawOperation(
        target=target, amount=amount, full=kind.startswith("full"), shares=shares
    )


@pytest.mark.parametrize("chain", [1, 8453])
def test_all_native_calldata_and_fund_routing_match_typed_operations(chain):
    raw = recorded(f"{chain}-operations")
    for step in raw["steps"]:
        operation = captured_operation(target_for(raw, step), step)
        assert plan_call(operation).model_dump(mode="json") == step["call"]
        assert operation.account == step["account"]
        assert operation.allocation == "position"


def test_loan_actions_cannot_use_collateral_and_full_exits_require_exact_shares():
    raw = recorded("1-operations")
    step = next(step for step in raw["steps"] if step["kind"] == "collateral_supply")
    target = target_for(raw, step)
    amount = AssetAmount(asset=target.asset, amount=1)
    for cls in (BorrowOperation, RepayOperation):
        with pytest.raises(ValueError, match="loan asset"):
            cls(target=target, amount=amount)
    with pytest.raises(ValueError, match="retained supply shares"):
        WithdrawOperation(target=target, amount=amount, full=True, shares=1)
    loan = target_for(raw, next(step for step in raw["steps"] if step["kind"] == "loan_supply"))
    with pytest.raises(ValueError, match="retained supply shares"):
        WithdrawOperation(target=loan, amount=AssetAmount(asset=loan.asset, amount=1), full=True)
    with pytest.raises(ValueError, match="uint128"):
        SupplyOperation(target=target, amount=AssetAmount(asset=target.asset, amount=2**128))


@pytest.mark.parametrize("chain", [1, 8453])
def test_all_economic_transitions_match_native_market_and_owned_balances(chain):
    from almanak.connectors.morpho_blue.accounting import accrue
    from almanak.connectors.morpho_blue.transitions import transition
    from tests.test_morpho_blue_math import market, position

    raw = recorded(f"{chain}-operations")
    for step in raw["steps"]:
        if step["kind"] in ("approve", "revoke"):
            continue
        operation = captured_operation(target_for(raw, step), step)
        before, after = step["before"], step["after"]
        accounting = market(before["market"])
        if step["kind"] != "collateral_supply":
            accrued = accrue(accounting, before["rate_at_target"], after["block"]["timestamp"])
            accounting = accrued.accounting
            assert accrued.rate_at_target == after["rate_at_target"]
        result = transition(
            accounting, position(before["accounts"][step["account"]]["position"]), operation
        )
        assert result.accounting == market(after["market"])
        assert result.position == position(after["accounts"][step["account"]]["position"])
        token = "loan" if operation.target.reserve.is_loan else "collateral"
        wallet_delta = (
            after["accounts"][step["account"]][token] - before["accounts"][step["account"]][token]
        )
        assert result.assets == abs(wallet_delta)


def test_full_share_exit_can_clear_zero_asset_dust():
    from almanak.connectors.morpho_blue.domain import Accounting, Position
    from almanak.connectors.morpho_blue.transitions import transition

    raw = recorded("1-operations")
    step = next(step for step in raw["steps"] if step["kind"] == "loan_supply")
    target = target_for(raw, step)
    operation = WithdrawOperation(
        target=target, amount=AssetAmount(asset=target.asset, amount=0), full=True, shares=1
    )
    accounting = Accounting(
        supply_assets=0, supply_shares=1, borrow_assets=0, borrow_shares=0, last_update=1, fee=0
    )
    result = transition(
        accounting, Position(supply_shares=1, borrow_shares=0, collateral=0), operation
    )
    assert result.assets == result.position.supply_shares == result.accounting.supply_shares == 0
    assert plan_call(operation).data
    with pytest.raises(ValueError, match="quantity or owned shares"):
        WithdrawOperation(
            target=target, amount=AssetAmount(asset=target.asset, amount=0), full=False
        )
