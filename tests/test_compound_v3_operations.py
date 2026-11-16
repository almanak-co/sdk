"""Typed Comet operations reproduce independently executed direct protocol calls."""

import gzip
import json
from pathlib import Path

import pytest
from eth_abi import decode

from almanak.caip import AssetType
from almanak.connectors.compound_v3 import operations as ops
from almanak.connectors.compound_v3.domain import Market, Reserve, Target
from almanak.gateway.accounts import ContractCall
from almanak.models import AssetAmount
from tests.retained_fixtures import retained

ROOT = Path(__file__).parents[1]
EVIDENCE = retained("2026-09-07-rollout/compound-v3-discovery")


def read(name):
    return json.loads(gzip.decompress((EVIDENCE / f"{name}.json.gz").read_bytes()))


def target(raw, step):
    call = ContractCall.model_validate_json(json.dumps(step["call"]))
    token, _ = decode(("address", "uint256"), bytes.fromhex(call.data[10:]))
    if step["kind"] in ("approve", "revoke"):
        token = call.to
    market = Market(
        chain=call.fund.chain,
        comet=raw["comet"],
        base_asset=AssetType(raw["base"]["identifier"]),
        base_price_feed=raw["addresses"]["baseTokenPriceFeed"],
    )
    feed = (
        market.base_price_feed
        if token == market.base_asset.reference
        else next(c["info"][2] for c in raw["collateral"] if c["info"][1] == token)
    )
    reserve = Reserve(
        market=market,
        asset=AssetType(f"{market.chain.chain_id}/erc20:{token}"),
        price_feed=feed,
    )
    return Target(
        allocation=step["account"] + "_" + token,
        account=step["account"],
        fund=call.fund,
        reserve=reserve,
    )


def operation(raw, step, allowances):
    bound = target(raw, step)
    name, kind = step["account"], step["kind"]
    _, quantity = decode(("address", "uint256"), bytes.fromhex(step["call"]["data"][10:]))
    if kind == "revoke":
        return ops.RevokeOperation(target=bound)
    if kind == "approve":
        purpose = "repay" if name == "borrower" and bound.reserve.is_base else "supply"
        return ops.ApproveOperation(
            target=bound,
            amount=AssetAmount(asset=bound.asset, amount=quantity),
            purpose=purpose,
        )
    full = kind.startswith("full_")
    if full:
        field = "cash" if bound.reserve.is_base else "weth"
        quantity = abs(
            step["after"]["accounts"][name][field] - step["before"]["accounts"][name][field]
        )
    amount = AssetAmount(asset=bound.asset, amount=quantity)
    if kind in ("base_supply", "collateral_supply"):
        return ops.SupplyOperation(target=bound, amount=amount)
    if kind == "borrow":
        return ops.BorrowOperation(target=bound, amount=amount)
    if kind in ("partial_repay", "full_repay"):
        if full:
            amount = amount.model_copy(update={"amount": allowances[(name, bound.asset)]})
        return ops.RepayOperation(target=bound, amount=amount, full=full)
    return ops.WithdrawOperation(target=bound, amount=amount, full=full)


@pytest.mark.parametrize("network", ["mainnet", "base"])
def test_every_direct_roundtrip_call_keeps_fund_asset_and_comet_scope(network):
    raw, run = read(network), read(network + "-operations")
    allowances = {}
    for step in run["steps"]:
        op = operation(raw, step, allowances)
        assert ops.plan_call(op) == ContractCall.model_validate_json(json.dumps(step["call"]))
        identity = step["account"], op.target.asset
        if isinstance(op, ops.ApproveOperation):
            allowances[identity] = op.amount.amount
        elif isinstance(op, ops.RevokeOperation):
            allowances[identity] = 0
        elif isinstance(op, (ops.SupplyOperation, ops.RepayOperation)):
            field = "cash" if op.target.reserve.is_base else "weth"
            before = step["before"]["accounts"][step["account"]][field]
            after = step["after"]["accounts"][step["account"]][field]
            allowances[identity] -= before - after
    assert all(value == 0 for value in allowances.values())


def test_collateral_cannot_be_borrowed_and_explicit_amounts_cannot_be_sentinels():
    raw, run = read("mainnet"), read("mainnet-operations")
    bound = target(raw, next(s for s in run["steps"] if s["kind"] == "collateral_supply"))
    with pytest.raises(ValueError, match="market base"):
        ops.BorrowOperation(target=bound, amount=AssetAmount(asset=bound.asset, amount=1))
    with pytest.raises(ValueError, match="sentinel"):
        ops.SupplyOperation(target=bound, amount=AssetAmount(asset=bound.asset, amount=2**256 - 1))
