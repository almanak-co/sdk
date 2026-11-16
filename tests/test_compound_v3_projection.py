"""Compare projections assembled from retained market terms and actual transaction states."""

import json

import pytest

from almanak.connectors.compound_v3.accounting import indices, validate_account
from almanak.connectors.compound_v3.domain import (
    AccountObservation,
    CollateralBalance,
    MarketObservation,
)
from almanak.connectors.compound_v3.math import present_value
from almanak.connectors.compound_v3.observations import Snapshot
from almanak.connectors.compound_v3.operations import (
    ApproveOperation,
    RepayOperation,
    RevokeOperation,
    SupplyOperation,
)
from almanak.connectors.compound_v3.projection import projected_account
from almanak.gateway.evm import native_asset
from almanak.models import AssetAmount
from tests.test_compound_v3_operations import operation, read
from tests.test_compound_v3_reader import recorded


def market_input(network, step):
    _, data = recorded(network, False)
    payload = data["market"]
    block = step["after"]["block"]

    def scope(value):
        if isinstance(value, dict):
            if "chain_id" in value:
                return block["chain"]
            if set(("chain", "number", "hash", "timestamp")) <= value.keys():
                return block
            return {k: scope(v) for k, v in value.items()}
        if isinstance(value, list):
            return [scope(v) for v in value]
        return value

    market = MarketObservation.model_validate_json(json.dumps(scope(payload)))
    totals = step["before"]["totals"]
    keys = (
        "supply_index",
        "borrow_index",
        "tracking_supply_index",
        "tracking_borrow_index",
        "total_supply_principal",
        "total_borrow_principal",
        "last_accrual_time",
        "pause_flags",
    )
    cash_delta = 2000 * 10**6 - sum(a["cash"] for a in step["before"]["accounts"].values())
    collaterals = tuple(
        c.model_copy(
            update={
                "total_supply": c.total_supply
                + sum(a["collateral"] for a in step["before"]["accounts"].values())
            }
        )
        if c.asset_type.reference
        == (
            "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
            if network == "mainnet"
            else "0x4200000000000000000000000000000000000006"
        )
        else c
        for c in market.collaterals
    )
    return market.model_copy(
        update={
            "accounting": market.accounting.model_copy(update=dict(zip(keys, totals, strict=True))),
            "base_available": market.base_available + cash_delta,
            "collaterals": collaterals,
        }
    )


def snapshot_input(network, step, op, allowance):
    market = market_input(network, step)
    raw = step["before"]["accounts"][step["account"]]
    weth = (
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
        if network == "mainnet"
        else "0x4200000000000000000000000000000000000006"
    )
    principal, tracking, accrued, bitmap, extended = raw["user"]
    balance = present_value(principal, *indices(market, market.block.timestamp))
    account = AccountObservation(
        market=market,
        fund=op.target.fund,
        principal=principal,
        tracking_index=tracking,
        tracking_accrued=accrued,
        assets_in=bitmap,
        extended_assets_in=extended,
        supplied_base=max(0, balance),
        borrowed_base=max(0, -balance),
        collaterals=tuple(
            CollateralBalance(
                asset=c.asset_type,
                balance=raw["collateral"] if c.asset_type.reference == weth else 0,
            )
            for c in market.collaterals
        ),
    )
    return Snapshot(
        target=op.target,
        account=account,
        wallet_balance=raw["cash"] if op.target.reserve.is_base else raw["weth"],
        allowance=allowance,
        payer=op.target.fund,
        native_balance=AssetAmount(asset=native_asset(op.target.fund.chain.chain_id), amount=0),
    )


@pytest.mark.parametrize("network", ["mainnet", "base"])
def test_projection_matches_recorded_indices_principal_totals_and_collateral(network):
    raw, run = read(network), read(network + "-operations")
    allowances = {}
    for step in run["steps"]:
        op = operation(raw, step, allowances)
        identity = step["account"], op.target.asset
        if isinstance(op, ApproveOperation):
            allowances[identity] = op.amount.amount
            continue
        if isinstance(op, RevokeOperation):
            allowances[identity] = 0
            continue
        before = snapshot_input(network, step, op, allowances.get(identity, 0))
        after = projected_account(before, op)
        expected = step["after"]["accounts"][step["account"]]
        assert after.principal == expected["user"][0]
        assert sum(c.balance for c in after.collaterals) == expected["collateral"]
        assert (after.assets_in, after.extended_assets_in) == tuple(expected["user"][3:])
        assert (
            after.market.accounting.supply_index,
            after.market.accounting.borrow_index,
        ) == tuple(step["after"]["totals"][:2])
        assert (
            after.market.accounting.total_supply_principal,
            after.market.accounting.total_borrow_principal,
        ) == tuple(step["after"]["totals"][4:6])
        assert (
            after.supplied_base == expected["supply"] and after.borrowed_base == expected["borrow"]
        )
        validate_account(after)
        if isinstance(op, (SupplyOperation, RepayOperation)):
            field = "cash" if op.target.reserve.is_base else "weth"
            allowances[identity] -= (
                step["before"]["accounts"][step["account"]][field] - expected[field]
            )
