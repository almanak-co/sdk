"""Configured Spoke and asset correlations are checked against generated Roles calls."""

import pytest

from almanak.connectors.identifiers import ConnectorId
from almanak.gateway.safe_fixture import calldata
from tests.strategy_fork.pool_lending_authority import wrapped
from tests.strategy_fork.uniswap_v4_base_authority import assert_permissions as exchange_permissions


def lending_attacks(config):
    supply, debt = config.allocation("collateral"), config.allocation("loan")
    profile = config.accounts.binding(supply.account).profile
    fund, other = profile.fund.address, profile.operator.address
    first, second = supply.reserve.reserve_id, debt.reserve.reserve_id
    for name, action, reserve, amount, recipient in (
        ("wrong_supply_asset", "supply", second, 1, fund),
        ("wrong_supply_recipient", "supply", first, 1, other),
        ("zero_supply", "supply", first, 0, fund),
        ("wrong_borrow_asset", "borrow", first, 1, fund),
        ("wrong_borrow_recipient", "borrow", second, 1, other),
        ("wrong_repay_asset", "repay", first, 1, fund),
        ("wrong_repay_recipient", "repay", second, 1, other),
        ("zero_repay", "repay", second, 0, fund),
        ("wrong_withdraw_asset", "withdraw", second, 2**256 - 1, fund),
        ("wrong_withdraw_recipient", "withdraw", first, 2**256 - 1, other),
        ("partial_withdraw", "withdraw", first, 1, fund),
    ):
        yield (
            name,
            supply.reserve.spoke,
            calldata(
                action + "(uint256,uint256,address)",
                ("uint256", "uint256", "address"),
                (reserve, amount, recipient),
            ),
        )
    for name, token, spender, amount in (
        ("wrong_approval_target", supply.reserve.hub, supply.reserve.spoke, 1),
        ("wrong_spender", supply.reserve.asset.reference, other, 1),
        ("wrong_repay_spender", debt.reserve.asset.reference, other, 1),
    ):
        yield (
            name,
            token,
            calldata("approve(address,uint256)", ("address", "uint256"), (spender, amount)),
        )
    for name, reserve, recipient in (
        ("wrong_collateral", second, fund),
        ("foreign_collateral", first, other),
    ):
        yield (
            name,
            supply.reserve.spoke,
            calldata(
                "setUsingAsCollateral(uint256,bool,address)",
                ("uint256", "bool", "address"),
                (reserve, True, recipient),
            ),
        )


def assert_permissions(node, config):
    lending = config.component(ConnectorId("aave_v4"))
    profile = lending.accounts.binding("fund").profile
    supply = lending.allocation("collateral")
    approval = calldata(
        "approve(address,uint256)", ("address", "uint256"), (supply.reserve.spoke, 0)
    )
    control = wrapped(profile, supply.reserve.asset.reference, approval)
    node._validate_origin()
    node.rpc("eth_call", [control, "latest"])
    rejected = []
    for name, target, data in lending_attacks(lending):
        request = wrapped(profile, target, data)
        with pytest.raises(RuntimeError, match="0xd0a9bf58"):
            node.rpc("eth_call", [request, "latest"])
        rejected.append({"case": name, "call": request, "error_selector": "0xd0a9bf58"})
    return {
        "lending": {"method": "eth_call", "allowed_control": control, "rejected": rejected},
        "exchange": exchange_permissions(node, config.component(ConnectorId("uniswap_v4"))),
    }
