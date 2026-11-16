"""Attempt malformed calls against freshly generated, installed Roles authority."""

import pytest

from almanak.gateway.safe_fixture import calldata
from almanak.models import SafeRoles


def forbidden_calls(config):
    collateral, loan = config.allocation("collateral"), config.allocation("loan")
    profile = config.accounts.binding(collateral.account).profile
    pool, fund, operator = collateral.reserve.pool, profile.fund.address, profile.submitter.address
    weth, usdc = collateral.reserve.asset.reference, loan.reserve.asset.reference
    calls = []
    for label, asset, amount, recipient, referral in (
        # Every allocation may supply its own reserve, so another asset is an
        # unconfigured token.
        ("supply_other_asset", operator, 1, fund, 0),
        ("supply_other_recipient", weth, 1, operator, 0),
        ("zero_supply", weth, 0, fund, 0),
        ("supply_referral", weth, 1, fund, 1),
    ):
        calls.append(
            (
                label,
                pool,
                calldata(
                    "supply(address,uint256,address,uint16)",
                    ("address", "uint256", "address", "uint16"),
                    (asset, amount, recipient, referral),
                ),
                0,
                0,
            )
        )
    for label, asset, amount, mode, recipient, referral in (
        ("borrow_other_asset", weth, 1, 2, fund, 0),
        ("borrow_other_recipient", usdc, 1, 2, operator, 0),
        ("borrow_other_rate_mode", usdc, 1, 1, fund, 0),
        ("borrow_referral", usdc, 1, 2, fund, 1),
    ):
        calls.append(
            (
                label,
                pool,
                calldata(
                    "borrow(address,uint256,uint256,uint16,address)",
                    ("address", "uint256", "uint256", "uint16", "address"),
                    (asset, amount, mode, referral, recipient),
                ),
                0,
                0,
            )
        )
    calls.extend(exit_calls(config))
    for label, token, spender, amount in (
        ("approval_other_spender", weth, operator, 1),
        ("loan_approval_other_spender", usdc, operator, 1),
    ):
        calls.append(
            (
                label,
                token,
                calldata(
                    "approve(address,uint256)",
                    ("address", "uint256"),
                    (spender, amount),
                ),
                0,
                0,
            )
        )
    valid = calldata("approve(address,uint256)", ("address", "uint256"), (pool, 1))
    calls.extend(
        (
            ("delegatecall", weth, valid, 0, 1),
            ("native_value", weth, valid, 1, 0),
            ("other_target", operator, valid, 0, 0),
        )
    )
    return calls


def exit_calls(config):
    collateral, loan = config.allocation("collateral"), config.allocation("loan")
    profile = config.accounts.binding(collateral.account).profile
    pool, fund, operator = collateral.reserve.pool, profile.fund.address, profile.submitter.address
    weth, usdc = collateral.reserve.asset.reference, loan.reserve.asset.reference
    calls = []
    for label, asset, amount, mode, recipient in (
        ("repay_other_asset", weth, 1, 2, fund),
        ("zero_repay", usdc, 0, 2, fund),
        ("repay_other_rate_mode", usdc, 1, 1, fund),
        ("repay_other_fund", usdc, 1, 2, operator),
    ):
        calls.append(
            (
                label,
                pool,
                calldata(
                    "repay(address,uint256,uint256,address)",
                    ("address", "uint256", "uint256", "address"),
                    (asset, amount, mode, recipient),
                ),
                0,
                0,
            )
        )
    for label, asset, recipient in (
        ("withdraw_other_asset", operator, fund),
        ("withdraw_other_recipient", weth, operator),
    ):
        calls.append(
            (
                label,
                pool,
                calldata(
                    "withdraw(address,uint256,address)",
                    ("address", "uint256", "address"),
                    (asset, 1, recipient),
                ),
                0,
                0,
            )
        )
    calls.append(
        (
            "collateral_other_asset",
            pool,
            calldata(
                "setUserUseReserveAsCollateral(address,bool)",
                ("address", "bool"),
                (usdc, True),
            ),
            0,
            0,
        )
    )
    return calls


def wrapped(profile, target, inner, value=0, operation=0):
    return {
        "from": profile.operator.address,
        "to": profile.roles_module,
        "data": calldata(
            "execTransactionWithRole(address,uint256,bytes,uint8,bytes32,bool)",
            ("address", "uint256", "bytes", "uint8", "bytes32", "bool"),
            (
                target,
                value,
                bytes.fromhex(inner[2:]),
                operation,
                bytes.fromhex(profile.role_key[2:]),
                True,
            ),
        ),
    }


def assert_permissions(node, config):
    profile = config.accounts.binding("fund").profile
    assert isinstance(profile, SafeRoles)
    node._validate_origin()
    collateral = config.allocation("collateral")
    approval = calldata(
        "approve(address,uint256)",
        ("address", "uint256"),
        (collateral.reserve.pool, 1),
    )
    control = wrapped(profile, collateral.reserve.asset.reference, approval)
    node.rpc("eth_call", [control, "latest"])
    checks = []
    for label, target, inner, value, operation in forbidden_calls(config):
        call = wrapped(profile, target, inner, value, operation)
        # Roles' ConditionViolation must reject the call before a Pool error
        # could disguise a missing recipient, asset, zero-amount or call-type bound.
        with pytest.raises(RuntimeError, match="0xd0a9bf58"):
            node.rpc("eth_call", [call, "latest"])
        checks.append({"case": label, "call": call, "error_selector": "0xd0a9bf58"})
    return {"method": "eth_call", "allowed_control": control, "rejected": checks}
