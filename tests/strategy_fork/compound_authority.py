"""Probe installed Comet Roles bounds without confusing economic and ABI authority."""

import pytest

from almanak.gateway.safe_fixture import calldata
from almanak.models import SafeRoles
from tests.strategy_fork.pool_lending_authority import wrapped


def forbidden_calls(config, account):
    calls = []
    profile = config.accounts.binding(account).profile
    for allocation in config.positions:
        if allocation.account != account:
            continue
        comet, token = allocation.reserve.market.comet, allocation.reserve.asset.reference
        # Scope-only grants admit any nonzero amount of the allocation's own asset,
        # including the sentinels, so the probes test zero amounts and other assets.
        for name in ("supply", "withdraw"):
            for label, asset, amount in (
                ("zero", token, 0),
                ("other_asset", profile.submitter.address, 1),
            ):
                calls.append(
                    (
                        allocation.name + "_" + name + "_" + label,
                        comet,
                        calldata(
                            name + "(address,uint256)", ("address", "uint256"), (asset, amount)
                        ),
                        0,
                        0,
                    )
                )
        calls.append(
            (
                allocation.name + "_other_spender",
                token,
                calldata(
                    "approve(address,uint256)",
                    ("address", "uint256"),
                    (profile.submitter.address, 1),
                ),
                0,
                0,
            )
        )
        for name in ("supplyTo", "withdrawTo"):
            calls.append(
                (
                    allocation.name + "_" + name,
                    comet,
                    calldata(
                        name + "(address,address,uint256)",
                        ("address", "address", "uint256"),
                        (profile.submitter.address, token, 1),
                    ),
                    0,
                    0,
                )
            )
        valid = calldata("approve(address,uint256)", ("address", "uint256"), (comet, 1))
        calls.extend(
            (
                (allocation.name + "_delegatecall", token, valid, 0, 1),
                (allocation.name + "_native_value", token, valid, 1, 0),
            )
        )
    market = config.allocation(
        next(a.name for a in config.positions if a.account == account)
    ).reserve.market
    calls.append(
        (
            "manager_authority",
            market.comet,
            calldata("allow(address,bool)", ("address", "bool"), (profile.submitter.address, True)),
            0,
            0,
        )
    )
    return calls


def assert_permissions(node, config):
    node._validate_origin()
    controls, checks = [], []
    for binding in config.accounts.bindings:
        profile = binding.profile
        assert isinstance(profile, SafeRoles)
        allocation = next(a for a in config.positions if a.account == binding.name)
        approval = calldata(
            "approve(address,uint256)", ("address", "uint256"), (allocation.reserve.market.comet, 1)
        )
        control = wrapped(profile, allocation.reserve.asset.reference, approval)
        node.rpc("eth_call", [control, "latest"])
        controls.append(control)
        checks.extend(assert_rejected(node, config, binding.name, profile))
    return {"method": "eth_call", "allowed_controls": controls, "rejected": checks}


def assert_rejected(node, config, account, profile):
    checks = []
    for label, target, inner, value, operation in forbidden_calls(config, account):
        call = wrapped(profile, target, inner, value, operation)
        with pytest.raises(RuntimeError, match="0xd0a9bf58"):
            node.rpc("eth_call", [call, "latest"])
        checks.append(
            {"account": account, "case": label, "call": call, "error_selector": "0xd0a9bf58"}
        )
    return checks
