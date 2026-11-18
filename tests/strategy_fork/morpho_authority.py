"""Installed Roles probes must fail before a protocol error can mask missing scope."""

import pytest
from eth_abi import encode
from eth_utils import keccak

from almanak.connectors.morpho_blue.operations import (
    BorrowOperation,
    RepayOperation,
    SupplyOperation,
    call_arguments,
)
from almanak.gateway.safe_fixture import calldata
from almanak.models import AssetAmount, SafeRoles
from tests.strategy_fork.pool_lending_authority import wrapped


def entry(config, allocation, amount):
    target = config.target(allocation.name)
    cls = BorrowOperation if allocation.borrow else SupplyOperation
    return cls(target=target, amount=AssetAmount(asset=target.asset, amount=amount))


def encode_call(name, types, values):
    payload = keccak(text=f"{name}({','.join(types)})")[:4] + encode(types, values)
    return "0x" + payload.hex()


def changed(values, index, value):
    result = list(values)
    result[index] = value
    return result


def market_changes(values, other):
    for index in range(5):
        params = changed(values[0], index, other if index < 4 else values[0][4] + 1)
        yield f"market_field_{index}", changed(values, 0, params)


def argument_changes(allocation, profile, values):
    # Scope-only grants take any positive amount, so only a zero amount is out of scope.
    yield "zero", changed(values, 1, 0)
    yield from market_changes(values, profile.submitter.address)
    owner_index = 3 if allocation.reserve.is_loan else 2
    yield "other_owner", changed(values, owner_index, profile.submitter.address)
    tail = profile.submitter.address if allocation.borrow else b"\x01"
    yield "callback_or_receiver", changed(values, owner_index + 1, tail)
    if allocation.reserve.is_loan:
        yield "assets_and_shares", changed(values, 2, 1)


def repay_changes(profile, values):
    # The repay asset amount has no upper bound; beneficiary, empty callback data
    # and a positive asset or share quantity remain bound.
    yield "repay_other_owner", changed(values, 3, profile.submitter.address)
    yield "repay_nonempty_callback", changed(values, 4, b"\x01")
    yield "repay_zero", changed(values, 1, 0)


def repay_calls(config, allocation, profile):
    target = config.target(allocation.name)
    repay = RepayOperation(target=target, amount=AssetAmount(asset=target.asset, amount=1))
    name, types, values = call_arguments(repay)
    return [
        (
            allocation.name + "_" + label,
            allocation.reserve.market.core,
            encode_call(name, types, args),
            0,
            0,
        )
        for label, args in repay_changes(profile, values)
    ]


def allocation_calls(config, allocation):
    profile = config.accounts.binding(allocation.account).profile
    operation = entry(config, allocation, 1)
    name, types, values = call_arguments(operation)
    core, token = allocation.reserve.market.core, allocation.reserve.asset.reference
    calls = [
        (allocation.name + "_" + label, core, encode_call(name, types, args), 0, 0)
        for label, args in argument_changes(allocation, profile, values)
    ]
    if allocation.borrow:
        calls.extend(repay_calls(config, allocation, profile))
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
    approval = calldata("approve(address,uint256)", ("address", "uint256"), (core, 1))
    calls.extend(
        (
            (allocation.name + "_delegatecall", token, approval, 0, 1),
            (allocation.name + "_native_value", token, approval, 1, 0),
        )
    )
    return calls


def forbidden_calls(config, account):
    calls = []
    for allocation in config.positions:
        if allocation.account == account:
            calls.extend(allocation_calls(config, allocation))
    allocation = next(a for a in config.positions if a.account == account)
    profile = config.accounts.binding(account).profile
    calls.append(
        (
            "delegated_manager",
            allocation.reserve.market.core,
            calldata(
                "setAuthorization(address,bool)",
                ("address", "bool"),
                (profile.submitter.address, True),
            ),
            0,
            0,
        )
    )
    return calls


def account_probes(node, config, binding):
    profile = binding.profile
    assert isinstance(profile, SafeRoles)
    allocation = next(a for a in config.positions if a.account == binding.name)
    approval = calldata(
        "approve(address,uint256)", ("address", "uint256"), (allocation.reserve.market.core, 1)
    )
    control = wrapped(profile, allocation.reserve.asset.reference, approval)
    node.rpc("eth_call", [control, "latest"])
    checks = []
    for label, target, inner, value, operation in forbidden_calls(config, binding.name):
        call = wrapped(profile, target, inner, value, operation)
        with pytest.raises(RuntimeError, match="0xd0a9bf58"):
            node.rpc("eth_call", [call, "latest"])
        checks.append(
            dict(account=binding.name, case=label, call=call, error_selector="0xd0a9bf58")
        )
    return control, checks


def assert_permissions(node, config):
    node._validate_origin()
    controls, checks = [], []
    for binding in config.accounts.bindings:
        control, rejected = account_probes(node, config, binding)
        controls.append(control)
        checks.extend(rejected)
    return dict(method="eth_call", allowed_controls=controls, rejected=checks)
