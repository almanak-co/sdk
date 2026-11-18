"""Installed Roles authority rejects widened swap routes, receivers and fees."""

import pytest
from eth_abi import decode

from almanak.connectors.gmx_v2.candidate_permissions import CREATE_ORDER, ORDER_PARAMS, calldata
from almanak.connectors.gmx_v2.exchange_abi import swap_data
from almanak.connectors.gmx_v2.exchange_operations import CreateSwap
from almanak.gateway.accounts import ContractCall, prepare_call
from almanak.models import AssetAmount, SafeRoles


def assert_permissions(node, state):
    allocation = state.config.allocations[0]
    target = state.config.target(allocation.name)
    profile = state.config.accounts.binding(allocation.account).profile
    if not isinstance(profile, SafeRoles):
        return None
    checks = []
    for label, payload in forbidden(state):
        execution = prepare_call(
            state.config.accounts,
            allocation.account,
            ContractCall(
                fund=target.fund,
                to=target.market.contracts.exchange_router,
                value=0,
                data="0x" + payload.hex(),
            ),
        )
        call = {"from": profile.operator.address, "to": execution.to, "data": execution.data}
        with pytest.raises(RuntimeError, match="0xd0a9bf58"):
            node.rpc("eth_call", [call, "latest"])
        checks.append({"case": label, "call": call, "error_selector": "0xd0a9bf58"})
    return {"method": "eth_call", "rejected": checks}


def request(state, *, reverse=False):
    allocation = state.config.allocations[0]
    target = state.config.target(allocation.name)
    input_asset, output_asset = target.assets[::-1] if reverse else target.assets
    return CreateSwap(
        target=target,
        amount=AssetAmount(asset=input_asset, amount=1),
        minimum_output=AssetAmount(asset=output_asset, amount=1),
        execution_fee=allocation.execution_fee,
        admission_deadline=1,
    )


def altered_order(operation, group, index, value):
    calls = list(decode(("bytes[]",), swap_data(operation)[4:])[0])
    params = list(decode((ORDER_PARAMS,), calls[-1][4:])[0])
    changed = list(params[group])
    changed[index] = value
    params[group] = tuple(changed)
    calls[-1] = calldata(CREATE_ORDER, (ORDER_PARAMS,), (tuple(params),))
    return calldata("multicall(bytes[])", ("bytes[]",), (tuple(calls),))


def forbidden(state):
    original, reverse = request(state), request(state, reverse=True)
    target = original.target
    yield (
        "changed fund receiver",
        altered_order(original, 0, 0, target.market.contracts.exchange_router),
    )
    yield (
        "different one-hop market",
        altered_order(original, 0, 6, (target.market.wrapped_native.reference,)),
    )
    yield "extra hop", altered_order(original, 0, 6, (target.market.share_asset.reference,) * 2)
    yield "zero minimum output", altered_order(original, 1, 6, 0)
    yield (
        "fee above exact fixed request fee",
        altered_order(original, 1, 4, original.execution_fee.amount + 1),
    )
    calls = decode(("bytes[]",), swap_data(original)[4:])[0]
    yield (
        "extra nested transfer",
        calldata("multicall(bytes[])", ("bytes[]",), ((*calls[:-1], *calls),)),
    )
    calls = list(decode(("bytes[]",), swap_data(reverse)[4:])[0])
    calls[1] = calldata(
        "sendTokens(address,address,uint256)",
        ("address", "address", "uint256"),
        (
            target.market.wrapped_native.reference,
            target.market.contracts.order_vault,
            reverse.execution_fee.amount - 1,
        ),
    ).ljust(len(calls[-1]), b"\0")
    yield (
        "separate fee transfer differs from request",
        calldata("multicall(bytes[])", ("bytes[]",), (tuple(calls),)),
    )
