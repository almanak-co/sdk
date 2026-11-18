"""Installed GM authority rejects widened requests: receivers, market, fee and floors.

The gateway approves each request's exact inputs right before submitting it, and
the router consumes them, so between requests every allowance is zero. Each
baseline therefore runs with those request-sized allowances as an `eth_call`
state override: the probe stays read-only while the supervised runtime keeps
running, and a baseline revert means Roles or GMX refused the request itself.
"""

import pytest
from eth_abi import decode

from almanak.asset_amounts import AssetAmounts
from almanak.connectors.gmx_v2.candidate_permissions import calldata
from almanak.connectors.gmx_v2.exchange_abi import swap_data
from almanak.connectors.gmx_v2.liquidity_abi import (
    CREATE_DEPOSIT,
    CREATE_WITHDRAWAL,
    DEPOSIT_PARAMS,
    REQUEST_BYTES,
    WITHDRAWAL_PARAMS,
    create_data,
)
from almanak.connectors.gmx_v2.liquidity_operations import CreateDeposit, CreateWithdrawal
from almanak.gateway.accounts import ContractCall, prepare_call
from almanak.models import SafeRoles
from tests.gmx_swap_permissions import request as swap_request

ALLOWANCE = ("allowance(address,address)", ("address", "address"))


def assert_permissions(node, state):
    allocation = state.config.allocations[0]
    profile = state.config.accounts.binding(allocation.account).profile
    if not isinstance(profile, SafeRoles) or state.mode != "running":
        return None
    accepted = []
    for operation in operations(state):
        call = permission_call(state, create_data(operation))
        overrides = request_allowances(node, operation)
        result = node.rpc("eth_call", [call, "latest", overrides])
        accepted.append(
            {
                "case": operation.request_kind + " baseline",
                "call": call,
                "state_override": overrides,
                "result": result,
            }
        )
    checks = []
    for label, payload in forbidden(state):
        call = permission_call(state, payload)
        with pytest.raises(RuntimeError, match="0xd0a9bf58"):
            node.rpc("eth_call", [call, "latest"])
        checks.append({"case": label, "call": call, "error_selector": "0xd0a9bf58"})
    return {"method": "eth_call", "accepted": accepted, "rejected": checks}


def permission_call(state, payload):
    allocation = state.config.allocations[0]
    target = state.config.target(allocation.name)
    profile = state.config.accounts.binding(allocation.account).profile
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
    return {"from": profile.operator.address, "to": execution.to, "data": execution.data}


def request_allowances(node, operation):
    owner, spender = operation.target.fund.address, operation.target.market.contracts.token_router
    overrides = {}
    for value in operation.inputs:
        if value.amount:
            token = value.asset.reference
            slot = allowance_slot(node, token, owner, spender)
            overrides[token] = {"stateDiff": {slot: word(value.amount)}}
    return overrides


def allowance_slot(node, token, owner, spender):
    """The storage key `allowance(owner, spender)` reads, found without assuming a layout."""
    call = {"to": token, "data": "0x" + calldata(*ALLOWANCE, (owner, spender)).hex()}
    access = node.rpc("eth_createAccessList", [call, "latest"])["accessList"]
    keys: list[str] = next(
        (entry["storageKeys"] for entry in access if entry["address"].lower() == token.lower()),
        [],
    )
    # A proxy also reads its implementation slot; only the allowance key echoes a probe value.
    probe = word(0x5EED)
    for key in keys:
        if node.rpc("eth_call", [call, "latest", {token: {"stateDiff": {key: probe}}}]) == probe:
            return key
    raise AssertionError(f"no allowance storage key found for {token}")


def word(amount):
    return "0x" + amount.to_bytes(32, "big").hex()


def operations(state):
    allocation = state.config.allocations[0]
    common = {
        "target": state.config.target(allocation.name),
        "execution_fee": allocation.execution_fee,
        "admission_deadline": 1,
    }
    return (
        CreateDeposit(
            **common,
            amounts=AssetAmounts(
                tuple(
                    value.model_copy(update={"amount": min(value.amount, 1)})
                    for value in allocation.entry_amounts.root
                )
            ),
            minimum_market_tokens=allocation.minimum_market_tokens,
        ),
        CreateWithdrawal(
            **common,
            market_tokens=allocation.partial_withdrawal,
            minimum_outputs=allocation.withdrawal_minimums,
        ),
    )


def altered(operation, index, value, *, address_index=None):
    deposit = isinstance(operation, CreateDeposit)
    abi = DEPOSIT_PARAMS if deposit else WITHDRAWAL_PARAMS
    signature = CREATE_DEPOSIT if deposit else CREATE_WITHDRAWAL
    calls = list(decode(("bytes[]",), create_data(operation)[4:])[0])
    params = list(decode((abi,), calls[-1][4:])[0])
    if address_index is None:
        params[index] = value
    else:
        addresses = list(params[0])
        addresses[address_index] = value
        params[0] = tuple(addresses)
    calls[-1] = calldata(signature, (abi,), (tuple(params),)).ljust(REQUEST_BYTES, b"\0")
    return calldata("multicall(bytes[])", ("bytes[]",), (tuple(calls),))


def forbidden(state):
    deposit, withdrawal = operations(state)
    for name, operation in (("deposit", deposit), ("withdrawal", withdrawal)):
        yield (
            name + " changed receiver",
            altered(
                operation, 0, operation.target.market.contracts.exchange_router, address_index=0
            ),
        )
        yield (
            name + " changed market",
            altered(
                operation, 0, operation.target.market.wrapped_native.reference, address_index=3
            ),
        )
        fee_index = 3 if name == "deposit" else 4
        yield (
            name + " changed fixed fee",
            altered(operation, fee_index, operation.execution_fee.amount + 1),
        )
        yield name + " zero minimum", altered(operation, 1, 0)
        calls = decode(("bytes[]",), create_data(operation)[4:])[0]
        yield (
            name + " extra transfer",
            calldata("multicall(bytes[])", ("bytes[]",), ((*calls[:-1], *calls),)),
        )
    yield "withdrawal second output below minimum", altered(withdrawal, 2, 0)
    yield "unsupported swap from liquidity capability", swap_data(swap_request(state))
