"""Installed Roles conditions reject widened collateral requests before protocol execution."""

import pytest

from almanak.connectors.gmx_v2.candidate_permissions import request_data
from almanak.connectors.gmx_v2.permissions import scope
from almanak.gateway.accounts import ContractCall, prepare_call
from almanak.models import SafeRoles


def assert_permissions(node, state):
    config = state.config
    allocation = config.allocations[0]
    target = config.target(allocation.name)
    profile = config.accounts.binding(allocation.account).profile
    if not isinstance(profile, SafeRoles):
        return None
    admitted = scope(state.capabilities.requirements[0])
    checks = []
    for label, data in forbidden(admitted, allocation):
        execution = prepare_call(
            config.accounts,
            allocation.account,
            ContractCall(
                fund=target.fund, to=admitted.exchange_router, value=0, data="0x" + data.hex()
            ),
        )
        call = {"from": profile.operator.address, "to": execution.to, "data": execution.data}
        with pytest.raises(RuntimeError, match="0xd0a9bf58"):
            node.rpc("eth_call", [call, "latest"])
        checks.append({"case": label, "call": call, "error_selector": "0xd0a9bf58"})
    return {"method": "eth_call", "rejected": checks}


def forbidden(admitted, allocation):
    fee = allocation.execution_fee.amount
    price = allocation.decrease_acceptable_price
    withdrawal = allocation.entry_collateral.amount // 10
    yield (
        "withdrawal keeper fee above the configured execution fee",
        request_data(admitted, 4, fee + 1, 0, fee + 1, price, collateral=withdrawal),
    )
    yield (
        "withdrawal changes fund receiver",
        request_data(
            admitted,
            4,
            fee,
            0,
            fee,
            price,
            collateral=withdrawal,
            receiver=admitted.exchange_router,
        ),
    )
    yield (
        "increase cannot encode a withdrawal delta",
        request_data(admitted, 2, fee, 0, fee, allocation.increase_acceptable_price, collateral=1),
    )
