"""Reject altered pool, route, recipient and call authority before venue execution."""

import pytest

from almanak.gateway.safe_fixture import calldata
from tests.strategy_fork.pool_lending_authority import wrapped


def swap_attacks(target):
    route = (
        target.assets[0].reference,
        target.assets[1].reference,
        target.pool.stable,
        target.pool.factory,
    )
    types = ("uint256", "uint256", "(address,address,bool,address)[]", "address", "uint256")
    values = (1, 1, (route,), target.fund.address, 2**64)
    for label, index, value in (
        ("zero_input", 0, 0),
        ("zero_output", 1, 0),
        ("empty_route", 2, ()),
        ("extra_route", 2, (route, route)),
        ("wrong_swap_recipient", 3, "0x" + "ab" * 20),
        ("zero_deadline", 4, 0),
    ):
        changed = list(values)
        changed[index] = value
        yield (
            label,
            target.contracts.router,
            calldata("swapExactTokensForTokens(" + ",".join(types) + ")", types, tuple(changed)),
        )
    for label, index, value in (
        ("wrong_input", 0, target.assets[1].reference),
        ("wrong_output", 1, target.assets[0].reference),
        ("wrong_invariant", 2, not target.pool.stable),
        ("zero_factory", 3, "0x" + "00" * 20),
        ("wrong_factory", 3, target.contracts.router),
    ):
        changed = list(route)
        changed[index] = value
        args = (1, 1, (tuple(changed),), target.fund.address, 2**64)
        yield (
            label,
            target.contracts.router,
            calldata("swapExactTokensForTokens(" + ",".join(types) + ")", types, args),
        )


def liquidity_attacks(target):
    prefix = (target.assets[0].reference, target.assets[1].reference, target.pool.stable)
    add_types = (
        "address",
        "address",
        "bool",
        "uint256",
        "uint256",
        "uint256",
        "uint256",
        "address",
        "uint256",
    )
    remove_types = (
        "address",
        "address",
        "bool",
        "uint256",
        "uint256",
        "uint256",
        "address",
        "uint256",
    )
    for label, signature, types, args, index, replacement in (
        (
            "wrong_mint_recipient",
            "addLiquidity",
            add_types,
            (*prefix, 1, 1, 0, 0, target.fund.address, 2**64),
            7,
            "0x" + "ab" * 20,
        ),
        (
            "zero_deposit0",
            "addLiquidity",
            add_types,
            (*prefix, 1, 1, 0, 0, target.fund.address, 2**64),
            3,
            0,
        ),
        (
            "wrong_remove_recipient",
            "removeLiquidity",
            remove_types,
            (*prefix, 1, 0, 0, target.fund.address, 2**64),
            6,
            "0x" + "ab" * 20,
        ),
        (
            "wrong_remove_pool",
            "removeLiquidity",
            remove_types,
            (*prefix, 1, 0, 0, target.fund.address, 2**64),
            2,
            not target.pool.stable,
        ),
        (
            "zero_shares",
            "removeLiquidity",
            remove_types,
            (*prefix, 1, 0, 0, target.fund.address, 2**64),
            3,
            0,
        ),
    ):
        values = list(args)
        values[index] = replacement
        yield (
            label,
            target.contracts.router,
            calldata(signature + "(" + ",".join(types) + ")", types, tuple(values)),
        )
    yield (
        "infinite_lp_approval",
        target.pool.pool,
        calldata(
            "approve(address,uint256)",
            ("address", "uint256"),
            (target.contracts.router, 2**256 - 1),
        ),
    )
    yield "foreign_pool_claim", target.pool.factory, calldata("claimFees()", (), ())


def assert_permissions(node, config):
    allocation = config.allocations[0]
    target = config.target(allocation.name)
    profile = config.accounts.binding(target.account).profile
    control = wrapped(
        profile,
        target.assets[0].reference,
        calldata("approve(address,uint256)", ("address", "uint256"), (target.contracts.router, 1)),
    )
    node.rpc("eth_call", [control, "latest"])
    cases = list(swap_attacks(target))
    if target.liquidity_enabled:
        cases.extend(liquidity_attacks(target))
    checks = []
    for label, destination, inner in cases:
        call = wrapped(profile, destination, inner)
        with pytest.raises(RuntimeError, match="0xd0a9bf58"):
            node.rpc("eth_call", [call, "latest"])
        checks.append({"case": label, "call": call, "error_selector": "0xd0a9bf58"})
    return {"method": "eth_call", "allowed_control": control, "rejected": checks}
