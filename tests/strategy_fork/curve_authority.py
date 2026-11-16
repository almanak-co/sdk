"""Exercise installed generated Roles conditions, including dynamic array boundaries."""

import pytest

from almanak.connectors.curve.pool_abi import layout, signature
from almanak.gateway.safe_fixture import calldata
from tests.strategy_fork.pool_lending_authority import wrapped


def variants(target, signature, types, values, attacks):
    for label, index, replacement in attacks:
        changed = list(values)
        changed[index] = replacement
        yield label, target.pool.pool, calldata(signature, types, tuple(changed))


def swap_attacks(target):
    return variants(
        target,
        signature("exchange", layout(target.pool).exchange),
        layout(target.pool).exchange,
        (0, 1, 1, 1) + layout(target.pool).receiver(target.fund.address),
        (
            # Scope-only authority (blueprint 27): any nonzero input, so no excess probe.
            ("zero_input", 2, 0),
            ("zero_output", 3, 0),
            ("same_coin", 1, 0),
            ("invalid_coin_word", 0, -1 if layout(target.pool).index == "int128" else 2**256 - 1),
            ("out_of_range_coin", 1, len(target.assets)),
            *native_attack(target, 4),
            ("wrong_swap_recipient", 4 + int(layout(target.pool).native_flag), "0x" + "ab" * 20),
        ),
    )


def dynamic_deposit_attacks(target):
    if layout(target.pool).array_parameter != 4:
        return ()
    count = len(target.assets)
    return (
        ("empty_deposit", 0, ()),
        ("short_deposit", 0, (1,) * (count - 1)),
        ("long_deposit", 0, (1,) * (count + 1)),
    )


def dynamic_removal_attacks(target):
    result = ()
    if layout(target.pool).array_parameter == 4:
        result = (("empty_minima", 1, ()), ("long_minima", 1, (0,) * (len(target.assets) + 1)))
    if layout(target.pool).admin_flag:
        result += (("admin_collection", 3 + int(layout(target.pool).native_flag), True),)
    return result


def native_attack(target, index):
    return (("native_convenience", index, True),) if layout(target.pool).native_flag else ()


def liquidity_attacks(target):
    count = len(target.assets)
    yield from variants(
        target,
        signature("add_liquidity", layout(target.pool).addition),
        layout(target.pool).addition,
        ((1,) * count, 1) + layout(target.pool).receiver(target.fund.address),
        (
            *dynamic_deposit_attacks(target),
            ("zero_lp_minimum", 1, 0),
            *native_attack(target, 2),
            ("wrong_mint_recipient", 2 + int(layout(target.pool).native_flag), "0x" + "ab" * 20),
        ),
    )
    yield from variants(
        target,
        signature("remove_liquidity", layout(target.pool).proportional),
        layout(target.pool).proportional,
        (1, (0,) * count)
        + layout(target.pool).receiver(target.fund.address)
        + ((False,) if layout(target.pool).admin_flag else ()),
        (
            ("zero_shares", 0, 0),
            *dynamic_removal_attacks(target),
            *native_attack(target, 2),
            ("wrong_remove_recipient", 2 + int(layout(target.pool).native_flag), "0x" + "ab" * 20),
        ),
    )
    if target.single_coin_exit is not None:
        index = target.assets.index(target.single_coin_exit)
        yield from variants(
            target,
            signature("remove_liquidity_one_coin", layout(target.pool).single_coin),
            layout(target.pool).single_coin,
            (1, index, 0) + layout(target.pool).receiver(target.fund.address),
            (
                ("wrong_exit_coin", 1, (index + 1) % count),
                *native_attack(target, 3),
                (
                    "wrong_single_recipient",
                    3 + int(layout(target.pool).native_flag),
                    "0x" + "ab" * 20,
                ),
            ),
        )
    yield (
        "lp_approval",
        target.pool.pool,
        calldata("approve(address,uint256)", ("address", "uint256"), (target.pool.pool, 1)),
    )


def assert_permissions(node, config):
    results = []
    for allocation in config.allocations:
        target = config.target(allocation.name)
        profile = config.accounts.binding(target.account).profile
        control = wrapped(
            profile,
            target.assets[0].reference,
            calldata("approve(address,uint256)", ("address", "uint256"), (target.pool.pool, 1)),
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
        results.append(
            {"allocation": allocation.name, "allowed_control": control, "rejected": checks}
        )
    return {"method": "eth_call", "allocations": results}
