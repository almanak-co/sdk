"""Pancake's deadline-bearing swap grants remain correlated with lending grants."""

import pytest

from almanak.connectors.aerodrome_slipstream.abi import SWAP_PARAMS as SLIPSTREAM_PARAMS
from almanak.connectors.pancakeswap_v3.abi import SWAP_PARAMS
from almanak.gateway.safe_fixture import calldata
from tests.strategy_fork.aave_v3_authority import assert_permissions as lending_permissions
from tests.strategy_fork.aave_v3_authority import wrapped


def forbidden_swaps(config):
    target = config.target("trade")
    slipstream = target.pool.kind == "aerodrome_slipstream"
    pool_key = target.pool.tick_spacing if slipstream else target.pool.fee
    swap_params = SLIPSTREAM_PARAMS if slipstream else SWAP_PARAMS
    values = [
        target.funding_currency.reference,
        target.counter_currency.reference,
        pool_key,
        target.fund.address,
        2**64,
        1,
        1,
        0,
    ]
    for label, index, replacement in forbidden_changes(config, target, pool_key):
        changed = list(values)
        changed[index] = replacement
        yield (
            label,
            target.contracts.router,
            calldata(f"exactInputSingle({swap_params})", (swap_params,), (tuple(changed),)),
        )
    yield forbidden_approval(config, target)


def forbidden_changes(config, target, pool_key):
    return (
        ("swap_other_input", 0, target.counter_currency.reference),
        ("swap_other_output", 1, target.funding_currency.reference),
        ("swap_other_pool_key", 2, pool_key + 1),
        (
            "swap_other_recipient",
            3,
            config.accounts.binding(target.account).profile.submitter.address,
        ),
        ("swap_zero_input", 5, 0),
        ("swap_price_limit", 7, 1),
    )


def forbidden_approval(config, target):
    # Scope-only grants (blueprint 27): an approval of any amount, but only to the router.
    other = config.accounts.binding(target.account).profile.submitter.address
    return (
        "router_approval_other_spender",
        target.funding_currency.reference,
        calldata("approve(address,uint256)", ("address", "uint256"), (other, 1)),
    )


def assert_permissions(node, config):
    result = lending_permissions(node, config)
    profile = config.accounts.binding("fund").profile
    for label, target, inner in forbidden_swaps(config):
        call = wrapped(profile, target, inner)
        with pytest.raises(RuntimeError, match="0xd0a9bf58"):
            node.rpc("eth_call", [call, "latest"])
        result["rejected"].append({"case": label, "call": call, "error_selector": "0xd0a9bf58"})
    return result
