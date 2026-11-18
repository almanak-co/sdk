"""Cross-connector grants keep swap arguments and approval pairs correlated on chain."""

import pytest

from almanak.connectors.uniswap_v3.abi import SWAP_PARAMS
from almanak.gateway.safe_fixture import calldata
from tests.strategy_fork.aave_v3_authority import assert_permissions as lending_permissions
from tests.strategy_fork.aave_v3_authority import wrapped


def swap_call(target, values, count=1):
    inner = calldata(f"exactInputSingle({SWAP_PARAMS})", (SWAP_PARAMS,), (tuple(values),))
    return calldata(
        "multicall(uint256,bytes[])",
        ("uint256", "bytes[]"),
        (2**64, [bytes.fromhex(inner[2:])] * count),
    )


def forbidden_swaps(config):
    target = config.target("trade")
    other = config.accounts.binding(target.account).profile.submitter.address
    values = [
        target.funding_currency.reference,
        target.counter_currency.reference,
        target.pool.fee,
        target.fund.address,
        1,
        1,
        0,
    ]
    changes = (
        ("swap_other_input", 0, target.counter_currency.reference),
        ("swap_other_output", 1, target.funding_currency.reference),
        ("swap_other_fee", 2, target.pool.fee + 1),
        ("swap_other_recipient", 3, other),
        ("swap_zero_input", 4, 0),
        ("swap_price_limit", 6, 1),
    )
    for label, index, replacement in changes:
        changed = list(values)
        changed[index] = replacement
        yield label, target.contracts.router, swap_call(target, changed)
    yield "swap_extra_call", target.contracts.router, swap_call(target, values, count=2)
    # Scope-only grants (blueprint 27): an approval of any amount, but only to the router.
    yield (
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
