"""Actual Arbitrum Roles admission checks from the reference-generated authority."""

import pytest

from almanak.connectors.uniswap_v4 import abi
from almanak.models import Account, AssetAmount
from tests.fork.test_uniswap_rejections import router_mutation
from tests.strategy_fork.pool_lending_authority import wrapped


def swap_attacks(target, deadline):
    amount = AssetAmount(asset=target.funding_currency, amount=1)
    swap = abi.swap(target, amount, 1, deadline)
    changed_pool = target.model_copy(update={"pool": target.pool.model_copy(update={"fee": 3000})})
    yield "wrong_pool", abi.swap(changed_pool, amount, 1, deadline)
    yield "wrong_target", swap.model_copy(update={"to": target.pool.pool_manager})
    yield "zero_swap_minimum", abi.swap(target, amount, 0, deadline)
    for name in ("extra_action", "recipient", "commands"):
        yield name, router_mutation(target, swap, name)
    yield "wrong_spender", abi.permit(target, amount.asset, target.pool.pool_manager, 1, deadline)
    yield (
        "wrong_asset",
        abi.approve(target, amount.asset, 1).model_copy(update={"to": target.pool.state_view}),
    )


def liquidity_attacks(target, deadline):
    # Scope-only authority (blueprint 27): amounts and ticks are not on-chain conditions,
    # so no quota, excess deposit or other-range probe exists; the gateway checks ranges.
    other_fund = target.model_copy(
        update={"fund": Account(chain=target.fund.chain, address="0x" + "12" * 20)}
    )
    yield "mint_recipient", abi.mint(other_fund, 1, 1, 1, deadline)
    yield "close_recipient", abi.close(other_fund, 1, 0, 0, deadline)
    yield "collect_recipient", abi.collect(other_fund, 1, deadline)
    yield "zero_nft", abi.collect(target, 0, deadline)


def assert_permissions(node, config):
    node._validate_origin()
    allocation = config.allocations[0]
    target = config.target(allocation.name)
    profile = config.accounts.binding(target.account).profile
    deadline = int(node.rpc("eth_getBlockByNumber", ["latest", False])["timestamp"], 16) + 300
    allowed = abi.approve(target, target.funding_currency, 0)
    control = wrapped(profile, allowed.to, allowed.data)
    node.rpc("eth_call", [control, "latest"])
    cases = list(swap_attacks(target, deadline))
    if target.range is not None:
        cases.extend(liquidity_attacks(target, deadline))
    checks = []
    for name, call in cases:
        request = wrapped(profile, call.to, call.data)
        with pytest.raises(RuntimeError, match="0xd0a9bf58"):
            node.rpc("eth_call", [request, "latest"])
        checks.append({"case": name, "call": request, "error_selector": "0xd0a9bf58"})
    return {"method": "eth_call", "allowed_control": control, "rejected": checks}
