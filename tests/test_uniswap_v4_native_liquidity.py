"""Native-currency V4 liquidity: PositionManager templates, Roles grants and receipts."""

import pytest
from eth_abi import decode
from pydantic import ValidationError

from almanak.connectors.uniswap_v4 import abi
from almanak.connectors.uniswap_v4.native_liquidity import (
    ADDRESS_THIS,
    CONTRACT_BALANCE,
    NATIVE,
    NATIVE_LP_WORDS,
    OPEN_DELTA,
)
from almanak.connectors.uniswap_v4.permissions import MARKERS, exit_grants, mint_grant
from almanak.connectors.uniswap_v4.receipts import movement
from almanak.gateway.ethereum import DataError
from almanak.gateway.roles_conditions import combine, flatten
from tests.uniswap_native_lp_support import (
    burn,
    collect,
    exit_receipt,
    lp_target,
    mint,
    mint_receipt,
)
from tests.uniswap_native_support import ETHEREUM, FUND, target, transfer, wrap_log, wrapped

POOL_KEY = "(address,address,uint24,int24,address)"
MINT_PARAMS = (POOL_KEY, "int24", "int24", "uint256", "uint128", "uint128", "address", "bytes")


def unlock(call):
    payload, _ = decode(("bytes", "uint256"), bytes.fromhex(call.data[10:]))
    assert len(payload) == NATIVE_LP_WORDS * 32 and call.value == 0
    return decode(("bytes", "bytes[]"), payload)


def test_a_native_mint_pulls_the_native_maximum_unwraps_and_sweeps_the_rest_back():
    operation = mint("eth_usdc")
    selected = operation.target
    call = abi.mint(selected, 10**12, 5 * 10**16, 200 * 10**6, 9)
    actions, params = unlock(call)
    assert call.to == selected.contracts.position_manager
    assert actions == bytes((0x0B, 0x0E, 0x16, 0x02, 0x0B, 0x0B, 0x15, 0x14))
    weth, usdc = selected.assets[0].reference, selected.assets[1].reference
    assert decode(("address", "uint256", "bool"), params[0]) == (weth, 5 * 10**16, True)
    assert decode(("address", "address", "uint256"), params[1]) == (weth, ADDRESS_THIS, OPEN_DELTA)
    assert decode(("uint256",), params[2]) == (CONTRACT_BALANCE,)
    key, lower, upper, liquidity, max0, max1, owner, _ = decode(MINT_PARAMS, params[3])
    assert (key[0], lower, upper, liquidity, max0, max1, owner) == (
        NATIVE,
        selected.range.lower,
        selected.range.upper,
        10**12,
        5 * 10**16,
        200 * 10**6,
        FUND,
    )
    assert decode(("address", "uint256", "bool"), params[4]) == (NATIVE, OPEN_DELTA, False)
    assert decode(("address", "uint256", "bool"), params[5]) == (usdc, OPEN_DELTA, True)
    assert decode(("uint256",), params[6]) == (CONTRACT_BALANCE,)
    assert decode(("address", "address"), params[7]) == (weth, FUND)


@pytest.mark.parametrize("closing", [False, True])
def test_native_exits_take_the_native_to_the_position_manager_and_sweep_it_wrapped(closing):
    selected = lp_target("eth_usdc")
    call = abi.close(selected, 7, 1, 2, 9) if closing else abi.collect(selected, 7, 9)
    actions, params = unlock(call)
    assert actions == bytes((0x03 if closing else 0x01, 0x0E, 0x0E, 0x15, 0x14))
    weth, usdc = selected.assets[0].reference, selected.assets[1].reference
    assert decode(("address", "address", "uint256"), params[1]) == (NATIVE, ADDRESS_THIS, 0)
    assert decode(("address", "address", "uint256"), params[2]) == (usdc, FUND, 0)
    assert decode(("address", "address"), params[4]) == (weth, FUND)


def native_grants(name="eth_usdc"):
    selected = lp_target(name)
    return [mint_grant(selected), *exit_grants(selected)]


def test_native_liquidity_grants_combine_without_any_amount_allowance():
    (combined,) = combine(native_grants())
    assert combined.target == lp_target("eth_usdc").contracts.position_manager
    assert not any(c.operator == 28 for c in flatten(combined.condition))


def test_the_native_maximum_is_variable_both_where_it_is_pulled_and_where_it_caps_the_mint():
    selected = lp_target("eth_usdc")
    call = abi.mint(selected, MARKERS[0], MARKERS[1], MARKERS[2], 1)
    payload, _ = decode(("bytes", "uint256"), bytes.fromhex(call.data[10:]))
    words = [int.from_bytes(payload[i : i + 32], "big") for i in range(0, len(payload), 32)]
    assert (words.count(MARKERS[1]), words.count(MARKERS[2])) == (2, 1)
    mint_grant(selected)


def test_native_and_erc20_liquidity_on_one_position_manager_are_refused_offline():
    from almanak.connectors.uniswap_v4.models import Range
    from tests.uniswap_hook_support import target as hooked

    erc20 = hooked("stable_pair", range=Range(lower=-10, upper=10))
    assert erc20.contracts.position_manager == lp_target("eth_usdc").contracts.position_manager
    with pytest.raises(ValueError, match="incompatible types"):
        combine([*native_grants(), mint_grant(erc20), *exit_grants(erc20)])


def other_pool(**update):
    first = lp_target("eth_usdc")
    pool = first.pool.model_copy(update=update)
    return type(first).model_validate({**first.model_dump(), "pool": pool.model_dump()})


def test_a_second_fee_tier_of_the_native_pair_shares_the_roles_grant():
    second = other_pool(fee=500, tick_spacing=10)
    (combined,) = combine([*native_grants(), mint_grant(second), *exit_grants(second)])
    assert len(combined.conditions) <= 256


def test_a_second_native_pair_on_one_chain_exceeds_the_roles_node_limit():
    # Each native pool's mint, collect and close need 182 condition nodes. Roles 2.1
    # indexes them with a uint8, and another token differs in too many words to share them.
    from almanak.caip import AssetType

    usdt = AssetType("eip155:1/erc20:0xdac17f958d2ee523a2206206994597c13d831ec7")
    second = other_pool(currency1=usdt)
    with pytest.raises(ValueError, match="exceeds the supported Roles contract limits"):
        combine([*native_grants(), mint_grant(second), *exit_grants(second)])


def test_native_liquidity_needs_the_reviewed_position_manager():
    selected = lp_target("eth_usdc")
    contracts = {**selected.contracts.model_dump(), "position_manager": "0x" + "12" * 20}
    with pytest.raises(ValidationError, match="reviewed position manager"):
        type(selected).model_validate({**selected.model_dump(), "contracts": contracts})


def test_native_liquidity_on_a_reviewed_hook_pool_is_admitted():
    assert lp_target("fables_weth_usdg").liquidity_enabled


@pytest.mark.parametrize("name", ["eth_usdc", "arb_eth_usdc"])
def test_a_native_mint_reconciles_on_each_chains_wrap_evidence(name):
    operation = mint(name)
    receipt = mint_receipt(operation, 7, native_used=2 * 10**16, token_used=19 * 10**6)
    deltas, nft, measured = movement(receipt, operation)
    assert deltas == (-2 * 10**16, -19 * 10**6) and nft.token_id == "7" and measured is None


def test_wrapped_native_already_in_the_position_manager_only_adds_to_the_return():
    operation = mint("eth_usdc")
    receipt = mint_receipt(operation, 7, native_used=2 * 10**16, token_used=19 * 10**6, dust=3)
    assert movement(receipt, operation)[0] == (-2 * 10**16 + 3, -19 * 10**6)


@pytest.mark.parametrize("name", ["eth_usdc", "arb_eth_usdc"])
@pytest.mark.parametrize("make", [collect, burn])
def test_native_exits_reconcile_the_sweep(name, make):
    operation = make(name)
    deltas, nft, _ = movement(exit_receipt(operation, 3 * 10**16, 25 * 10**6), operation)
    assert deltas == (3 * 10**16, 25 * 10**6)
    assert (nft is None) == (make is collect)


def test_wrapped_native_moved_off_the_native_liquidity_path_is_refused():
    operation = mint("eth_usdc")
    receipt = mint_receipt(operation, 7, native_used=2 * 10**16, token_used=19 * 10**6)
    receipt["logs"][-1] = transfer(wrapped(ETHEREUM).reference, FUND, "0x" + "cd" * 20, 1)
    with pytest.raises(DataError, match="outside the native liquidity path"):
        movement(receipt, operation)


def test_a_mint_whose_take_differs_from_the_pull_is_refused():
    operation = mint("eth_usdc")
    receipt = mint_receipt(operation, 7, native_used=2 * 10**16, token_used=19 * 10**6)
    selected = operation.target
    receipt["logs"][1] = transfer(
        wrapped(ETHEREUM).reference,
        selected.pool.pool_manager,
        selected.contracts.position_manager,
        operation.maximum0.amount - 1,
    )
    with pytest.raises(DataError, match="unwrap differs from the pulled native"):
        movement(receipt, operation)


def test_an_exit_that_sweeps_less_than_it_wrapped_is_refused():
    operation = burn("eth_usdc")
    receipt = exit_receipt(operation, 3 * 10**16, 25 * 10**6)
    selected = operation.target
    receipt["logs"][-1] = transfer(
        wrapped(ETHEREUM).reference, selected.contracts.position_manager, FUND, 3 * 10**16 - 1
    )
    with pytest.raises(DataError, match="swept less than it wrapped"):
        movement(receipt, operation)


def test_a_native_mint_above_its_maximum_is_refused():
    operation = mint("eth_usdc", maximum1=10 * 10**6)
    receipt = mint_receipt(operation, 7, native_used=2 * 10**16, token_used=19 * 10**6)
    with pytest.raises(DataError, match="exceed the retained deposit ceilings"):
        movement(receipt, operation)


def test_a_trading_target_on_a_native_pool_keeps_its_router_templates():
    selected = target("eth_usdc")
    assert not selected.liquidity_enabled


def test_a_native_mint_charges_permit2_the_whole_pull_not_the_net_deposit():
    from almanak.connectors.uniswap_v4.receipts import fund_spend
    from almanak.connectors.uniswap_v4.reconciliation import allowance_after

    operation = mint("eth_usdc")
    receipt = mint_receipt(operation, 7, native_used=2 * 10**16, token_used=19 * 10**6)
    spent = fund_spend(receipt, operation.target)
    assert spent == (-operation.maximum0.amount, -19 * 10**6)
    assert movement(receipt, operation)[0] == (-2 * 10**16, -19 * 10**6)
    selected = operation.target
    from types import SimpleNamespace

    permit = SimpleNamespace(
        asset=selected.assets[0],
        spender=selected.contracts.position_manager,
        amount=10**18,
        expiration=2**40,
        model_copy=lambda update: SimpleNamespace(**{"amount": update["amount"]}),
    )
    (after,) = allowance_after(SimpleNamespace(allowances=(permit,)), operation, spent, 0)
    assert after.amount == 10**18 - operation.maximum0.amount


def test_a_one_sided_mint_with_a_position_manager_surplus_records_the_return():
    operation = mint("eth_usdc")
    receipt = mint_receipt(operation, 7, native_used=0, token_used=19 * 10**6, dust=1)
    deltas, nft, _ = movement(receipt, operation)
    assert deltas == (1, -19 * 10**6) and nft.token_id == "7"


def test_a_mint_pulling_more_than_its_native_maximum_is_refused():
    operation = mint("eth_usdc")
    receipt = mint_receipt(operation, 7, native_used=2 * 10**16, token_used=19 * 10**6)
    selected, extra = operation.target, operation.maximum0.amount + 1
    weth = wrapped(ETHEREUM).reference
    manager, position_manager = selected.pool.pool_manager, selected.contracts.position_manager
    # A consistent path (pull, take and unwrap all equal) that only exceeds the maximum.
    receipt["logs"][0] = transfer(weth, FUND, manager, extra)
    receipt["logs"][1] = transfer(weth, manager, position_manager, extra)
    receipt["logs"][2] = wrap_log(ETHEREUM, position_manager, extra, wrapping=False)
    with pytest.raises(DataError, match="pulled more than its native maximum"):
        movement(receipt, operation)


def collect_pending(name, fee_growth=(0, 0)):
    from types import SimpleNamespace

    position = SimpleNamespace(liquidity=2**128, fee_growth=fee_growth)
    return SimpleNamespace(
        operation=collect(name), before=SimpleNamespace(owned=SimpleNamespace(position=position))
    )


def test_a_native_collect_records_a_position_manager_surplus_as_income_not_an_issue():
    from almanak.connectors.uniswap_v4.reconciliation import fees

    pending = collect_pending("eth_usdc")
    # Measured fees are (10, 5); a donated wei in the PositionManager was swept along.
    assert fees(pending, (11, 5), (10, 5), True) == ((10, 5), ())
    earned, issues = fees(pending, (10, 6), (10, 5), True)
    assert issues == ("collected token fees differ from measured fee growth",)
    # Without growth evidence the native fee stays unmeasured instead of absorbing a surplus.
    assert fees(pending, (11, 5), None, True) == ((None, 5), ())
