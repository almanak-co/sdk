"""Native-currency V4 pools: wrapped custody, router templates, Roles grants and receipts."""

import pytest
from eth_abi import decode
from pydantic import ValidationError

from almanak.caip import AssetType
from almanak.connectors.uniswap_v4 import abi
from almanak.connectors.uniswap_v4.custody import custody_asset
from almanak.connectors.uniswap_v4.native_router import (
    ADDRESS_THIS,
    CONTRACT_BALANCE,
    ETH_IN_COMMANDS,
    ETH_OUT_COMMANDS,
    NATIVE,
)
from almanak.connectors.uniswap_v4.permissions import MARKERS, swap_grant
from almanak.connectors.uniswap_v4.receipts import movement
from almanak.gateway.ethereum import DataError
from almanak.gateway.roles_conditions import flatten
from almanak.models import AssetAmount
from tests.uniswap_native_support import (
    AVALANCHE,
    BNB,
    BNB_USDT,
    ETHEREUM,
    FUND,
    POLYGON,
    erc20,
    eth_in_receipt,
    eth_out_receipt,
    liquidity_range,
    native,
    swap,
    target,
    transfer,
    wrap_log,
    wrapped,
)

SWAP_PARAMS = "((address,address,uint24,int24,address),bool,uint128,uint128,bytes)"


def router_call(call):
    return decode(("bytes", "bytes[]", "uint256"), bytes.fromhex(call.data[10:]))


def v4_params(payload):
    return decode(("bytes", "bytes[]"), payload)


def test_a_native_currency_is_held_as_the_wrapped_token_in_pool_key_order():
    selected = target("eth_usdc")
    assert selected.pool.currency0 == native(ETHEREUM)
    assert selected.assets == (wrapped(ETHEREUM), erc20(ETHEREUM, selected.assets[1].reference))
    assert selected.native_index == 0 and selected.funding_currency == wrapped(ETHEREUM)
    assert abi.pool_key(selected.pool)[0] == NATIVE


def test_pools_a_wrapped_custody_cannot_hold_are_refused():
    with pytest.raises(ValidationError, match="one asset for both currencies"):
        target("eth_weth")
    assert target("eth_usdc", range=liquidity_range("eth_usdc")).liquidity_enabled
    with pytest.raises(ValueError, match="differs from the chain native asset"):
        custody_asset(AssetType("eip155:56/slip44:60"))
    assert custody_asset(erc20(BNB, BNB_USDT)) == erc20(BNB, BNB_USDT)


@pytest.mark.parametrize(
    "name, chain, symbol",
    [
        ("eth_usdc", ETHEREUM, "slip44:60"),
        ("pol_usdc", POLYGON, "slip44:966"),
        ("bnb_usdt", BNB, "slip44:714"),
        ("avax_usdc", AVALANCHE, "slip44:9005"),
    ],
)
def test_each_chains_own_native_coin_is_held_as_its_wrapped_token(name, chain, symbol):
    selected = target(name)
    assert str(selected.pool.currency0).endswith(symbol) and selected.native_index == 0
    assert selected.assets[0] == wrapped(chain) == custody_asset(native(chain))
    assert selected.funding_currency == wrapped(chain)


NON_ETH = ["pol_usdc", "bnb_usdt", "avax_usdc"]


@pytest.mark.parametrize(
    "name", ["eth_usdc", "arb_eth_usdc", "fables_weth_usdg", "pons_novaai", *NON_ETH]
)
def test_eth_in_pulls_unwraps_and_swaps_the_full_credit_without_value(name):
    operation = swap(name, eth_in=True, amount=10**17, minimum=7)
    call = abi.swap(operation.target, operation.amount, operation.minimum.amount, 99)
    commands, inputs, deadline = router_call(call)
    assert (call.value, commands, deadline) == (0, ETH_IN_COMMANDS, 99)
    pull, unwrap, v4 = inputs
    assert len(pull) == len(v4) and set(pull[96:]) == {0}
    token, recipient, amount = decode(("address", "address", "uint160"), pull[:96])
    assert (token, recipient, amount) == (operation.amount.asset.reference, ADDRESS_THIS, 10**17)
    assert decode(("address", "uint256"), unwrap) == (ADDRESS_THIS, 0)
    actions, params = v4_params(v4)
    assert actions == bytes((0x0B, 0x06, 0x0F))
    assert decode(("address", "uint256", "bool"), params[0]) == (NATIVE, CONTRACT_BALANCE, False)
    assert decode(("address", "uint256"), params[2]) == (operation.minimum.asset.reference, 7)


@pytest.mark.parametrize("name", ["eth_usdc", *NON_ETH])
def test_eth_out_swaps_the_input_and_wraps_the_whole_native_output_to_the_fund(name):
    operation = swap(name, eth_in=False, amount=250 * 10**6, minimum=9)
    commands, (v4, wrap), _ = router_call(
        abi.swap(operation.target, operation.amount, operation.minimum.amount, 99)
    )
    assert commands == ETH_OUT_COMMANDS
    actions, params = v4_params(v4)
    assert actions == bytes((0x06, 0x0C, 0x0E))
    ((key, zero_for_one, amount_in, minimum, _),) = decode((SWAP_PARAMS,), params[0])
    assert (key[0], zero_for_one, amount_in, minimum) == (NATIVE, False, 250 * 10**6, 9)
    assert decode(("address", "address", "uint256"), params[2]) == (NATIVE, ADDRESS_THIS, 0)
    assert decode(("address", "uint256"), wrap) == (FUND, CONTRACT_BALANCE)


def grant_for(name, direction):
    return swap_grant(target(name), direction)


@pytest.mark.parametrize("name", ["eth_usdc", *NON_ETH])
def test_both_native_templates_carry_no_amount_and_share_one_first_input_shape(name):
    eth_in, eth_out = grant_for(name, True), grant_for(name, False)
    for grant in (eth_in, eth_out):
        # Scope only: no cumulative allowance anywhere in the tree.
        assert not any(c.operator == 28 for c in flatten(grant.condition))
    root_in, root_out = eth_in.condition, eth_out.condition
    inputs_in, inputs_out = root_in.children[1].children, root_out.children[1].children
    assert (len(inputs_in), len(inputs_out)) == (3, 2)
    # The unwrap and wrap inputs carry no variable and are fixed byte for byte.
    assert (inputs_in[1].parameter, inputs_out[1].parameter) == (2, 2)
    assert len(inputs_in[0].children) == len(inputs_in[2].children) == len(inputs_out[0].children)


def test_erc20_templates_keep_their_single_input_shape():
    from tests.uniswap_hook_support import target as hooked

    grant = swap_grant(hooked("fables"), True)
    assert len(grant.condition.children[1].children) == 1


@pytest.mark.parametrize("name", ["eth_usdc", "arb_eth_usdc", *NON_ETH])
def test_eth_in_reconciles_on_each_chains_wrap_evidence(name):
    operation = swap(name, eth_in=True, amount=10**17, minimum=10)
    deltas, nft, measured = movement(eth_in_receipt(operation, 250 * 10**6), operation)
    assert (deltas, nft, measured) == ((-(10**17), 250 * 10**6), None, None)


def test_wrapped_native_already_in_the_router_only_adds_to_the_input():
    operation = swap("eth_usdc", eth_in=True, amount=10**17, minimum=10)
    receipt = eth_in_receipt(operation, 250 * 10**6, router_dust=5)
    assert movement(receipt, operation)[0] == (-(10**17), 250 * 10**6)


@pytest.mark.parametrize("name", ["eth_usdc", "arb_eth_usdc", *NON_ETH])
def test_eth_out_reconciles_the_router_wrap(name):
    operation = swap(name, eth_in=False, amount=250 * 10**6, minimum=10)
    receipt = eth_out_receipt(operation, 99 * 10**15)
    assert movement(receipt, operation) == ((99 * 10**15, -250 * 10**6), None, None)


def test_native_forced_into_the_router_only_adds_to_the_eth_out_wrap():
    operation = swap("eth_usdc", eth_in=False, amount=250 * 10**6, minimum=10)
    receipt = eth_out_receipt(operation, 99 * 10**15, router_dust=1)
    assert movement(receipt, operation) == ((99 * 10**15 + 1, -250 * 10**6), None, None)


@pytest.mark.parametrize("dust, measured_take", [(10**15, 29 * 10**15), (5 * 10**16, 0)])
def test_router_surplus_on_a_native_pons_exit_understates_the_take_but_still_records(
    dust, measured_take
):
    operation = swap("pons_novaai", eth_in=False, amount=10**22, minimum=10)
    receipt = eth_out_receipt(operation, 10**18, take=3 * 10**16, router_dust=dust)
    deltas, _, measured = movement(receipt, operation)
    assert deltas == (10**18 - 3 * 10**16 + dust, -(10**22))
    assert measured == (measured_take, 10**18)


def test_the_other_chains_wrap_evidence_is_refused():
    operation = swap("eth_usdc", eth_in=False, amount=250 * 10**6, minimum=10)
    receipt = eth_out_receipt(operation, 99 * 10**15)
    selected = operation.target
    receipt["logs"][2] = transfer(
        wrapped(ETHEREUM).reference, "0x" + "00" * 20, selected.contracts.router, 99 * 10**15
    )
    with pytest.raises(DataError, match="through Transfer"):
        movement(receipt, operation)


def test_wrapped_native_paid_anywhere_but_the_router_is_refused():
    operation = swap("eth_usdc", eth_in=True, amount=10**17, minimum=10)
    receipt = eth_in_receipt(operation, 250 * 10**6)
    receipt["logs"][0] = transfer(wrapped(ETHEREUM).reference, FUND, "0x" + "cd" * 20, 10**17)
    with pytest.raises(DataError, match="outside the fund and router"):
        movement(receipt, operation)


def test_a_wrap_short_of_what_the_fund_received_or_below_the_minimum_is_refused():
    operation = swap("eth_usdc", eth_in=False, amount=250 * 10**6, minimum=10**18)
    with pytest.raises(DataError, match="below the retained minimum"):
        movement(eth_out_receipt(operation, 99 * 10**15), operation)
    operation = swap("eth_usdc", eth_in=False, amount=250 * 10**6, minimum=10)
    receipt = eth_out_receipt(operation, 99 * 10**15)
    receipt["logs"][2] = wrap_log(ETHEREUM, operation.target.contracts.router, 98 * 10**15, True)
    with pytest.raises(DataError, match="router wrap differs"):
        movement(receipt, operation)


def test_a_native_pons_take_is_the_gross_output_less_the_wrap():
    operation = swap("pons_novaai", eth_in=False, amount=10**22, minimum=10)
    receipt = eth_out_receipt(operation, 10**18, take=3 * 10**16)
    deltas, _, measured = movement(receipt, operation)
    assert deltas == (10**18 - 3 * 10**16, -(10**22)) and measured == (3 * 10**16, 10**18)


def test_an_erc20_pons_take_with_native_input_is_read_from_its_transfer():
    operation = swap("pons_novaai", eth_in=True, amount=10**18, minimum=10)
    deltas, _, measured = movement(eth_in_receipt(operation, 10**22, take=3 * 10**20), operation)
    assert deltas == (-(10**18), 10**22 - 3 * 10**20) and measured == (3 * 10**20, 10**22)


def test_a_quote_amount_marker_fits_the_permit2_pull():
    selected = target("eth_usdc")
    call = abi.swap(selected, AssetAmount(asset=selected.assets[0], amount=MARKERS[0]), 1, 1)
    _, (pull, _, _), _ = router_call(call)
    assert decode(("address", "address", "uint160"), pull[:96])[2] == MARKERS[0]


def test_both_native_directions_combine_and_a_mix_with_an_erc20_pool_is_refused_offline():
    from almanak.gateway.roles_conditions import combine
    from tests.uniswap_hook_support import target as hooked

    for name in NON_ETH:
        (non_eth,) = combine([grant_for(name, True), grant_for(name, False)])
        assert non_eth.condition.operator == 2
    native_grants = [grant_for("eth_usdc", True), grant_for("eth_usdc", False)]
    (combined,) = combine(native_grants)
    assert combined.condition.operator == 2
    erc20_grant = swap_grant(hooked("stable_pair"), True)
    assert erc20_grant.target == combined.target
    with pytest.raises(ValueError, match="incompatible types"):
        combine([*native_grants, erc20_grant])


def test_native_swaps_need_a_reviewed_router_wrapping_the_chains_weth():
    from almanak.caip import ChainId
    from almanak.chains import evm_network
    from almanak.connectors.uniswap_v4.native_router import routes

    for chain, route in routes().items():
        wrapped_native = evm_network(ChainId(chain)).wrapped_native.reference
        assert route["router_weth"] == route["wrapped_native"]["address"] == wrapped_native
    selected = target("eth_usdc")
    with pytest.raises(ValidationError, match="reviewed router"):
        type(selected).model_validate(
            {
                **selected.model_dump(),
                "contracts": {**selected.contracts.model_dump(), "router": "0x" + "12" * 20},
            }
        )


def test_the_routers_wrapped_native_is_pinned_with_its_proxy_implementation():
    from almanak.connectors.uniswap_v4.deployments import verify_identities
    from almanak.connectors.uniswap_v4.native_router import wrapped_identity

    selected = target("fables_weth_usdg")
    pinned = wrapped_identity(selected)
    assert pinned.implementation is not None
    verify_identities(selected, (pinned,))
    upgraded = pinned.model_copy(update={"implementation_hash": "0x" + "34" * 32})
    with pytest.raises(DataError, match="differs from the verified"):
        verify_identities(selected, (upgraded,))


def test_support_claims_native_pools_exactly_where_a_router_route_is_reviewed():
    from almanak.connectors.support import (
        NATIVE_V4_CURRENCIES,
        NATIVE_V4_LIQUIDITY,
        uniswap_support,
    )
    from almanak.connectors.uniswap_v4.native_router import routes

    claimed = {
        str(entry.chain)
        for entry in uniswap_support()
        if any("native coin held as its wrapped token" in r for r in entry.restrictions)
    }
    assert claimed == set(routes())
    assert {"eip155:137", "eip155:56", "eip155:43114", "eip155:8453"} <= claimed
    # Native liquidity shipped beside native trading, with its per-chain limits stated.
    for entry in uniswap_support():
        if str(entry.chain) in claimed:
            assert NATIVE_V4_CURRENCIES.endswith("for trading and liquidity")
            assert {NATIVE_V4_CURRENCIES, NATIVE_V4_LIQUIDITY} <= set(entry.restrictions)


def native_selection(chain_id):
    from almanak.caip import ChainId
    from almanak.chains import evm_network
    from almanak.connectors.uniswap_v4.resolution import MarketSelection
    from almanak.models import Block, Chain

    chain = Chain(chain_id=ChainId(chain_id), environment="mainnet")
    block = Block(chain=chain, number=1, hash="0x" + "ab" * 32, timestamp=1)

    class ScopeOnly:
        """Answers only the scope and anchor checks; any pool read fails the test."""

        def __init__(self):
            self.chain = chain

        def pin_block(self, number):
            return block

        def __getattr__(self, name):
            raise AssertionError(f"read {name} before the native refusal")

    selection = MarketSelection(
        connector="uniswap_v4",
        chain=chain.chain_id,
        currency0=evm_network(chain.chain_id).native_asset,
        currency1=erc20(chain, "0x" + "55" * 20),
        fee=3000,
        tick_spacing=60,
    )
    return ScopeOnly(), selection, block


@pytest.mark.parametrize("chain_id", ["eip155:56", "eip155:43114", "eip155:137"])
def test_a_native_pool_without_a_reviewed_route_is_refused_before_any_pool_read(
    chain_id, monkeypatch
):
    from almanak.connectors.uniswap_v4 import native_router
    from almanak.connectors.uniswap_v4.resolution import resolve_market

    reviewed = {k: v for k, v in native_router.routes().items() if k != chain_id}
    monkeypatch.setattr(native_router, "routes", lambda: reviewed)
    with pytest.raises(DataError, match="no reviewed route on this chain"):
        resolve_market(*native_selection(chain_id))


@pytest.mark.parametrize("chain_id", ["eip155:56", "eip155:43114", "eip155:137"])
def test_a_reviewed_non_eth_native_pool_passes_the_holding_refusal(chain_id):
    from almanak.connectors.uniswap_v4.resolution import resolve_market

    # The first pool read is the stand-in's failure: no refusal came before it.
    with pytest.raises(AssertionError, match="before the native refusal"):
        resolve_market(*native_selection(chain_id))


def test_a_native_identity_check_without_a_reviewed_route_refuses(monkeypatch):
    from almanak.connectors.uniswap_v4 import deployments, native_router

    selected = target("eth_usdc")
    monkeypatch.setattr(native_router, "routes", lambda: {})
    with pytest.raises(DataError, match="no reviewed route"):
        deployments.verify_identities(selected, ())


@pytest.mark.parametrize("chain_id", ["eip155:1", "eip155:137"])
def test_a_native_wrapped_native_pool_is_refused_before_any_pool_read(chain_id):
    from almanak.caip import ChainId
    from almanak.chains import evm_network
    from almanak.connectors.uniswap_v4.resolution import resolve_market

    reader, selection, block = native_selection(chain_id)
    paired = selection.model_copy(
        update={"currency1": evm_network(ChainId(chain_id)).wrapped_native}
    )
    with pytest.raises(DataError, match="one asset for both currencies"):
        resolve_market(reader, paired, block)
