"""Router ABI changes must preserve the selected pool and execution bounds."""

from dataclasses import replace
from unittest.mock import patch

import pytest
from eth_abi import decode

from almanak.connectors.uniswap_v4.addresses import UNISWAP_V4
from almanak.connectors.uniswap_v4.pool_key import PoolKey
from almanak.connectors.uniswap_v4.router_deployments import ROUTER_DEPLOYMENTS, RouterABI, router_deployment
from almanak.connectors.uniswap_v4.sdk import SwapQuote, UniswapV4SDK

ZERO = "0x" + "0" * 40
TOKEN = "0x" + "1" * 40
WALLET = "0x" + "2" * 40
HOOK = "0x" + "3" * 36 + "0080"


def test_every_registered_router_has_an_explicit_layout():
    assert set(ROUTER_DEPLOYMENTS) == set(UNISWAP_V4)
    for chain, addresses in UNISWAP_V4.items():
        assert router_deployment(chain, addresses["universal_router"]).address == addresses["universal_router"].lower()


@pytest.mark.parametrize("chain", sorted(UNISWAP_V4))
@pytest.mark.parametrize("fee,hook,data", [(0, ZERO, b""), (31100, ZERO, b""), (0x800000, HOOK, b"abc" * 23)])
def test_layout_preserves_full_key_amounts_hook_data_and_slippage(chain, fee, hook, data):
    sdk = UniswapV4SDK(chain)
    key = PoolKey(ZERO, TOKEN, fee, 17, hook)
    quote = SwapQuote(1000, 900, fee, ZERO, TOKEN, pool_key=key, hook_data=data)
    tx = sdk.build_swap_tx(quote, WALLET, 50, 123456)
    commands, inputs, deadline = decode(["bytes", "bytes[]", "uint256"], bytes.fromhex(tx.data[10:]))
    assert commands[0] == 0x10
    assert deadline == 123456
    actions, params = decode(["bytes", "bytes[]"], inputs[0])
    assert actions == bytes([0x06, 0x0C, 0x0E])
    newer = ROUTER_DEPLOYMENTS[chain].abi is RouterABI.V4_HOP_PRICE
    abi = "((address,address,uint24,int24,address),bool,uint128,uint128," + ("uint256," if newer else "") + "bytes)"
    fields = decode([abi], params[0])[0]
    assert fields[:4] == ((ZERO, TOKEN, fee, 17, hook), True, 1000, 895)
    assert fields[-1] == data
    if newer:
        assert fields[4] == 0
    assert decode(["address", "uint256"], params[1]) == (ZERO, 1000)
    assert tx.value == 1000


def test_unknown_router_address_does_not_inherit_chain_layout():
    sdk = UniswapV4SDK("base")
    sdk.router = TOKEN
    with pytest.raises(ValueError, match="no qualified calldata layout"):
        sdk.build_swap_tx(SwapQuote(1000, 900, 3000, ZERO, TOKEN), WALLET)


def test_layout_selection_follows_deployment_profile_not_chain_branch():
    old = ROUTER_DEPLOYMENTS["base"]
    with patch.dict(ROUTER_DEPLOYMENTS, {"base": replace(old, abi=RouterABI.V4_HOP_PRICE)}):
        sdk = UniswapV4SDK("base")
        raw = sdk._encode_exact_input_single_params(SwapQuote(1000, 900, 3000, ZERO, TOKEN), 895)
        fields = decode(
            ["((address,address,uint24,int24,address),bool,uint128,uint128,uint256,bytes)"], bytes.fromhex(raw)
        )[0]
        assert fields[4:] == (0, b"")
