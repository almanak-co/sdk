"""Pool identity must survive quote, intent serialization, and router encoding."""

from dataclasses import FrozenInstanceError, replace
from unittest.mock import patch

import pytest
from eth_abi import decode, encode

from almanak.connectors.uniswap_v4.pool_key import PoolKey
from almanak.connectors.uniswap_v4.routing import resolve_swap_selection
from almanak.connectors.uniswap_v4.sdk import SwapQuote, UniswapV4SDK
from almanak.framework.intents.vocabulary import SwapIntent

ZERO = "0x" + "0" * 40
TOKEN = "0x" + "1" * 40
WRAPPED = "0x4200000000000000000000000000000000000006"
HOOK = "0x" + "1" * 36 + "0080"


@pytest.mark.parametrize("fee", [0, 1, 777, 31100, 999999, 1000000, 0x800000])
def test_protocol_fee_field_roundtrip(fee):
    key = PoolKey(ZERO, TOKEN, fee, 17, HOOK if fee == 0x800000 else ZERO)
    assert PoolKey.from_wire(key.to_wire()) == key
    with pytest.raises(FrozenInstanceError):
        key.fee = 5


@pytest.mark.parametrize("fee", [True, 1.0, "1", -1, 1000001, 0x400000, 0x800001, 0xC00000])
def test_invalid_fee_fields_cannot_create_pool(fee):
    with pytest.raises(ValueError):
        PoolKey(ZERO, TOKEN, fee, 17)


@pytest.mark.parametrize("spacing", [True, 0, -1, 32768, 1.5])
def test_spacing_is_protocol_valid_integer(spacing):
    with pytest.raises(ValueError):
        PoolKey(ZERO, TOKEN, 0, spacing)


def test_dynamic_identity_requires_hook_and_does_not_include_observed_fee():
    with pytest.raises(ValueError, match="requires a nonzero hook"):
        PoolKey(ZERO, TOKEN, 0x800000, 1)
    key = PoolKey(ZERO, TOKEN, 0x800000, 17, HOOK)
    assert key.pool_id != replace(key, fee=3000).pool_id
    assert key.pool_id != replace(key, tick_spacing=18).pool_id


@pytest.mark.parametrize("hook", ["0x" + "1" * 36 + flag for flag in ("0001", "0002", "0004", "0008")])
def test_hook_delta_requires_callback(hook):
    with pytest.raises(ValueError, match="corresponding callback"):
        PoolKey(ZERO, TOKEN, 0x800000, 1, hook)


def test_intent_serializes_static_zero_and_full_key():
    key = PoolKey(ZERO, TOKEN, 0, 17)
    intent = SwapIntent(
        from_token="ETH",
        to_token="USDC",
        amount="1",
        protocol="uniswap_v4",
        swap_params={"pool_key": key.to_wire(), "fee_tier": 0},
    )
    assert SwapIntent.model_validate_json(intent.model_dump_json()).swap_params == intent.swap_params


def test_gateway_lookup_must_return_matching_hash():
    key = PoolKey(ZERO, TOKEN, 1234, 17)
    with pytest.raises(ValueError, match="hash"):
        resolve_swap_selection(
            {"pool_id": "0x" + "0" * 64}, token_in=ZERO, token_out=TOKEN, default_fee=3000, lookup=lambda _: key
        )


def test_explicit_key_does_not_default_or_reinterpret_assets():
    key = PoolKey(ZERO, TOKEN, 1234, 17)
    for params in ({"pool_key": key.to_wire(), "fee_tier": 3000}, {"pool_key": key.to_wire(), "typo": 1}):
        with pytest.raises(ValueError):
            resolve_swap_selection(params, token_in=ZERO, token_out=TOKEN, default_fee=3000)
    with pytest.raises(ValueError, match="native and wrapped"):
        resolve_swap_selection({"pool_key": key.to_wire()}, token_in=WRAPPED, token_out=TOKEN, default_fee=3000)


def test_absent_hook_data_is_not_explicit_empty():
    key = PoolKey(ZERO, TOKEN, 0x800000, 17, HOOK)
    with pytest.raises(ValueError, match="absent"):
        resolve_swap_selection({"pool_key": key.to_wire()}, token_in=ZERO, token_out=TOKEN, default_fee=3000)
    selection = resolve_swap_selection(
        {"pool_key": key.to_wire(), "hook_data": "0x"}, token_in=ZERO, token_out=TOKEN, default_fee=3000
    )
    assert selection.hook_data == b""


@pytest.mark.parametrize("fee,hook_data", [(0, b""), (777, b""), (31100, b""), (0x800000, b"\x12\x34" * 23)])
def test_quoter_and_router_use_identical_full_key_and_hook_data(fee, hook_data):
    sdk = UniswapV4SDK("base")
    key = PoolKey(ZERO, TOKEN, fee, 17, HOOK if hook_data else ZERO)
    with patch(
        "almanak.connectors.uniswap_v4.sdk.eth_call", return_value=encode(["uint256", "uint256"], [900, 30000])
    ) as rpc:
        quote = sdk.get_quote(ZERO, TOKEN, 1000, pool_key=key, hook_data=hook_data)
    encoded_quote = bytes.fromhex(rpc.call_args.kwargs["data"][10:])
    quote_params = decode(["((address,address,uint24,int24,address),bool,uint128,bytes)"], encoded_quote)[0]
    tx = sdk.build_swap_tx(quote, TOKEN, deadline=2000000000)
    commands, inputs, _ = decode(["bytes", "bytes[]", "uint256"], bytes.fromhex(tx.data[10:]))
    assert commands == b"\x10\x04"
    actions, params = decode(["bytes", "bytes[]"], inputs[0])
    assert actions == b"\x06\x0c\x0e"
    assert decode(["address", "uint256"], params[1]) == (ZERO, 1000)
    swap = decode(["((address,address,uint24,int24,address),bool,uint128,uint128,bytes)"], params[0])[0]
    assert swap[0] == quote_params[0] == tuple(key.to_wire().values())
    assert swap[1:3] == quote_params[1:3] == (True, 1000)
    assert swap[3] == 895
    assert swap[4] == quote_params[3] == hook_data
    assert tx.value == 1000


def test_native_output_is_swept_and_wrapped_output_remains_erc20():
    sdk = UniswapV4SDK("base")
    for output, commands in ((ZERO, b"\x10\x04"), (WRAPPED, b"\x10")):
        key = PoolKey(TOKEN, output, 500, 10)
        quote = SwapQuote(1000, 900, 500, TOKEN, output, pool_key=key)
        tx = sdk.build_swap_tx(quote, TOKEN, deadline=2000000000)
        assert decode(["bytes", "bytes[]", "uint256"], bytes.fromhex(tx.data[10:]))[0] == commands


@pytest.mark.parametrize("amount,minimum", [(0, 1), (-1, 1), (1 << 128, 1), (1, 1 << 128)])
def test_router_refuses_uint128_overflow_instead_of_clamping(amount, minimum):
    quote = SwapQuote(amount, 100, 3000, ZERO, TOKEN)
    with pytest.raises(ValueError, match="uint128"):
        UniswapV4SDK("base")._encode_exact_input_single_params(quote, minimum)


@pytest.mark.parametrize("by_id", [False, True])
def test_explicit_hook_pin_accepts_checksum_case_but_rejects_different_address(by_id):
    hook = "0x" + "a" * 36 + "0080"
    key = PoolKey(ZERO, TOKEN, 0x800000, 17, hook)
    params = {"pool_id": key.pool_id} if by_id else {"pool_key": key.to_wire()}
    params.update(hooks="0x" + hook[2:].upper(), hook_data="0x")
    assert resolve_swap_selection(params, token_in=ZERO, token_out=TOKEN, default_fee=3000, lookup=lambda _: key).key == key
    params["hooks"] = "0x" + "b" * 36 + "0080"
    with pytest.raises(ValueError, match="hooks conflicts"):
        resolve_swap_selection(params, token_in=ZERO, token_out=TOKEN, default_fee=3000, lookup=lambda _: key)
