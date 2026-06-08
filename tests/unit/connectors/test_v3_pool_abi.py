"""Unit tests for the shared V3 pool ABI helpers."""

from __future__ import annotations

import pytest

from almanak.connectors._strategy_base.v3_pool_abi import (
    V3_GET_POOL_SELECTOR,
    encode_get_pool,
    encode_v3_get_pool,
)

TOKEN_A = "0x" + "a" * 40
TOKEN_B = "0x" + "b" * 40
# Aerodrome CL getPool(address,address,int24) — the signed-int24 path.
AERODROME_CL_SELECTOR = "0x28af8d0b"


class TestEncodeGetPool:
    def test_layout_selector_plus_three_words(self) -> None:
        calldata = encode_get_pool(V3_GET_POOL_SELECTOR, TOKEN_A, TOKEN_B, 3000)
        assert calldata.startswith(V3_GET_POOL_SELECTOR)
        # selector + three 32-byte (64 hex char) words.
        assert len(calldata) == len(V3_GET_POOL_SELECTOR) + 3 * 64

    def test_token_addresses_lowercased_and_left_padded(self) -> None:
        calldata = encode_get_pool(V3_GET_POOL_SELECTOR, TOKEN_A.upper(), TOKEN_B, 500)
        body = calldata[len(V3_GET_POOL_SELECTOR) :]
        assert body[:64] == "0" * 24 + "a" * 40
        assert body[64:128] == "0" * 24 + "b" * 40

    def test_positive_fee_tiers_round_trip_unsigned(self) -> None:
        for fee in (100, 500, 2500, 3000, 10000):
            word = bytes.fromhex(encode_get_pool(V3_GET_POOL_SELECTOR, TOKEN_A, TOKEN_B, fee)[-64:])
            assert int.from_bytes(word, "big", signed=False) == fee

    def test_zero_spacing(self) -> None:
        calldata = encode_get_pool(AERODROME_CL_SELECTOR, TOKEN_A, TOKEN_B, 0)
        assert calldata[-64:] == "0" * 64

    def test_negative_int24_is_two_complement_sign_extended(self) -> None:
        # -1 sign-extends to a full 32-byte word of 0xff (plain hex() slicing
        # would emit malformed "...00x1" calldata here).
        calldata = encode_get_pool(AERODROME_CL_SELECTOR, TOKEN_A, TOKEN_B, -1)
        assert calldata[-64:] == "f" * 64

    def test_negative_int24_round_trips_as_signed(self) -> None:
        for spacing in (-1, -50, -200, -8388608):  # down to int24 min
            word = bytes.fromhex(encode_get_pool(AERODROME_CL_SELECTOR, TOKEN_A, TOKEN_B, spacing)[-64:])
            assert int.from_bytes(word, "big", signed=True) == spacing

    def test_non_int_spacing_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match="fee_or_spacing must be int"):
            encode_get_pool(V3_GET_POOL_SELECTOR, TOKEN_A, TOKEN_B, "3000")  # type: ignore[arg-type]


class TestEncodeV3GetPool:
    def test_delegates_with_canonical_v3_selector(self) -> None:
        expected = encode_get_pool(V3_GET_POOL_SELECTOR, TOKEN_A, TOKEN_B, 3000)
        result = encode_v3_get_pool(TOKEN_A, TOKEN_B, 3000)
        assert result == expected
        assert result.startswith(V3_GET_POOL_SELECTOR)
