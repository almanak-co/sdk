"""Hardening tests for the shared V3 gateway TWAP pipeline.

Covers the three follow-up fixes raised by Gemini on PR #2856 (the byte-identical
relocation of the Uniswap V3 gateway TWAP path into
``almanak/connectors/_base/v3_gateway_twap.py`` via PR #2853):

1. Solidity truncation-toward-zero for the average tick (``_twap_tick_from_cumulatives``)
   — Python ``//`` floors toward -inf; the on-chain oracle truncates toward zero.
2. ``observe()`` decode bounds (``_read_word`` / ``_decode_observe_response``) — a
   truncated payload must raise, not silently decode ``b""`` to ``0``.
3. token0()/token1() bounds (``_fetch_pool_tokens_and_decimals``) — truncated returns
   raise a clear error instead of a confusing checksum-address ValueError.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.connectors._base.v3_gateway_twap import (
    _decode_observe_response,
    _fetch_pool_tokens_and_decimals,
    _read_word,
    _tick_to_price,
    _twap_call_observe,
    _twap_tick_from_cumulatives,
    fetch_v3_twap_observation,
)
from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

# Stable Uniswap-V3 ABI selectors (no 0x prefix), mirroring the module's constants.
_OBSERVE = "883bdbfd"
_TOKEN0 = "0dfe1681"
_TOKEN1 = "d21220a7"
_DECIMALS = "313ce567"

# Base canonical addresses (any checksum-free lowercase addresses work for the fake).
_WETH = "0x4200000000000000000000000000000000000006"
_USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"


# --------------------------------------------------------------------------- #
# ABI fixtures
# --------------------------------------------------------------------------- #


def _uint_word(n: int) -> bytes:
    return int(n).to_bytes(32, byteorder="big")


def _int_word(n: int) -> bytes:
    return int(n).to_bytes(32, byteorder="big", signed=True)


def _addr_word(addr: str) -> bytes:
    return bytes(12) + bytes.fromhex(addr[2:])


def _encode_observe_response(
    tick_cumulatives: list[int],
    liquidity_cumulatives: list[int] | None = None,
) -> bytes:
    """ABI-encode ``observe`` return data ``(int56[] ticks, uint160[] liq)``."""
    if liquidity_cumulatives is None:
        liquidity_cumulatives = [0] * len(tick_cumulatives)
    offset_ticks = 64  # right after the two head words
    tick_block = _uint_word(len(tick_cumulatives)) + b"".join(_int_word(t) for t in tick_cumulatives)
    offset_liq = offset_ticks + len(tick_block)
    liq_block = _uint_word(len(liquidity_cumulatives)) + b"".join(_uint_word(x) for x in liquidity_cumulatives)
    return _uint_word(offset_ticks) + _uint_word(offset_liq) + tick_block + liq_block


def _make_fake_web3(*, observe_payload: bytes, t0_dec: int = 18, t1_dec: int = 6):
    """Fake AsyncWeb3 whose eth.call dispatches on the calldata selector."""

    async def _call(tx, block_identifier=None):
        to = tx["to"].lower()
        sel = tx["data"][2:10]
        if sel == _OBSERVE:
            return observe_payload
        if sel == _TOKEN0:
            return _addr_word(_WETH)
        if sel == _TOKEN1:
            return _addr_word(_USDC)
        if sel == _DECIMALS:
            if to == _WETH.lower():
                return _uint_word(t0_dec)
            if to == _USDC.lower():
                return _uint_word(t1_dec)
        raise AssertionError(f"unexpected eth.call sel={sel} to={to}")

    return SimpleNamespace(eth=SimpleNamespace(call=_call), to_checksum_address=lambda a: a)


def _make_servicer(web3) -> SimpleNamespace:
    async def _get_web3(_chain):
        return web3

    return SimpleNamespace(_get_web3=_get_web3)


# --------------------------------------------------------------------------- #
# Fix 1 — Solidity truncation-toward-zero (HIGH)
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "tick_diff,seconds_elapsed,expected",
    [
        # Positive numerators: floor == truncation, no adjustment.
        (1200, 600, 2),
        (1201, 600, 2),  # positive non-divisible truncates down, same as floor
        (601, 600, 1),
        # Exact multiples: no remainder either sign.
        (-1200, 600, -2),
        # Negative, NON-divisible: floor would give -2, truncation must give -1.
        (-601, 600, -1),
        (-1, 600, 0),  # floor -> -1, truncation -> 0
        (-1199, 600, -1),  # floor -> -2, truncation -> -1
        (0, 600, 0),
    ],
)
def test_twap_tick_truncates_toward_zero(tick_diff: int, seconds_elapsed: int, expected: int) -> None:
    assert _twap_tick_from_cumulatives(tick_diff, seconds_elapsed) == expected


def test_twap_tick_matches_int_truncation_across_signs() -> None:
    """The integer helper agrees with C-style ``int()`` truncation on small values.

    (Float division is unsafe on large int56 cumulatives — see the module
    docstring — but for small magnitudes it is a convenient oracle.)
    """
    for tick_diff in range(-2000, 2001):
        for seconds_elapsed in (1, 7, 600, 999):
            assert _twap_tick_from_cumulatives(tick_diff, seconds_elapsed) == int(tick_diff / seconds_elapsed)


def test_twap_tick_preserves_precision_on_large_int56() -> None:
    """Large int56 cumulatives must not lose a tick to float rounding.

    ``int(tick_diff / seconds_elapsed)`` (Gemini's suggestion) would round the
    numerator into a 53-bit float and could be off by one; the integer helper
    stays exact.
    """
    tick_diff = -(2**55) - 1  # negative, odd, beyond float53 exact range
    seconds_elapsed = 2
    q, r = divmod(tick_diff, seconds_elapsed)
    # divmod floors; truncation toward zero is one higher for a negative remainder.
    assert _twap_tick_from_cumulatives(tick_diff, seconds_elapsed) == q + 1
    # Exact integer division landmark: (2**55 + 1) / 2 truncated toward zero.
    assert _twap_tick_from_cumulatives(tick_diff, seconds_elapsed) == -(2**54)


def test_fetch_v3_twap_observation_truncates_negative_tick() -> None:
    """End-to-end regression: a negative, non-divisible tick_diff truncates toward zero.

    tick_diff = 399 - 1000 = -601 over a 600s window. Floor (Python ``//``)
    gives -2; the on-chain oracle truncates to -1. The emitted price must match
    the -1 tick, NOT the -2 tick (a ~1bp error).
    """
    payload = _encode_observe_response([1000, 399])
    web3 = _make_fake_web3(observe_payload=payload, t0_dec=18, t1_dec=6)
    servicer = _make_servicer(web3)

    point = asyncio.run(
        fetch_v3_twap_observation(
            servicer,
            chain="base",
            pool_address="0xpool",
            secs_ago_start=600,
            secs_ago_end=0,
            as_of_block=None,
            protocol="uniswap_v3",
        )
    )

    truncated_price = _tick_to_price(-1, 18, 6)
    floored_price = _tick_to_price(-2, 18, 6)
    assert Decimal(point.price) == truncated_price
    assert Decimal(point.price) != floored_price
    assert point.tick_observation_count == 2


# --------------------------------------------------------------------------- #
# Fix 2 — observe() decode bounds (_read_word / _decode_observe_response)
# --------------------------------------------------------------------------- #


def test_read_word_rejects_short_payload() -> None:
    with pytest.raises(ValueError, match="truncated"):
        _read_word(b"\x00" * 16, 0)


def test_read_word_reads_full_word() -> None:
    assert _read_word(_uint_word(12345), 0) == 12345
    assert _read_word(_int_word(-5), 0, signed=True) == -5


def test_decode_observe_roundtrips_well_formed_payload() -> None:
    ticks, liq = _decode_observe_response(_encode_observe_response([1000, 399], [7, 9]))
    assert ticks == [1000, 399]
    assert liq == [7, 9]


def test_decode_observe_preserves_negative_int56() -> None:
    ticks, _liq = _decode_observe_response(_encode_observe_response([-(2**40), 2**40]))
    assert ticks == [-(2**40), 2**40]


def test_decode_observe_rejects_too_short() -> None:
    with pytest.raises(ValueError, match="too short"):
        _decode_observe_response(b"\x00" * 64)


def test_decode_observe_rejects_truncated_array_element() -> None:
    """A length word that over-claims its element count must raise, not silently zero.

    Header is well-formed and the payload clears the 128-byte minimum, but the
    tick array declares 2 elements with only 1 present — reading element 1 runs
    past the payload. ``int.from_bytes(b"", ...)`` would otherwise return 0.
    """
    truncated = (
        _uint_word(64)  # offset_ticks
        + _uint_word(160)  # offset_liquidity (unreached)
        + _uint_word(2)  # tick_array_len = 2
        + _int_word(500)  # only element 0 present; element 1 missing
    )
    assert len(truncated) == 128  # clears the `< 128` guard
    with pytest.raises(ValueError, match="truncated"):
        _decode_observe_response(truncated)


def test_decode_observe_rejects_offset_past_end() -> None:
    """An array offset pointing past the payload must raise on the length read."""
    bad_offset = (
        _uint_word(4096)  # offset_ticks points way past the data
        + _uint_word(4096)  # offset_liquidity (unreached)
        + bytes(64)  # pad so we clear the 128-byte minimum
    )
    with pytest.raises(ValueError, match="truncated"):
        _decode_observe_response(bad_offset)


def test_twap_call_observe_normalizes_truncated_payload() -> None:
    """Defense-in-depth: a truncated payload surfaces as RateHistoryUnavailable.

    The caller already wraps decode in try/except, so the bounds check inside
    ``_decode_observe_response`` is belt-and-suspenders — but the typed error
    path must hold.
    """
    truncated = _uint_word(64) + _uint_word(160) + _uint_word(2) + _int_word(500)

    async def _call(tx, block_identifier=None):
        return truncated

    web3 = SimpleNamespace(eth=SimpleNamespace(call=_call), to_checksum_address=lambda a: a)
    with pytest.raises(RateHistoryUnavailable):
        asyncio.run(
            _twap_call_observe(
                web3,
                pool_checksum="0xpool",
                seconds_agos=[600, 0],
                block_identifier="latest",
                protocol="uniswap_v3",
                pool_address="0xpool",
            )
        )


# --------------------------------------------------------------------------- #
# Fix 3 — token0()/token1() data bounds (_fetch_pool_tokens_and_decimals)
# --------------------------------------------------------------------------- #


def _make_token_web3(*, t0_data: bytes, t1_data: bytes):
    async def _call(tx, block_identifier=None):
        sel = tx["data"][2:10]
        if sel == _TOKEN0:
            return t0_data
        if sel == _TOKEN1:
            return t1_data
        raise AssertionError(f"unexpected eth.call sel={sel}")

    return SimpleNamespace(eth=SimpleNamespace(call=_call), to_checksum_address=lambda a: a)


def test_fetch_pool_tokens_rejects_empty_token0() -> None:
    """Empty token0() data raises a clear 'truncated' error, not a checksum ValueError."""
    web3 = _make_token_web3(t0_data=b"", t1_data=_addr_word(_USDC))
    with pytest.raises(ValueError, match="truncated"):
        asyncio.run(_fetch_pool_tokens_and_decimals(web3, "0xpool", "latest"))


def test_fetch_pool_tokens_rejects_short_token1() -> None:
    web3 = _make_token_web3(t0_data=_addr_word(_WETH), t1_data=b"\x00" * 19)
    with pytest.raises(ValueError, match="truncated"):
        asyncio.run(_fetch_pool_tokens_and_decimals(web3, "0xpool", "latest"))
