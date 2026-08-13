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
    _block_at_or_before,
    _decode_observe_response,
    _fetch_pool_tokens_and_decimals,
    _historical_blocks_for_grid,
    _read_word,
    _tick_to_price,
    _twap_call_observe,
    _twap_tick_from_cumulatives,
    fetch_v3_pool_state_series,
    fetch_v3_twap_observation,
    fetch_v3_twap_series,
)
from almanak.connectors._base.v3_pool_abi import encode_v3_get_pool
from almanak.connectors.uniswap_v3.gateway.provider import UniswapV3GatewayConnector
from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

# Stable Uniswap-V3 ABI selectors (no 0x prefix), mirroring the module's constants.
_OBSERVE = "883bdbfd"
_TOKEN0 = "0dfe1681"
_TOKEN1 = "d21220a7"
_DECIMALS = "313ce567"
_SLOT0 = "3850c7bd"
_LIQUIDITY = "1a686502"
_FEE = "ddca3f43"
_GET_POOL = "1698ee82"
_BALANCE_OF = "70a08231"

# Base canonical addresses (any checksum-free lowercase addresses work for the fake).
_WETH = "0x4200000000000000000000000000000000000006"
_USDC = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
_POOL = "0x0000000000000000000000000000000000000003"
_FACTORY = "0x0000000000000000000000000000000000000004"


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


def _make_historical_web3(*, latest_block: int = 10, genesis_timestamp: int = 1_000):
    """Linear fake chain plus archive-call ledger for historical series tests."""
    calls: list[tuple[str, int | str | None, str, str]] = []

    async def _get_block(block_identifier):
        number = latest_block if block_identifier == "latest" else int(block_identifier)
        return {"number": number, "timestamp": genesis_timestamp + number * 10}

    async def _call(tx, block_identifier=None):
        to = tx["to"].lower()
        sel = tx["data"][2:10]
        calls.append((sel, block_identifier, to, tx["data"]))
        if sel == _OBSERVE:
            # First dynamic-array element is secondsAgos[0].
            window = int(tx["data"][10 + 64 + 64 : 10 + 64 + 128], 16)
            block = int(block_identifier)
            return _encode_observe_response([0, block * window])
        if sel == _TOKEN0:
            return _addr_word(_WETH)
        if sel == _TOKEN1:
            return _addr_word(_USDC)
        if sel == _DECIMALS:
            return _uint_word(18 if to == _WETH.lower() else 6)
        if sel == _SLOT0:
            return _uint_word(2**96 + int(block_identifier)) + _int_word(-7) + bytes(32 * 5)
        if sel == _LIQUIDITY:
            return _uint_word(123 + int(block_identifier))
        if sel == _FEE:
            return _uint_word(500)
        if sel == _GET_POOL:
            return _addr_word(_POOL)
        if sel == _BALANCE_OF:
            return _uint_word((10**18 if to == _WETH.lower() else 2 * 10**6) + int(block_identifier))
        raise AssertionError(f"unexpected eth.call sel={sel} to={to}")

    web3 = SimpleNamespace(
        eth=SimpleNamespace(call=_call, get_block=_get_block),
        to_checksum_address=lambda address: address,
    )
    return web3, calls


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


def test_historical_block_grid_resolves_latest_block_at_or_before_each_target() -> None:
    web3, _calls = _make_historical_web3()

    samples = asyncio.run(
        _historical_blocks_for_grid(
            web3,
            start_ts=1_025,
            end_ts=1_045,
            interval_secs=20,
            protocol="uniswap_v3",
        )
    )

    assert samples == [(2, 1_020), (4, 1_040)]


def test_historical_pool_state_uses_exact_archive_blocks_and_identity() -> None:
    web3, calls = _make_historical_web3()

    points = asyncio.run(
        fetch_v3_pool_state_series(
            _make_servicer(web3),
            chain="base",
            pool_address=_POOL,
            start_ts=1_025,
            end_ts=1_045,
            interval_secs=20,
            protocol="uniswap_v3",
            factory_address=_FACTORY,
        )
    )

    assert [(point.block_number, point.timestamp) for point in points] == [(2, 1_020), (4, 1_040)]
    assert all(point.token0 == _WETH and point.token1 == _USDC for point in points)
    assert all(point.fee_tier == 500 and point.tick == -7 for point in points)
    assert points[0].reserve0_raw == 10**18 + 2
    assert points[1].reserve1_raw == 2 * 10**6 + 4
    state_calls = [call for call in calls if call[0] in {_SLOT0, _LIQUIDITY, _BALANCE_OF}]
    assert {call[1] for call in state_calls} == {2, 4}
    factory_calls = [call for call in calls if call[0] == _GET_POOL]
    assert factory_calls == [(_GET_POOL, 4, _FACTORY.lower(), encode_v3_get_pool(_WETH, _USDC, 500))]


def test_historical_pool_state_rejects_truncated_slot0_tick_word() -> None:
    web3, _calls = _make_historical_web3()
    original_call = web3.eth.call

    async def truncated_slot0(tx, block_identifier=None):
        if tx["data"][2:10] == _SLOT0:
            return _uint_word(2**96)
        return await original_call(tx, block_identifier=block_identifier)

    web3.eth.call = truncated_slot0

    with pytest.raises(RateHistoryUnavailable, match="truncated data"):
        asyncio.run(
            fetch_v3_pool_state_series(
                _make_servicer(web3),
                chain="base",
                pool_address="0x0000000000000000000000000000000000000003",
                start_ts=1_025,
                end_ts=1_025,
                interval_secs=20,
                protocol="uniswap_v3",
                factory_address=_FACTORY,
            )
        )


def test_historical_pool_state_rejects_wrong_factory_pool() -> None:
    web3, _calls = _make_historical_web3()
    original_call = web3.eth.call

    async def wrong_factory(tx, block_identifier=None):
        if tx["data"][2:10] == _GET_POOL and tx["to"].lower() == _FACTORY.lower():
            return _addr_word("0x0000000000000000000000000000000000000005")
        return await original_call(tx, block_identifier=block_identifier)

    web3.eth.call = wrong_factory
    with pytest.raises(RateHistoryUnavailable, match="registered factory returned"):
        asyncio.run(
            fetch_v3_pool_state_series(
                _make_servicer(web3),
                chain="base",
                pool_address=_POOL,
                start_ts=1_025,
                end_ts=1_025,
                interval_secs=20,
                protocol="uniswap_v3",
                factory_address=_FACTORY,
            )
        )


@pytest.mark.parametrize("factory_address", ["", "0x1234", "not-an-address"])
def test_historical_pool_state_rejects_invalid_factory_address(factory_address: str) -> None:
    web3, calls = _make_historical_web3()

    with pytest.raises(RateHistoryUnavailable, match="invalid or missing factory address"):
        asyncio.run(
            fetch_v3_pool_state_series(
                _make_servicer(web3),
                chain="base",
                pool_address=_POOL,
                start_ts=1_025,
                end_ts=1_025,
                interval_secs=20,
                protocol="uniswap_v3",
                factory_address=factory_address,
            )
        )

    assert calls == []


def test_uniswap_pool_state_rejects_chain_without_factory() -> None:
    with pytest.raises(RateHistoryUnavailable, match="no authenticated Uniswap V3 factory"):
        asyncio.run(
            UniswapV3GatewayConnector().fetch_pool_state_series(
                SimpleNamespace(),
                chain="unsupported",
                pool_address=_POOL,
                start_ts=1_025,
                end_ts=1_025,
                interval_secs=20,
            )
        )


def test_historical_block_resolver_bisects_after_interpolation_stalls() -> None:
    """A timestamp cliff forces the bounded bisection fallback after eight probes."""
    probed_blocks: list[int] = []

    async def _get_block(block_identifier):
        number = int(block_identifier)
        probed_blocks.append(number)
        return {"number": number, "timestamp": 0 if number < 1_000 else 1_000_000}

    web3 = SimpleNamespace(eth=SimpleNamespace(get_block=_get_block))
    resolved = asyncio.run(
        _block_at_or_before(
            web3,
            500,
            lower=(0, 0),
            upper=(1_000, 1_000_000),
            protocol="uniswap_v3",
        )
    )

    assert resolved == (999, 0)
    assert resolved[1] <= 500
    assert any(block > 100 for block in probed_blocks), probed_blocks


def test_historical_series_preserves_grid_cardinality_and_block_provenance() -> None:
    """A stalled block is read once but re-expanded to every requested tick."""
    web3, calls = _make_historical_web3()
    servicer = _make_servicer(web3)

    points = asyncio.run(
        fetch_v3_twap_series(
            servicer,
            chain="ethereum",
            pool_address="0xpool",
            start_ts=1_020,
            end_ts=1_021,
            interval_secs=1,
            window_secs=1_800,
            protocol="uniswap_v3",
        )
    )

    assert len(points) == 2
    assert [point.timestamp for point in points] == [1_020, 1_020]
    assert [point.block_number for point in points] == [2, 2]
    assert [point.price for point in points] == [_tick_to_price(2, 18, 6)] * 2
    observe_calls = [call for call in calls if call[0] == _OBSERVE]
    assert [call[:3] for call in observe_calls] == [(_OBSERVE, 2, "0xpool")]
    # Immutable pool metadata is measured once for the series, not per tick.
    assert sum(call[0] in {_TOKEN0, _TOKEN1, _DECIMALS} for call in calls) == 4


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
