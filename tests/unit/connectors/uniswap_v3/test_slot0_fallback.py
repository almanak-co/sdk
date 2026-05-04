"""Tests for ``almanak.framework.connectors.uniswap_v3.slot0_fallback``.

VIB-3893 / VIB-3940: when an LP_OPEN or LP_CLOSE receipt has no Swap event
on the pool, the receipt parser leaves ``current_tick=None``. The runner
calls ``enrich_lp_open_with_slot0`` / ``enrich_lp_close_with_slot0`` after
parsing to fetch the live tick via the gateway's ``slot0()`` eth_call.

These tests pin the fail-open contract (any error path returns the input
unchanged) and the happy path (a valid eth_call result fills the field).
"""

from __future__ import annotations

import dataclasses
from typing import Any
from unittest.mock import MagicMock

import pytest

from almanak.framework.connectors.uniswap_v3.slot0_fallback import (
    SLOT0_SELECTOR,
    _decode_slot0_tick,
    enrich_lp_close_with_slot0,
    enrich_lp_open_with_slot0,
    fetch_slot0_tick,
)
from almanak.framework.execution.extracted_data import LPCloseData, LPOpenData

POOL_ADDR = "0xc31e54c7a869b9fcbecc14363cf510d1c41fa443"
CHAIN = "arbitrum"


# Helper: build a slot0() return blob with a given tick at offset 32 (word 2)
def _slot0_blob(tick: int) -> str:
    """Encode a slot0() return blob with the desired int24 tick.

    slot0 returns several values; we only care about the tick at word 2.
    Values are 32-byte right-aligned. For a signed int24 stored as int256,
    two's-complement encoding works because we read the full 256-bit word.
    """
    sqrt_price_word = "00" * 32  # word 1
    tick_word = f"{tick & ((1 << 256) - 1):064x}"  # word 2 (two's complement)
    # We only need 2 words minimum but a real slot0 has more — append zeros
    rest = "00" * 32 * 5  # observationIndex, observationCardinality, ...
    return "0x" + sqrt_price_word + tick_word + rest


# ---------------------------------------------------------------------------
# _decode_slot0_tick
# ---------------------------------------------------------------------------


class TestDecodeSlot0Tick:
    def test_returns_none_for_empty(self) -> None:
        assert _decode_slot0_tick("") is None
        assert _decode_slot0_tick("0x") is None

    def test_returns_none_for_too_short(self) -> None:
        # Less than 2 words (128 hex chars)
        assert _decode_slot0_tick("0x" + "00" * 30) is None

    def test_decodes_positive_tick(self) -> None:
        assert _decode_slot0_tick(_slot0_blob(12345)) == 12345

    def test_decodes_negative_tick_two_complement(self) -> None:
        assert _decode_slot0_tick(_slot0_blob(-12345)) == -12345

    def test_decodes_zero_tick(self) -> None:
        assert _decode_slot0_tick(_slot0_blob(0)) == 0

    def test_handles_no_0x_prefix(self) -> None:
        blob = _slot0_blob(42)[2:]  # strip 0x
        assert _decode_slot0_tick(blob) == 42


# ---------------------------------------------------------------------------
# fetch_slot0_tick
# ---------------------------------------------------------------------------


class TestFetchSlot0Tick:
    def test_returns_none_when_inputs_missing(self) -> None:
        assert fetch_slot0_tick(None, CHAIN, POOL_ADDR) is None
        assert fetch_slot0_tick(MagicMock(), "", POOL_ADDR) is None
        assert fetch_slot0_tick(MagicMock(), CHAIN, "") is None

    def test_returns_none_when_eth_call_raises(self) -> None:
        client = MagicMock()
        client.eth_call.side_effect = RuntimeError("rpc down")
        assert fetch_slot0_tick(client, CHAIN, POOL_ADDR) is None

    def test_returns_decoded_tick_on_success(self) -> None:
        client = MagicMock()
        client.eth_call.return_value = _slot0_blob(99)
        out = fetch_slot0_tick(client, CHAIN, POOL_ADDR)
        assert out == 99
        # Selector + chain + address forwarded to the client
        client.eth_call.assert_called_once_with(CHAIN, POOL_ADDR, SLOT0_SELECTOR)

    def test_handles_bytes_response(self) -> None:
        client = MagicMock()
        # Bytes response: convert to hex via the helper's normalisation
        blob = _slot0_blob(77)[2:]
        client.eth_call.return_value = bytes.fromhex(blob)
        assert fetch_slot0_tick(client, CHAIN, POOL_ADDR) == 77

    def test_handles_bytearray_response(self) -> None:
        client = MagicMock()
        blob = _slot0_blob(-77)[2:]
        client.eth_call.return_value = bytearray(bytes.fromhex(blob))
        assert fetch_slot0_tick(client, CHAIN, POOL_ADDR) == -77

    def test_handles_none_response(self) -> None:
        client = MagicMock()
        client.eth_call.return_value = None
        # _decode_slot0_tick("") returns None — fail-open
        assert fetch_slot0_tick(client, CHAIN, POOL_ADDR) is None

    def test_handles_garbage_response(self) -> None:
        client = MagicMock()
        # Non-hex string causes int(..., 16) to raise inside _decode_slot0_tick
        client.eth_call.return_value = "not-a-hex-blob-but-long-enough" * 6
        assert fetch_slot0_tick(client, CHAIN, POOL_ADDR) is None


# ---------------------------------------------------------------------------
# enrich_lp_open_with_slot0
# ---------------------------------------------------------------------------


def _open(**kwargs: Any) -> LPOpenData:
    """Build an LPOpenData with sensible defaults."""
    base = dict(
        position_id=1,
        tick_lower=-100,
        tick_upper=100,
        liquidity=1000,
        amount0=10,
        amount1=20,
        current_tick=None,
        pool_address=POOL_ADDR,
    )
    base.update(kwargs)
    return LPOpenData(**base)


class TestEnrichLpOpenSlot0:
    def test_returns_none_when_input_none(self) -> None:
        assert enrich_lp_open_with_slot0(None, gateway_client=MagicMock(), chain=CHAIN) is None

    def test_returns_input_when_not_lp_open(self) -> None:
        out = enrich_lp_open_with_slot0("not an LPOpenData", gateway_client=MagicMock(), chain=CHAIN)
        assert out == "not an LPOpenData"

    def test_returns_unchanged_when_current_tick_already_set(self) -> None:
        lp = _open(current_tick=42)
        out = enrich_lp_open_with_slot0(lp, gateway_client=MagicMock(), chain=CHAIN)
        assert out is lp

    def test_returns_unchanged_when_pool_address_empty(self) -> None:
        lp = _open(pool_address="")
        out = enrich_lp_open_with_slot0(lp, gateway_client=MagicMock(), chain=CHAIN)
        assert out is lp

    def test_returns_unchanged_when_eth_call_fails(self) -> None:
        client = MagicMock()
        client.eth_call.side_effect = RuntimeError("boom")
        lp = _open()
        out = enrich_lp_open_with_slot0(lp, gateway_client=client, chain=CHAIN)
        assert out is lp

    def test_fills_current_tick_on_success(self) -> None:
        client = MagicMock()
        client.eth_call.return_value = _slot0_blob(123)
        lp = _open()
        out = enrich_lp_open_with_slot0(lp, gateway_client=client, chain=CHAIN)
        assert out is not lp  # New instance via dataclasses.replace
        assert isinstance(out, LPOpenData)
        assert out.current_tick == 123
        # All other fields preserved
        assert out.position_id == lp.position_id
        assert out.liquidity == lp.liquidity
        assert out.pool_address == lp.pool_address


# ---------------------------------------------------------------------------
# enrich_lp_close_with_slot0
# ---------------------------------------------------------------------------


def _close(**kwargs: Any) -> LPCloseData:
    base = dict(
        amount0_collected=100,
        amount1_collected=200,
        fees0=5,
        fees1=10,
        liquidity_removed=1000,
        current_tick=None,
        pool_address=POOL_ADDR,
    )
    base.update(kwargs)
    return LPCloseData(**base)


class TestEnrichLpCloseSlot0:
    def test_returns_none_when_input_none(self) -> None:
        assert enrich_lp_close_with_slot0(None, gateway_client=MagicMock(), chain=CHAIN) is None

    def test_returns_input_when_not_lp_close(self) -> None:
        out = enrich_lp_close_with_slot0("not LPCloseData", gateway_client=MagicMock(), chain=CHAIN)
        assert out == "not LPCloseData"

    def test_returns_unchanged_when_current_tick_already_set(self) -> None:
        lp = _close(current_tick=99)
        out = enrich_lp_close_with_slot0(lp, gateway_client=MagicMock(), chain=CHAIN)
        assert out is lp

    def test_returns_unchanged_when_pool_address_empty(self) -> None:
        lp = _close(pool_address="")
        out = enrich_lp_close_with_slot0(lp, gateway_client=MagicMock(), chain=CHAIN)
        assert out is lp

    def test_returns_unchanged_when_tick_fetch_fails(self) -> None:
        client = MagicMock()
        client.eth_call.side_effect = RuntimeError("boom")
        lp = _close()
        out = enrich_lp_close_with_slot0(lp, gateway_client=client, chain=CHAIN)
        assert out is lp

    def test_fills_current_tick_on_success(self) -> None:
        client = MagicMock()
        client.eth_call.return_value = _slot0_blob(-321)
        lp = _close()
        out = enrich_lp_close_with_slot0(lp, gateway_client=client, chain=CHAIN)
        assert isinstance(out, LPCloseData)
        assert out.current_tick == -321
        # Other fields preserved
        assert out.amount0_collected == lp.amount0_collected
        assert out.fees0 == lp.fees0


# ---------------------------------------------------------------------------
# Module exports
# ---------------------------------------------------------------------------


class TestExports:
    def test_slot0_selector_constant(self) -> None:
        # keccak256("slot0()")[:4] = 0x3850c7bd — the selector the on-chain
        # call must use; encoding it wrong silently breaks every fallback.
        assert SLOT0_SELECTOR == "0x3850c7bd"
