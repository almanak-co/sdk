"""``read_v3_position_state`` decodes ``positions(uint256)`` for close-minimum sizing."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from almanak.connectors._strategy_base import v3_pool_validation as module
from almanak.connectors._strategy_base.v3_pool_validation import (
    V3PositionBindingReadError,
    V3PositionState,
    read_v3_position_state,
)

TOKEN0 = "0x" + "aa" * 20
TOKEN1 = "0x" + "bb" * 20
MANAGER = "0x" + "cc" * 20


def _word(value: int) -> bytes:
    return (value % 2**256).to_bytes(32, "big")


def _positions_blob(
    *,
    token0: str = TOKEN0,
    token1: str = TOKEN1,
    fee: int = 500,
    tick_lower: int = -202100,
    tick_upper: int = -200090,
    liquidity: int = 10**15,
    words: int = 12,
) -> bytes:
    fields = [0, 0, int(token0, 16), int(token1, 16), fee, tick_lower, tick_upper, liquidity, 0, 0, 0, 0]
    return b"".join(_word(value) for value in fields[:words])


def test_decodes_pair_fee_signed_ticks_and_liquidity() -> None:
    with patch.object(module, "eth_call", return_value=_positions_blob()) as call:
        state = read_v3_position_state(MANAGER, 42, "http://rpc", chain="arbitrum", gateway_client=None)

    assert state == V3PositionState(
        token0=TOKEN0, token1=TOKEN1, fee_tier=500, tick_lower=-202100, tick_upper=-200090, liquidity=10**15
    )
    assert call.call_args.kwargs["raise_errors"] is True
    assert call.call_args.args[2] == "0x99fbab88" + (42).to_bytes(32, "big").hex()


def test_positive_ticks_decode_without_sign_extension() -> None:
    with patch.object(module, "eth_call", return_value=_positions_blob(tick_lower=100, tick_upper=200)):
        state = read_v3_position_state(MANAGER, 1, "http://rpc")
    assert state is not None and (state.tick_lower, state.tick_upper) == (100, 200)


@pytest.mark.parametrize(
    "blob",
    [
        _positions_blob(words=7),
        _positions_blob(token0="0x" + "00" * 20),
        _positions_blob(fee=0),
        _positions_blob(fee=2_000_000),
        _positions_blob(tick_lower=100, tick_upper=100),
        _positions_blob(tick_lower=200, tick_upper=100),
        _positions_blob(tick_lower=-900000),
        _positions_blob(liquidity=2**128),
        b"",
    ],
    ids=(
        "short",
        "zero-token0",
        "zero-fee",
        "fee-above-100pct",
        "empty-range",
        "inverted-range",
        "tick-below-min",
        "liquidity-overflows-uint128",
        "empty",
    ),
)
def test_malformed_position_is_none_not_a_guess(blob: bytes) -> None:
    with patch.object(module, "eth_call", return_value=blob):
        assert read_v3_position_state(MANAGER, 1, "http://rpc") is None


def test_transport_failure_raises_a_typed_error() -> None:
    with (
        patch.object(module, "eth_call", side_effect=ValueError("gateway unavailable")),
        pytest.raises(V3PositionBindingReadError, match="positions\\(7\\) read unavailable"),
    ):
        read_v3_position_state(MANAGER, 7, None, chain="arbitrum")
