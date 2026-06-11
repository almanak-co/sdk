"""Unit tests for GatewayClient.query_v4_position_state() (VIB-5024).

Exercises the response-parsing branches directly (the valuer tests stub this
method, so the wrapper body is otherwise uncovered): success → V4PositionState,
``success=False`` → None, unparseable / Empty numeric fields → None, missing
fee fields → None (a complete HIGH read requires measured fees), and the
not-connected / RpcError short-circuits. Empty ("") ≠ measured-zero ("0").
"""

from __future__ import annotations

from unittest.mock import MagicMock

import grpc

from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig, V4PositionState


def _make_client() -> GatewayClient:
    config = GatewayClientConfig(host="localhost", port=50051, timeout=10.0)
    client = GatewayClient(config)
    client._rpc_stub = MagicMock()
    return client


def _response(
    *,
    success=True,
    liquidity="1002136843936",
    tick_lower=-887220,
    tick_upper=887220,
    current_tick=-50,
    sqrt_price_x96="79228162514264337593543950336",
    pool_id="0x" + "ab" * 32,
    tokens_owed0="0",
    tokens_owed1="0",
    error="",
):
    r = MagicMock()
    r.success = success
    r.liquidity = liquidity
    r.tick_lower = tick_lower
    r.tick_upper = tick_upper
    r.current_tick = current_tick
    r.sqrt_price_x96 = sqrt_price_x96
    r.pool_id = pool_id
    r.tokens_owed0 = tokens_owed0
    r.tokens_owed1 = tokens_owed1
    r.error = error
    return r


def _call(client):
    return client.query_v4_position_state(
        chain="base",
        position_manager="0x7C5f5A4bBd8fD63184577525326123B519429bDc",
        state_view="0xA3c0c9b65baD0b08107Aa264b0f3dB444b867A71",
        token_id=4242,
    )


class TestSuccess:
    def test_parses_full_state(self):
        client = _make_client()
        client._rpc_stub.QueryV4PositionState.return_value = _response(
            tokens_owed0="500", tokens_owed1="7"
        )
        state = _call(client)
        assert isinstance(state, V4PositionState)
        assert state.liquidity == 1002136843936
        assert state.tick_lower == -887220
        assert state.tick_upper == 887220
        assert state.current_tick == -50
        assert state.sqrt_price_x96 == 79228162514264337593543950336
        assert state.tokens_owed0 == 500
        assert state.tokens_owed1 == 7
        assert state.pool_id.startswith("0x")

    def test_measured_zero_fees_preserved(self):
        """Empty≠Zero: "0" owed is a measured zero, not unmeasured."""
        client = _make_client()
        client._rpc_stub.QueryV4PositionState.return_value = _response(
            liquidity="0", tokens_owed0="0", tokens_owed1="0"
        )
        state = _call(client)
        assert state is not None
        assert state.liquidity == 0
        assert state.tokens_owed0 == 0
        assert state.tokens_owed1 == 0


class TestNoLiveState:
    def test_not_connected_returns_none(self):
        config = GatewayClientConfig(host="localhost", port=50051, timeout=10.0)
        client = GatewayClient(config)
        client._rpc_stub = None
        assert _call(client) is None

    def test_success_false_returns_none(self):
        client = _make_client()
        client._rpc_stub.QueryV4PositionState.return_value = _response(
            success=False, error="StateView.getSlot0 failed"
        )
        assert _call(client) is None

    def test_rpc_error_returns_none(self):
        client = _make_client()
        client._rpc_stub.QueryV4PositionState.side_effect = grpc.RpcError("boom")
        assert _call(client) is None

    def test_unparseable_numeric_returns_none(self):
        client = _make_client()
        client._rpc_stub.QueryV4PositionState.return_value = _response(liquidity="not-a-number")
        assert _call(client) is None

    def test_empty_liquidity_returns_none(self):
        """Empty ("") liquidity on a claimed-success read → no live state."""
        client = _make_client()
        client._rpc_stub.QueryV4PositionState.return_value = _response(liquidity="")
        assert _call(client) is None

    def test_empty_sqrt_price_returns_none(self):
        client = _make_client()
        client._rpc_stub.QueryV4PositionState.return_value = _response(sqrt_price_x96="")
        assert _call(client) is None

    def test_missing_fee_fields_returns_none(self):
        """A complete HIGH read requires measured fees; "" owed → no live state."""
        client = _make_client()
        client._rpc_stub.QueryV4PositionState.return_value = _response(tokens_owed0="")
        assert _call(client) is None

        client._rpc_stub.QueryV4PositionState.return_value = _response(tokens_owed1="")
        assert _call(client) is None
