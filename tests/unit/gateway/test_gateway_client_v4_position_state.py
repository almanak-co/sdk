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


# ---------------------------------------------------------------------------
# query_v4_position_closure — teardown tri-state (VIB-5634)
# ---------------------------------------------------------------------------


def _closure_call(client, block=None):
    return client.query_v4_position_closure(
        chain="base",
        position_manager="0x7C5f5A4bBd8fD63184577525326123B519429bDc",
        state_view="0xA3c0c9b65baD0b08107Aa264b0f3dB444b867A71",
        token_id=4242,
        block=block,
    )


def _closure_response(*, success=False, closed=False, liquidity="", tokens_owed0="", tokens_owed1="", error=""):
    r = MagicMock()
    r.success = success
    r.closed = closed
    r.liquidity = liquidity
    r.tokens_owed0 = tokens_owed0
    r.tokens_owed1 = tokens_owed1
    r.error = error
    return r


class TestQueryV4PositionClosure:
    def test_gateway_closed_flag_is_closed(self):
        """Empty-return (burned) → gateway sets closed=True → MEASURED closure."""
        client = _make_client()
        client._rpc_stub.QueryV4PositionState.return_value = _closure_response(success=False, closed=True)
        read = _closure_call(client)
        assert read.closed is True
        assert read.unmeasured is False

    def test_measured_full_drain_is_closed(self):
        """success + liquidity 0 + no owed fees → measured full drain → closed."""
        client = _make_client()
        client._rpc_stub.QueryV4PositionState.return_value = _closure_response(
            success=True, closed=False, liquidity="0", tokens_owed0="0", tokens_owed1="0"
        )
        read = _closure_call(client)
        assert read.closed is True
        assert read.unmeasured is False

    def test_measured_residual_liquidity_is_open(self):
        """success + residual liquidity → measured-open → FAILED (closed=False)."""
        client = _make_client()
        client._rpc_stub.QueryV4PositionState.return_value = _closure_response(
            success=True, closed=False, liquidity="123", tokens_owed0="0", tokens_owed1="0"
        )
        read = _closure_call(client)
        assert read.closed is False
        assert read.unmeasured is False
        assert read.residual_liquidity == 123

    def test_measured_residual_fees_is_open(self):
        """success + liquidity 0 but owed fees > 0 → still a residual (open)."""
        client = _make_client()
        client._rpc_stub.QueryV4PositionState.return_value = _closure_response(
            success=True, closed=False, liquidity="0", tokens_owed0="0", tokens_owed1="9"
        )
        read = _closure_call(client)
        assert read.closed is False
        assert read.unmeasured is False
        assert read.residual_owed1 == 9

    def test_read_fault_is_unmeasured(self):
        """success=False WITHOUT closed → honest read fault → UNVERIFIED, not FAILED."""
        client = _make_client()
        client._rpc_stub.QueryV4PositionState.return_value = _closure_response(
            success=False, closed=False, error="execution reverted"
        )
        read = _closure_call(client)
        assert read.closed is False
        assert read.unmeasured is True

    def test_not_connected_is_unmeasured(self):
        client = _make_client()
        client._rpc_stub = None
        read = _closure_call(client)
        assert read.unmeasured is True

    def test_rpc_error_is_unmeasured(self):
        client = _make_client()
        client._rpc_stub.QueryV4PositionState.side_effect = grpc.RpcError("boom")
        read = _closure_call(client)
        assert read.unmeasured is True

    def test_unparseable_numeric_on_success_is_unmeasured(self):
        """success but a garbage numeric field must NOT coerce to a residual/closed."""
        client = _make_client()
        client._rpc_stub.QueryV4PositionState.return_value = _closure_response(
            success=True, closed=False, liquidity="not-a-number", tokens_owed0="0", tokens_owed1="0"
        )
        read = _closure_call(client)
        assert read.unmeasured is True


class TestQueryV4PositionClosureNeverRaises:
    """VIB-5634 (Gemini #2): query_v4_position_closure must honour its "Never
    raises" contract — an UNEXPECTED error (not just grpc.RpcError) degrades to
    unmeasured=True (fail-safe -> UNVERIFIED), never a false closure and never a
    crash into the teardown-verification caller."""

    def test_encode_block_tag_raise_is_unmeasured(self):
        # A bool block makes _encode_block_tag raise (bool is rejected as a block)
        # BEFORE the grpc call — the general except must catch it.
        client = _make_client()
        client._rpc_stub.QueryV4PositionState.return_value = _closure_response(success=False, closed=True)
        read = client.query_v4_position_closure(
            chain="base",
            position_manager="0x7C5f5A4bBd8fD63184577525326123B519429bDc",
            state_view="0xA3c0c9b65baD0b08107Aa264b0f3dB444b867A71",
            token_id=4242,
            block=True,  # bool -> _encode_block_tag raises
        )
        assert read.unmeasured is True
        assert read.closed is False

    def test_unexpected_stub_exception_is_unmeasured(self):
        # A non-RpcError exception from the stub must NOT propagate.
        client = _make_client()
        client._rpc_stub.QueryV4PositionState.side_effect = RuntimeError("kaboom")
        read = _closure_call(client)
        assert read.unmeasured is True
        assert read.closed is False
