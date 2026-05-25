"""Regression tests for GatewayPolymarketClient.create_and_post_order kwargs.

VIB-3694: ``ClobActionHandler`` (the runner-side CLOB executor) was extended
in the V2 cutover to pass ``market=markets[0]`` to
``ClobClient.create_and_post_order`` so the connector can route neg-risk vs
binary CTF V2 and validate tick / min-size locally. The gateway-routed
wrapper (``GatewayPolymarketClient.create_and_post_order``) was not updated
to accept the same kwarg, so every BUY/SELL through the gateway raised
``TypeError: ... got an unexpected keyword argument 'market'`` at runtime.

The fix accepts ``market=`` and ignores it — the gateway server re-fetches
the market from ``token_id`` (see
``almanak.connectors.polymarket.gateway.service.PolymarketServiceServicer.CreateAndPostOrder``),
so the wrapper does not need (and must not propagate) the local Gamma
object across the gRPC boundary.

These tests use the *real* ``GatewayPolymarketClient`` constructor with a
mocked Polymarket gRPC stub so that the wrapper code path executes — a
top-level mock of the wrapper class would mask the regression entirely.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.framework.connectors.polymarket.gateway_client import (
    GatewayPolymarketClient,
)
from almanak.framework.connectors.polymarket.models import OrderResponse, OrderStatus
from almanak.framework.gateway_client import GatewayClient, GatewayClientConfig
from almanak.gateway.proto import gateway_pb2


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _success_response() -> gateway_pb2.PolymarketOrderResponse:
    """A populated PolymarketOrderResponse that satisfies OrderResponse parsing."""
    return gateway_pb2.PolymarketOrderResponse(
        order_id="order-abc-123",
        status="LIVE",
        size_matched="0",
        success=True,
        price="0.5",
        size="10",
        avg_fill_price="",
        created_at="2026-04-29T00:00:00Z",
    )


def _make_gateway_client(stub: MagicMock) -> GatewayClient:
    """Build a real GatewayClient with a pre-injected polymarket stub.

    We bypass ``connect()`` (which would open a real gRPC channel) by setting
    the connection-state attributes directly. The wrapper only reads
    ``is_connected`` (during construction) and ``polymarket`` (per call) and
    ``config.timeout`` (per call), so this is the minimal surface needed to
    exercise the wrapper end-to-end without a live gateway.
    """
    client = GatewayClient(config=GatewayClientConfig(host="localhost", port=50051, timeout=5.0))
    # ``is_connected`` is a property that returns ``self._connected and
    # self._channel is not None`` — set both flags so the constructor's
    # gating ``if not gateway_client.is_connected`` check passes.
    client._connected = True
    client._channel = MagicMock()
    client._polymarket_stub = stub
    return client


@pytest.fixture
def stub_with_success() -> MagicMock:
    """Mocked Polymarket gRPC stub that returns a success on CreateAndPostOrder."""
    stub = MagicMock()
    stub.CreateAndPostOrder.return_value = _success_response()
    return stub


@pytest.fixture
def fake_market() -> SimpleNamespace:
    """A stand-in for a GammaMarket — the wrapper must accept any object and
    not introspect it (the gateway re-fetches the canonical market server-side)."""
    return SimpleNamespace(
        id="mkt-12345",
        condition_id="0x" + "ab" * 32,
        clob_token_ids=["111", "222"],
        order_price_min_tick_size=Decimal("0.01"),
        order_min_size=Decimal("5"),
        # Intentionally include a property that would crash if the wrapper
        # tried to JSON-serialize it — proves the wrapper truly ignores it.
        _unserializable=object(),
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestCreateAndPostOrderAcceptsMarketKwarg:
    """VIB-3694: the wrapper must accept the ``market`` kwarg without raising
    and must not propagate it across the gRPC boundary."""

    def test_passing_market_kwarg_does_not_raise_type_error(
        self,
        stub_with_success: MagicMock,
        fake_market: SimpleNamespace,
    ) -> None:
        """Pre-fix this raised ``TypeError: ... got an unexpected keyword
        argument 'market'`` and every Polymarket BUY/SELL through
        ``ClobActionHandler`` failed before reaching the network."""
        client = GatewayPolymarketClient(_make_gateway_client(stub_with_success))

        response = client.create_and_post_order(
            token_id="111",
            price=Decimal("0.5"),
            size=Decimal("10"),
            side="BUY",
            market=fake_market,
        )

        assert isinstance(response, OrderResponse)
        assert response.order_id == "order-abc-123"
        assert response.status == OrderStatus.LIVE

    def test_market_kwarg_is_not_serialized_into_grpc_request(
        self,
        stub_with_success: MagicMock,
        fake_market: SimpleNamespace,
    ) -> None:
        """The gateway re-fetches the market server-side (see
        ``polymarket_service.CreateAndPostOrder``), so the wire request must
        only carry the V2 wire fields. Putting the local market object on the
        wire would either ProtoBuf-encode-fail or — if the proto ever grew a
        ``market_json`` field — silently override the gateway's canonical
        lookup."""
        client = GatewayPolymarketClient(_make_gateway_client(stub_with_success))

        client.create_and_post_order(
            token_id="111",
            price=Decimal("0.5"),
            size=Decimal("10"),
            side="BUY",
            time_in_force="IOC",
            expiration=1745930000,
            market=fake_market,
        )

        # Exactly one CreateAndPostOrder call — no retries, no duplicate sends.
        assert stub_with_success.CreateAndPostOrder.call_count == 1
        call = stub_with_success.CreateAndPostOrder.call_args
        request = call.args[0]
        assert isinstance(request, gateway_pb2.PolymarketCreateOrderRequest)
        assert request.token_id == "111"
        assert request.price == "0.5"
        assert request.size == "10"
        assert request.side == "BUY"
        assert request.time_in_force == "IOC"
        assert request.expiration == 1745930000
        # The proto has no ``market`` field — assert that no field on the
        # message references the local object (it must stay strategy-side).
        for field, _value in request.ListFields():
            assert field.name != "market"

    def test_omitting_market_kwarg_still_works_backwards_compat(
        self,
        stub_with_success: MagicMock,
    ) -> None:
        """Callers that hit the wrapper without ``market=`` (e.g. the
        adapter's pre-signed payload path that calls a different method, or
        any future internal caller) must continue to work — the kwarg is
        accept-and-ignore, not require-or-die."""
        client = GatewayPolymarketClient(_make_gateway_client(stub_with_success))

        response = client.create_and_post_order(
            token_id="222",
            price=Decimal("0.7"),
            size=Decimal("5"),
            side="SELL",
        )

        assert isinstance(response, OrderResponse)
        assert response.order_id == "order-abc-123"
        # Defaults make it through.
        request = stub_with_success.CreateAndPostOrder.call_args.args[0]
        assert request.time_in_force == "GTC"
        assert request.expiration == 0

    def test_market_kwarg_accepts_none_explicitly(
        self,
        stub_with_success: MagicMock,
    ) -> None:
        """``market=None`` must behave identically to omitting the kwarg —
        callers may forward an ``Optional[GammaMarket]`` from elsewhere
        without conditionalizing the call site."""
        client = GatewayPolymarketClient(_make_gateway_client(stub_with_success))

        response = client.create_and_post_order(
            token_id="333",
            price=Decimal("0.25"),
            size=Decimal("4"),
            side="BUY",
            market=None,
        )

        assert response.order_id == "order-abc-123"
        assert stub_with_success.CreateAndPostOrder.call_count == 1
