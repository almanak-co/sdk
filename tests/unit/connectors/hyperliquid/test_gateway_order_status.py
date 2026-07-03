"""Gateway-side orderStatus egress for Hyperliquid (VIB-5597).

The third-party Info-API egress lives on the gateway connector (gateway-boundary
rule). These tests exercise the transport helper + the capability method with a
faked aiohttp session — no real network.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from almanak.connectors._base.gateway_capabilities import GatewayOrderStatusCapability
from almanak.connectors.hyperliquid.gateway.provider import (
    HyperliquidGatewayConnector,
    OrderStatusUnavailable,
    _hyperliquid_post_order_status,
)
from almanak.gateway.services.perp_fill_service import OrderStatusData

_WALLET = "0x1234567890123456789012345678901234567890"


class _FakeResponse:
    def __init__(self, status: int, payload: Any):
        self.status = status
        self._payload = payload

    async def __aenter__(self) -> _FakeResponse:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        return None

    async def json(self) -> Any:
        return self._payload

    async def text(self) -> str:
        return str(self._payload)


class _FakeSession:
    def __init__(self, response: _FakeResponse | Exception):
        self._response = response
        self.last_json: Any = None

    def post(self, url: str, *, json: Any, headers: Any) -> Any:
        self.last_json = json
        if isinstance(self._response, Exception):
            raise self._response
        return self._response


class TestPostOrderStatus:
    @pytest.mark.asyncio
    async def test_returns_json_on_200(self):
        payload = {"status": "order", "order": {"status": "filled", "order": {}}}
        session = _FakeSession(_FakeResponse(200, payload))
        out = await _hyperliquid_post_order_status(session, wallet_address=_WALLET, cloid=0x1234)
        assert out == payload
        # The wire body was built by the pure connector helper (single source of truth).
        assert session.last_json["type"] == "orderStatus"
        assert session.last_json["user"] == _WALLET
        assert session.last_json["oid"].endswith("1234")

    @pytest.mark.asyncio
    async def test_non_200_raises_unavailable(self):
        session = _FakeSession(_FakeResponse(500, "server error"))
        with pytest.raises(OrderStatusUnavailable):
            await _hyperliquid_post_order_status(session, wallet_address=_WALLET, cloid=1)

    @pytest.mark.asyncio
    async def test_transport_error_raises_unavailable(self):
        session = _FakeSession(RuntimeError("connection reset"))
        with pytest.raises(OrderStatusUnavailable):
            await _hyperliquid_post_order_status(session, wallet_address=_WALLET, cloid=1)

    @pytest.mark.asyncio
    async def test_non_object_payload_raises_unavailable(self):
        session = _FakeSession(_FakeResponse(200, ["not", "an", "object"]))
        with pytest.raises(OrderStatusUnavailable):
            await _hyperliquid_post_order_status(session, wallet_address=_WALLET, cloid=1)


class TestCapabilityMethod:
    def test_connector_implements_capability(self):
        conn = HyperliquidGatewayConnector()
        assert isinstance(conn, GatewayOrderStatusCapability)
        assert conn.order_status_venue() == "hyperliquid"

    @pytest.mark.asyncio
    async def test_fetch_order_status_uses_shared_session(self):
        # The provider now parses the raw payload connector-side and returns a
        # neutral gateway-side OrderStatusData (so the gateway holds no connector
        # import). This rejected payload parses to a REJECTED verdict.
        payload = {"status": "order", "order": {"status": "rejected", "order": {}}}
        session = _FakeSession(_FakeResponse(200, payload))
        servicer = AsyncMock()
        servicer._get_http_session = AsyncMock(return_value=session)

        conn = HyperliquidGatewayConnector()
        out = await conn.fetch_order_status(servicer, wallet_address=_WALLET, cloid=0xABCD, chain="hyperevm")
        assert isinstance(out, OrderStatusData)
        assert out.status == "rejected"
        assert out.filled_size == ""  # Empty != Zero — venue reported no fill
        assert out.avg_fill_price == ""
        servicer._get_http_session.assert_awaited_once()
