"""Regression tests for VIB-3696: dead V1 ``order_payload`` codepath removal.

The handler used to branch on ``bundle.metadata["order_payload"]`` and call
``self._clob.submit_order_payload(...)`` -- but the gateway-routed
``GatewayPolymarketClient`` doesn't expose that method, so anything that
landed in the legacy branch crashed with ``AttributeError`` at runtime.

Under V2 the strategy container holds no private keys; the gateway signs
server-side. The framework only assembles a plain ``order_request`` dict and
``ClobActionHandler`` calls ``create_and_post_order`` (which signs + posts in
one round-trip).

These tests lock the contract:
1. order_request bundles flow through to ``create_and_post_order``.
2. Bundles missing both keys are rejected by ``can_handle``.
3. Bundles with ONLY the legacy ``order_payload`` are rejected -- the dead
   branch is gone, no fallback.
"""

import asyncio
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.execution.clob_handler import (
    ClobActionHandler,
    ClobOrderStatus,
)
from almanak.framework.models.reproduction_bundle import ActionBundle


@pytest.fixture
def mock_clob_client():
    """Mock ClobClient pre-configured with a non-empty market lookup.

    The V2 path in ``ClobActionHandler.execute`` looks up a GammaMarket from
    the token_id (so ``create_and_post_order`` can route neg-risk vs binary
    CTF V2). Provide a stand-in market so the lookup succeeds.
    """
    client = MagicMock()
    market = MagicMock()
    market.id = "market-456"
    market.condition_id = "0xcondition"
    client.get_markets.return_value = [market]
    return client


@pytest.fixture
def handler(mock_clob_client):
    return ClobActionHandler(clob_client=mock_clob_client)


@pytest.fixture
def order_request_bundle():
    """V2 CLOB bundle -- only ``order_request``, no ``order_payload``.

    ``metadata["order_request"]`` is the single source of truth for V2; we
    deliberately do NOT mirror those values at the top level so a regression
    where ``ClobActionHandler.execute()`` reads ``metadata["side"]`` (etc.)
    instead of the nested payload would surface as ``KeyError`` rather than
    silently working off a stale duplicate.
    """
    return ActionBundle(
        intent_type="PREDICTION_BUY",
        transactions=[],
        metadata={
            "protocol": "polymarket",
            "intent_id": "vib-3696-test",
            "order_request": {
                "token_id": "12345",
                "side": "BUY",
                "price": "0.50",
                "size": "100",
                "time_in_force": "GTC",
                "expiration": 0,
            },
        },
    )


def _live_response():
    """Build a minimal LIVE OrderResponse stand-in for the happy path."""
    response = MagicMock()
    response.order_id = "order-abc"
    response.status = MagicMock()
    response.status.value = "LIVE"
    response.filled_size = Decimal("0")
    response.avg_fill_price = None
    return response


class TestOrderRequestPathSucceeds:
    """V2 happy path: order_request bundle reaches create_and_post_order."""

    def test_order_request_bundle_executes_via_create_and_post_order(
        self, handler, mock_clob_client, order_request_bundle
    ):
        mock_clob_client.create_and_post_order.return_value = _live_response()

        result = asyncio.run(handler.execute(order_request_bundle))

        assert result.success is True
        assert result.order_id == "order-abc"
        assert result.status == ClobOrderStatus.LIVE
        # Gateway-routed path used; legacy V1 method was NOT called.
        mock_clob_client.create_and_post_order.assert_called_once()
        mock_clob_client.submit_order_payload.assert_not_called()
        # Market lookup happened (so create_and_post_order received market=).
        mock_clob_client.get_markets.assert_called_once()
        call_kwargs = mock_clob_client.create_and_post_order.call_args.kwargs
        assert call_kwargs["token_id"] == "12345"
        assert call_kwargs["market"] is mock_clob_client.get_markets.return_value[0]
        assert call_kwargs["side"] == "BUY"
        assert call_kwargs["price"] == Decimal("0.50")
        assert call_kwargs["size"] == Decimal("100")
        assert call_kwargs["time_in_force"] == "GTC"
        assert call_kwargs["expiration"] == 0


class TestMissingMetadataRejected:
    """Bundles missing both keys must be rejected, not crash on access."""

    def test_can_handle_returns_false_when_neither_key_present(self, handler):
        bundle = ActionBundle(
            intent_type="PREDICTION_BUY",
            transactions=[],
            metadata={
                "protocol": "polymarket",
                # No order_request, no order_payload.
            },
        )
        assert handler.can_handle(bundle) is False

    def test_execute_returns_failure_when_neither_key_present(self, handler, mock_clob_client):
        bundle = ActionBundle(
            intent_type="PREDICTION_BUY",
            transactions=[],
            metadata={
                "protocol": "polymarket",
                "intent_id": "vib-3696-no-keys",
            },
        )

        result = asyncio.run(handler.execute(bundle))

        assert result.success is False
        assert result.error == "Bundle is not a CLOB order"
        # Neither V2 nor legacy V1 method should be reached.
        mock_clob_client.create_and_post_order.assert_not_called()
        mock_clob_client.submit_order_payload.assert_not_called()


class TestLegacyOrderPayloadRejected:
    """VIB-3696 regression guard: ``order_payload`` alone must NOT be accepted.

    The dead branch that called ``submit_order_payload`` on the gateway
    client (which has no such method) is gone. Bundles that only carry the
    legacy V1 key must be rejected by ``can_handle`` so they NEVER reach
    ``execute`` and never crash with AttributeError in production.
    """

    def test_can_handle_rejects_legacy_order_payload_only(self, handler):
        bundle = ActionBundle(
            intent_type="PREDICTION_BUY",
            transactions=[],
            metadata={
                "protocol": "polymarket",
                "order_payload": {
                    "order": {"salt": 1, "tokenId": "12345"},
                    "signature": "0xdeadbeef",
                    "orderType": "GTC",
                },
            },
        )
        assert handler.can_handle(bundle) is False

    def test_execute_does_not_call_submit_order_payload_for_legacy_only_bundle(
        self, handler, mock_clob_client
    ):
        """Even if execute() somehow reached for a legacy-only bundle, it must
        bail out at can_handle and never invoke the deprecated method."""
        bundle = ActionBundle(
            intent_type="PREDICTION_BUY",
            transactions=[],
            metadata={
                "protocol": "polymarket",
                "intent_id": "vib-3696-legacy-only",
                "order_payload": {
                    "order": {"salt": 1, "tokenId": "12345"},
                    "signature": "0xdeadbeef",
                    "orderType": "GTC",
                },
            },
        )

        result = asyncio.run(handler.execute(bundle))

        assert result.success is False
        assert result.error == "Bundle is not a CLOB order"
        mock_clob_client.submit_order_payload.assert_not_called()
        mock_clob_client.create_and_post_order.assert_not_called()
