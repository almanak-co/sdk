"""Tests for EnsoService gateway implementation."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.enso_service import EnsoServiceServicer


@pytest.fixture
def settings():
    """Create test settings."""
    return GatewaySettings()


@pytest_asyncio.fixture
async def enso_service(settings):
    """Create EnsoService instance."""
    service = EnsoServiceServicer(settings)
    yield service
    await service.close()


@pytest.fixture
def mock_context():
    """Create mock gRPC context."""
    context = MagicMock()
    context.set_code = MagicMock()
    context.set_details = MagicMock()
    return context


class TestEnsoServiceAmountOutNormalization:
    """Ensure amountOut supports scalar and list response formats."""

    @pytest.mark.asyncio
    async def test_get_route_accepts_scalar_amount_out(self, enso_service, mock_context):
        """GetRoute should accept scalar amountOut payloads."""
        request = gateway_pb2.EnsoRouteRequest(
            chain="arbitrum",
            token_in="0x1111111111111111111111111111111111111111",
            token_out="0x2222222222222222222222222222222222222222",
            amount_in="1000000",
            from_address="0x3333333333333333333333333333333333333333",
        )
        mock_payload = {
            "tx": {"to": "0x4444444444444444444444444444444444444444", "data": "0xabc", "value": "0", "gas": "210000"},
            "amountOut": "123456",
            "priceImpact": 12,
            "gas": "210000",
            "bridgeFee": "0",
            "route": [],
        }

        with patch.object(enso_service, "_request", AsyncMock(return_value=(True, mock_payload, None))):
            response = await enso_service.GetRoute(request, mock_context)

        assert response.success is True
        assert response.amount_out == "123456"

    @pytest.mark.asyncio
    async def test_get_quote_accepts_scalar_amount_out(self, enso_service, mock_context):
        """GetQuote should accept scalar amountOut payloads."""
        request = gateway_pb2.EnsoQuoteRequest(
            chain="arbitrum",
            token_in="0x1111111111111111111111111111111111111111",
            token_out="0x2222222222222222222222222222222222222222",
            amount_in="1000000",
            from_address="0x3333333333333333333333333333333333333333",
        )
        mock_payload = {
            "amountOut": 987654,
            "priceImpact": 8,
            "gas": "190000",
        }

        with patch.object(enso_service, "_request", AsyncMock(return_value=(True, mock_payload, None))):
            response = await enso_service.GetQuote(request, mock_context)

        assert response.success is True
        assert response.amount_out == "987654"

    @pytest.mark.asyncio
    async def test_get_quote_still_supports_list_amount_out(self, enso_service, mock_context):
        """GetQuote should continue to support list amountOut payloads."""
        request = gateway_pb2.EnsoQuoteRequest(
            chain="arbitrum",
            token_in="0x1111111111111111111111111111111111111111",
            token_out="0x2222222222222222222222222222222222222222",
            amount_in="1000000",
            from_address="0x3333333333333333333333333333333333333333",
        )
        mock_payload = {
            "amountOut": ["555"],
            "priceImpact": 5,
            "gas": "170000",
        }

        with patch.object(enso_service, "_request", AsyncMock(return_value=(True, mock_payload, None))):
            response = await enso_service.GetQuote(request, mock_context)

        assert response.success is True
        assert response.amount_out == "555"
