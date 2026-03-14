"""Tests for BatchGetBalances RPC in MarketService.

Tests concurrent balance queries across multiple tokens/chains with
partial success handling.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.market_service import MarketServiceServicer


@pytest.fixture
def settings():
    """Create mock gateway settings."""
    from almanak.gateway.core.settings import GatewaySettings

    return GatewaySettings(
        grpc_host="localhost",
        grpc_port=50051,
        network="mainnet",
    )


@pytest_asyncio.fixture
async def market_service(settings):
    """Create MarketServiceServicer."""
    service = MarketServiceServicer(settings)
    yield service
    await service.close()


@pytest.fixture
def mock_context():
    """Create mock gRPC context."""
    ctx = AsyncMock()
    ctx.set_code = MagicMock()
    ctx.set_details = MagicMock()
    return ctx


class TestBatchGetBalances:
    """Test BatchGetBalances RPC."""

    @pytest.mark.asyncio
    async def test_empty_batch(self, market_service, mock_context):
        """Empty request returns empty response."""
        request = gateway_pb2.BatchBalanceRequest(requests=[])
        response = await market_service.BatchGetBalances(request, mock_context)
        assert len(response.responses) == 0

    @pytest.mark.asyncio
    async def test_single_request(self, market_service, mock_context):
        """Single request in batch works correctly."""
        # Mock the balance provider
        mock_provider = AsyncMock()
        mock_result = MagicMock()
        mock_result.balance = 1000.5
        mock_result.address = "0xUsdc"
        mock_result.decimals = 6
        mock_result.raw_balance = 1000500000
        mock_result.timestamp = MagicMock()
        mock_result.timestamp.timestamp.return_value = 1234567890
        mock_result.stale = False
        mock_provider.get_balance = AsyncMock(return_value=mock_result)

        # Mock price aggregator
        mock_price = MagicMock()
        mock_price.price = 1.0
        mock_aggregator = AsyncMock()
        mock_aggregator.get_aggregated_price = AsyncMock(return_value=mock_price)

        market_service._initialized = True
        market_service._price_aggregator = mock_aggregator

        with patch.object(market_service, "_get_balance_provider", return_value=mock_provider):
            request = gateway_pb2.BatchBalanceRequest(
                requests=[
                    gateway_pb2.BalanceRequest(
                        token="USDC",
                        chain="arbitrum",
                        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
                    )
                ]
            )
            response = await market_service.BatchGetBalances(request, mock_context)

        assert len(response.responses) == 1
        assert response.responses[0].balance == "1000.5"

    @pytest.mark.asyncio
    async def test_invalid_chain_returns_per_response_error(self, market_service, mock_context):
        """Invalid chain returns error in the individual response, not overall failure."""
        market_service._initialized = True

        request = gateway_pb2.BatchBalanceRequest(
            requests=[
                gateway_pb2.BalanceRequest(
                    token="USDC",
                    chain="invalid_chain",
                    wallet_address="0x1234567890abcdef1234567890abcdef12345678",
                )
            ]
        )
        response = await market_service.BatchGetBalances(request, mock_context)

        assert len(response.responses) == 1
        assert response.responses[0].error != ""

    @pytest.mark.asyncio
    async def test_invalid_address_returns_per_response_error(self, market_service, mock_context):
        """Invalid wallet address returns error in the individual response."""
        market_service._initialized = True

        request = gateway_pb2.BatchBalanceRequest(
            requests=[
                gateway_pb2.BalanceRequest(
                    token="USDC",
                    chain="arbitrum",
                    wallet_address="not-an-address",
                )
            ]
        )
        response = await market_service.BatchGetBalances(request, mock_context)

        assert len(response.responses) == 1
        assert response.responses[0].error != ""
