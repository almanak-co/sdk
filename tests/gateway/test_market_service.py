"""Tests for MarketService gateway implementation."""

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.market_service import MarketServiceServicer


@pytest.fixture
def settings():
    """Create test settings."""
    return GatewaySettings()


@pytest.fixture
def market_service(settings):
    """Create MarketService instance."""
    return MarketServiceServicer(settings)


@pytest.fixture
def mock_context():
    """Create mock gRPC context."""
    context = MagicMock()
    context.set_code = MagicMock()
    context.set_details = MagicMock()
    return context


class TestMarketServiceGetPrice:
    """Tests for MarketService.GetPrice."""

    @pytest.mark.asyncio
    async def test_get_price_success(self, market_service, mock_context):
        """GetPrice returns price from aggregator."""
        from datetime import UTC, datetime

        from almanak.framework.data.interfaces import PriceResult

        # Mock the price aggregator
        mock_result = PriceResult(
            price=Decimal("2500.50"),
            source="coingecko",
            timestamp=datetime.now(UTC),
            confidence=0.95,
            stale=False,
        )

        with patch.object(market_service, "_price_aggregator") as mock_aggregator:
            mock_aggregator.get_aggregated_price = AsyncMock(return_value=mock_result)
            market_service._initialized = True

            request = gateway_pb2.PriceRequest(token="ETH", quote="USD")
            response = await market_service.GetPrice(request, mock_context)

            assert response.price == "2500.50"
            assert response.source == "coingecko"
            assert response.confidence == 0.95
            assert response.stale is False

    @pytest.mark.asyncio
    async def test_get_price_default_quote(self, market_service, mock_context):
        """GetPrice defaults to USD when quote not specified."""
        from datetime import UTC, datetime

        from almanak.framework.data.interfaces import PriceResult

        mock_result = PriceResult(
            price=Decimal("100.00"),
            source="test",
            timestamp=datetime.now(UTC),
            confidence=1.0,
            stale=False,
        )

        with patch.object(market_service, "_price_aggregator") as mock_aggregator:
            mock_aggregator.get_aggregated_price = AsyncMock(return_value=mock_result)
            market_service._initialized = True

            request = gateway_pb2.PriceRequest(token="WBTC")  # No quote specified
            await market_service.GetPrice(request, mock_context)

            # Verify USD was used as default
            mock_aggregator.get_aggregated_price.assert_called_once_with("WBTC", "USD")

    @pytest.mark.asyncio
    async def test_get_price_error_handling(self, market_service, mock_context):
        """GetPrice handles errors gracefully."""
        with patch.object(market_service, "_price_aggregator") as mock_aggregator:
            mock_aggregator.get_aggregated_price = AsyncMock(side_effect=Exception("API error"))
            market_service._initialized = True

            request = gateway_pb2.PriceRequest(token="INVALID", quote="USD")
            response = await market_service.GetPrice(request, mock_context)

            # Should return empty response and set error code
            assert response.price == ""
            mock_context.set_code.assert_called()


class TestMarketServiceGetBalance:
    """Tests for MarketService.GetBalance."""

    @pytest.mark.asyncio
    async def test_get_balance_requires_wallet(self, market_service, mock_context):
        """GetBalance requires wallet_address."""
        request = gateway_pb2.BalanceRequest(token="WETH", chain="arbitrum")
        await market_service.GetBalance(request, mock_context)

        mock_context.set_code.assert_called()
        mock_context.set_details.assert_called_with("wallet_address: required")

    @pytest.mark.asyncio
    async def test_get_balance_success(self, market_service, mock_context):
        """GetBalance returns balance from provider."""
        from datetime import UTC, datetime

        from almanak.framework.data.interfaces import BalanceResult

        # Use valid Ethereum address format (0x + 40 hex chars)
        valid_address = "0x1234567890123456789012345678901234567890"

        mock_result = BalanceResult(
            balance=Decimal("10.5"),
            token="WETH",
            address=valid_address,
            decimals=18,
            raw_balance=10500000000000000000,
            timestamp=datetime.now(UTC),
            stale=False,
        )

        # Mock balance provider
        mock_provider = MagicMock()
        mock_provider.get_balance = AsyncMock(return_value=mock_result)

        with patch.object(market_service, "_get_balance_provider", return_value=mock_provider):
            market_service._initialized = True

            # Also mock price aggregator for USD conversion
            with patch.object(market_service, "_price_aggregator") as mock_aggregator:
                mock_aggregator.get_aggregated_price = AsyncMock(side_effect=Exception("Skip USD"))

                request = gateway_pb2.BalanceRequest(
                    token="WETH",
                    chain="arbitrum",
                    wallet_address=valid_address,
                )
                response = await market_service.GetBalance(request, mock_context)

                assert response.balance == "10.5"
                assert response.decimals == 18


class TestMarketServiceInitialization:
    """Tests for MarketService price source initialization."""

    @pytest.mark.asyncio
    async def test_no_cg_key_uses_onchain_primary(self):
        """Without CG key, on-chain source is first in aggregator."""
        settings = GatewaySettings(coingecko_api_key=None, chains=["arbitrum"])
        service = MarketServiceServicer(settings)

        try:
            with patch("almanak.gateway.data.price.onchain.get_rpc_url", return_value="http://localhost:8545"):
                await service._ensure_initialized()

            assert service._price_aggregator is not None
            sources = service._price_aggregator.sources
            assert len(sources) == 2
            assert sources[0].source_name == "onchain"
            assert sources[1].source_name == "coingecko"

            coingecko_sources = [source for source in sources if source.source_name == "coingecko"]
            assert len(coingecko_sources) == 1
            assert coingecko_sources[0]._api_key == ""
        finally:
            await service.close()

    @pytest.mark.asyncio
    async def test_with_cg_key_uses_coingecko_primary(self):
        """With CG key, CoinGecko source is first in aggregator."""
        settings = GatewaySettings(coingecko_api_key="test-key-123", chains=["arbitrum"])
        service = MarketServiceServicer(settings)

        try:
            with patch("almanak.gateway.data.price.onchain.get_rpc_url", return_value="http://localhost:8545"):
                await service._ensure_initialized()

            sources = service._price_aggregator.sources
            assert len(sources) == 2
            assert sources[0].source_name == "coingecko"
            assert sources[1].source_name == "onchain"
            assert sources[0]._api_key == "test-key-123"
        finally:
            await service.close()

    @pytest.mark.asyncio
    async def test_both_sources_registered_when_chain_configured(self):
        """Aggregator has 2 sources when a chain is configured, regardless of CG key."""
        for cg_key in [None, "key-123"]:
            settings = GatewaySettings(coingecko_api_key=cg_key, chains=["arbitrum"])
            service = MarketServiceServicer(settings)

            try:
                with patch("almanak.gateway.data.price.onchain.get_rpc_url", return_value="http://localhost:8545"):
                    await service._ensure_initialized()

                assert len(service._price_aggregator.sources) == 2
            finally:
                await service.close()

    @pytest.mark.asyncio
    async def test_uses_first_configured_chain(self):
        """On-chain source uses first chain from settings."""
        settings = GatewaySettings(chains=["base", "arbitrum"])
        service = MarketServiceServicer(settings)

        try:
            with patch("almanak.gateway.data.price.onchain.get_rpc_url", return_value="http://localhost:8545"):
                await service._ensure_initialized()

            # Find the on-chain source
            onchain_sources = [s for s in service._price_aggregator.sources if s.source_name == "onchain"]
            assert len(onchain_sources) == 1
            assert onchain_sources[0]._chain == "base"
        finally:
            await service.close()

    @pytest.mark.asyncio
    async def test_no_chains_disables_onchain_pricing(self):
        """Without chains configured, on-chain pricing is disabled (CoinGecko only)."""
        settings = GatewaySettings(chains=[])
        service = MarketServiceServicer(settings)

        try:
            await service._ensure_initialized()

            # Only CoinGecko source when no chain is configured
            assert len(service._price_aggregator.sources) == 1
            assert service._price_aggregator.sources[0].source_name == "coingecko"
        finally:
            await service.close()


class TestMarketServiceGetIndicator:
    """Tests for MarketService.GetIndicator."""

    @pytest.mark.asyncio
    async def test_get_indicator_unsupported_type(self, market_service, mock_context):
        """GetIndicator rejects unsupported indicator types."""
        request = gateway_pb2.IndicatorRequest(
            indicator_type="INVALID",
            token="ETH",
        )
        await market_service.GetIndicator(request, mock_context)

        mock_context.set_code.assert_called()
        assert "not supported" in str(mock_context.set_details.call_args)
