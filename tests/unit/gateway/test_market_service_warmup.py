"""Tests for MarketService warmup (VIB-2392).

Validates that the gateway pre-warms price caches and balance providers
on startup so the first strategy price()/balance() call doesn't block
for 30s+ while HTTP connections and caches initialize.
"""

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.gateway.core.settings import GatewaySettings


@pytest.fixture
def settings():
    """Create minimal GatewaySettings for testing."""
    s = MagicMock(spec=GatewaySettings)
    s.chains = ["arbitrum"]
    s.network = "mainnet"
    s.coingecko_api_key = None
    return s


@pytest.fixture
def market_servicer(settings):
    """Create MarketServiceServicer with mocked dependencies."""
    from almanak.gateway.services.market_service import MarketServiceServicer

    servicer = MarketServiceServicer(settings=settings)
    return servicer


class TestMarketServiceWarmup:
    """Test warmup() pre-warms price caches and balance providers."""

    @pytest.mark.asyncio
    async def test_warmup_initializes_and_fetches_price(self, market_servicer):
        """warmup() should call _ensure_initialized and fetch ETH/USD."""
        mock_aggregator = AsyncMock()
        mock_aggregator.get_aggregated_price = AsyncMock(
            return_value=MagicMock(price=Decimal("3000"))
        )

        market_servicer._initialized = True
        market_servicer._price_aggregator = mock_aggregator

        await market_servicer.warmup()

        mock_aggregator.get_aggregated_price.assert_called_once_with("ETH", "USD")

    @pytest.mark.asyncio
    async def test_warmup_prewarms_balance_provider(self, market_servicer):
        """warmup(wallet_address=...) should pre-create balance provider."""
        market_servicer._initialized = True
        market_servicer._price_aggregator = AsyncMock()
        market_servicer._price_aggregator.get_aggregated_price = AsyncMock()

        mock_provider = MagicMock()
        with patch.object(
            market_servicer, "_get_balance_provider", new_callable=AsyncMock, return_value=mock_provider
        ) as mock_get_bp:
            await market_servicer.warmup(wallet_address="0x1234567890abcdef1234567890abcdef12345678")

            mock_get_bp.assert_called_once_with("arbitrum", "0x1234567890abcdef1234567890abcdef12345678")

    @pytest.mark.asyncio
    async def test_warmup_no_wallet_skips_balance(self, market_servicer):
        """warmup() without wallet should skip balance provider warmup."""
        market_servicer._initialized = True
        market_servicer._price_aggregator = AsyncMock()
        market_servicer._price_aggregator.get_aggregated_price = AsyncMock()

        with patch.object(
            market_servicer, "_get_balance_provider", new_callable=AsyncMock
        ) as mock_get_bp:
            await market_servicer.warmup()  # No wallet_address

            mock_get_bp.assert_not_called()

    @pytest.mark.asyncio
    async def test_warmup_price_failure_does_not_raise(self, market_servicer):
        """warmup() should log warning, not raise, on price fetch failure."""
        market_servicer._initialized = True
        mock_aggregator = AsyncMock()
        mock_aggregator.get_aggregated_price = AsyncMock(side_effect=Exception("CoinGecko timeout"))
        market_servicer._price_aggregator = mock_aggregator

        # Should not raise
        await market_servicer.warmup()

    @pytest.mark.asyncio
    async def test_warmup_balance_failure_does_not_raise(self, market_servicer):
        """warmup() should log warning, not raise, on balance provider failure."""
        market_servicer._initialized = True
        market_servicer._price_aggregator = AsyncMock()
        market_servicer._price_aggregator.get_aggregated_price = AsyncMock()

        with patch.object(
            market_servicer,
            "_get_balance_provider",
            new_callable=AsyncMock,
            side_effect=Exception("RPC connection failed"),
        ):
            # Should not raise
            await market_servicer.warmup(wallet_address="0x1234567890abcdef1234567890abcdef12345678")

    @pytest.mark.asyncio
    async def test_warmup_no_chains_skips_balance(self, market_servicer, settings):
        """warmup() with no chains configured should skip balance warmup."""
        settings.chains = []
        market_servicer._initialized = True
        market_servicer._price_aggregator = AsyncMock()
        market_servicer._price_aggregator.get_aggregated_price = AsyncMock()

        with patch.object(
            market_servicer, "_get_balance_provider", new_callable=AsyncMock
        ) as mock_get_bp:
            await market_servicer.warmup(wallet_address="0x1234")
            mock_get_bp.assert_not_called()


class TestServerResolveWalletAddress:
    """Test GatewayServer._resolve_wallet_address()."""

    def test_resolve_from_wallet_registry(self):
        """Should return first wallet from registry."""
        from almanak.gateway.server import GatewayServer

        settings = MagicMock(spec=GatewaySettings)
        settings.host = "localhost"
        settings.port = 50051
        settings.chains = ["arbitrum"]
        settings.private_key = None
        settings.safe_address = None
        settings.safe_mode = None

        server = GatewayServer(settings=settings)

        mock_registry = MagicMock()
        mock_registry.all_chains.return_value = ["arbitrum"]
        mock_resolved = MagicMock()
        mock_resolved.account_address = "0xABCDEF1234567890ABCDEF1234567890ABCDEF12"
        mock_registry.resolve.return_value = mock_resolved
        server._wallet_registry = mock_registry

        result = server._resolve_wallet_address()
        assert result == "0xABCDEF1234567890ABCDEF1234567890ABCDEF12"

    def test_resolve_from_safe_address(self):
        """Should return safe address when safe mode is enabled."""
        from almanak.gateway.server import GatewayServer

        settings = MagicMock(spec=GatewaySettings)
        settings.host = "localhost"
        settings.port = 50051
        settings.chains = ["arbitrum"]
        settings.private_key = "0x" + "ab" * 32
        settings.safe_address = "0xSAFE_ADDRESS"
        settings.safe_mode = "direct"

        server = GatewayServer(settings=settings)
        server._wallet_registry = None

        result = server._resolve_wallet_address()
        assert result == "0xSAFE_ADDRESS"

    def test_resolve_from_safe_address_without_private_key(self):
        """Should return safe address when safe mode is enabled, even without private key."""
        from almanak.gateway.server import GatewayServer

        settings = MagicMock(spec=GatewaySettings)
        settings.host = "localhost"
        settings.port = 50051
        settings.chains = ["arbitrum"]
        settings.private_key = None
        settings.safe_address = "0xSAFE_ADDRESS"
        settings.safe_mode = "direct"

        server = GatewayServer(settings=settings)
        server._wallet_registry = None

        result = server._resolve_wallet_address()
        assert result == "0xSAFE_ADDRESS"

    def test_resolve_none_when_no_config(self):
        """Should return None when no wallet config available."""
        from almanak.gateway.server import GatewayServer

        settings = MagicMock(spec=GatewaySettings)
        settings.host = "localhost"
        settings.port = 50051
        settings.chains = ["arbitrum"]
        settings.private_key = None
        settings.safe_address = None
        settings.safe_mode = None

        server = GatewayServer(settings=settings)
        server._wallet_registry = None

        result = server._resolve_wallet_address()
        assert result is None
