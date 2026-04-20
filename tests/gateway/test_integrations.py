"""Tests for gateway integrations."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.gateway.integrations.base import (
    BaseIntegration,
    CacheEntry,
    HealthMetrics,
    IntegrationError,
    IntegrationRateLimitError,
    IntegrationRegistry,
    RateLimiter,
)
from almanak.gateway.integrations.binance import BinanceIntegration
from almanak.gateway.integrations.coingecko import CoinGeckoIntegration
from almanak.gateway.integrations.thegraph import TheGraphIntegration

# =============================================================================
# Base Integration Tests
# =============================================================================


class TestRateLimiter:
    """Tests for RateLimiter."""

    @pytest.mark.asyncio
    async def test_allows_requests_under_limit(self):
        """Requests under limit are allowed immediately."""
        limiter = RateLimiter(requests_per_minute=60)

        for _ in range(5):
            wait_time = await limiter.acquire()
            assert wait_time == 0.0

    def test_get_wait_time_returns_zero_when_available(self):
        """get_wait_time returns 0 when tokens available."""
        limiter = RateLimiter(requests_per_minute=60)

        wait_time = limiter.get_wait_time()
        assert wait_time == 0.0


class TestCacheEntry:
    """Tests for CacheEntry."""

    def test_not_expired_when_fresh(self):
        """Cache entry is not expired when fresh."""
        from datetime import UTC, datetime

        entry = CacheEntry(
            data={"test": "data"},
            cached_at=datetime.now(UTC),
            ttl_seconds=60,
        )

        assert entry.is_expired() is False

    def test_expired_when_old(self):
        """Cache entry is expired after TTL."""
        from datetime import UTC, datetime, timedelta

        entry = CacheEntry(
            data={"test": "data"},
            cached_at=datetime.now(UTC) - timedelta(seconds=120),
            ttl_seconds=60,
        )

        assert entry.is_expired() is True


class TestHealthMetrics:
    """Tests for HealthMetrics."""

    def test_success_rate_100_when_no_requests(self):
        """Success rate is 100% when no requests made."""
        metrics = HealthMetrics()
        assert metrics.success_rate == 100.0

    def test_success_rate_calculation(self):
        """Success rate is calculated correctly."""
        metrics = HealthMetrics(total_requests=10, successful_requests=8)
        assert metrics.success_rate == 80.0

    def test_average_latency_calculation(self):
        """Average latency is calculated correctly."""
        metrics = HealthMetrics(
            successful_requests=4,
            total_latency_ms=100.0,
        )
        assert metrics.average_latency_ms == 25.0


class TestIntegrationRegistry:
    """Tests for IntegrationRegistry."""

    def setup_method(self):
        """Reset registry before each test."""
        IntegrationRegistry.reset()

    def test_singleton_pattern(self):
        """Registry is a singleton."""
        registry1 = IntegrationRegistry.get_instance()
        registry2 = IntegrationRegistry.get_instance()
        assert registry1 is registry2

    def test_register_integration(self):
        """Integration can be registered."""

        class TestIntegration(BaseIntegration):
            name = "test"

            async def health_check(self) -> bool:
                return True

        registry = IntegrationRegistry.get_instance()
        integration = TestIntegration()

        registry.register(integration)
        assert registry.get("test") is integration

    def test_list_integrations(self):
        """List returns all registered integration names."""

        class TestIntegration(BaseIntegration):
            async def health_check(self) -> bool:
                return True

        registry = IntegrationRegistry.get_instance()

        int1 = TestIntegration()
        int1.name = "integration1"
        int2 = TestIntegration()
        int2.name = "integration2"

        registry.register(int1)
        registry.register(int2)

        names = registry.list_integrations()
        assert "integration1" in names
        assert "integration2" in names


# =============================================================================
# Binance Integration Tests
# =============================================================================


class TestBinanceIntegration:
    """Tests for BinanceIntegration."""

    @pytest.fixture
    def binance(self):
        """Create Binance integration."""
        return BinanceIntegration()

    def test_initialization(self, binance):
        """Binance integration initializes correctly."""
        assert binance.name == "binance"
        assert binance.rate_limit_requests == 1200

    def test_valid_intervals(self, binance):
        """Valid intervals are defined."""
        assert "1m" in binance.VALID_INTERVALS
        assert "1h" in binance.VALID_INTERVALS
        assert "1d" in binance.VALID_INTERVALS

    @pytest.mark.asyncio
    async def test_get_ticker_caches_result(self, binance):
        """get_ticker caches the result."""
        mock_data = {
            "symbol": "BTCUSDT",
            "lastPrice": "50000.00",
            "priceChange": "1000.00",
        }

        with patch.object(binance, "_fetch", return_value=mock_data):
            # First call fetches
            result1 = await binance.get_ticker("BTCUSDT")

            # Second call should use cache
            result2 = await binance.get_ticker("BTCUSDT")

            assert result1 == result2
            # _fetch should only be called once
            binance._fetch.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_klines_validates_interval(self, binance):
        """get_klines validates interval parameter."""
        with pytest.raises(ValueError, match="Invalid interval"):
            await binance.get_klines("BTCUSDT", interval="invalid")


# =============================================================================
# CoinGecko Integration Tests
# =============================================================================


class TestCoinGeckoIntegration:
    """Tests for CoinGeckoIntegration."""

    @pytest.fixture
    def coingecko(self, monkeypatch):
        """Create CoinGecko integration without API key (free tier)."""
        # Ensure no API key in environment so we test free tier behavior
        monkeypatch.delenv("COINGECKO_API_KEY", raising=False)
        return CoinGeckoIntegration()

    def test_initialization_free_tier(self, coingecko):
        """CoinGecko free tier initialization."""
        assert coingecko.name == "coingecko"
        # Free tier has rate limit of 30 requests/min
        assert coingecko.rate_limit_requests == 30

    def test_initialization_pro_tier(self):
        """CoinGecko pro tier initialization."""
        coingecko = CoinGeckoIntegration(api_key="test-key")
        assert coingecko.rate_limit_requests == 500

    @pytest.mark.asyncio
    async def test_get_price_returns_dict(self, coingecko):
        """get_price returns price dictionary."""
        mock_data = {"ethereum": {"usd": 2500.50, "eur": 2300.25}}

        with patch.object(coingecko, "_fetch", return_value=mock_data):
            result = await coingecko.get_price("ethereum", vs_currencies=["usd", "eur"])

            assert "usd" in result
            assert "eur" in result
            assert result["usd"] == "2500.5"

    @pytest.mark.asyncio
    async def test_get_prices_returns_dict_of_dicts(self, coingecko):
        """get_prices returns nested dictionary."""
        mock_data = {
            "ethereum": {"usd": 2500.50},
            "bitcoin": {"usd": 45000.00},
        }

        with patch.object(coingecko, "_fetch", return_value=mock_data):
            result = await coingecko.get_prices(["ethereum", "bitcoin"], vs_currencies=["usd"])

            assert "ethereum" in result
            assert "bitcoin" in result
            assert result["ethereum"]["usd"] == "2500.5"


# =============================================================================
# TheGraph Integration Tests
# =============================================================================


class TestTheGraphIntegration:
    """Tests for TheGraphIntegration."""

    @pytest.fixture
    def thegraph(self):
        """Create TheGraph integration."""
        return TheGraphIntegration()

    def test_initialization(self, thegraph):
        """TheGraph integration initializes correctly."""
        assert thegraph.name == "thegraph"

    def test_default_allowed_subgraphs(self, thegraph):
        """Default subgraphs are in allowlist."""
        assert "uniswap-v3-arbitrum" in thegraph.list_allowed_subgraphs()
        assert "aave-v3-arbitrum" in thegraph.list_allowed_subgraphs()

    def test_get_subgraph_url_returns_url_for_allowed(self, thegraph):
        """get_subgraph_url returns URL for allowed subgraphs."""
        url = thegraph.get_subgraph_url("uniswap-v3-ethereum")
        assert url is not None
        assert "thegraph" in url

    def test_get_subgraph_url_returns_none_for_unknown(self, thegraph):
        """get_subgraph_url returns None for unknown subgraphs."""
        url = thegraph.get_subgraph_url("unknown-subgraph")
        assert url is None

    def test_add_allowed_subgraph(self, thegraph):
        """Subgraphs can be added to allowlist."""
        thegraph.add_allowed_subgraph("custom", "https://custom.subgraph.url")

        url = thegraph.get_subgraph_url("custom")
        assert url == "https://custom.subgraph.url"

    @pytest.mark.asyncio
    async def test_query_rejects_unallowed_subgraph(self, thegraph):
        """query rejects subgraphs not in allowlist."""
        with pytest.raises(IntegrationError, match="not in allowlist"):
            await thegraph.query(
                subgraph_id="unknown-subgraph",
                query="{ _meta { block { number } } }",
            )
