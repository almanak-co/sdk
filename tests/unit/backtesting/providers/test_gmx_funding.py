"""Unit tests for GMX V2 Funding Rate Provider.

This module tests the GMXFundingProvider class in providers/perp/gmx_funding.py,
covering:
- Provider initialization and configuration
- Supported chains and API URL mapping
- Funding rate fetching with mocked responses
- Fallback behavior when API unavailable
- Error handling for API failures
- Rate limiting integration
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from almanak.core.enums import Chain
from almanak.framework.backtesting.pnl.providers.perp.gmx_funding import (
    DATA_SOURCE,
    DEFAULT_REQUESTS_PER_MINUTE,
    GMX_API_URLS,
    GMX_MARKET_TOKENS,
    GMXAPIError,
    GMXClientConfig,
    GMXFundingProvider,
    GMXMarketInfo,
    GMXRateLimitError,
    SUPPORTED_CHAINS,
)
from almanak.framework.backtesting.pnl.providers.rate_limiter import TokenBucketRateLimiter
from almanak.framework.backtesting.pnl.types import DataConfidence


class TestGMXFundingProviderInitialization:
    """Tests for GMXFundingProvider initialization."""

    def test_init_default(self):
        """Test provider initializes with default settings."""
        provider = GMXFundingProvider()
        assert provider.supported_chains == SUPPORTED_CHAINS
        assert provider.config.chain == Chain.ARBITRUM
        assert provider.config.requests_per_minute == DEFAULT_REQUESTS_PER_MINUTE
        assert provider._owns_rate_limiter is True

    def test_init_with_custom_config(self):
        """Test provider initializes with custom config."""
        config = GMXClientConfig(
            requests_per_minute=50,
            chain=Chain.AVALANCHE,
            fallback_rate=Decimal("0.0002"),
        )
        provider = GMXFundingProvider(config=config)
        assert provider.config.chain == Chain.AVALANCHE
        assert provider.config.requests_per_minute == 50
        assert provider.config.fallback_rate == Decimal("0.0002")

    def test_init_with_provided_rate_limiter(self):
        """Test provider uses provided rate limiter."""
        rate_limiter = TokenBucketRateLimiter(requests_per_minute=25)
        provider = GMXFundingProvider(rate_limiter=rate_limiter)
        assert provider.rate_limiter is rate_limiter
        assert provider._owns_rate_limiter is False

    def test_supported_chains_property_returns_copy(self):
        """Test supported_chains returns a copy, not the original."""
        provider = GMXFundingProvider()
        chains1 = provider.supported_chains
        chains2 = provider.supported_chains
        assert chains1 == chains2
        assert chains1 is not chains2


class TestSupportedChains:
    """Tests for supported chains configuration."""

    def test_supported_chains_include_required_networks(self):
        """Test that required networks are supported."""
        # US-015 requires Arbitrum (primary GMX V2 chain)
        assert Chain.ARBITRUM in SUPPORTED_CHAINS
        assert Chain.AVALANCHE in SUPPORTED_CHAINS

    def test_all_supported_chains_have_api_urls(self):
        """Test all supported chains have API URLs."""
        for chain in SUPPORTED_CHAINS:
            assert chain in GMX_API_URLS
            assert GMX_API_URLS[chain]  # Non-empty

    def test_api_urls_are_valid_format(self):
        """Test API URLs have valid format."""
        for chain, url in GMX_API_URLS.items():
            assert url.startswith("https://")
            assert "gmxinfra.io" in url


class TestGMXMarketTokens:
    """Tests for market token configuration."""

    def test_market_tokens_defined(self):
        """Test that common markets have token addresses defined."""
        assert "ETH-USD" in GMX_MARKET_TOKENS
        assert "BTC-USD" in GMX_MARKET_TOKENS

    def test_market_tokens_have_arbitrum_addresses(self):
        """Test that markets have Arbitrum addresses."""
        for market, chains in GMX_MARKET_TOKENS.items():
            assert "arbitrum" in chains
            assert chains["arbitrum"].startswith("0x")


class TestGetFundingRates:
    """Tests for get_funding_rates method."""

    @pytest.mark.asyncio
    async def test_get_funding_rates_success(self):
        """Test successfully fetching funding rate data."""
        provider = GMXFundingProvider()

        # Mock the HTTP response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value=[
                {
                    "name": "ETH/USD [WETH-USDC]",
                    "marketToken": "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
                    "indexToken": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
                    "longToken": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
                    "shortToken": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                    "fundingRateLong": "1000000000000000000000000000",  # ~1e27 in raw format
                    "fundingRateShort": "-500000000000000000000000000",
                    "netRateLong": "2000000000000000000000000000",
                    "netRateShort": "1500000000000000000000000000",
                }
            ]
        )

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response)))

        with patch.object(provider, "_get_session", return_value=mock_session):
            with patch.object(provider._rate_limiter, "acquire", new_callable=AsyncMock):
                rates = await provider.get_funding_rates(
                    market="ETH-USD",
                    start_date=datetime(2024, 1, 15, tzinfo=UTC),
                    end_date=datetime(2024, 1, 15, 1, 0, tzinfo=UTC),  # 1 hour range
                )

        # Should return at least 1 data point (hourly intervals)
        assert len(rates) >= 1
        assert rates[0].source_info.source == DATA_SOURCE
        # Using current rate for historical, so MEDIUM confidence
        assert rates[0].source_info.confidence == DataConfidence.MEDIUM

    @pytest.mark.asyncio
    async def test_get_funding_rates_multiple_hours(self):
        """Test fetching funding rates for multiple hours."""
        provider = GMXFundingProvider()

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value=[
                {
                    "name": "ETH/USD [WETH-USDC]",
                    "marketToken": "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
                    "indexToken": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
                    "longToken": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
                    "shortToken": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                    "fundingRateLong": "1000000000000000000000000000",
                    "fundingRateShort": "-500000000000000000000000000",
                    "netRateLong": "2000000000000000000000000000",
                    "netRateShort": "1500000000000000000000000000",
                }
            ]
        )

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response)))

        with patch.object(provider, "_get_session", return_value=mock_session):
            with patch.object(provider._rate_limiter, "acquire", new_callable=AsyncMock):
                rates = await provider.get_funding_rates(
                    market="ETH-USD",
                    start_date=datetime(2024, 1, 15, 0, 0, tzinfo=UTC),
                    end_date=datetime(2024, 1, 15, 5, 0, tzinfo=UTC),  # 5 hour range
                )

        # Should return 6 data points (hours 0-5 inclusive)
        assert len(rates) == 6
        # All should have same rate (current rate used for all)
        first_rate = rates[0].rate
        for rate in rates:
            assert rate.rate == first_rate

    @pytest.mark.asyncio
    async def test_get_funding_rates_market_not_found(self):
        """Test behavior when market not found."""
        provider = GMXFundingProvider()

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value=[
                {
                    "name": "ETH/USD [WETH-USDC]",
                    "marketToken": "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
                    "indexToken": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
                    "longToken": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
                    "shortToken": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                    "fundingRateLong": "1000000000000000000000000000",
                    "fundingRateShort": "-500000000000000000000000000",
                    "netRateLong": "2000000000000000000000000000",
                    "netRateShort": "1500000000000000000000000000",
                }
            ]
        )

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response)))

        with patch.object(provider, "_get_session", return_value=mock_session):
            with patch.object(provider._rate_limiter, "acquire", new_callable=AsyncMock):
                rates = await provider.get_funding_rates(
                    market="NONEXISTENT-USD",  # Market doesn't exist
                    start_date=datetime(2024, 1, 15, tzinfo=UTC),
                    end_date=datetime(2024, 1, 15, 1, 0, tzinfo=UTC),
                )

        # Should return fallback results with LOW confidence
        assert len(rates) >= 1
        for rate in rates:
            assert rate.source_info.confidence == DataConfidence.LOW
            assert rate.source_info.source == "fallback"

    @pytest.mark.asyncio
    async def test_get_funding_rates_adds_timezone_if_missing(self):
        """Test that timezone is added to naive datetimes."""
        provider = GMXFundingProvider()

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value=[
                {
                    "name": "ETH/USD [WETH-USDC]",
                    "marketToken": "0x123",
                    "indexToken": "0x456",
                    "longToken": "0x789",
                    "shortToken": "0xabc",
                    "fundingRateLong": "1000000000000000000000000000",
                    "fundingRateShort": "-500000000000000000000000000",
                    "netRateLong": "2000000000000000000000000000",
                    "netRateShort": "1500000000000000000000000000",
                }
            ]
        )

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response)))

        with patch.object(provider, "_get_session", return_value=mock_session):
            with patch.object(provider._rate_limiter, "acquire", new_callable=AsyncMock):
                # Pass naive datetimes (no timezone)
                rates = await provider.get_funding_rates(
                    market="ETH-USD",
                    start_date=datetime(2024, 1, 15),  # No timezone
                    end_date=datetime(2024, 1, 15, 1, 0),  # No timezone
                )

        # Should still work without error
        assert len(rates) >= 1


class TestErrorHandling:
    """Tests for error handling in funding rate fetching."""

    @pytest.mark.asyncio
    async def test_rate_limit_error_returns_fallback(self):
        """Test that rate limit error returns fallback results."""
        config = GMXClientConfig(fallback_rate=Decimal("0.0005"))
        provider = GMXFundingProvider(config=config)

        mock_response = AsyncMock()
        mock_response.status = 429

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response)))

        with patch.object(provider, "_get_session", return_value=mock_session):
            with patch.object(provider._rate_limiter, "acquire", new_callable=AsyncMock):
                with patch.object(provider._rate_limiter, "on_rate_limit_response", new_callable=AsyncMock):
                    rates = await provider.get_funding_rates(
                        market="ETH-USD",
                        start_date=datetime(2024, 1, 15, tzinfo=UTC),
                        end_date=datetime(2024, 1, 15, 1, 0, tzinfo=UTC),
                    )

        assert len(rates) >= 1
        for rate in rates:
            assert rate.rate == Decimal("0.0005")  # Fallback rate
            assert rate.source_info.confidence == DataConfidence.LOW

    @pytest.mark.asyncio
    async def test_api_error_returns_fallback(self):
        """Test that API error returns fallback results."""
        provider = GMXFundingProvider()

        mock_response = AsyncMock()
        mock_response.status = 500
        mock_response.text = AsyncMock(return_value="Internal Server Error")

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response)))

        with patch.object(provider, "_get_session", return_value=mock_session):
            with patch.object(provider._rate_limiter, "acquire", new_callable=AsyncMock):
                rates = await provider.get_funding_rates(
                    market="ETH-USD",
                    start_date=datetime(2024, 1, 15, tzinfo=UTC),
                    end_date=datetime(2024, 1, 15, 1, 0, tzinfo=UTC),
                )

        assert len(rates) >= 1
        for rate in rates:
            assert rate.source_info.confidence == DataConfidence.LOW

    @pytest.mark.asyncio
    async def test_connection_error_returns_fallback(self):
        """Test that connection error returns fallback results."""
        provider = GMXFundingProvider()

        mock_session = MagicMock()
        mock_session.get = MagicMock(
            return_value=AsyncMock(__aenter__=AsyncMock(side_effect=aiohttp.ClientError("Connection failed")))
        )

        with patch.object(provider, "_get_session", return_value=mock_session):
            with patch.object(provider._rate_limiter, "acquire", new_callable=AsyncMock):
                rates = await provider.get_funding_rates(
                    market="ETH-USD",
                    start_date=datetime(2024, 1, 15, tzinfo=UTC),
                    end_date=datetime(2024, 1, 15, 1, 0, tzinfo=UTC),
                )

        assert len(rates) >= 1
        for rate in rates:
            assert rate.source_info.confidence == DataConfidence.LOW


class TestGetCurrentFundingRate:
    """Tests for get_current_funding_rate method."""

    @pytest.mark.asyncio
    async def test_get_current_funding_rate_success(self):
        """Test successfully fetching current funding rate."""
        provider = GMXFundingProvider()

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value=[
                {
                    "name": "ETH/USD [WETH-USDC]",
                    "marketToken": "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
                    "indexToken": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
                    "longToken": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
                    "shortToken": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                    "fundingRateLong": "1000000000000000000000000000",
                    "fundingRateShort": "-500000000000000000000000000",
                    "netRateLong": "2000000000000000000000000000",
                    "netRateShort": "1500000000000000000000000000",
                }
            ]
        )

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response)))

        with patch.object(provider, "_get_session", return_value=mock_session):
            with patch.object(provider._rate_limiter, "acquire", new_callable=AsyncMock):
                rate = await provider.get_current_funding_rate("ETH-USD")

        assert rate.source_info.confidence == DataConfidence.HIGH
        assert rate.source_info.source == DATA_SOURCE

    @pytest.mark.asyncio
    async def test_get_current_funding_rate_market_not_found(self):
        """Test current rate when market not found."""
        provider = GMXFundingProvider()

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=[])  # Empty markets

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response)))

        with patch.object(provider, "_get_session", return_value=mock_session):
            with patch.object(provider._rate_limiter, "acquire", new_callable=AsyncMock):
                rate = await provider.get_current_funding_rate("NONEXISTENT-USD")

        assert rate.source_info.confidence == DataConfidence.LOW
        assert rate.source_info.source == "fallback"


class TestContextManager:
    """Tests for async context manager behavior."""

    @pytest.mark.asyncio
    async def test_context_manager_closes_session(self):
        """Test that context manager closes session on exit."""
        provider = GMXFundingProvider()

        # Create a mock session
        mock_session = MagicMock()
        mock_session.closed = False
        mock_session.close = AsyncMock()
        provider._session = mock_session

        async with provider:
            pass

        mock_session.close.assert_called_once()


class TestMarketFinding:
    """Tests for market finding functionality."""

    def test_find_market_by_symbol(self):
        """Test finding market by symbol pattern."""
        provider = GMXFundingProvider()

        markets = [
            GMXMarketInfo(
                name="ETH/USD [WETH-USDC]",
                market_token="0x123",
                index_token="0x456",
                long_token="0x789",
                short_token="0xabc",
                funding_rate_long=Decimal("0.0001"),
                funding_rate_short=Decimal("-0.00005"),
                net_rate_long=Decimal("0.0002"),
                net_rate_short=Decimal("0.00015"),
            ),
            GMXMarketInfo(
                name="BTC/USD [WBTC-USDC]",
                market_token="0x321",
                index_token="0x654",
                long_token="0x987",
                short_token="0xcba",
                funding_rate_long=Decimal("0.00008"),
                funding_rate_short=Decimal("-0.00004"),
                net_rate_long=Decimal("0.00016"),
                net_rate_short=Decimal("0.00012"),
            ),
        ]

        eth_market = provider._find_market(markets, "ETH-USD")
        assert eth_market is not None
        assert "ETH" in eth_market.name.upper()

        btc_market = provider._find_market(markets, "BTC-USD")
        assert btc_market is not None
        assert "BTC" in btc_market.name.upper()

        nonexistent = provider._find_market(markets, "DOGE-USD")
        assert nonexistent is None


class TestMarketInfoParsing:
    """Tests for GMX market info parsing."""

    def test_parse_market_info(self):
        """Test parsing market info from API response."""
        provider = GMXFundingProvider()

        raw_data = {
            "name": "ETH/USD [WETH-USDC]",
            "marketToken": "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
            "indexToken": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            "longToken": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            "shortToken": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            "fundingRateLong": "1000000000000000000000000000",
            "fundingRateShort": "-500000000000000000000000000",
            "netRateLong": "2000000000000000000000000000",
            "netRateShort": "1500000000000000000000000000",
        }

        market_info = provider._parse_market_info(raw_data)

        assert market_info.name == "ETH/USD [WETH-USDC]"
        assert market_info.market_token == "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336"
        # Rates should be parsed (converted from raw format)
        assert isinstance(market_info.funding_rate_long, Decimal)
        assert isinstance(market_info.funding_rate_short, Decimal)

    def test_parse_market_info_missing_fields(self):
        """Test parsing with missing optional fields."""
        provider = GMXFundingProvider()

        raw_data = {
            "name": "ETH/USD",
            # Missing most fields
        }

        market_info = provider._parse_market_info(raw_data)

        assert market_info.name == "ETH/USD"
        assert market_info.market_token == ""
        assert market_info.funding_rate_long == Decimal("0")


class TestCaching:
    """Tests for market info caching."""

    @pytest.mark.asyncio
    async def test_cache_is_used_within_ttl(self):
        """Test that cached market info is used within TTL."""
        provider = GMXFundingProvider()

        # Pre-populate cache (key is chain.value which is "ARBITRUM")
        provider._market_cache["ARBITRUM"] = [
            GMXMarketInfo(
                name="ETH/USD [cached]",
                market_token="0x123",
                index_token="0x456",
                long_token="0x789",
                short_token="0xabc",
                funding_rate_long=Decimal("0.001"),
                funding_rate_short=Decimal("-0.0005"),
                net_rate_long=Decimal("0.002"),
                net_rate_short=Decimal("0.0015"),
            )
        ]
        # Use per-chain timestamp (cache_key is chain.value)
        provider._cache_timestamps[Chain.ARBITRUM.value] = datetime.now(UTC)

        # Mock session (should not be called if cache is used)
        mock_session = MagicMock()
        mock_session.get = MagicMock()

        with patch.object(provider, "_get_session", return_value=mock_session):
            with patch.object(provider._rate_limiter, "acquire", new_callable=AsyncMock):
                markets = await provider._fetch_markets_info(Chain.ARBITRUM)

        # Should return cached data
        assert len(markets) == 1
        assert markets[0].name == "ETH/USD [cached]"
        # HTTP call should not have been made
        mock_session.get.assert_not_called()
