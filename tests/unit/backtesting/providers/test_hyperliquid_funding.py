"""Unit tests for Hyperliquid Funding Rate Provider.

This module tests the HyperliquidFundingProvider class in providers/perp/hyperliquid_funding.py,
covering:
- Provider initialization and configuration
- Funding rate fetching with mocked responses
- Fallback behavior when API unavailable
- Error handling for API failures
- Rate limiting integration
- Market symbol normalization
- Chunked requests for long date ranges
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from almanak.framework.backtesting.pnl.providers.perp.hyperliquid_funding import (
    DATA_SOURCE,
    DEFAULT_REQUESTS_PER_MINUTE,
    HYPERLIQUID_API_URL,
    MAX_HOURS_PER_REQUEST,
    HyperliquidAPIError,
    HyperliquidClientConfig,
    HyperliquidFundingEntry,
    HyperliquidFundingProvider,
    HyperliquidRateLimitError,
)
from almanak.framework.backtesting.pnl.providers.rate_limiter import TokenBucketRateLimiter
from almanak.framework.backtesting.pnl.types import DataConfidence


class TestHyperliquidFundingProviderInitialization:
    """Tests for HyperliquidFundingProvider initialization."""

    def test_init_default(self):
        """Test provider initializes with default settings."""
        provider = HyperliquidFundingProvider()
        assert provider.config.requests_per_minute == DEFAULT_REQUESTS_PER_MINUTE
        assert provider.config.fallback_rate == Decimal("0.0001")
        assert provider._owns_rate_limiter is True

    def test_init_with_custom_config(self):
        """Test provider initializes with custom config."""
        config = HyperliquidClientConfig(
            requests_per_minute=50,
            fallback_rate=Decimal("0.0002"),
            timeout_seconds=60,
        )
        provider = HyperliquidFundingProvider(config=config)
        assert provider.config.requests_per_minute == 50
        assert provider.config.fallback_rate == Decimal("0.0002")
        assert provider.config.timeout_seconds == 60

    def test_init_with_provided_rate_limiter(self):
        """Test provider uses provided rate limiter."""
        rate_limiter = TokenBucketRateLimiter(requests_per_minute=25)
        provider = HyperliquidFundingProvider(rate_limiter=rate_limiter)
        assert provider.rate_limiter is rate_limiter
        assert provider._owns_rate_limiter is False


class TestMarketSymbolNormalization:
    """Tests for market symbol normalization."""

    def test_normalize_eth_usd(self):
        """Test normalizing ETH-USD to ETH."""
        provider = HyperliquidFundingProvider()
        assert provider._normalize_market_symbol("ETH-USD") == "ETH"

    def test_normalize_btc_usd(self):
        """Test normalizing BTC-USD to BTC."""
        provider = HyperliquidFundingProvider()
        assert provider._normalize_market_symbol("BTC-USD") == "BTC"

    def test_normalize_slash_format(self):
        """Test normalizing ETH/USD format."""
        provider = HyperliquidFundingProvider()
        assert provider._normalize_market_symbol("ETH/USD") == "ETH"

    def test_normalize_perp_suffix(self):
        """Test normalizing ETH-PERP format."""
        provider = HyperliquidFundingProvider()
        assert provider._normalize_market_symbol("ETH-PERP") == "ETH"

    def test_normalize_plain_symbol(self):
        """Test normalizing plain symbol."""
        provider = HyperliquidFundingProvider()
        assert provider._normalize_market_symbol("ETH") == "ETH"

    def test_normalize_lowercase(self):
        """Test normalizing lowercase symbols."""
        provider = HyperliquidFundingProvider()
        assert provider._normalize_market_symbol("eth-usd") == "ETH"


class TestGetFundingRates:
    """Tests for get_funding_rates method."""

    @pytest.mark.asyncio
    async def test_get_funding_rates_success(self):
        """Test successfully fetching funding rate data."""
        provider = HyperliquidFundingProvider()

        # Mock the HTTP response
        mock_response = AsyncMock()
        mock_response.status = 200
        # Return funding entries for 3 hours
        mock_response.json = AsyncMock(
            return_value=[
                {
                    "coin": "ETH",
                    "fundingRate": "0.0001",
                    "premium": "0.00005",
                    "time": 1705312800000,  # 2024-01-15 10:00 UTC
                },
                {
                    "coin": "ETH",
                    "fundingRate": "0.00012",
                    "premium": "0.00006",
                    "time": 1705316400000,  # 2024-01-15 11:00 UTC
                },
                {
                    "coin": "ETH",
                    "fundingRate": "0.00008",
                    "premium": "0.00004",
                    "time": 1705320000000,  # 2024-01-15 12:00 UTC
                },
            ]
        )

        mock_session = MagicMock()
        mock_session.post = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response)))

        with patch.object(provider, "_get_session", return_value=mock_session):
            with patch.object(provider._rate_limiter, "acquire", new_callable=AsyncMock):
                rates = await provider.get_funding_rates(
                    market="ETH-USD",
                    start_date=datetime(2024, 1, 15, 10, 0, tzinfo=UTC),
                    end_date=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
                )

        # Should return 3 data points
        assert len(rates) == 3
        assert rates[0].source_info.source == DATA_SOURCE
        assert rates[0].source_info.confidence == DataConfidence.HIGH
        assert rates[0].rate == Decimal("0.0001")

    @pytest.mark.asyncio
    async def test_get_funding_rates_preserves_order(self):
        """Test that results are sorted by timestamp."""
        provider = HyperliquidFundingProvider()

        # Return entries out of order
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value=[
                {
                    "coin": "ETH",
                    "fundingRate": "0.0002",
                    "premium": "0.0001",
                    "time": 1705320000000,  # 12:00 UTC (later)
                },
                {
                    "coin": "ETH",
                    "fundingRate": "0.0001",
                    "premium": "0.00005",
                    "time": 1705312800000,  # 10:00 UTC (earlier)
                },
            ]
        )

        mock_session = MagicMock()
        mock_session.post = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response)))

        with patch.object(provider, "_get_session", return_value=mock_session):
            with patch.object(provider._rate_limiter, "acquire", new_callable=AsyncMock):
                rates = await provider.get_funding_rates(
                    market="ETH-USD",
                    start_date=datetime(2024, 1, 15, 10, 0, tzinfo=UTC),
                    end_date=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
                )

        # Should be sorted by timestamp (earlier first)
        assert len(rates) == 2
        assert rates[0].rate == Decimal("0.0001")  # 10:00 entry
        assert rates[1].rate == Decimal("0.0002")  # 12:00 entry

    @pytest.mark.asyncio
    async def test_get_funding_rates_no_data_returns_fallback(self):
        """Test that empty response returns fallback results."""
        provider = HyperliquidFundingProvider()

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=[])

        mock_session = MagicMock()
        mock_session.post = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response)))

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
            assert rate.source_info.source == "fallback"

    @pytest.mark.asyncio
    async def test_get_funding_rates_adds_timezone_if_missing(self):
        """Test that timezone is added to naive datetimes."""
        provider = HyperliquidFundingProvider()

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value=[
                {
                    "coin": "ETH",
                    "fundingRate": "0.0001",
                    "premium": "0.00005",
                    "time": 1705312800000,
                },
            ]
        )

        mock_session = MagicMock()
        mock_session.post = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response)))

        with patch.object(provider, "_get_session", return_value=mock_session):
            with patch.object(provider._rate_limiter, "acquire", new_callable=AsyncMock):
                # Pass naive datetimes (no timezone)
                rates = await provider.get_funding_rates(
                    market="ETH-USD",
                    start_date=datetime(2024, 1, 15),
                    end_date=datetime(2024, 1, 15, 1, 0),
                )

        # Should still work without error
        assert len(rates) >= 1


class TestChunkedRequests:
    """Tests for chunked requests for long date ranges."""

    @pytest.mark.asyncio
    async def test_long_range_makes_multiple_requests(self):
        """Test that long date ranges are chunked into multiple requests."""
        provider = HyperliquidFundingProvider()

        # Create a range that spans more than MAX_HOURS_PER_REQUEST (500 hours = ~21 days)
        start_date = datetime(2024, 1, 1, tzinfo=UTC)
        end_date = datetime(2024, 2, 1, tzinfo=UTC)  # 31 days = 744 hours

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value=[
                {
                    "coin": "ETH",
                    "fundingRate": "0.0001",
                    "premium": "0.00005",
                    "time": 1704067200000,  # 2024-01-01
                },
            ]
        )

        mock_session = MagicMock()
        mock_session.post = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response)))

        with patch.object(provider, "_get_session", return_value=mock_session):
            with patch.object(provider._rate_limiter, "acquire", new_callable=AsyncMock):
                await provider.get_funding_rates(
                    market="ETH-USD",
                    start_date=start_date,
                    end_date=end_date,
                )

        # Should have made multiple POST calls (744 hours / 500 hours = 2 chunks minimum)
        assert mock_session.post.call_count >= 2


class TestErrorHandling:
    """Tests for error handling in funding rate fetching."""

    @pytest.mark.asyncio
    async def test_rate_limit_error_returns_fallback(self):
        """Test that rate limit error returns fallback results."""
        config = HyperliquidClientConfig(fallback_rate=Decimal("0.0005"))
        provider = HyperliquidFundingProvider(config=config)

        mock_response = AsyncMock()
        mock_response.status = 429

        mock_session = MagicMock()
        mock_session.post = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response)))

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
        provider = HyperliquidFundingProvider()

        mock_response = AsyncMock()
        mock_response.status = 500
        mock_response.text = AsyncMock(return_value="Internal Server Error")

        mock_session = MagicMock()
        mock_session.post = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response)))

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
        provider = HyperliquidFundingProvider()

        mock_session = MagicMock()
        mock_session.post = MagicMock(
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
        provider = HyperliquidFundingProvider()

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value=[
                {
                    "coin": "ETH",
                    "fundingRate": "0.00015",
                    "premium": "0.00007",
                    "time": int(datetime.now(UTC).timestamp() * 1000),
                },
            ]
        )

        mock_session = MagicMock()
        mock_session.post = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response)))

        with patch.object(provider, "_get_session", return_value=mock_session):
            with patch.object(provider._rate_limiter, "acquire", new_callable=AsyncMock):
                rate = await provider.get_current_funding_rate("ETH-USD")

        assert rate.source_info.confidence == DataConfidence.HIGH
        assert rate.source_info.source == DATA_SOURCE
        assert rate.rate == Decimal("0.00015")

    @pytest.mark.asyncio
    async def test_get_current_funding_rate_no_data_returns_fallback(self):
        """Test current rate when no data available."""
        provider = HyperliquidFundingProvider()

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=[])

        mock_session = MagicMock()
        mock_session.post = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response)))

        with patch.object(provider, "_get_session", return_value=mock_session):
            with patch.object(provider._rate_limiter, "acquire", new_callable=AsyncMock):
                rate = await provider.get_current_funding_rate("NONEXISTENT")

        assert rate.source_info.confidence == DataConfidence.LOW
        assert rate.source_info.source == "fallback"


class TestContextManager:
    """Tests for async context manager behavior."""

    @pytest.mark.asyncio
    async def test_context_manager_closes_session(self):
        """Test that context manager closes session on exit."""
        provider = HyperliquidFundingProvider()

        # Create a mock session
        mock_session = MagicMock()
        mock_session.closed = False
        mock_session.close = AsyncMock()
        provider._session = mock_session

        async with provider:
            pass

        mock_session.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_handles_already_closed_session(self):
        """Test that close handles already closed session."""
        provider = HyperliquidFundingProvider()

        # Session already closed
        mock_session = MagicMock()
        mock_session.closed = True
        provider._session = mock_session

        # Should not raise
        await provider.close()

    @pytest.mark.asyncio
    async def test_close_handles_none_session(self):
        """Test that close handles None session."""
        provider = HyperliquidFundingProvider()
        provider._session = None

        # Should not raise
        await provider.close()


class TestFundingEntryParsing:
    """Tests for funding entry parsing."""

    def test_parse_funding_entry(self):
        """Test parsing funding entry from API response."""
        provider = HyperliquidFundingProvider()

        raw_data = {
            "coin": "ETH",
            "fundingRate": "0.00012345",
            "premium": "0.00006789",
            "time": 1705312800000,  # 2024-01-15 10:00 UTC
        }

        entry = provider._parse_funding_entry(raw_data)

        assert entry.coin == "ETH"
        assert entry.funding_rate == Decimal("0.00012345")
        assert entry.premium == Decimal("0.00006789")
        assert entry.timestamp == datetime(2024, 1, 15, 10, 0, tzinfo=UTC)

    def test_parse_funding_entry_missing_fields(self):
        """Test parsing with missing optional fields."""
        provider = HyperliquidFundingProvider()

        raw_data = {
            "coin": "BTC",
            # Missing fundingRate, premium
            "time": 1705312800000,
        }

        entry = provider._parse_funding_entry(raw_data)

        assert entry.coin == "BTC"
        assert entry.funding_rate == Decimal("0")
        assert entry.premium == Decimal("0")

    def test_parse_funding_entry_negative_rate(self):
        """Test parsing negative funding rate."""
        provider = HyperliquidFundingProvider()

        raw_data = {
            "coin": "ETH",
            "fundingRate": "-0.0001",
            "premium": "-0.00005",
            "time": 1705312800000,
        }

        entry = provider._parse_funding_entry(raw_data)

        assert entry.funding_rate == Decimal("-0.0001")
        assert entry.premium == Decimal("-0.00005")


class TestFallbackGeneration:
    """Tests for fallback result generation."""

    def test_generate_fallback_results(self):
        """Test generating fallback results for date range."""
        config = HyperliquidClientConfig(fallback_rate=Decimal("0.0002"))
        provider = HyperliquidFundingProvider(config=config)

        start = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)
        end = datetime(2024, 1, 15, 5, 0, tzinfo=UTC)  # 5 hours

        results = provider._generate_fallback_results(start, end)

        # Should have 6 results (hours 0-5 inclusive)
        assert len(results) == 6
        for result in results:
            assert result.rate == Decimal("0.0002")
            assert result.source_info.confidence == DataConfidence.LOW
            assert result.source_info.source == "fallback"

    def test_create_fallback_result(self):
        """Test creating single fallback result."""
        config = HyperliquidClientConfig(fallback_rate=Decimal("0.0003"))
        provider = HyperliquidFundingProvider(config=config)

        timestamp = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        result = provider._create_fallback_result(timestamp)

        assert result.rate == Decimal("0.0003")
        assert result.source_info.confidence == DataConfidence.LOW
        assert result.source_info.source == "fallback"
        assert result.source_info.timestamp == timestamp


class TestAPIEndpoint:
    """Tests for API endpoint configuration."""

    def test_api_url_is_valid(self):
        """Test API URL is valid."""
        assert HYPERLIQUID_API_URL == "https://api.hyperliquid.xyz/info"
        assert HYPERLIQUID_API_URL.startswith("https://")

    def test_max_hours_per_request(self):
        """Test max hours per request is configured."""
        assert MAX_HOURS_PER_REQUEST == 500

    def test_data_source_identifier(self):
        """Test data source identifier."""
        assert DATA_SOURCE == "hyperliquid_api"


class TestExceptionClasses:
    """Tests for exception classes."""

    def test_hyperliquid_api_error(self):
        """Test HyperliquidAPIError can be raised."""
        with pytest.raises(HyperliquidAPIError):
            raise HyperliquidAPIError("Test error")

    def test_hyperliquid_rate_limit_error(self):
        """Test HyperliquidRateLimitError is subclass of HyperliquidAPIError."""
        assert issubclass(HyperliquidRateLimitError, HyperliquidAPIError)

        with pytest.raises(HyperliquidAPIError):
            raise HyperliquidRateLimitError("Rate limit exceeded")
