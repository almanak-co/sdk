"""Unit tests for Funding Rate Provider.

This module tests the FundingRateProvider class in providers/funding_rates.py, covering:
- Provider initialization and configuration
- Historical funding rate fetching with mocked API responses
- Caching behavior with 1-hour TTL
- Rate limit handling and backoff
- GMX and Hyperliquid API support
- Error handling for failed queries
"""

import time
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.backtesting.pnl.providers.funding_rates import (
    DEFAULT_CACHE_TTL_SECONDS,
    DEFAULT_FUNDING_RATES,
    GMX_MARKETS,
    HYPERLIQUID_MARKETS,
    SUPPORTED_PROTOCOLS,
    CachedFundingRate,
    FundingRateData,
    FundingRateNotFoundError,
    FundingRateProvider,
    FundingRateRateLimitError,
    RateLimitState,
    UnsupportedProtocolError,
)


class TestFundingRateProviderInitialization:
    """Tests for FundingRateProvider initialization."""

    def test_init_default_chain(self):
        """Test provider initializes with default arbitrum chain."""
        provider = FundingRateProvider()
        assert provider.chain == "arbitrum"
        assert provider.provider_name == "funding_rates_arbitrum"

    def test_init_avalanche_chain(self):
        """Test provider initializes with avalanche chain."""
        provider = FundingRateProvider(chain="avalanche")
        assert provider.chain == "avalanche"
        assert provider.provider_name == "funding_rates_avalanche"

    def test_init_unsupported_chain_raises(self):
        """Test provider raises ValueError for unsupported chain."""
        with pytest.raises(ValueError) as exc_info:
            FundingRateProvider(chain="unsupported_chain")
        assert "Unsupported chain" in str(exc_info.value)

    def test_init_cache_ttl(self):
        """Test provider initializes with custom cache TTL."""
        provider = FundingRateProvider(cache_ttl_seconds=7200)
        assert provider._cache_ttl_seconds == 7200

    def test_init_default_cache_ttl(self):
        """Test provider uses default 1-hour cache TTL."""
        provider = FundingRateProvider()
        assert provider._cache_ttl_seconds == DEFAULT_CACHE_TTL_SECONDS
        assert provider._cache_ttl_seconds == 3600

    def test_init_request_timeout(self):
        """Test provider initializes with custom request timeout."""
        provider = FundingRateProvider(request_timeout=60)
        assert provider._request_timeout == 60

    def test_init_requests_per_minute(self):
        """Test provider initializes with custom requests per minute."""
        provider = FundingRateProvider(requests_per_minute=10)
        assert provider._requests_per_minute == 10

    def test_chain_case_insensitive(self):
        """Test chain parameter is case insensitive."""
        provider = FundingRateProvider(chain="ARBITRUM")
        assert provider.chain == "arbitrum"

        provider = FundingRateProvider(chain="Avalanche")
        assert provider.chain == "avalanche"


class TestFundingRateData:
    """Tests for FundingRateData dataclass."""

    def test_basic_creation(self):
        """Test creating FundingRateData with required fields."""
        data = FundingRateData(
            protocol="gmx",
            market="ETH-USD",
            timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            rate=Decimal("0.0001"),
        )
        assert data.protocol == "gmx"
        assert data.market == "ETH-USD"
        assert data.rate == Decimal("0.0001")
        assert data.source == "api"

    def test_annualized_rate_calculation(self):
        """Test automatic annualized rate calculation."""
        data = FundingRateData(
            protocol="gmx",
            market="ETH-USD",
            timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            rate=Decimal("0.0001"),  # 0.01% per hour
        )
        # 0.0001 * 8760 * 100 = 87.6% APR
        expected_apr = Decimal("0.0001") * Decimal("8760") * Decimal("100")
        assert data.annualized_rate_pct == expected_apr

    def test_to_dict_serialization(self):
        """Test serialization to dictionary."""
        data = FundingRateData(
            protocol="hyperliquid",
            market="BTC-USD",
            timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            rate=Decimal("0.00015"),
            open_interest_long=Decimal("1000000"),
            open_interest_short=Decimal("800000"),
            source="hyperliquid_api",
        )
        d = data.to_dict()

        assert d["protocol"] == "hyperliquid"
        assert d["market"] == "BTC-USD"
        assert d["rate"] == "0.00015"
        assert d["source"] == "hyperliquid_api"
        assert d["open_interest_long"] == "1000000"
        assert d["open_interest_short"] == "800000"

    def test_from_dict_deserialization(self):
        """Test deserialization from dictionary."""
        d = {
            "protocol": "gmx",
            "market": "ETH-USD",
            "timestamp": "2024-01-15T12:00:00+00:00",
            "rate": "0.0001",
            "annualized_rate_pct": "87.6",
            "source": "gmx_api",
        }
        data = FundingRateData.from_dict(d)

        assert data.protocol == "gmx"
        assert data.market == "ETH-USD"
        assert data.rate == Decimal("0.0001")
        assert data.annualized_rate_pct == Decimal("87.6")
        assert data.source == "gmx_api"

    def test_roundtrip_serialization(self):
        """Test serialization roundtrip preserves data."""
        original = FundingRateData(
            protocol="gmx",
            market="ETH-USD",
            timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            rate=Decimal("0.0001"),
            open_interest_long=Decimal("5000000"),
            source="gmx_subgraph",
        )
        restored = FundingRateData.from_dict(original.to_dict())

        assert restored.protocol == original.protocol
        assert restored.market == original.market
        assert restored.rate == original.rate
        assert restored.source == original.source


class TestCachedFundingRate:
    """Tests for CachedFundingRate caching behavior."""

    def test_not_expired_when_fresh(self):
        """Test cached data is not expired when fresh."""
        data = FundingRateData(
            protocol="gmx",
            market="ETH-USD",
            timestamp=datetime.now(UTC),
            rate=Decimal("0.0001"),
        )
        cached = CachedFundingRate(
            data=data,
            fetched_at=time.time(),
            ttl_seconds=3600,
        )
        assert not cached.is_expired

    def test_expired_when_stale(self):
        """Test cached data is expired when past TTL."""
        data = FundingRateData(
            protocol="gmx",
            market="ETH-USD",
            timestamp=datetime.now(UTC),
            rate=Decimal("0.0001"),
        )
        cached = CachedFundingRate(
            data=data,
            fetched_at=time.time() - 3700,  # Over 1 hour ago
            ttl_seconds=3600,
        )
        assert cached.is_expired


class TestRateLimitState:
    """Tests for RateLimitState tracking."""

    def test_initial_state(self):
        """Test initial rate limit state."""
        state = RateLimitState()
        assert state.last_limit_time is None
        assert state.backoff_seconds == 1.0
        assert state.consecutive_limits == 0
        assert state.get_wait_time() == 0.0

    def test_record_rate_limit_increases_backoff(self):
        """Test backoff increases on rate limits."""
        state = RateLimitState()

        state.record_rate_limit()
        assert state.consecutive_limits == 1
        assert state.backoff_seconds == 1.0

        state.record_rate_limit()
        assert state.consecutive_limits == 2
        assert state.backoff_seconds == 2.0

        state.record_rate_limit()
        assert state.consecutive_limits == 3
        assert state.backoff_seconds == 4.0

    def test_record_success_resets_backoff(self):
        """Test success resets backoff state."""
        state = RateLimitState()
        state.record_rate_limit()
        state.record_rate_limit()
        assert state.consecutive_limits == 2

        state.record_success()
        assert state.consecutive_limits == 0
        assert state.backoff_seconds == 1.0

    def test_backoff_capped_at_max(self):
        """Test backoff is capped at maximum."""
        state = RateLimitState()
        for _ in range(10):  # More than needed to hit max
            state.record_rate_limit()

        assert state.backoff_seconds == 32.0  # Max backoff

    def test_request_tracking(self):
        """Test request counting per minute."""
        state = RateLimitState()
        state.record_request()
        state.record_request()
        state.record_request()
        assert state.requests_this_minute == 3


class TestFundingRateProviderCaching:
    """Tests for FundingRateProvider caching behavior."""

    def test_cache_hit(self):
        """Test that cached data is returned on cache hit."""
        provider = FundingRateProvider()

        # Manually add to cache
        data = FundingRateData(
            protocol="gmx",
            market="ETH-USD",
            timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            rate=Decimal("0.0001"),
        )
        provider._add_to_cache(data)

        # Get from cache
        cached = provider._get_from_cache(
            "gmx", "ETH-USD", datetime(2024, 1, 15, 12, 30, tzinfo=UTC)
        )
        assert cached is not None
        assert cached.rate == Decimal("0.0001")

    def test_cache_miss(self):
        """Test cache miss returns None."""
        provider = FundingRateProvider()
        cached = provider._get_from_cache(
            "gmx", "ETH-USD", datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        )
        assert cached is None

    def test_expired_cache_returns_none(self):
        """Test expired cache entry returns None."""
        provider = FundingRateProvider(cache_ttl_seconds=1)

        data = FundingRateData(
            protocol="gmx",
            market="ETH-USD",
            timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            rate=Decimal("0.0001"),
        )

        # Manually insert with old timestamp
        key = provider._get_cache_key("gmx", "ETH-USD", data.timestamp)
        provider._cache[key] = CachedFundingRate(
            data=data,
            fetched_at=time.time() - 10,  # 10 seconds ago, TTL is 1 second
            ttl_seconds=1,
        )

        cached = provider._get_from_cache("gmx", "ETH-USD", data.timestamp)
        assert cached is None

    def test_clear_cache(self):
        """Test clearing cache removes all entries."""
        provider = FundingRateProvider()

        # Add some entries
        for market in ["ETH-USD", "BTC-USD", "SOL-USD"]:
            data = FundingRateData(
                protocol="gmx",
                market=market,
                timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
                rate=Decimal("0.0001"),
            )
            provider._add_to_cache(data)

        assert len(provider._cache) == 3

        provider.clear_cache()
        assert len(provider._cache) == 0

    def test_cache_stats(self):
        """Test cache statistics reporting."""
        provider = FundingRateProvider()

        # Add fresh entry
        data = FundingRateData(
            protocol="gmx",
            market="ETH-USD",
            timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            rate=Decimal("0.0001"),
        )
        provider._add_to_cache(data)

        stats = provider.get_cache_stats()
        assert stats["total_entries"] == 1
        assert stats["valid_entries"] == 1
        assert stats["expired_entries"] == 0


class TestFundingRateProviderGMX:
    """Tests for GMX-specific functionality."""

    @pytest.mark.asyncio
    async def test_fetch_gmx_funding_rate_success(self):
        """Test successful GMX funding rate fetch."""
        provider = FundingRateProvider()

        # Mock the HTTP session and response
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value={
                "fundingRate": "0.00012",
                "longOpenInterest": "50000000",
                "shortOpenInterest": "45000000",
            }
        )
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.closed = False

        provider._session = mock_session

        data = await provider._fetch_gmx_funding_rate(
            "ETH-USD",
            datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
        )

        assert data.rate == Decimal("0.00012")
        assert data.market == "ETH-USD"
        assert data.source == "gmx_api"

    @pytest.mark.asyncio
    async def test_fetch_gmx_market_not_found(self):
        """Test GMX returns error for unknown market."""
        provider = FundingRateProvider()

        with pytest.raises(FundingRateNotFoundError) as exc_info:
            await provider._fetch_gmx_funding_rate(
                "UNKNOWN-USD",
                datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            )

        assert exc_info.value.protocol == "gmx"
        assert exc_info.value.market == "UNKNOWN-USD"

    def test_gmx_markets_defined(self):
        """Test GMX markets are defined."""
        assert "ETH-USD" in GMX_MARKETS["arbitrum"]
        assert "BTC-USD" in GMX_MARKETS["arbitrum"]
        assert "ETH-USD" in GMX_MARKETS["avalanche"]


class TestFundingRateProviderHyperliquid:
    """Tests for Hyperliquid-specific functionality."""

    @pytest.mark.asyncio
    async def test_fetch_hyperliquid_funding_rate_success(self):
        """Test successful Hyperliquid funding rate fetch."""
        provider = FundingRateProvider()

        # Mock Hyperliquid API response structure
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value=[
                {
                    "universe": [
                        {"name": "BTC"},
                        {"name": "ETH"},
                        {"name": "SOL"},
                    ]
                },
                [
                    {"funding": "0.0008", "markPx": "45000", "openInterest": "1000"},
                    {"funding": "0.0006", "markPx": "2500", "openInterest": "5000"},
                    {"funding": "0.0004", "markPx": "100", "openInterest": "10000"},
                ],
            ]
        )
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.post = MagicMock(return_value=mock_response)
        mock_session.closed = False

        provider._session = mock_session

        data = await provider._fetch_hyperliquid_funding_rate(
            "ETH-USD",
            datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
        )

        # ETH is at index 1, funding is 0.0006 per 8h = 0.000075 per hour
        expected_hourly = Decimal("0.0006") / Decimal("8")
        assert data.rate == expected_hourly
        assert data.market == "ETH-USD"
        assert data.source == "hyperliquid_api"

    def test_hyperliquid_markets_defined(self):
        """Test Hyperliquid markets are defined."""
        assert "ETH-USD" in HYPERLIQUID_MARKETS
        assert "BTC-USD" in HYPERLIQUID_MARKETS
        assert "SOL-USD" in HYPERLIQUID_MARKETS


class TestFundingRateProviderErrors:
    """Tests for error handling."""

    @pytest.mark.asyncio
    async def test_unsupported_protocol_raises(self):
        """Test unsupported protocol raises UnsupportedProtocolError."""
        provider = FundingRateProvider()

        with pytest.raises(UnsupportedProtocolError) as exc_info:
            await provider.get_historical_funding_rate(
                protocol="unsupported",
                market="ETH-USD",
                timestamp=datetime.now(UTC),
            )

        assert exc_info.value.protocol == "unsupported"

    @pytest.mark.asyncio
    async def test_rate_limit_error_handling(self):
        """Test rate limit error triggers backoff recording."""
        provider = FundingRateProvider()

        mock_response = MagicMock()
        mock_response.status = 429
        mock_response.headers = {"Retry-After": "30"}
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.closed = False

        provider._session = mock_session

        with pytest.raises(FundingRateRateLimitError):
            await provider._fetch_gmx_funding_rate(
                "ETH-USD",
                datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            )

    @pytest.mark.asyncio
    async def test_fallback_to_default_on_not_found(self):
        """Test fallback to default rate when data not found."""
        provider = FundingRateProvider()

        # Mock session that returns 404
        mock_response = MagicMock()
        mock_response.status = 404
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.post = MagicMock(return_value=mock_response)
        mock_session.closed = False

        provider._session = mock_session

        # This should fall back to default rate instead of raising
        data = await provider.get_historical_funding_rate(
            protocol="gmx",
            market="ETH-USD",
            timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
        )

        assert data.source == "fallback"
        assert data.rate == DEFAULT_FUNDING_RATES["gmx"]


class TestFundingRateProviderDefaults:
    """Tests for default funding rates."""

    def test_get_default_rate_gmx(self):
        """Test getting default rate for GMX."""
        provider = FundingRateProvider()
        rate = provider.get_default_rate("gmx")
        assert rate == Decimal("0.0001")

    def test_get_default_rate_hyperliquid(self):
        """Test getting default rate for Hyperliquid."""
        provider = FundingRateProvider()
        rate = provider.get_default_rate("hyperliquid")
        assert rate == Decimal("0.0001")

    def test_get_default_rate_unknown_protocol(self):
        """Test getting default rate for unknown protocol returns fallback."""
        provider = FundingRateProvider()
        rate = provider.get_default_rate("unknown")
        assert rate == Decimal("0.0001")

    def test_default_funding_rates_defined(self):
        """Test default funding rates are defined for all protocols."""
        for protocol in SUPPORTED_PROTOCOLS:
            assert protocol in DEFAULT_FUNDING_RATES


class TestFundingRateProviderSerialization:
    """Tests for provider serialization."""

    def test_to_dict(self):
        """Test provider config serialization."""
        provider = FundingRateProvider(
            chain="arbitrum",
            cache_ttl_seconds=7200,
            request_timeout=60,
            requests_per_minute=20,
        )

        d = provider.to_dict()

        assert d["chain"] == "arbitrum"
        assert d["cache_ttl_seconds"] == 7200
        assert d["request_timeout"] == 60
        assert d["requests_per_minute"] == 20
        assert d["supported_protocols"] == SUPPORTED_PROTOCOLS
