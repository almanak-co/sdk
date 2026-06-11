"""Unit tests for Funding Rate Provider.

This module tests the FundingRateProvider class in providers/funding_rates.py, covering:
- Provider initialization and configuration
- Historical funding rate fetching with a mocked gateway seam
- Caching behavior with 1-hour TTL
- Manifest-derived protocol/chain validation
- Fallback behavior when the gateway has no data

The provider is a thin ``RateHistoryService`` client since VIB-4851 Phase D —
tests mock ``fetch_funding_points`` (the gateway seam), never HTTP.
"""

import time
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import patch

import pytest

from almanak.framework.backtesting.pnl.providers.funding_rates import (
    DEFAULT_CACHE_TTL_SECONDS,
    DEFAULT_FUNDING_RATE,
    CachedFundingRate,
    FundingRateData,
    FundingRateProvider,
    RateLimitState,
    UnsupportedProtocolError,
    supported_protocols,
)
from almanak.framework.backtesting.pnl.providers.perp._gateway_history import (
    FundingHistoryPoint,
)
from almanak.framework.data.interfaces import DataSourceUnavailable

_GATEWAY_SEAM = "almanak.framework.backtesting.pnl.providers.funding_rates.fetch_funding_points"


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

    def test_chain_validation_is_manifest_derived(self):
        """The accepted chain set is the union of declared funding chains."""
        from almanak.connectors._strategy_base.funding_history_registry import (
            FundingHistoryRegistry,
        )

        for chain in FundingHistoryRegistry.all_declared_chains():
            assert FundingRateProvider(chain=chain).chain == chain


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
        assert data.source == "gateway"

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
            source="gateway",
        )
        d = data.to_dict()

        assert d["protocol"] == "hyperliquid"
        assert d["market"] == "BTC-USD"
        assert d["rate"] == "0.00015"
        assert d["source"] == "gateway"
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
            "source": "gateway",
        }
        data = FundingRateData.from_dict(d)

        assert data.protocol == "gmx"
        assert data.market == "ETH-USD"
        assert data.rate == Decimal("0.0001")
        assert data.annualized_rate_pct == Decimal("87.6")
        assert data.source == "gateway"

    def test_roundtrip_serialization(self):
        """Test serialization roundtrip preserves data."""
        original = FundingRateData(
            protocol="gmx",
            market="ETH-USD",
            timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            rate=Decimal("0.0001"),
            open_interest_long=Decimal("5000000"),
            source="fallback",
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
        cached = provider._get_from_cache("gmx", "ETH-USD", datetime(2024, 1, 15, 12, 30, tzinfo=UTC))
        assert cached is not None
        assert cached.rate == Decimal("0.0001")

    def test_cache_miss(self):
        """Test cache miss returns None."""
        provider = FundingRateProvider()
        cached = provider._get_from_cache("gmx", "ETH-USD", datetime(2024, 1, 15, 12, 0, tzinfo=UTC))
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


class TestFundingRateProviderGateway:
    """Tests for the gateway-backed fetch path (mocked at the RPC seam)."""

    @pytest.mark.asyncio
    async def test_historical_rate_picks_latest_point(self):
        """The rate at T is the latest measured point at or before T."""
        provider = FundingRateProvider()
        t0 = int(datetime(2024, 1, 15, 9, 0, tzinfo=UTC).timestamp())
        points = [
            FundingHistoryPoint(timestamp=t0, rate_hourly=Decimal("0.0001")),
            FundingHistoryPoint(timestamp=t0 + 3600, rate_hourly=Decimal("0.0002")),
            FundingHistoryPoint(timestamp=t0 + 7200, rate_hourly=Decimal("0.0003")),
        ]

        with patch(_GATEWAY_SEAM, return_value=points) as seam:
            data = await provider.get_historical_funding_rate(
                protocol="gmx",
                market="eth-usd",
                timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            )

        assert data.rate == Decimal("0.0003")
        assert data.market == "ETH-USD"
        assert data.protocol == "gmx"
        assert data.source == "gateway"

        kwargs = seam.call_args.kwargs
        # The legacy "gmx" alias resolves to the gmx_v2 venue via the manifest.
        assert kwargs["venue"] == "gmx_v2"
        assert kwargs["chain"] == "arbitrum"
        assert kwargs["market"] == "ETH-USD"
        # No look-ahead: the window ends at the requested timestamp.
        assert kwargs["end_ts"] == int(datetime(2024, 1, 15, 12, 0, tzinfo=UTC).timestamp())
        assert kwargs["start_ts"] == kwargs["end_ts"] - 86_400

    @pytest.mark.asyncio
    async def test_naive_timestamp_is_read_as_utc(self, monkeypatch: pytest.MonkeyPatch):
        """A naive timestamp queries the same window as its UTC-aware twin.

        Bare ``datetime.timestamp()`` reads naive datetimes in the host
        timezone — the provider must pin them to UTC so backtest windows are
        identical on every machine (matches the venue providers' contract).
        Forces a non-UTC host timezone so the assertion bites on UTC CI
        runners too.
        """
        provider = FundingRateProvider()
        points = [FundingHistoryPoint(timestamp=1, rate_hourly=Decimal("0.0001"))]
        aware = datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        naive = aware.replace(tzinfo=None)

        monkeypatch.setenv("TZ", "America/New_York")
        time.tzset()
        try:
            with patch(_GATEWAY_SEAM, return_value=points) as seam:
                await provider.get_historical_funding_rate(
                    protocol="gmx",
                    market="ETH-USD",
                    timestamp=naive,
                )
        finally:
            monkeypatch.undo()
            time.tzset()

        assert seam.call_args.kwargs["end_ts"] == int(aware.timestamp())

    @pytest.mark.asyncio
    async def test_hyperliquid_sends_empty_chain(self):
        """Chain-agnostic venues get an empty chain per the RPC contract."""
        provider = FundingRateProvider()
        points = [FundingHistoryPoint(timestamp=1, rate_hourly=Decimal("0.0005"))]

        with patch(_GATEWAY_SEAM, return_value=points) as seam:
            data = await provider.get_historical_funding_rate(
                protocol="hyperliquid",
                market="BTC-USD",
                timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            )

        assert data.rate == Decimal("0.0005")
        assert seam.call_args.kwargs["venue"] == "hyperliquid"
        assert seam.call_args.kwargs["chain"] == ""

    @pytest.mark.asyncio
    async def test_empty_points_fall_back_to_default(self):
        """An empty history window falls back to the default rate."""
        provider = FundingRateProvider()

        with patch(_GATEWAY_SEAM, return_value=[]):
            data = await provider.get_historical_funding_rate(
                protocol="gmx_v2",
                market="ETH-USD",
                timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            )

        assert data.source == "fallback"
        assert data.rate == DEFAULT_FUNDING_RATE

    @pytest.mark.asyncio
    async def test_gateway_unavailable_falls_back_to_default(self):
        """A failed gateway round-trip degrades to the default rate."""
        provider = FundingRateProvider()

        with patch(_GATEWAY_SEAM, side_effect=DataSourceUnavailable(source="gateway", reason="down")):
            data = await provider.get_historical_funding_rate(
                protocol="hyperliquid",
                market="ETH-USD",
                timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            )

        assert data.source == "fallback"
        assert data.rate == DEFAULT_FUNDING_RATE

    @pytest.mark.asyncio
    async def test_successful_fetch_is_cached(self):
        """A gateway result is served from cache within the same hour."""
        provider = FundingRateProvider()
        points = [FundingHistoryPoint(timestamp=1, rate_hourly=Decimal("0.0007"))]

        with patch(_GATEWAY_SEAM, return_value=points) as seam:
            first = await provider.get_historical_funding_rate(
                "gmx", "ETH-USD", datetime(2024, 1, 15, 12, 5, tzinfo=UTC)
            )
            second = await provider.get_historical_funding_rate(
                "gmx", "ETH-USD", datetime(2024, 1, 15, 12, 55, tzinfo=UTC)
            )

        assert first.rate == second.rate == Decimal("0.0007")
        assert seam.call_count == 1

    @pytest.mark.asyncio
    async def test_current_rate_queries_now(self):
        """get_current_funding_rate delegates to the historical path at now."""
        provider = FundingRateProvider()
        points = [FundingHistoryPoint(timestamp=1, rate_hourly=Decimal("0.0009"))]

        with patch(_GATEWAY_SEAM, return_value=points):
            data = await provider.get_current_funding_rate("hyperliquid", "ETH-USD")

        assert data.rate == Decimal("0.0009")
        assert data.source == "gateway"


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

    def test_supported_protocols_manifest_derived(self):
        """The accepted identifier set derives from connector manifests."""
        assert supported_protocols() == ["gmx", "gmx_v2", "hyperliquid"]


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
        assert d["supported_protocols"] == supported_protocols()
