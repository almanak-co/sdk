"""Unit tests for Lending APY Provider.

This module tests the LendingAPYProvider class in providers/lending_apy.py, covering:
- Provider initialization and configuration
- Historical APY fetching with mocked subgraph responses
- Caching behavior with 1-hour TTL
- Rate limit handling and backoff
- Aave V3 and Compound V3 subgraph support
- Error handling for failed queries
"""

import time
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.backtesting.pnl.providers.lending_apy import (
    AAVE_V3_MARKETS,
    COMPOUND_V3_MARKETS,
    DEFAULT_BORROW_APYS,
    DEFAULT_CACHE_TTL_SECONDS,
    DEFAULT_SUPPLY_APYS,
    SUPPORTED_PROTOCOLS,
    CachedLendingAPY,
    LendingAPYData,
    LendingAPYNotFoundError,
    LendingAPYProvider,
    LendingAPYRateLimitError,
    RateLimitState,
    UnsupportedProtocolError,
)


class TestLendingAPYProviderInitialization:
    """Tests for LendingAPYProvider initialization."""

    def test_init_default_chain(self):
        """Test provider initializes with default ethereum chain."""
        provider = LendingAPYProvider()
        assert provider.chain == "ethereum"
        assert provider.provider_name == "lending_apy_ethereum"

    def test_init_arbitrum_chain(self):
        """Test provider initializes with arbitrum chain."""
        provider = LendingAPYProvider(chain="arbitrum")
        assert provider.chain == "arbitrum"
        assert provider.provider_name == "lending_apy_arbitrum"

    def test_init_unsupported_chain_raises(self):
        """Test provider raises ValueError for unsupported chain."""
        with pytest.raises(ValueError) as exc_info:
            LendingAPYProvider(chain="unsupported_chain")
        assert "Unsupported chain" in str(exc_info.value)

    def test_init_cache_ttl(self):
        """Test provider initializes with custom cache TTL."""
        provider = LendingAPYProvider(cache_ttl_seconds=7200)
        assert provider._cache_ttl_seconds == 7200

    def test_init_default_cache_ttl(self):
        """Test provider uses default 1-hour cache TTL."""
        provider = LendingAPYProvider()
        assert provider._cache_ttl_seconds == DEFAULT_CACHE_TTL_SECONDS
        assert provider._cache_ttl_seconds == 3600

    def test_init_request_timeout(self):
        """Test provider initializes with custom request timeout."""
        provider = LendingAPYProvider(request_timeout=60)
        assert provider._request_timeout == 60

    def test_init_requests_per_minute(self):
        """Test provider initializes with custom requests per minute."""
        provider = LendingAPYProvider(requests_per_minute=10)
        assert provider._requests_per_minute == 10

    def test_chain_case_insensitive(self):
        """Test chain parameter is case insensitive."""
        provider = LendingAPYProvider(chain="ETHEREUM")
        assert provider.chain == "ethereum"

        provider = LendingAPYProvider(chain="Arbitrum")
        assert provider.chain == "arbitrum"


class TestLendingAPYData:
    """Tests for LendingAPYData dataclass."""

    def test_basic_creation(self):
        """Test creating LendingAPYData with required fields."""
        data = LendingAPYData(
            protocol="aave_v3",
            market="USDC",
            timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            supply_apy=Decimal("0.03"),
            borrow_apy=Decimal("0.05"),
        )
        assert data.protocol == "aave_v3"
        assert data.market == "USDC"
        assert data.supply_apy == Decimal("0.03")
        assert data.borrow_apy == Decimal("0.05")
        assert data.source == "subgraph"

    def test_percentage_apy_calculation(self):
        """Test automatic percentage APY calculation."""
        data = LendingAPYData(
            protocol="aave_v3",
            market="USDC",
            timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            supply_apy=Decimal("0.03"),  # 3%
            borrow_apy=Decimal("0.05"),  # 5%
        )
        assert data.supply_apy_pct == Decimal("3")  # 3%
        assert data.borrow_apy_pct == Decimal("5")  # 5%

    def test_to_dict_serialization(self):
        """Test serialization to dictionary."""
        data = LendingAPYData(
            protocol="compound_v3",
            market="WETH",
            timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            supply_apy=Decimal("0.025"),
            borrow_apy=Decimal("0.045"),
            utilization_rate=Decimal("0.75"),
            total_supply_usd=Decimal("1000000000"),
            total_borrow_usd=Decimal("750000000"),
            source="compound_v3_subgraph",
        )
        d = data.to_dict()

        assert d["protocol"] == "compound_v3"
        assert d["market"] == "WETH"
        assert d["supply_apy"] == "0.025"
        assert d["borrow_apy"] == "0.045"
        assert d["source"] == "compound_v3_subgraph"
        assert d["utilization_rate"] == "0.75"
        assert d["total_supply_usd"] == "1000000000"
        assert d["total_borrow_usd"] == "750000000"

    def test_from_dict_deserialization(self):
        """Test deserialization from dictionary."""
        d = {
            "protocol": "aave_v3",
            "market": "USDC",
            "timestamp": "2024-01-15T12:00:00+00:00",
            "supply_apy": "0.03",
            "borrow_apy": "0.05",
            "supply_apy_pct": "3",
            "borrow_apy_pct": "5",
            "source": "aave_v3_subgraph",
        }
        data = LendingAPYData.from_dict(d)

        assert data.protocol == "aave_v3"
        assert data.market == "USDC"
        assert data.supply_apy == Decimal("0.03")
        assert data.borrow_apy == Decimal("0.05")
        assert data.supply_apy_pct == Decimal("3")
        assert data.borrow_apy_pct == Decimal("5")
        assert data.source == "aave_v3_subgraph"

    def test_roundtrip_serialization(self):
        """Test serialization roundtrip preserves data."""
        original = LendingAPYData(
            protocol="aave_v3",
            market="WBTC",
            timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            supply_apy=Decimal("0.02"),
            borrow_apy=Decimal("0.04"),
            utilization_rate=Decimal("0.65"),
            source="aave_v3_subgraph",
        )
        restored = LendingAPYData.from_dict(original.to_dict())

        assert restored.protocol == original.protocol
        assert restored.market == original.market
        assert restored.supply_apy == original.supply_apy
        assert restored.borrow_apy == original.borrow_apy
        assert restored.source == original.source


class TestCachedLendingAPY:
    """Tests for CachedLendingAPY caching behavior."""

    def test_not_expired_when_fresh(self):
        """Test cached data is not expired when fresh."""
        data = LendingAPYData(
            protocol="aave_v3",
            market="USDC",
            timestamp=datetime.now(UTC),
            supply_apy=Decimal("0.03"),
            borrow_apy=Decimal("0.05"),
        )
        cached = CachedLendingAPY(
            data=data,
            fetched_at=time.time(),
            ttl_seconds=3600,
        )
        assert not cached.is_expired

    def test_expired_when_stale(self):
        """Test cached data is expired when past TTL."""
        data = LendingAPYData(
            protocol="aave_v3",
            market="USDC",
            timestamp=datetime.now(UTC),
            supply_apy=Decimal("0.03"),
            borrow_apy=Decimal("0.05"),
        )
        cached = CachedLendingAPY(
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


class TestLendingAPYProviderCaching:
    """Tests for LendingAPYProvider caching behavior."""

    def test_cache_hit(self):
        """Test that cached data is returned on cache hit."""
        provider = LendingAPYProvider()

        # Manually add to cache
        data = LendingAPYData(
            protocol="aave_v3",
            market="USDC",
            timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            supply_apy=Decimal("0.03"),
            borrow_apy=Decimal("0.05"),
        )
        provider._add_to_cache(data)

        # Get from cache
        cached = provider._get_from_cache(
            "aave_v3", "USDC", datetime(2024, 1, 15, 12, 30, tzinfo=UTC)
        )
        assert cached is not None
        assert cached.supply_apy == Decimal("0.03")
        assert cached.borrow_apy == Decimal("0.05")

    def test_cache_miss(self):
        """Test cache miss returns None."""
        provider = LendingAPYProvider()
        cached = provider._get_from_cache(
            "aave_v3", "USDC", datetime(2024, 1, 15, 12, 0, tzinfo=UTC)
        )
        assert cached is None

    def test_expired_cache_returns_none(self):
        """Test expired cache entry returns None."""
        provider = LendingAPYProvider(cache_ttl_seconds=1)

        data = LendingAPYData(
            protocol="aave_v3",
            market="USDC",
            timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            supply_apy=Decimal("0.03"),
            borrow_apy=Decimal("0.05"),
        )

        # Manually insert with old timestamp
        key = provider._get_cache_key("aave_v3", "USDC", data.timestamp)
        provider._cache[key] = CachedLendingAPY(
            data=data,
            fetched_at=time.time() - 10,  # 10 seconds ago, TTL is 1 second
            ttl_seconds=1,
        )

        cached = provider._get_from_cache("aave_v3", "USDC", data.timestamp)
        assert cached is None

    def test_clear_cache(self):
        """Test clearing cache removes all entries."""
        provider = LendingAPYProvider()

        # Add some entries
        for market in ["USDC", "WETH", "WBTC"]:
            data = LendingAPYData(
                protocol="aave_v3",
                market=market,
                timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
                supply_apy=Decimal("0.03"),
                borrow_apy=Decimal("0.05"),
            )
            provider._add_to_cache(data)

        assert len(provider._cache) == 3

        provider.clear_cache()
        assert len(provider._cache) == 0

    def test_cache_stats(self):
        """Test cache statistics reporting."""
        provider = LendingAPYProvider()

        # Add fresh entry
        data = LendingAPYData(
            protocol="aave_v3",
            market="USDC",
            timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            supply_apy=Decimal("0.03"),
            borrow_apy=Decimal("0.05"),
        )
        provider._add_to_cache(data)

        stats = provider.get_cache_stats()
        assert stats["total_entries"] == 1
        assert stats["valid_entries"] == 1
        assert stats["expired_entries"] == 0


class TestLendingAPYProviderAaveV3:
    """Tests for Aave V3-specific functionality."""

    @pytest.mark.asyncio
    async def test_fetch_aave_v3_apy_success(self):
        """Test successful Aave V3 APY fetch with mocked subgraph response."""
        provider = LendingAPYProvider()

        # Mock the HTTP session and response
        # Aave stores rates as ray (1e27)
        # 3% APY = 0.03 * 1e27 = 30000000000000000000000000
        supply_rate_ray = str(int(Decimal("0.03") * Decimal("1000000000000000000000000000")))
        borrow_rate_ray = str(int(Decimal("0.05") * Decimal("1000000000000000000000000000")))

        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value={
                "data": {
                    "reserveParamsHistoryItems": [
                        {
                            "reserve": {"symbol": "USDC", "underlyingAsset": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"},
                            "timestamp": 1705320000,
                            "liquidityRate": supply_rate_ray,
                            "variableBorrowRate": borrow_rate_ray,
                            "utilizationRate": str(int(Decimal("0.75") * Decimal("1000000000000000000000000000"))),
                            "totalLiquidity": "1000000000",
                            "totalCurrentVariableDebt": "750000000",
                        }
                    ]
                }
            }
        )
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.post = MagicMock(return_value=mock_response)
        mock_session.closed = False

        provider._session = mock_session

        data = await provider._fetch_aave_v3_apy(
            "USDC",
            datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
        )

        assert data.supply_apy == Decimal("0.03")
        assert data.borrow_apy == Decimal("0.05")
        assert data.market == "USDC"
        assert data.source == "aave_v3_subgraph"

    @pytest.mark.asyncio
    async def test_fetch_aave_v3_market_not_found(self):
        """Test Aave V3 returns error for unknown market."""
        provider = LendingAPYProvider()

        with pytest.raises(LendingAPYNotFoundError) as exc_info:
            await provider._fetch_aave_v3_apy(
                "UNKNOWN",
                datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            )

        assert exc_info.value.protocol == "aave_v3"
        assert exc_info.value.market == "UNKNOWN"

    def test_aave_v3_markets_defined(self):
        """Test Aave V3 markets are defined."""
        assert "USDC" in AAVE_V3_MARKETS["ethereum"]
        assert "WETH" in AAVE_V3_MARKETS["ethereum"]
        assert "USDC" in AAVE_V3_MARKETS["arbitrum"]
        assert "WETH" in AAVE_V3_MARKETS["arbitrum"]


class TestLendingAPYProviderCompoundV3:
    """Tests for Compound V3-specific functionality."""

    @pytest.mark.asyncio
    async def test_fetch_compound_v3_apy_success(self):
        """Test successful Compound V3 APY fetch with mocked subgraph response."""
        provider = LendingAPYProvider()

        # Mock Compound V3 subgraph response structure
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value={
                "data": {
                    "marketHourlySnapshots": [
                        {
                            "market": {"id": "0xc3d688b66703497daa19211eedff47f25384cdc3", "name": "cUSDCv3"},
                            "timestamp": 1705320000,
                            "supplyAPY": "0.025",
                            "borrowAPY": "0.045",
                            "utilization": "0.70",
                            "totalSupply": "2000000000",
                            "totalBorrow": "1400000000",
                        }
                    ]
                }
            }
        )
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.post = MagicMock(return_value=mock_response)
        mock_session.closed = False

        provider._session = mock_session

        data = await provider._fetch_compound_v3_apy(
            "USDC",
            datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
        )

        assert data.supply_apy == Decimal("0.025")
        assert data.borrow_apy == Decimal("0.045")
        assert data.market == "USDC"
        assert data.source == "compound_v3_subgraph"

    @pytest.mark.asyncio
    async def test_fetch_compound_v3_market_not_found(self):
        """Test Compound V3 returns error for unknown market."""
        provider = LendingAPYProvider()

        with pytest.raises(LendingAPYNotFoundError) as exc_info:
            await provider._fetch_compound_v3_apy(
                "UNKNOWN",
                datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            )

        assert exc_info.value.protocol == "compound_v3"
        assert exc_info.value.market == "UNKNOWN"

    def test_compound_v3_markets_defined(self):
        """Test Compound V3 markets are defined."""
        assert "USDC" in COMPOUND_V3_MARKETS["ethereum"]
        assert "WETH" in COMPOUND_V3_MARKETS["ethereum"]


class TestLendingAPYProviderErrors:
    """Tests for error handling."""

    @pytest.mark.asyncio
    async def test_unsupported_protocol_raises(self):
        """Test unsupported protocol raises UnsupportedProtocolError."""
        provider = LendingAPYProvider()

        with pytest.raises(UnsupportedProtocolError) as exc_info:
            await provider.get_historical_apy(
                protocol="unsupported",
                market="USDC",
                timestamp=datetime.now(UTC),
            )

        assert exc_info.value.protocol == "unsupported"

    @pytest.mark.asyncio
    async def test_rate_limit_error_handling(self):
        """Test rate limit error is raised correctly."""
        provider = LendingAPYProvider()

        mock_response = MagicMock()
        mock_response.status = 429
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.post = MagicMock(return_value=mock_response)
        mock_session.closed = False

        provider._session = mock_session

        with pytest.raises(LendingAPYRateLimitError):
            await provider._fetch_aave_v3_apy(
                "USDC",
                datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            )

    @pytest.mark.asyncio
    async def test_fallback_to_default_on_not_found(self):
        """Test fallback to default APY when data not found."""
        provider = LendingAPYProvider()

        # Mock session that returns empty data
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value={
                "data": {
                    "reserveParamsHistoryItems": []
                }
            }
        )
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.post = MagicMock(return_value=mock_response)
        mock_session.closed = False

        provider._session = mock_session

        # This should fall back to default rate instead of raising
        data = await provider.get_historical_apy(
            protocol="aave_v3",
            market="USDC",
            timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
        )

        assert data.source == "fallback"
        assert data.supply_apy == DEFAULT_SUPPLY_APYS["aave_v3"]
        assert data.borrow_apy == DEFAULT_BORROW_APYS["aave_v3"]


class TestLendingAPYProviderDefaults:
    """Tests for default APY rates."""

    def test_get_default_supply_apy_aave_v3(self):
        """Test getting default supply APY for Aave V3."""
        provider = LendingAPYProvider()
        rate = provider.get_default_supply_apy("aave_v3")
        assert rate == Decimal("0.03")

    def test_get_default_borrow_apy_aave_v3(self):
        """Test getting default borrow APY for Aave V3."""
        provider = LendingAPYProvider()
        rate = provider.get_default_borrow_apy("aave_v3")
        assert rate == Decimal("0.05")

    def test_get_default_supply_apy_compound_v3(self):
        """Test getting default supply APY for Compound V3."""
        provider = LendingAPYProvider()
        rate = provider.get_default_supply_apy("compound_v3")
        assert rate == Decimal("0.025")

    def test_get_default_borrow_apy_compound_v3(self):
        """Test getting default borrow APY for Compound V3."""
        provider = LendingAPYProvider()
        rate = provider.get_default_borrow_apy("compound_v3")
        assert rate == Decimal("0.045")

    def test_get_default_apy_unknown_protocol(self):
        """Test getting default APY for unknown protocol returns fallback."""
        provider = LendingAPYProvider()
        supply = provider.get_default_supply_apy("unknown")
        borrow = provider.get_default_borrow_apy("unknown")
        assert supply == Decimal("0.03")  # Default fallback
        assert borrow == Decimal("0.05")  # Default fallback

    def test_default_apys_defined(self):
        """Test default APYs are defined for all protocols."""
        for protocol in SUPPORTED_PROTOCOLS:
            assert protocol in DEFAULT_SUPPLY_APYS
            assert protocol in DEFAULT_BORROW_APYS


class TestLendingAPYProviderSerialization:
    """Tests for provider serialization."""

    def test_to_dict(self):
        """Test provider config serialization."""
        provider = LendingAPYProvider(
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


class TestLendingAPYProviderIntegration:
    """Integration tests for the full flow."""

    @pytest.mark.asyncio
    async def test_get_historical_apy_with_cache(self):
        """Test get_historical_apy uses cache on second call."""
        provider = LendingAPYProvider()

        # First call: manually add to cache to simulate a fetch
        data = LendingAPYData(
            protocol="aave_v3",
            market="USDC",
            timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
            supply_apy=Decimal("0.035"),
            borrow_apy=Decimal("0.055"),
            source="aave_v3_subgraph",
        )
        provider._add_to_cache(data)

        # Second call should return cached data
        result = await provider.get_historical_apy(
            protocol="aave_v3",
            market="USDC",
            timestamp=datetime(2024, 1, 15, 12, 30, tzinfo=UTC),
        )

        assert result.supply_apy == Decimal("0.035")
        assert result.borrow_apy == Decimal("0.055")
        assert result.source == "aave_v3_subgraph"

    def test_normalize_timestamp(self):
        """Test timestamp normalization to hourly boundary."""
        provider = LendingAPYProvider()

        # Test that timestamps in the same hour normalize to the same value
        ts1 = datetime(2024, 1, 15, 12, 15, 30, tzinfo=UTC)
        ts2 = datetime(2024, 1, 15, 12, 45, 0, tzinfo=UTC)

        normalized1 = provider._normalize_timestamp(ts1)
        normalized2 = provider._normalize_timestamp(ts2)

        assert normalized1 == normalized2
        assert normalized1.minute == 0
        assert normalized1.second == 0
        assert normalized1.microsecond == 0
