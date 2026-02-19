"""Unit tests for Subgraph Volume Provider.

This module tests the SubgraphVolumeProvider class in providers/subgraph.py, covering:
- Provider initialization and configuration
- Pool volume data fetching with mocked responses
- Caching behavior with 1-hour TTL
- Rate limit handling and backoff
- Error handling for failed queries
"""

from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.backtesting.pnl.providers.subgraph import (
    DEFAULT_CACHE_TTL_SECONDS,
    SUPPORTED_CHAINS,
    UNISWAP_V3_SUBGRAPHS,
    CachedVolume,
    PoolVolumeData,
    RateLimitState,
    SubgraphQueryError,
    SubgraphRateLimitError,
    SubgraphVolumeProvider,
)


class TestSubgraphProviderInitialization:
    """Tests for SubgraphVolumeProvider initialization."""

    def test_init_default_chain(self):
        """Test provider initializes with default arbitrum chain."""
        provider = SubgraphVolumeProvider()
        assert provider.chain == "arbitrum"
        assert provider.provider_name == "subgraph_arbitrum"

    def test_init_ethereum_chain(self):
        """Test provider initializes with ethereum chain."""
        provider = SubgraphVolumeProvider(chain="ethereum")
        assert provider.chain == "ethereum"
        assert provider.provider_name == "subgraph_ethereum"

    def test_init_base_chain(self):
        """Test provider initializes with base chain."""
        provider = SubgraphVolumeProvider(chain="base")
        assert provider.chain == "base"
        assert provider.provider_name == "subgraph_base"

    def test_init_unsupported_chain_raises(self):
        """Test provider raises ValueError for unsupported chain."""
        with pytest.raises(ValueError) as exc_info:
            SubgraphVolumeProvider(chain="unsupported_chain")
        assert "Unsupported chain" in str(exc_info.value)
        assert "unsupported_chain" in str(exc_info.value)

    def test_init_with_api_key(self):
        """Test provider initializes with API key."""
        provider = SubgraphVolumeProvider(api_key="test-api-key")
        assert provider._api_key == "test-api-key"

    def test_init_cache_ttl(self):
        """Test provider initializes with custom cache TTL."""
        provider = SubgraphVolumeProvider(cache_ttl_seconds=7200)
        assert provider._cache_ttl_seconds == 7200

    def test_init_default_cache_ttl(self):
        """Test provider uses default 1-hour cache TTL."""
        provider = SubgraphVolumeProvider()
        assert provider._cache_ttl_seconds == DEFAULT_CACHE_TTL_SECONDS
        assert provider._cache_ttl_seconds == 3600

    def test_init_request_timeout(self):
        """Test provider initializes with custom request timeout."""
        provider = SubgraphVolumeProvider(request_timeout=60)
        assert provider._request_timeout == 60

    def test_init_requests_per_minute(self):
        """Test provider initializes with custom requests per minute."""
        provider = SubgraphVolumeProvider(requests_per_minute=10)
        assert provider._requests_per_minute == 10

    def test_chain_case_insensitive(self):
        """Test chain parameter is case insensitive."""
        provider = SubgraphVolumeProvider(chain="ETHEREUM")
        assert provider.chain == "ethereum"

        provider = SubgraphVolumeProvider(chain="Arbitrum")
        assert provider.chain == "arbitrum"

    def test_subgraph_url_arbitrum(self):
        """Test subgraph URL for arbitrum."""
        provider = SubgraphVolumeProvider(chain="arbitrum")
        assert provider.subgraph_url == UNISWAP_V3_SUBGRAPHS["arbitrum"]

    def test_use_hosted_service(self):
        """Test using hosted service instead of gateway."""
        provider = SubgraphVolumeProvider(chain="ethereum", use_hosted_service=True)
        assert "api.thegraph.com" in provider.subgraph_url


class TestSupportedChains:
    """Tests for supported chains configuration."""

    def test_supported_chains_include_major_networks(self):
        """Test that major networks are supported."""
        assert "ethereum" in SUPPORTED_CHAINS
        assert "arbitrum" in SUPPORTED_CHAINS
        assert "base" in SUPPORTED_CHAINS
        assert "optimism" in SUPPORTED_CHAINS
        assert "polygon" in SUPPORTED_CHAINS

    def test_all_supported_chains_have_subgraph_urls(self):
        """Test all supported chains have subgraph URLs."""
        for chain in SUPPORTED_CHAINS:
            assert chain in UNISWAP_V3_SUBGRAPHS


class TestPoolVolumeData:
    """Tests for PoolVolumeData dataclass."""

    def test_pool_volume_data_creation(self):
        """Test creating PoolVolumeData with all fields."""
        volume = PoolVolumeData(
            pool_address="0x123abc",
            date=date(2024, 1, 15),
            volume_usd=Decimal("1000000"),
            volume_token0=Decimal("500"),
            volume_token1=Decimal("1000000"),
            fees_usd=Decimal("3000"),
            tvl_usd=Decimal("10000000"),
            liquidity=1000000000,
            token0_price=Decimal("2000"),
            token1_price=Decimal("1"),
        )
        assert volume.pool_address == "0x123abc"
        assert volume.date == date(2024, 1, 15)
        assert volume.volume_usd == Decimal("1000000")
        assert volume.fees_usd == Decimal("3000")

    def test_pool_volume_data_defaults(self):
        """Test PoolVolumeData default values."""
        volume = PoolVolumeData(
            pool_address="0x123abc",
            date=date(2024, 1, 15),
            volume_usd=Decimal("1000000"),
        )
        assert volume.volume_token0 == Decimal("0")
        assert volume.volume_token1 == Decimal("0")
        assert volume.fees_usd == Decimal("0")
        assert volume.tvl_usd == Decimal("0")
        assert volume.liquidity == 0


class TestCachedVolume:
    """Tests for CachedVolume caching behavior."""

    def test_cached_volume_not_expired(self):
        """Test cached volume is not expired when fresh."""
        import time

        volume_data = PoolVolumeData(
            pool_address="0x123",
            date=date(2024, 1, 15),
            volume_usd=Decimal("1000000"),
        )
        cached = CachedVolume(
            data=volume_data,
            fetched_at=time.time(),
            ttl_seconds=3600,
        )
        assert not cached.is_expired

    def test_cached_volume_expired(self):
        """Test cached volume is expired after TTL."""
        import time

        volume_data = PoolVolumeData(
            pool_address="0x123",
            date=date(2024, 1, 15),
            volume_usd=Decimal("1000000"),
        )
        # Set fetched_at to 2 hours ago
        cached = CachedVolume(
            data=volume_data,
            fetched_at=time.time() - 7200,
            ttl_seconds=3600,
        )
        assert cached.is_expired


class TestRateLimitState:
    """Tests for RateLimitState behavior."""

    def test_record_rate_limit_increases_backoff(self):
        """Test recording rate limit increases backoff."""
        state = RateLimitState()
        assert state.backoff_seconds == 1.0

        state.record_rate_limit()
        assert state.backoff_seconds == 1.0  # 2^0
        assert state.consecutive_limits == 1

        state.record_rate_limit()
        assert state.backoff_seconds == 2.0  # 2^1
        assert state.consecutive_limits == 2

        state.record_rate_limit()
        assert state.backoff_seconds == 4.0  # 2^2

    def test_record_success_resets_backoff(self):
        """Test recording success resets backoff."""
        state = RateLimitState()
        state.record_rate_limit()
        state.record_rate_limit()
        state.record_rate_limit()
        assert state.backoff_seconds == 4.0
        assert state.consecutive_limits == 3

        state.record_success()
        assert state.backoff_seconds == 1.0
        assert state.consecutive_limits == 0

    def test_max_backoff_capped(self):
        """Test maximum backoff is capped at 32 seconds."""
        state = RateLimitState()
        for _ in range(10):
            state.record_rate_limit()
        assert state.backoff_seconds == 32.0

    def test_get_wait_time_zero_initially(self):
        """Test wait time is zero when no rate limit hit."""
        state = RateLimitState()
        assert state.get_wait_time() == 0.0

    def test_record_request_counts(self):
        """Test request counting per minute."""
        state = RateLimitState()
        assert state.requests_this_minute == 0

        state.record_request()
        assert state.requests_this_minute == 1

        state.record_request()
        assert state.requests_this_minute == 2


class TestGetPoolVolume:
    """Tests for get_pool_volume method."""

    @pytest.mark.asyncio
    async def test_get_pool_volume_success(self):
        """Test successfully fetching pool volume data."""
        provider = SubgraphVolumeProvider(chain="arbitrum")

        # Mock the _execute_query method
        mock_response = {
            "poolDayDatas": [
                {
                    "id": "0x123-12345",
                    "date": 1705276800,  # 2024-01-15 00:00:00 UTC
                    "volumeUSD": "1500000.50",
                    "volumeToken0": "750.25",
                    "volumeToken1": "1500000.00",
                    "feesUSD": "4500.15",
                    "tvlUSD": "25000000.00",
                    "liquidity": "5000000000",
                    "token0Price": "2000.00",
                    "token1Price": "1.00",
                }
            ]
        }

        with patch.object(provider, "_execute_query", new_callable=AsyncMock) as mock_query:
            mock_query.return_value = mock_response

            volume = await provider.get_pool_volume(
                pool_address="0x123abc",
                target_date=date(2024, 1, 15),
            )

            assert volume is not None
            assert volume.pool_address == "0x123abc"
            assert volume.date == date(2024, 1, 15)
            assert volume.volume_usd == Decimal("1500000.50")
            assert volume.fees_usd == Decimal("4500.15")
            assert volume.tvl_usd == Decimal("25000000.00")

            # Verify query was called
            mock_query.assert_called_once()

        await provider.close()

    @pytest.mark.asyncio
    async def test_get_pool_volume_not_found(self):
        """Test handling when no volume data exists for date."""
        provider = SubgraphVolumeProvider(chain="arbitrum")

        with patch.object(provider, "_execute_query", new_callable=AsyncMock) as mock_query:
            mock_query.return_value = {"poolDayDatas": []}

            volume = await provider.get_pool_volume(
                pool_address="0x123abc",
                target_date=date(2024, 1, 15),
            )

            assert volume is None

        await provider.close()

    @pytest.mark.asyncio
    async def test_get_pool_volume_uses_cache(self):
        """Test that subsequent calls use cached data."""
        provider = SubgraphVolumeProvider(chain="arbitrum")

        mock_response = {
            "poolDayDatas": [
                {
                    "id": "0x123-12345",
                    "date": 1705276800,
                    "volumeUSD": "1000000",
                    "volumeToken0": "500",
                    "volumeToken1": "1000000",
                    "feesUSD": "3000",
                    "tvlUSD": "10000000",
                    "liquidity": "1000000000",
                    "token0Price": "2000",
                    "token1Price": "1",
                }
            ]
        }

        with patch.object(provider, "_execute_query", new_callable=AsyncMock) as mock_query:
            mock_query.return_value = mock_response

            # First call - should query subgraph
            volume1 = await provider.get_pool_volume(
                pool_address="0x123abc",
                target_date=date(2024, 1, 15),
            )
            assert volume1 is not None
            assert mock_query.call_count == 1

            # Second call - should use cache
            volume2 = await provider.get_pool_volume(
                pool_address="0x123abc",
                target_date=date(2024, 1, 15),
            )
            assert volume2 is not None
            assert mock_query.call_count == 1  # No additional call

            # Values should be the same
            assert volume1.volume_usd == volume2.volume_usd

        await provider.close()

    @pytest.mark.asyncio
    async def test_get_pool_volume_lowercase_address(self):
        """Test that pool addresses are normalized to lowercase."""
        provider = SubgraphVolumeProvider(chain="arbitrum")

        mock_response = {
            "poolDayDatas": [
                {
                    "id": "test",
                    "date": 1705276800,
                    "volumeUSD": "1000000",
                }
            ]
        }

        with patch.object(provider, "_execute_query", new_callable=AsyncMock) as mock_query:
            mock_query.return_value = mock_response

            volume = await provider.get_pool_volume(
                pool_address="0xABC123DEF",
                target_date=date(2024, 1, 15),
            )

            assert volume is not None
            assert volume.pool_address == "0xabc123def"

        await provider.close()


class TestGetPoolVolumeRange:
    """Tests for get_pool_volume_range method."""

    @pytest.mark.asyncio
    async def test_get_pool_volume_range_success(self):
        """Test successfully fetching volume data for a date range."""
        provider = SubgraphVolumeProvider(chain="arbitrum")

        mock_response = {
            "poolDayDatas": [
                {
                    "id": "0x123-1",
                    "date": 1705276800,  # 2024-01-15
                    "volumeUSD": "1000000",
                },
                {
                    "id": "0x123-2",
                    "date": 1705363200,  # 2024-01-16
                    "volumeUSD": "1100000",
                },
                {
                    "id": "0x123-3",
                    "date": 1705449600,  # 2024-01-17
                    "volumeUSD": "1200000",
                },
            ]
        }

        with patch.object(provider, "_execute_query", new_callable=AsyncMock) as mock_query:
            mock_query.return_value = mock_response

            volumes = await provider.get_pool_volume_range(
                pool_address="0x123abc",
                start_date=date(2024, 1, 15),
                end_date=date(2024, 1, 17),
            )

            assert len(volumes) == 3
            assert volumes[0].date == date(2024, 1, 15)
            assert volumes[1].date == date(2024, 1, 16)
            assert volumes[2].date == date(2024, 1, 17)

        await provider.close()

    @pytest.mark.asyncio
    async def test_get_pool_volume_range_uses_cache(self):
        """Test that range query uses cache for previously fetched dates."""
        provider = SubgraphVolumeProvider(chain="arbitrum")

        # Pre-populate cache with one date
        import time

        cached_data = PoolVolumeData(
            pool_address="0x123abc",
            date=date(2024, 1, 15),
            volume_usd=Decimal("999999"),
        )
        cache_key = ("0x123abc", date(2024, 1, 15))
        provider._cache[cache_key] = CachedVolume(
            data=cached_data,
            fetched_at=time.time(),
            ttl_seconds=3600,
        )

        mock_response = {
            "poolDayDatas": [
                {
                    "id": "0x123-2",
                    "date": 1705363200,  # 2024-01-16
                    "volumeUSD": "1100000",
                },
            ]
        }

        with patch.object(provider, "_execute_query", new_callable=AsyncMock) as mock_query:
            mock_query.return_value = mock_response

            volumes = await provider.get_pool_volume_range(
                pool_address="0x123abc",
                start_date=date(2024, 1, 15),
                end_date=date(2024, 1, 16),
            )

            # Should have both dates
            assert len(volumes) == 2
            # First one from cache
            assert volumes[0].volume_usd == Decimal("999999")
            # Second one from query
            assert volumes[1].volume_usd == Decimal("1100000")

        await provider.close()


class TestRateLimitHandling:
    """Tests for rate limit handling."""

    @pytest.mark.asyncio
    async def test_rate_limit_error_raised(self):
        """Test that SubgraphRateLimitError is raised on 429."""
        provider = SubgraphVolumeProvider(chain="arbitrum")

        mock_response = MagicMock()
        mock_response.status = 429
        mock_response.headers = {"Retry-After": "60"}

        with patch.object(provider, "_get_session", new_callable=AsyncMock) as mock_session:
            mock_session_instance = MagicMock()
            mock_session.return_value = mock_session_instance

            mock_cm = MagicMock()
            mock_cm.__aenter__ = AsyncMock(return_value=mock_response)
            mock_cm.__aexit__ = AsyncMock(return_value=None)
            mock_session_instance.post.return_value = mock_cm

            with pytest.raises(SubgraphRateLimitError) as exc_info:
                await provider._execute_query("query { test }")

            assert exc_info.value.retry_after_seconds == 60.0

        await provider.close()


class TestCacheManagement:
    """Tests for cache management methods."""

    def test_clear_cache(self):
        """Test clearing the cache."""
        import time

        provider = SubgraphVolumeProvider(chain="arbitrum")

        # Add some cached data
        cached_data = PoolVolumeData(
            pool_address="0x123",
            date=date(2024, 1, 15),
            volume_usd=Decimal("1000000"),
        )
        provider._cache[("0x123", date(2024, 1, 15))] = CachedVolume(
            data=cached_data,
            fetched_at=time.time(),
            ttl_seconds=3600,
        )

        assert len(provider._cache) == 1
        provider.clear_cache()
        assert len(provider._cache) == 0

    def test_get_cache_stats(self):
        """Test getting cache statistics."""
        import time

        provider = SubgraphVolumeProvider(chain="arbitrum")

        # Add fresh and expired entries
        cached_fresh = CachedVolume(
            data=PoolVolumeData(
                pool_address="0x123",
                date=date(2024, 1, 15),
                volume_usd=Decimal("1000000"),
            ),
            fetched_at=time.time(),
            ttl_seconds=3600,
        )
        cached_expired = CachedVolume(
            data=PoolVolumeData(
                pool_address="0x456",
                date=date(2024, 1, 16),
                volume_usd=Decimal("1000000"),
            ),
            fetched_at=time.time() - 7200,  # 2 hours ago
            ttl_seconds=3600,
        )

        provider._cache[("0x123", date(2024, 1, 15))] = cached_fresh
        provider._cache[("0x456", date(2024, 1, 16))] = cached_expired

        stats = provider.get_cache_stats()
        assert stats["total_entries"] == 2
        assert stats["expired_entries"] == 1
        assert stats["valid_entries"] == 1
        assert stats["cache_ttl_seconds"] == 3600


class TestWarmCache:
    """Tests for cache warming functionality."""

    @pytest.mark.asyncio
    async def test_warm_cache_single_pool(self):
        """Test warming cache for a single pool."""
        provider = SubgraphVolumeProvider(chain="arbitrum")

        mock_response = {
            "poolDayDatas": [
                {"id": "1", "date": 1705276800, "volumeUSD": "1000000"},
                {"id": "2", "date": 1705363200, "volumeUSD": "1100000"},
            ]
        }

        with patch.object(provider, "_execute_query", new_callable=AsyncMock) as mock_query:
            mock_query.return_value = mock_response

            results = await provider.warm_cache(
                pool_addresses=["0x123abc"],
                start_date=date(2024, 1, 15),
                end_date=date(2024, 1, 16),
            )

            assert results["0x123abc"] == 2

        await provider.close()

    @pytest.mark.asyncio
    async def test_warm_cache_multiple_pools(self):
        """Test warming cache for multiple pools."""
        provider = SubgraphVolumeProvider(chain="arbitrum")

        mock_response = {
            "poolDayDatas": [
                {"id": "1", "date": 1705276800, "volumeUSD": "1000000"},
            ]
        }

        with patch.object(provider, "_execute_query", new_callable=AsyncMock) as mock_query:
            mock_query.return_value = mock_response

            results = await provider.warm_cache(
                pool_addresses=["0x111", "0x222", "0x333"],
                start_date=date(2024, 1, 15),
                end_date=date(2024, 1, 15),
            )

            assert len(results) == 3
            assert all(v == 1 for v in results.values())

        await provider.close()


class TestQueryExecution:
    """Tests for query execution and error handling."""

    @pytest.mark.asyncio
    async def test_query_error_handling(self):
        """Test handling of query errors."""
        provider = SubgraphVolumeProvider(chain="arbitrum")

        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(
            return_value={
                "errors": [{"message": "Invalid query"}]
            }
        )

        with patch.object(provider, "_get_session", new_callable=AsyncMock) as mock_session:
            mock_session_instance = MagicMock()
            mock_session.return_value = mock_session_instance

            mock_cm = MagicMock()
            mock_cm.__aenter__ = AsyncMock(return_value=mock_response)
            mock_cm.__aexit__ = AsyncMock(return_value=None)
            mock_session_instance.post.return_value = mock_cm

            with pytest.raises(SubgraphQueryError) as exc_info:
                await provider._execute_query("query { invalid }")

            assert "Invalid query" in str(exc_info.value)

        await provider.close()

    @pytest.mark.asyncio
    async def test_query_with_api_key(self):
        """Test that API key is included in headers."""
        provider = SubgraphVolumeProvider(chain="arbitrum", api_key="test-key")

        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={"data": {}})

        with patch.object(provider, "_get_session", new_callable=AsyncMock) as mock_session:
            mock_session_instance = MagicMock()
            mock_session.return_value = mock_session_instance

            mock_cm = MagicMock()
            mock_cm.__aenter__ = AsyncMock(return_value=mock_response)
            mock_cm.__aexit__ = AsyncMock(return_value=None)
            mock_session_instance.post.return_value = mock_cm

            await provider._execute_query("query { test }")

            # Verify API key was included
            call_args = mock_session_instance.post.call_args
            headers = call_args[1]["headers"]
            assert headers["Authorization"] == "Bearer test-key"

        await provider.close()
