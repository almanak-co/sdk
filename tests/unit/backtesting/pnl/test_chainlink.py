"""Unit tests for Chainlink data provider.

This module tests the ChainlinkDataProvider class, covering:
- Price fetching with mocked Chainlink responses
- Staleness detection based on heartbeat intervals
- Caching behavior with TTL
- Feed configuration and token mapping
- Historical data iteration
- Edge cases and error handling
"""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.backtesting.pnl.providers.chainlink import (
    CHAINLINK_HEARTBEATS,
    CHAINLINK_PRICE_FEEDS,
    DECIMALS_SELECTOR,
    LATEST_ROUND_DATA_SELECTOR,
    MAX_MULTICALL_BATCH_SIZE,
    TOKEN_TO_PAIR,
    BinarySearchResult,
    CachedPrice,
    ChainlinkDataProvider,
    ChainlinkPriceFeed,
    ChainlinkRoundData,
    ChainlinkStaleDataError,
    PersistentCacheConfig,
    PriceCache,
)
from almanak.framework.backtesting.pnl.types import DataConfidence
from almanak.framework.data.interfaces import DataSourceUnavailable


class TestChainlinkProviderInitialization:
    """Tests for ChainlinkDataProvider initialization."""

    def test_init_default_chain(self):
        """Test provider initializes with default ethereum chain."""
        provider = ChainlinkDataProvider()
        assert provider._chain == "ethereum"
        assert provider.provider_name == "chainlink_ethereum"

    def test_init_arbitrum_chain(self):
        """Test provider initializes with arbitrum chain."""
        provider = ChainlinkDataProvider(chain="arbitrum")
        assert provider._chain == "arbitrum"
        assert provider.provider_name == "chainlink_arbitrum"

    def test_init_unsupported_chain_raises(self):
        """Test provider raises ValueError for unsupported chain."""
        with pytest.raises(ValueError) as exc_info:
            ChainlinkDataProvider(chain="unsupported_chain")
        assert "Unsupported chain" in str(exc_info.value)
        assert "unsupported_chain" in str(exc_info.value)

    def test_init_with_rpc_url(self):
        """Test provider initializes with RPC URL."""
        provider = ChainlinkDataProvider(rpc_url="https://eth-mainnet.example.com")
        assert provider._rpc_url == "https://eth-mainnet.example.com"

    def test_init_cache_ttl(self):
        """Test provider initializes with custom cache TTL."""
        provider = ChainlinkDataProvider(cache_ttl_seconds=120)
        assert provider.cache_ttl_seconds == 120
        assert provider._cache is not None
        assert provider._cache.ttl_seconds == 120

    def test_init_cache_disabled(self):
        """Test provider initializes with caching disabled."""
        provider = ChainlinkDataProvider(cache_ttl_seconds=0)
        assert provider._cache is None

    def test_init_priority(self):
        """Test provider initializes with custom priority."""
        provider = ChainlinkDataProvider(priority=5)
        assert provider.priority == 5

    def test_init_default_priority(self):
        """Test provider uses default priority."""
        provider = ChainlinkDataProvider()
        assert provider.priority == ChainlinkDataProvider.DEFAULT_PRIORITY


class TestBatchRoundQueries:
    @pytest.mark.asyncio
    async def test_batch_round_queries_preserve_bounded_concurrency(self):
        provider = ChainlinkDataProvider(rpc_url="https://eth-mainnet.example.com")
        in_flight = 0
        max_in_flight = 0
        calls: list[int] = []

        async def fake_query_round_data(feed_address: str, round_id: int) -> ChainlinkRoundData:
            nonlocal in_flight, max_in_flight
            assert feed_address == "0xfeed"
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
            await asyncio.sleep(0)
            calls.append(round_id)
            in_flight -= 1
            return ChainlinkRoundData(
                round_id=round_id,
                answer=round_id * 100,
                started_at=0,
                updated_at=0,
                answered_in_round=round_id,
            )

        provider._query_round_data = fake_query_round_data  # type: ignore[method-assign]
        round_ids = list(range(1, MAX_MULTICALL_BATCH_SIZE + 3))

        results = await provider._batch_query_rounds("0xfeed", round_ids)

        assert sorted(calls) == round_ids
        assert [result.round_id if result else None for result in results] == round_ids
        assert max_in_flight == MAX_MULTICALL_BATCH_SIZE

    def test_prefetch_round_ids_refuses_cross_phase_ranges(self):
        start_round = (1 << 64) + 10
        end_round = (2 << 64) + 5
        start = BinarySearchResult(
            round_id=start_round,
            round_data=ChainlinkRoundData(
                round_id=start_round,
                answer=250000000000,
                started_at=0,
                updated_at=0,
                answered_in_round=start_round,
            ),
            iterations=1,
        )
        end = BinarySearchResult(
            round_id=end_round,
            round_data=ChainlinkRoundData(
                round_id=end_round,
                answer=250100000000,
                started_at=0,
                updated_at=0,
                answered_in_round=end_round,
            ),
            iterations=1,
        )

        assert ChainlinkDataProvider._prefetch_round_ids(start, end) == []


class TestFeedConfiguration:
    """Tests for feed address and configuration retrieval."""

    def test_get_feed_address_eth(self):
        """Test getting feed address for ETH on ethereum."""
        provider = ChainlinkDataProvider(chain="ethereum")
        address = provider.get_feed_address("ETH")
        assert address == "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419"

    def test_get_feed_address_weth(self):
        """Test WETH maps to ETH/USD feed."""
        provider = ChainlinkDataProvider(chain="ethereum")
        address = provider.get_feed_address("WETH")
        assert address == provider.get_feed_address("ETH")

    def test_get_feed_address_btc(self):
        """Test getting feed address for BTC."""
        provider = ChainlinkDataProvider(chain="ethereum")
        address = provider.get_feed_address("BTC")
        assert address == "0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c"

    def test_get_feed_address_unknown_token(self):
        """Test getting feed address for unknown token returns None."""
        provider = ChainlinkDataProvider(chain="ethereum")
        address = provider.get_feed_address("UNKNOWN_TOKEN_XYZ")
        assert address is None

    def test_get_feed_address_arbitrum(self):
        """Test getting feed address on arbitrum chain."""
        provider = ChainlinkDataProvider(chain="arbitrum")
        address = provider.get_feed_address("ETH")
        assert address == "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612"

    def test_get_feed_config_eth(self):
        """Test getting full feed configuration for ETH."""
        provider = ChainlinkDataProvider(chain="ethereum")
        config = provider.get_feed_config("ETH")

        assert config is not None
        assert config.pair == "ETH/USD"
        assert config.decimals == 8
        assert config.heartbeat_seconds == 3600  # 1 hour
        assert config.deviation_threshold == Decimal("0.5")

    def test_get_feed_config_usdc(self):
        """Test getting feed configuration for stablecoin USDC."""
        provider = ChainlinkDataProvider(chain="ethereum")
        config = provider.get_feed_config("USDC")

        assert config is not None
        assert config.pair == "USDC/USD"
        assert config.heartbeat_seconds == 86400  # 24 hours for stablecoins
        assert config.deviation_threshold == Decimal("0.25")

    def test_get_feed_config_unknown_token(self):
        """Test getting feed configuration for unknown token returns None."""
        provider = ChainlinkDataProvider(chain="ethereum")
        config = provider.get_feed_config("UNKNOWN_TOKEN")
        assert config is None


class TestSupportedTokensAndChains:
    """Tests for supported tokens and chains lists."""

    def test_supported_tokens_ethereum(self):
        """Test supported tokens on ethereum chain."""
        provider = ChainlinkDataProvider(chain="ethereum")
        tokens = provider.supported_tokens

        assert "ETH" in tokens
        assert "WETH" in tokens
        assert "BTC" in tokens
        assert "USDC" in tokens
        assert "LINK" in tokens

    def test_supported_tokens_arbitrum(self):
        """Test supported tokens on arbitrum chain."""
        provider = ChainlinkDataProvider(chain="arbitrum")
        tokens = provider.supported_tokens

        assert "ETH" in tokens
        assert "GMX" in tokens  # GMX is on Arbitrum
        assert "ARB" in tokens

    def test_supported_chains(self):
        """Test list of supported chains."""
        provider = ChainlinkDataProvider()
        chains = provider.supported_chains

        assert "ethereum" in chains
        assert "arbitrum" in chains
        assert "base" in chains
        assert "optimism" in chains
        assert "polygon" in chains
        assert "avalanche" in chains


class TestTokenToPairMapping:
    """Tests for token to pair mapping."""

    def test_eth_maps_to_eth_usd(self):
        """Test ETH maps to ETH/USD pair."""
        assert TOKEN_TO_PAIR["ETH"] == "ETH/USD"

    def test_weth_maps_to_eth_usd(self):
        """Test WETH maps to ETH/USD pair."""
        assert TOKEN_TO_PAIR["WETH"] == "ETH/USD"

    def test_steth_maps_to_wsteth_usd(self):
        """Test stETH maps to WSTETH/USD pair."""
        assert TOKEN_TO_PAIR["STETH"] == "WSTETH/USD"

    def test_wbtc_maps_to_btc_usd(self):
        """Test WBTC maps to BTC/USD pair."""
        assert TOKEN_TO_PAIR["WBTC"] == "BTC/USD"


class TestPriceConversion:
    """Tests for raw price conversion to Decimal."""

    def test_convert_price_8_decimals(self):
        """Test converting price with 8 decimals (standard Chainlink)."""
        provider = ChainlinkDataProvider()

        # $2,500.00 with 8 decimals = 250000000000
        raw_answer = 250000000000
        price = provider._convert_price(raw_answer, 8)

        assert price == Decimal("2500")

    def test_convert_price_different_decimals(self):
        """Test converting price with different decimal counts."""
        provider = ChainlinkDataProvider()

        # Test with 6 decimals
        raw_answer = 2500000000  # $2,500 with 6 decimals
        price = provider._convert_price(raw_answer, 6)
        assert price == Decimal("2500")

    def test_convert_price_fractional(self):
        """Test converting price with fractional value."""
        provider = ChainlinkDataProvider()

        # $2,543.12345678 with 8 decimals
        raw_answer = 254312345678
        price = provider._convert_price(raw_answer, 8)

        assert price == Decimal("2543.12345678")


class TestStalenessDetection:
    """Tests for staleness detection based on heartbeat intervals."""

    def test_is_data_stale_fresh_data(self):
        """Test fresh data is not marked as stale."""
        provider = ChainlinkDataProvider()

        # Data updated 30 minutes ago (heartbeat is 1 hour for ETH)
        updated_at = datetime.now(UTC) - timedelta(minutes=30)
        is_stale = provider.is_data_stale(updated_at, "ETH")

        assert is_stale is False

    def test_is_data_stale_old_data(self):
        """Test old data is marked as stale."""
        provider = ChainlinkDataProvider()

        # Data updated 2 hours ago (heartbeat is 1 hour for ETH)
        updated_at = datetime.now(UTC) - timedelta(hours=2)
        is_stale = provider.is_data_stale(updated_at, "ETH")

        assert is_stale is True

    def test_is_data_stale_within_10_percent_buffer(self):
        """Test data within 10% buffer is not stale."""
        provider = ChainlinkDataProvider()

        # ETH heartbeat is 3600s, 10% buffer = 3960s
        # Data updated 3700s ago should NOT be stale (within buffer)
        updated_at = datetime.now(UTC) - timedelta(seconds=3700)
        is_stale = provider.is_data_stale(updated_at, "ETH")

        assert is_stale is False

    def test_is_data_stale_beyond_10_percent_buffer(self):
        """Test data beyond 10% buffer is stale."""
        provider = ChainlinkDataProvider()

        # ETH heartbeat is 3600s, 10% buffer = 3960s
        # Data updated 4000s ago should be stale (beyond buffer)
        updated_at = datetime.now(UTC) - timedelta(seconds=4000)
        is_stale = provider.is_data_stale(updated_at, "ETH")

        assert is_stale is True

    def test_is_data_stale_stablecoin_longer_heartbeat(self):
        """Test stablecoin staleness with longer heartbeat (24 hours)."""
        provider = ChainlinkDataProvider()

        # USDC heartbeat is 24 hours
        # Data updated 12 hours ago should not be stale
        updated_at = datetime.now(UTC) - timedelta(hours=12)
        is_stale = provider.is_data_stale(updated_at, "USDC")

        assert is_stale is False

        # Data updated 27 hours ago should be stale (beyond 26.4h buffer)
        updated_at_old = datetime.now(UTC) - timedelta(hours=27)
        is_stale_old = provider.is_data_stale(updated_at_old, "USDC")

        assert is_stale_old is True

    def test_is_data_stale_custom_current_time(self):
        """Test staleness check with custom current time."""
        provider = ChainlinkDataProvider()

        updated_at = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)
        current_time = datetime(2024, 6, 15, 13, 30, 0, tzinfo=UTC)  # 1.5 hours later

        is_stale = provider.is_data_stale(updated_at, "ETH", current_time=current_time)

        # 1.5 hours > 1.1 hours (heartbeat + 10%), so should be stale
        assert is_stale is True

    def test_check_staleness_raises_exception(self):
        """Test _check_staleness raises ChainlinkStaleDataError when stale."""
        provider = ChainlinkDataProvider()

        round_data = ChainlinkRoundData(
            round_id=1,
            answer=250000000000,
            started_at=int((datetime.now(UTC) - timedelta(hours=2)).timestamp()),
            updated_at=int((datetime.now(UTC) - timedelta(hours=2)).timestamp()),
            answered_in_round=1,
        )

        with pytest.raises(ChainlinkStaleDataError) as exc_info:
            provider._check_staleness(round_data, "ETH", raise_on_stale=True)

        assert exc_info.value.token == "ETH"
        assert exc_info.value.heartbeat_seconds == 3600

    def test_check_staleness_no_raise(self):
        """Test _check_staleness returns stale flag without raising."""
        provider = ChainlinkDataProvider()

        round_data = ChainlinkRoundData(
            round_id=1,
            answer=250000000000,
            started_at=int((datetime.now(UTC) - timedelta(hours=2)).timestamp()),
            updated_at=int((datetime.now(UTC) - timedelta(hours=2)).timestamp()),
            answered_in_round=1,
        )

        is_stale, age_seconds = provider._check_staleness(round_data, "ETH", raise_on_stale=False)

        assert is_stale is True
        assert age_seconds > 7000  # ~2 hours in seconds


class TestChainlinkStaleDataError:
    """Tests for ChainlinkStaleDataError exception class."""

    def test_error_message(self):
        """Test error message format."""
        error = ChainlinkStaleDataError(
            token="ETH",
            age_seconds=7200.5,
            heartbeat_seconds=3600,
            updated_at=datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC),
        )

        assert "ETH" in str(error)
        assert "7200" in str(error)  # Age rounded
        assert "3600" in str(error)

    def test_error_attributes(self):
        """Test error attributes are accessible."""
        updated = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)
        error = ChainlinkStaleDataError(
            token="BTC",
            age_seconds=5000,
            heartbeat_seconds=3600,
            updated_at=updated,
        )

        assert error.token == "BTC"
        assert error.age_seconds == 5000
        assert error.heartbeat_seconds == 3600
        assert error.updated_at == updated


class TestCachedPrice:
    """Tests for CachedPrice dataclass."""

    def test_cached_price_creation(self):
        """Test creating a CachedPrice entry."""
        price = CachedPrice(
            price=Decimal("2500"),
            timestamp=datetime.now(UTC),
            ttl_seconds=60,
        )

        assert price.price == Decimal("2500")
        assert price.is_expired is False

    def test_cached_price_expired(self):
        """Test CachedPrice expiration."""
        price = CachedPrice(
            price=Decimal("2500"),
            timestamp=datetime.now(UTC),
            fetched_at=datetime.now(UTC) - timedelta(seconds=120),
            ttl_seconds=60,
        )

        assert price.is_expired is True
        assert price.age_seconds > 100

    def test_cached_price_not_expired(self):
        """Test CachedPrice not expired."""
        price = CachedPrice(
            price=Decimal("2500"),
            timestamp=datetime.now(UTC),
            fetched_at=datetime.now(UTC) - timedelta(seconds=30),
            ttl_seconds=60,
        )

        assert price.is_expired is False
        assert price.age_seconds < 60


class TestPriceCache:
    """Tests for PriceCache class."""

    def test_price_cache_creation(self):
        """Test creating an empty PriceCache."""
        cache = PriceCache()

        assert cache.data == {}
        assert cache.ttl_seconds == 60

    def test_get_price_at_empty_cache(self):
        """Test getting price from empty cache returns None."""
        cache = PriceCache()
        price = cache.get_price_at("ETH", datetime.now(UTC))

        assert price is None

    def test_get_price_at_with_data(self):
        """Test getting price at specific timestamp."""
        cache = PriceCache()
        cache.data["ETH"] = [
            (datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC), Decimal("2500")),
            (datetime(2024, 6, 15, 13, 0, 0, tzinfo=UTC), Decimal("2550")),
            (datetime(2024, 6, 15, 14, 0, 0, tzinfo=UTC), Decimal("2600")),
        ]

        # Get price at 12:30 - should return 2500 (price at or before)
        price = cache.get_price_at("ETH", datetime(2024, 6, 15, 12, 30, 0, tzinfo=UTC))
        assert price == Decimal("2500")

        # Get price at 13:00 exactly
        price = cache.get_price_at("ETH", datetime(2024, 6, 15, 13, 0, 0, tzinfo=UTC))
        assert price == Decimal("2550")

    def test_get_price_at_before_any_data(self):
        """Test getting price before any cached data returns None."""
        cache = PriceCache()
        cache.data["ETH"] = [
            (datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC), Decimal("2500")),
        ]

        price = cache.get_price_at("ETH", datetime(2024, 6, 15, 11, 0, 0, tzinfo=UTC))
        assert price is None

    def test_get_price_at_case_insensitive(self):
        """Test token lookup is case insensitive."""
        cache = PriceCache()
        cache.data["ETH"] = [
            (datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC), Decimal("2500")),
        ]

        price = cache.get_price_at("eth", datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC))
        assert price == Decimal("2500")

    def test_set_live_price(self):
        """Test setting a live price in cache."""
        cache = PriceCache()
        cache.set_live_price("ETH", Decimal("2500"))

        cached = cache.get_live_price("ETH")
        assert cached is not None
        assert cached.price == Decimal("2500")

    def test_get_live_price_expired(self):
        """Test getting expired live price returns None."""
        cache = PriceCache(ttl_seconds=60)
        cache._live_cache["ETH"] = CachedPrice(
            price=Decimal("2500"),
            timestamp=datetime.now(UTC),
            fetched_at=datetime.now(UTC) - timedelta(seconds=120),
            ttl_seconds=60,
        )

        cached = cache.get_live_price("ETH")
        assert cached is None

    def test_clear_live_cache_specific_token(self):
        """Test clearing live cache for specific token."""
        cache = PriceCache()
        cache.set_live_price("ETH", Decimal("2500"))
        cache.set_live_price("BTC", Decimal("60000"))

        cache.clear_live_cache("ETH")

        assert cache.get_live_price("ETH") is None
        assert cache.get_live_price("BTC") is not None

    def test_clear_live_cache_all(self):
        """Test clearing all live cache entries."""
        cache = PriceCache()
        cache.set_live_price("ETH", Decimal("2500"))
        cache.set_live_price("BTC", Decimal("60000"))

        cache.clear_live_cache()

        assert cache.get_live_price("ETH") is None
        assert cache.get_live_price("BTC") is None

    def test_get_cache_stats(self):
        """Test getting cache statistics."""
        cache = PriceCache(ttl_seconds=60)
        cache.set_live_price("ETH", Decimal("2500"))
        cache.data["BTC"] = [
            (datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC), Decimal("60000")),
            (datetime(2024, 6, 15, 13, 0, 0, tzinfo=UTC), Decimal("60500")),
        ]

        stats = cache.get_cache_stats()

        assert "ETH" in stats["live_tokens"]
        assert stats["live_count"] == 1
        assert "BTC" in stats["historical_tokens"]
        assert stats["historical_count"] == 1
        assert stats["total_historical_points"] == 2
        assert stats["ttl_seconds"] == 60


class TestCachingBehavior:
    """Tests for provider caching behavior."""

    def test_set_historical_prices(self):
        """Test setting historical prices in cache."""
        provider = ChainlinkDataProvider()

        prices = [
            (datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC), Decimal("2500")),
            (datetime(2024, 6, 15, 13, 0, 0, tzinfo=UTC), Decimal("2550")),
        ]
        provider.set_historical_prices("ETH", prices)

        assert provider._cache is not None
        assert "ETH" in provider._cache.data
        assert len(provider._cache.data["ETH"]) == 2

    def test_set_historical_prices_sorts_data(self):
        """Test historical prices are sorted by timestamp."""
        provider = ChainlinkDataProvider()

        # Provide unsorted data
        prices = [
            (datetime(2024, 6, 15, 14, 0, 0, tzinfo=UTC), Decimal("2600")),
            (datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC), Decimal("2500")),
            (datetime(2024, 6, 15, 13, 0, 0, tzinfo=UTC), Decimal("2550")),
        ]
        provider.set_historical_prices("ETH", prices)

        # Verify sorted
        cached = provider._cache.data["ETH"]
        assert cached[0][1] == Decimal("2500")
        assert cached[1][1] == Decimal("2550")
        assert cached[2][1] == Decimal("2600")

    def test_clear_cache_specific_token(self):
        """Test clearing cache for specific token."""
        provider = ChainlinkDataProvider()

        provider.set_historical_prices(
            "ETH",
            [(datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC), Decimal("2500"))],
        )
        provider.set_historical_prices(
            "BTC",
            [(datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC), Decimal("60000"))],
        )

        provider.clear_cache("ETH")

        assert "ETH" not in provider._cache.data
        assert "BTC" in provider._cache.data

    def test_clear_cache_all(self):
        """Test clearing all cache."""
        provider = ChainlinkDataProvider()

        provider.set_historical_prices(
            "ETH",
            [(datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC), Decimal("2500"))],
        )
        provider.set_historical_prices(
            "BTC",
            [(datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC), Decimal("60000"))],
        )

        provider.clear_cache()

        assert len(provider._cache.data) == 0

    def test_get_cache_stats_provider(self):
        """Test getting cache stats from provider."""
        provider = ChainlinkDataProvider()

        provider.set_historical_prices(
            "ETH",
            [(datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC), Decimal("2500"))],
        )

        stats = provider.get_cache_stats()

        assert stats["caching_enabled"] is True
        assert "ETH" in stats["historical_tokens"]

    def test_get_cache_stats_disabled(self):
        """Test cache stats when caching is disabled."""
        provider = ChainlinkDataProvider(cache_ttl_seconds=0)

        stats = provider.get_cache_stats()

        assert stats["caching_enabled"] is False
        assert stats["live_count"] == 0

    def test_set_cache_ttl(self):
        """Test updating cache TTL."""
        provider = ChainlinkDataProvider(cache_ttl_seconds=60)

        provider.set_cache_ttl(120)

        assert provider.cache_ttl_seconds == 120
        assert provider._cache.ttl_seconds == 120


class TestPersistentCache:
    """Tests for persistent Chainlink round cache load/save behavior."""

    @staticmethod
    def _write_cache(tmp_path, payload: dict) -> None:
        cache_path = tmp_path / "chainlink_rounds_ethereum.json"
        cache_path.write_text(json.dumps(payload))

    def test_load_persistent_cache_valid_file(self, tmp_path):
        payload = {
            "cached_at": datetime.now(UTC).isoformat(),
            "chain": "ethereum",
            "rounds": {
                "0xfeed": [
                    {"round_id": 1, "updated_at": 1718450000, "price": "2500.00"},
                    {"round_id": 2, "updated_at": 1718453600, "price": "2510.00"},
                ]
            },
            "decimals": {"0xfeed": 8},
            "round_bounds": {"0xfeed": {"min": 1, "max": 2}},
        }
        self._write_cache(tmp_path, payload)

        provider = ChainlinkDataProvider(
            chain="ethereum",
            persistent_cache_config=PersistentCacheConfig(enabled=True, cache_directory=str(tmp_path)),
        )

        assert provider._round_cache == {
            "0xfeed": [
                (1, 1718450000, Decimal("2500.00")),
                (2, 1718453600, Decimal("2510.00")),
            ]
        }
        assert provider._decimals_cache == {"0xfeed": 8}
        assert provider._round_bounds_cache == {"0xfeed": (1, 2)}

    def test_malformed_persistent_cache_does_not_partially_hydrate_state(self, tmp_path):
        payload = {
            "cached_at": datetime.now(UTC).isoformat(),
            "chain": "ethereum",
            "rounds": {
                "0xgood": [{"round_id": 1, "updated_at": 1718450000, "price": "2500.00"}],
                "0xbad": [{"round_id": 2, "updated_at": 1718453600}],
            },
            "decimals": {"0xgood": 8},
            "round_bounds": {"0xgood": {"min": 1, "max": 1}},
        }
        self._write_cache(tmp_path, payload)

        provider = ChainlinkDataProvider(
            chain="ethereum",
            persistent_cache_config=PersistentCacheConfig(enabled=True, cache_directory=str(tmp_path)),
        )

        assert provider._round_cache == {}
        assert provider._decimals_cache == {}
        assert provider._round_bounds_cache == {}

    def test_expired_persistent_cache_is_ignored(self, tmp_path):
        payload = {
            "cached_at": datetime(2024, 1, 1, tzinfo=UTC).isoformat(),
            "rounds": {"0xfeed": [{"round_id": 1, "updated_at": 1718450000, "price": "2500.00"}]},
            "decimals": {"0xfeed": 8},
            "round_bounds": {"0xfeed": {"min": 1, "max": 1}},
        }
        self._write_cache(tmp_path, payload)

        provider = ChainlinkDataProvider(
            chain="ethereum",
            persistent_cache_config=PersistentCacheConfig(
                enabled=True,
                cache_directory=str(tmp_path),
                max_age_days=1,
            ),
        )

        assert provider._round_cache == {}
        assert provider._decimals_cache == {}
        assert provider._round_bounds_cache == {}

    def test_save_persistent_cache_serializes_rounds_and_bounds(self, tmp_path):
        provider = ChainlinkDataProvider(
            chain="ethereum",
            persistent_cache_config=PersistentCacheConfig(enabled=True, cache_directory=str(tmp_path)),
        )
        provider._round_cache = {"0xfeed": [(1, 1718450000, Decimal("2500.00"))]}
        provider._decimals_cache = {"0xfeed": 8}
        provider._round_bounds_cache = {"0xfeed": (1, 10)}

        provider._save_persistent_cache()

        cache_path = tmp_path / "chainlink_rounds_ethereum.json"
        saved = json.loads(cache_path.read_text())
        assert saved["chain"] == "ethereum"
        assert saved["rounds"]["0xfeed"] == [
            {"round_id": 1, "updated_at": 1718450000, "price": "2500.00"}
        ]
        assert saved["decimals"] == {"0xfeed": 8}
        assert saved["round_bounds"] == {"0xfeed": {"min": 1, "max": 10}}


class TestHistoricalPriceCacheLookup:
    """Tests for Chainlink historical cache lookup helpers."""

    @pytest.mark.asyncio
    async def test_get_historical_price_uses_latest_cached_timestamp_before_target(self):
        provider = ChainlinkDataProvider()
        first_ts = int(datetime(2024, 6, 15, 12, 0, tzinfo=UTC).timestamp())
        second_ts = int(datetime(2024, 6, 15, 13, 0, tzinfo=UTC).timestamp())
        provider._historical_cache["ETH"] = {
            first_ts: Decimal("2500"),
            second_ts: Decimal("2550"),
        }

        price = await provider._get_historical_price("ETH", datetime(2024, 6, 15, 13, 30, tzinfo=UTC))

        assert price == Decimal("2550")

    @pytest.mark.asyncio
    async def test_get_historical_price_falls_back_to_main_cache(self):
        provider = ChainlinkDataProvider()
        provider.set_historical_prices(
            "ETH",
            [(datetime(2024, 6, 15, 12, 0, tzinfo=UTC), Decimal("2500"))],
        )

        price = await provider._get_historical_price("ETH", datetime(2024, 6, 15, 12, 30, tzinfo=UTC))

        assert price == Decimal("2500")

    @pytest.mark.asyncio
    async def test_get_historical_price_uses_persistent_round_cache(self):
        provider = ChainlinkDataProvider()
        feed = provider.get_feed_config("ETH")
        assert feed is not None
        provider._round_cache[feed.address] = [
            (1, int(datetime(2024, 6, 15, 12, 0, tzinfo=UTC).timestamp()), Decimal("2500")),
            (2, int(datetime(2024, 6, 15, 13, 0, tzinfo=UTC).timestamp()), Decimal("2550")),
        ]

        price = await provider._get_historical_price("ETH", datetime(2024, 6, 15, 13, 30, tzinfo=UTC))

        assert price == Decimal("2550")


class TestPrefetchRoundsForRange:
    """Tests for Chainlink range prefetch cache behavior."""

    @pytest.mark.asyncio
    async def test_prefetch_rounds_deduplicates_existing_round_ids(self):
        provider = ChainlinkDataProvider(chain="ethereum", rpc_url="https://example.com")
        feed = provider.get_feed_config("ETH")
        assert feed is not None
        first_round = ChainlinkRoundData(
            round_id=1,
            answer=250000000000,
            started_at=1718450000,
            updated_at=1718450000,
            answered_in_round=1,
        )
        second_round = ChainlinkRoundData(
            round_id=2,
            answer=255000000000,
            started_at=1718453600,
            updated_at=1718453600,
            answered_in_round=2,
        )

        async def fake_binary_search(feed_address: str, target_ts: int) -> BinarySearchResult:
            assert feed_address == feed.address
            if target_ts == first_round.updated_at:
                return BinarySearchResult(1, first_round, iterations=1, exact_match=True)
            return BinarySearchResult(2, second_round, iterations=1, exact_match=True)

        provider._get_decimals_cached = AsyncMock(return_value=8)  # type: ignore[method-assign]
        provider._binary_search_round_for_timestamp = fake_binary_search  # type: ignore[method-assign]
        provider._batch_query_rounds = AsyncMock(return_value=[first_round, second_round])  # type: ignore[method-assign]
        provider._save_persistent_cache = MagicMock()  # type: ignore[method-assign]

        start = datetime.fromtimestamp(first_round.updated_at, tz=UTC)
        end = datetime.fromtimestamp(second_round.updated_at, tz=UTC)

        first_count = await provider.prefetch_rounds_for_range("ETH", start, end)
        second_count = await provider.prefetch_rounds_for_range("ETH", start, end)

        assert first_count == 2
        assert second_count == 2
        assert provider._round_cache[feed.address] == [
            (1, first_round.updated_at, Decimal("2500")),
            (2, second_round.updated_at, Decimal("2550")),
        ]
        assert provider._historical_cache["ETH"] == {
            first_round.updated_at: Decimal("2500"),
            second_round.updated_at: Decimal("2550"),
        }


class TestBinarySearchRoundForTimestamp:
    """Tests for Chainlink timestamp-to-round binary search."""

    @staticmethod
    def _round(round_id: int, updated_at: int, answer: int | None = None) -> ChainlinkRoundData:
        return ChainlinkRoundData(
            round_id=round_id,
            answer=answer if answer is not None else round_id * 100000000,
            started_at=updated_at,
            updated_at=updated_at,
            answered_in_round=round_id,
        )

    @pytest.mark.asyncio
    async def test_binary_search_returns_exact_match(self):
        provider = ChainlinkDataProvider(rpc_url="https://example.com")
        rounds = {
            1: self._round(1, 100),
            2: self._round(2, 200),
            3: self._round(3, 300),
        }
        provider._round_bounds_cache["0xfeed"] = (1, 3)
        provider._query_round_data = AsyncMock(side_effect=lambda _feed, round_id: rounds.get(round_id))  # type: ignore[method-assign]

        result = await provider._binary_search_round_for_timestamp("0xfeed", 200)

        assert result is not None
        assert result.round_id == 2
        assert result.exact_match is True

    @pytest.mark.asyncio
    async def test_binary_search_returns_latest_when_target_after_latest(self):
        provider = ChainlinkDataProvider(rpc_url="https://example.com")
        latest = self._round(3, 300)
        provider._round_bounds_cache["0xfeed"] = (1, 3)
        provider._query_round_data = AsyncMock(return_value=latest)  # type: ignore[method-assign]

        result = await provider._binary_search_round_for_timestamp("0xfeed", 400)

        assert result is not None
        assert result.round_id == 3
        assert result.round_data is latest
        assert result.exact_match is False

    @pytest.mark.asyncio
    async def test_binary_search_skips_round_gap_and_returns_best_before_target(self):
        provider = ChainlinkDataProvider(rpc_url="https://example.com")
        rounds = {
            1: self._round(1, 100),
            2: self._round(2, 200),
            4: self._round(4, 400),
            5: self._round(5, 500),
        }
        provider._round_bounds_cache["0xfeed"] = (1, 5)
        provider._query_round_data = AsyncMock(side_effect=lambda _feed, round_id: rounds.get(round_id))  # type: ignore[method-assign]

        result = await provider._binary_search_round_for_timestamp("0xfeed", 450)

        assert result is not None
        assert result.round_id == 4
        assert result.round_data.updated_at == 400
        assert result.exact_match is False

    @pytest.mark.asyncio
    async def test_binary_search_returns_none_before_earliest_round(self):
        provider = ChainlinkDataProvider(rpc_url="https://example.com")
        rounds = {
            1: self._round(1, 100),
            2: self._round(2, 200),
            3: self._round(3, 300),
        }
        provider._round_bounds_cache["0xfeed"] = (1, 3)
        provider._query_round_data = AsyncMock(side_effect=lambda _feed, round_id: rounds.get(round_id))  # type: ignore[method-assign]

        result = await provider._binary_search_round_for_timestamp("0xfeed", 50)

        assert result is None


class TestGetPriceAtTimestamp:
    """Tests for Chainlink timestamp price result construction."""

    @staticmethod
    def _round(round_id: int, updated_at: int, answer: int = 250000000000) -> ChainlinkRoundData:
        return ChainlinkRoundData(
            round_id=round_id,
            answer=answer,
            started_at=updated_at,
            updated_at=updated_at,
            answered_in_round=round_id,
        )

    @pytest.mark.asyncio
    async def test_get_price_at_timestamp_unknown_token_returns_none(self):
        provider = ChainlinkDataProvider(rpc_url="https://example.com")

        result = await provider.get_price_at_timestamp("UNKNOWN", datetime(2024, 6, 15, tzinfo=UTC))

        assert result is None

    @pytest.mark.asyncio
    async def test_get_price_at_timestamp_without_round_returns_none(self):
        provider = ChainlinkDataProvider(rpc_url="https://example.com")
        provider._get_decimals_cached = AsyncMock(return_value=8)  # type: ignore[method-assign]
        provider._binary_search_round_for_timestamp = AsyncMock(return_value=None)  # type: ignore[method-assign]

        result = await provider.get_price_at_timestamp("ETH", datetime(2024, 6, 15, tzinfo=UTC))

        assert result is None

    @pytest.mark.asyncio
    async def test_get_price_at_timestamp_exact_match_is_high_confidence(self):
        provider = ChainlinkDataProvider(rpc_url="https://example.com")
        target = datetime(2024, 6, 15, 12, 0, tzinfo=UTC)
        round_data = self._round(10, int(target.timestamp()))
        provider._get_decimals_cached = AsyncMock(return_value=8)  # type: ignore[method-assign]
        provider._binary_search_round_for_timestamp = AsyncMock(  # type: ignore[method-assign]
            return_value=BinarySearchResult(
                round_id=10,
                round_data=round_data,
                iterations=1,
                exact_match=True,
            )
        )

        result = await provider.get_price_at_timestamp("ETH", target)

        assert result is not None
        assert result.price == Decimal("2500")
        assert result.round_id == 10
        assert result.confidence == DataConfidence.HIGH
        assert result.source_info.confidence == DataConfidence.HIGH
        assert result.is_stale is False

    @pytest.mark.asyncio
    async def test_get_price_at_timestamp_nearby_round_is_medium_confidence(self):
        provider = ChainlinkDataProvider(rpc_url="https://example.com")
        target = datetime(2024, 6, 15, 12, 10, tzinfo=UTC)
        round_data = self._round(10, int(datetime(2024, 6, 15, 12, 0, tzinfo=UTC).timestamp()))
        provider._get_decimals_cached = AsyncMock(return_value=8)  # type: ignore[method-assign]
        provider._binary_search_round_for_timestamp = AsyncMock(  # type: ignore[method-assign]
            return_value=BinarySearchResult(
                round_id=10,
                round_data=round_data,
                iterations=1,
                exact_match=False,
            )
        )

        result = await provider.get_price_at_timestamp("ETH", target)

        assert result is not None
        assert result.confidence == DataConfidence.MEDIUM
        assert result.is_stale is False

    @pytest.mark.asyncio
    async def test_get_price_at_timestamp_old_round_is_low_confidence_and_stale(self):
        provider = ChainlinkDataProvider(rpc_url="https://example.com")
        target = datetime(2024, 6, 15, 14, 0, tzinfo=UTC)
        round_data = self._round(10, int(datetime(2024, 6, 15, 12, 0, tzinfo=UTC).timestamp()))
        provider._get_decimals_cached = AsyncMock(return_value=8)  # type: ignore[method-assign]
        provider._binary_search_round_for_timestamp = AsyncMock(  # type: ignore[method-assign]
            return_value=BinarySearchResult(
                round_id=10,
                round_data=round_data,
                iterations=1,
                exact_match=False,
            )
        )

        result = await provider.get_price_at_timestamp("ETH", target)

        assert result is not None
        assert result.confidence == DataConfidence.LOW
        assert result.is_stale is True


class TestFetchHistoricalRounds:
    """Tests for Chainlink historical round traversal."""

    @staticmethod
    def _round(round_id: int, updated_at: int, answer: int | None = None) -> ChainlinkRoundData:
        return ChainlinkRoundData(
            round_id=round_id,
            answer=answer if answer is not None else round_id * 100000000,
            started_at=updated_at,
            updated_at=updated_at,
            answered_in_round=round_id,
        )

    @pytest.mark.asyncio
    async def test_fetch_historical_rounds_unknown_token_returns_empty(self):
        provider = ChainlinkDataProvider(rpc_url="https://example.com")

        prices = await provider._fetch_historical_rounds(
            "UNKNOWN",
            datetime(2024, 6, 15, 12, 0, tzinfo=UTC),
            datetime(2024, 6, 15, 13, 0, tzinfo=UTC),
        )

        assert prices == []

    @pytest.mark.asyncio
    async def test_fetch_historical_rounds_without_latest_round_returns_empty(self):
        provider = ChainlinkDataProvider(rpc_url="https://example.com")
        provider._get_decimals_cached = AsyncMock(return_value=8)  # type: ignore[method-assign]
        provider._query_latest_round_data = AsyncMock(return_value=None)  # type: ignore[method-assign]
        provider._query_round_data = AsyncMock()  # type: ignore[method-assign]

        prices = await provider._fetch_historical_rounds(
            "ETH",
            datetime(2024, 6, 15, 12, 0, tzinfo=UTC),
            datetime(2024, 6, 15, 13, 0, tzinfo=UTC),
        )

        assert prices == []
        provider._query_round_data.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_fetch_historical_rounds_collects_range_sorted_and_caches(self):
        provider = ChainlinkDataProvider(rpc_url="https://example.com")
        start = datetime(2024, 6, 15, 12, 0, tzinfo=UTC)
        end = datetime(2024, 6, 15, 14, 0, tzinfo=UTC)
        start_ts = int(start.timestamp())
        rounds = {
            5: self._round(5, int(end.timestamp()), 260000000000),
            4: self._round(4, int(datetime(2024, 6, 15, 13, 0, tzinfo=UTC).timestamp()), 255000000000),
            3: self._round(3, start_ts, 250000000000),
            2: self._round(2, start_ts - 3600, 240000000000),
        }
        provider._get_decimals_cached = AsyncMock(return_value=8)  # type: ignore[method-assign]
        provider._query_latest_round_data = AsyncMock(return_value=rounds[5])  # type: ignore[method-assign]
        provider._query_round_data = AsyncMock(side_effect=lambda _feed, round_id: rounds.get(round_id))  # type: ignore[method-assign]

        prices = await provider._fetch_historical_rounds("ETH", start, end)

        assert prices == [
            (start, Decimal("2500")),
            (datetime(2024, 6, 15, 13, 0, tzinfo=UTC), Decimal("2550")),
            (end, Decimal("2600")),
        ]
        assert provider._historical_cache["ETH"] == {
            start_ts: Decimal("2500"),
            int(datetime(2024, 6, 15, 13, 0, tzinfo=UTC).timestamp()): Decimal("2550"),
            int(end.timestamp()): Decimal("2600"),
        }

    @pytest.mark.asyncio
    async def test_fetch_historical_rounds_skips_round_gaps(self):
        provider = ChainlinkDataProvider(rpc_url="https://example.com")
        start = datetime(2024, 6, 15, 12, 0, tzinfo=UTC)
        end = datetime(2024, 6, 15, 14, 0, tzinfo=UTC)
        rounds = {
            5: self._round(5, int(end.timestamp()), 260000000000),
            3: self._round(3, int(start.timestamp()), 250000000000),
            2: self._round(2, int(start.timestamp()) - 3600, 240000000000),
        }
        provider._get_decimals_cached = AsyncMock(return_value=8)  # type: ignore[method-assign]
        provider._query_latest_round_data = AsyncMock(return_value=rounds[5])  # type: ignore[method-assign]
        provider._query_round_data = AsyncMock(side_effect=lambda _feed, round_id: rounds.get(round_id))  # type: ignore[method-assign]

        prices = await provider._fetch_historical_rounds("ETH", start, end)

        assert prices == [
            (start, Decimal("2500")),
            (end, Decimal("2600")),
        ]

    @pytest.mark.asyncio
    async def test_fetch_historical_rounds_stops_after_many_gaps(self):
        provider = ChainlinkDataProvider(rpc_url="https://example.com")
        latest = self._round(150, int(datetime(2024, 6, 15, 14, 0, tzinfo=UTC).timestamp()))
        provider._get_decimals_cached = AsyncMock(return_value=8)  # type: ignore[method-assign]
        provider._query_latest_round_data = AsyncMock(return_value=latest)  # type: ignore[method-assign]
        provider._query_round_data = AsyncMock(return_value=None)  # type: ignore[method-assign]

        prices = await provider._fetch_historical_rounds(
            "ETH",
            datetime(2024, 6, 15, 12, 0, tzinfo=UTC),
            datetime(2024, 6, 15, 14, 0, tzinfo=UTC),
        )

        assert prices == []
        assert provider._query_round_data.await_count == 101


class TestMockedPriceFetching:
    """Tests for price fetching with mocked Chainlink responses."""

    def test_query_latest_round_data_no_rpc(self):
        """Test querying without RPC URL returns None."""
        provider = ChainlinkDataProvider(rpc_url="")

        result = provider._query_latest_round_data_sync("0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419")

        assert result is None

    def test_query_latest_round_data_success(self):
        """Test successful latestRoundData query."""
        # Mock Web3 module for lazy import inside the method
        with patch.dict("sys.modules", {"web3": MagicMock()}):
            mock_web3_instance = MagicMock()
            mock_web3_class = MagicMock(return_value=mock_web3_instance)
            mock_web3_class.HTTPProvider = MagicMock()
            mock_web3_instance.to_checksum_address.return_value = "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419"

            # Create mock response (5 * 32 bytes = 160 bytes)
            # roundId=1, answer=250000000000, startedAt=1718450000, updatedAt=1718450000, answeredInRound=1
            mock_result = (
                int.to_bytes(1, 32, "big")  # roundId
                + int.to_bytes(250000000000, 32, "big")  # answer ($2500)
                + int.to_bytes(1718450000, 32, "big")  # startedAt
                + int.to_bytes(1718450000, 32, "big")  # updatedAt
                + int.to_bytes(1, 32, "big")  # answeredInRound
            )
            mock_web3_instance.eth.call.return_value = mock_result

            with patch("web3.Web3", mock_web3_class):
                provider = ChainlinkDataProvider(rpc_url="https://eth-mainnet.example.com")
                result = provider._query_latest_round_data_sync("0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419")

            assert result is not None
            assert result.round_id == 1
            assert result.answer == 250000000000
            assert result.updated_at == 1718450000

    def test_query_latest_round_data_short_response(self):
        """Test handling short response from aggregator."""
        with patch.dict("sys.modules", {"web3": MagicMock()}):
            mock_web3_instance = MagicMock()
            mock_web3_class = MagicMock(return_value=mock_web3_instance)
            mock_web3_class.HTTPProvider = MagicMock()
            mock_web3_instance.to_checksum_address.return_value = "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419"

            # Return too short response (< 160 bytes)
            mock_web3_instance.eth.call.return_value = b"\x00" * 100

            with patch("web3.Web3", mock_web3_class):
                provider = ChainlinkDataProvider(rpc_url="https://eth-mainnet.example.com")
                result = provider._query_latest_round_data_sync("0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419")

            assert result is None

    def test_query_latest_round_data_rejects_zero_answer(self):
        """Test latest round data rejects non-positive oracle answers."""
        with patch.dict("sys.modules", {"web3": MagicMock()}):
            mock_web3_instance = MagicMock()
            mock_web3_class = MagicMock(return_value=mock_web3_instance)
            mock_web3_class.HTTPProvider = MagicMock()
            mock_web3_instance.to_checksum_address.return_value = "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419"

            current_time = int(datetime.now(UTC).timestamp())
            mock_result = (
                int.to_bytes(1, 32, "big")
                + int.to_bytes(0, 32, "big", signed=True)
                + int.to_bytes(current_time, 32, "big")
                + int.to_bytes(current_time, 32, "big")
                + int.to_bytes(1, 32, "big")
            )
            mock_web3_instance.eth.call.return_value = mock_result

            with patch("web3.Web3", mock_web3_class):
                provider = ChainlinkDataProvider(rpc_url="https://eth-mainnet.example.com")
                result = provider._query_latest_round_data_sync("0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419")

            assert result is None

    def test_query_latest_round_data_exception(self):
        """Test handling exception during query."""
        with patch.dict("sys.modules", {"web3": MagicMock()}):
            mock_web3_instance = MagicMock()
            mock_web3_class = MagicMock(return_value=mock_web3_instance)
            mock_web3_class.HTTPProvider = MagicMock()
            mock_web3_instance.to_checksum_address.return_value = "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419"
            mock_web3_instance.eth.call.side_effect = Exception("RPC error")

            with patch("web3.Web3", mock_web3_class):
                provider = ChainlinkDataProvider(rpc_url="https://eth-mainnet.example.com")
                result = provider._query_latest_round_data_sync("0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419")

            assert result is None

    def test_get_latest_price_sync_unknown_token(self):
        """Test get_latest_price_sync raises for unknown token."""
        provider = ChainlinkDataProvider()

        with pytest.raises(ValueError) as exc_info:
            provider.get_latest_price_sync("UNKNOWN_XYZ")

        assert "Unknown token" in str(exc_info.value)

    def test_get_latest_price_sync_no_feed(self):
        """Test get_latest_price_sync raises when no feed on chain."""
        # Create provider with a chain that doesn't have GMX feed (e.g., base)
        provider = ChainlinkDataProvider(chain="base")

        with pytest.raises(ValueError) as exc_info:
            provider.get_latest_price_sync("GMX")  # GMX feed only on Arbitrum

        assert "No Chainlink feed" in str(exc_info.value)

    def test_get_latest_price_sync_with_cache(self):
        """Test get_latest_price_sync uses cache when available."""
        provider = ChainlinkDataProvider(rpc_url="https://eth-mainnet.example.com")

        # Pre-populate cache
        provider._cache.set_live_price("ETH", Decimal("2500"))

        # Should return cached value without making RPC call
        price = provider.get_latest_price_sync("ETH", use_cache=True)

        assert price == Decimal("2500")

    def test_get_latest_price_sync_bypass_cache(self):
        """Test get_latest_price_sync bypasses cache when requested."""
        with patch.dict("sys.modules", {"web3": MagicMock()}):
            mock_web3_instance = MagicMock()
            mock_web3_class = MagicMock(return_value=mock_web3_instance)
            mock_web3_class.HTTPProvider = MagicMock()
            mock_web3_instance.to_checksum_address.return_value = "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419"

            # Fresh response
            current_time = int(datetime.now(UTC).timestamp())
            mock_result = (
                int.to_bytes(1, 32, "big")  # roundId
                + int.to_bytes(260000000000, 32, "big")  # answer ($2600)
                + int.to_bytes(current_time, 32, "big")  # startedAt
                + int.to_bytes(current_time, 32, "big")  # updatedAt (fresh)
                + int.to_bytes(1, 32, "big")  # answeredInRound
            )
            mock_web3_instance.eth.call.return_value = mock_result

            with patch("web3.Web3", mock_web3_class):
                provider = ChainlinkDataProvider(rpc_url="https://eth-mainnet.example.com")

                # Pre-populate cache with stale price
                provider._cache.set_live_price("ETH", Decimal("2500"))

                # Bypass cache
                price = provider.get_latest_price_sync("ETH", use_cache=False)

            assert price == Decimal("2600")

    def test_get_latest_price_sync_stale_without_raise_returns_none(self):
        """Test sync get_latest_price returns None for stale data when configured."""
        provider = ChainlinkDataProvider(rpc_url="https://eth-mainnet.example.com")
        stale_updated_at = datetime(2024, 1, 1, tzinfo=UTC)
        round_data = ChainlinkRoundData(
            round_id=1,
            answer=250000000000,
            started_at=int(stale_updated_at.timestamp()),
            updated_at=int(stale_updated_at.timestamp()),
            answered_in_round=1,
        )
        provider._query_latest_round_data_sync = MagicMock(return_value=round_data)  # type: ignore[method-assign]

        price = provider.get_latest_price_sync("ETH", raise_on_stale=False, use_cache=False)

        assert price is None

    @pytest.mark.asyncio
    async def test_get_latest_price_unknown_token(self):
        """Test async get_latest_price raises for unknown token."""
        provider = ChainlinkDataProvider()

        with pytest.raises(ValueError) as exc_info:
            await provider.get_latest_price("UNKNOWN_XYZ")

        assert "Unknown token" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_latest_price_no_feed(self):
        """Test async get_latest_price raises when no feed is available."""
        provider = ChainlinkDataProvider(chain="base")

        with pytest.raises(ValueError) as exc_info:
            await provider.get_latest_price("GMX")

        assert "No Chainlink feed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_latest_price_with_cache(self):
        """Test async get_latest_price uses live cache when enabled."""
        provider = ChainlinkDataProvider(rpc_url="https://eth-mainnet.example.com")
        provider._cache.set_live_price("ETH", Decimal("2500"))
        provider._query_latest_round_data = AsyncMock()  # type: ignore[method-assign]

        price = await provider.get_latest_price("ETH", use_cache=True)

        assert price == Decimal("2500")
        provider._query_latest_round_data.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_get_latest_price_query_failure_returns_none(self):
        """Test async get_latest_price returns None when latest round query fails."""
        provider = ChainlinkDataProvider(rpc_url="https://eth-mainnet.example.com")
        provider._query_latest_round_data = AsyncMock(return_value=None)  # type: ignore[method-assign]

        price = await provider.get_latest_price("ETH", use_cache=False)

        assert price is None

    @pytest.mark.asyncio
    async def test_get_latest_price_converts_and_caches_fresh_round(self):
        """Test async get_latest_price converts decimals and stores live cache."""
        provider = ChainlinkDataProvider(rpc_url="https://eth-mainnet.example.com")
        current_time = int(datetime.now(UTC).timestamp())
        round_data = ChainlinkRoundData(
            round_id=1,
            answer=2500000000,
            started_at=current_time,
            updated_at=current_time,
            answered_in_round=1,
        )
        provider._query_latest_round_data = AsyncMock(return_value=round_data)  # type: ignore[method-assign]
        provider._query_decimals = AsyncMock(return_value=6)  # type: ignore[method-assign]

        price = await provider.get_latest_price("ETH", use_cache=False)

        assert price == Decimal("2500")
        cached = provider._cache.get_live_price("ETH")
        assert cached is not None
        assert cached.price == Decimal("2500")

    @pytest.mark.asyncio
    async def test_get_latest_price_stale_without_raise_returns_none(self):
        """Test async get_latest_price returns None for stale data when configured."""
        provider = ChainlinkDataProvider(rpc_url="https://eth-mainnet.example.com")
        stale_updated_at = datetime(2024, 1, 1, tzinfo=UTC)
        round_data = ChainlinkRoundData(
            round_id=1,
            answer=250000000000,
            started_at=int(stale_updated_at.timestamp()),
            updated_at=int(stale_updated_at.timestamp()),
            answered_in_round=1,
        )
        provider._query_latest_round_data = AsyncMock(return_value=round_data)  # type: ignore[method-assign]
        provider._query_decimals = AsyncMock(return_value=8)  # type: ignore[method-assign]

        price = await provider.get_latest_price("ETH", raise_on_stale=False, use_cache=False)

        assert price is None
        provider._query_decimals.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_query_round_data_rejects_negative_answer(self):
        """Test historical round data rejects negative oracle answers."""
        provider = ChainlinkDataProvider(rpc_url="https://eth-mainnet.example.com")
        current_time = int(datetime.now(UTC).timestamp())
        mock_result = (
            int.to_bytes(10, 32, "big")
            + (-1).to_bytes(32, "big", signed=True)
            + int.to_bytes(current_time, 32, "big")
            + int.to_bytes(current_time, 32, "big")
            + int.to_bytes(10, 32, "big")
        )

        with patch("web3.AsyncWeb3") as mock_web3_class, patch("web3.AsyncHTTPProvider"):
            mock_web3 = MagicMock()
            mock_web3.to_checksum_address.return_value = "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419"
            mock_web3.eth.call = AsyncMock(return_value=mock_result)
            mock_web3_class.return_value = mock_web3

            result = await provider._query_round_data("0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419", 10)

        assert result is None


class TestChainlinkRoundData:
    """Tests for ChainlinkRoundData dataclass."""

    def test_round_data_creation(self):
        """Test creating ChainlinkRoundData."""
        data = ChainlinkRoundData(
            round_id=12345,
            answer=250000000000,
            started_at=1718450000,
            updated_at=1718450100,
            answered_in_round=12345,
        )

        assert data.round_id == 12345
        assert data.answer == 250000000000
        assert data.started_at == 1718450000
        assert data.updated_at == 1718450100
        assert data.answered_in_round == 12345


class TestChainlinkPriceFeed:
    """Tests for ChainlinkPriceFeed dataclass."""

    def test_price_feed_creation(self):
        """Test creating ChainlinkPriceFeed."""
        feed = ChainlinkPriceFeed(
            address="0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419",
            pair="ETH/USD",
        )

        assert feed.address == "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419"
        assert feed.pair == "ETH/USD"
        assert feed.decimals == 8  # Default
        assert feed.heartbeat_seconds == 3600  # Default

    def test_price_feed_custom_values(self):
        """Test creating ChainlinkPriceFeed with custom values."""
        feed = ChainlinkPriceFeed(
            address="0xABC",
            pair="CUSTOM/USD",
            decimals=6,
            heartbeat_seconds=7200,
            deviation_threshold=Decimal("0.5"),
        )

        assert feed.decimals == 6
        assert feed.heartbeat_seconds == 7200
        assert feed.deviation_threshold == Decimal("0.5")


class TestHistoricalDataIteration:
    """Tests for historical data iteration."""

    @pytest.mark.asyncio
    async def test_iterate_with_cached_data(self):
        """Test iterating through cached historical data."""
        from almanak.framework.backtesting.pnl.data_provider import HistoricalDataConfig

        provider = ChainlinkDataProvider()

        # Pre-load historical data
        provider.set_historical_prices(
            "ETH",
            [
                (datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC), Decimal("2500")),
                (datetime(2024, 6, 15, 13, 0, 0, tzinfo=UTC), Decimal("2550")),
                (datetime(2024, 6, 15, 14, 0, 0, tzinfo=UTC), Decimal("2600")),
            ],
        )

        config = HistoricalDataConfig(
            start_time=datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC),
            end_time=datetime(2024, 6, 15, 14, 0, 0, tzinfo=UTC),
            interval_seconds=3600,
            tokens=["ETH"],
        )

        data_points = []
        async for timestamp, market_state in provider.iterate(config):
            data_points.append((timestamp, market_state.get_price("ETH")))

        assert len(data_points) == 3
        assert data_points[0][1] == Decimal("2500")
        assert data_points[1][1] == Decimal("2550")
        assert data_points[2][1] == Decimal("2600")

    @pytest.mark.asyncio
    async def test_iterate_includes_metadata(self):
        """Test iteration includes chainlink metadata."""
        from almanak.framework.backtesting.pnl.data_provider import HistoricalDataConfig

        provider = ChainlinkDataProvider()

        provider.set_historical_prices(
            "ETH",
            [(datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC), Decimal("2500"))],
        )

        config = HistoricalDataConfig(
            start_time=datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC),
            end_time=datetime(2024, 6, 15, 13, 0, 0, tzinfo=UTC),  # end > start
            interval_seconds=3600,
            tokens=["ETH"],
        )

        async for _timestamp, market_state in provider.iterate(config):
            # Without archive access, data_source is "chainlink_cache"
            # With archive access, it would be "chainlink_historical"
            assert market_state.metadata.get("data_source") in [
                "chainlink_cache",
                "chainlink_historical",
            ]
            # Also verify tracking metrics are included
            assert "historical_price_hits" in market_state.metadata
            assert "fallback_price_hits" in market_state.metadata
            break  # Only need to verify first iteration

    @pytest.mark.asyncio
    async def test_iterate_fetches_archive_prices_for_uncached_tokens(self):
        """Test archive mode preloads uncached token prices before iteration."""
        from almanak.framework.backtesting.pnl.data_provider import HistoricalDataConfig

        provider = ChainlinkDataProvider(rpc_url="https://eth-mainnet.example.com")
        provider._verify_archive_access = AsyncMock(return_value=True)  # type: ignore[method-assign]
        start = datetime(2024, 6, 15, 12, 0, tzinfo=UTC)
        end = datetime(2024, 6, 15, 13, 0, tzinfo=UTC)
        provider._fetch_historical_rounds = AsyncMock(  # type: ignore[method-assign]
            return_value=[
                (start, Decimal("2500")),
                (end, Decimal("2550")),
            ]
        )
        config = HistoricalDataConfig(
            start_time=start,
            end_time=end,
            interval_seconds=3600,
            tokens=["ETH"],
            include_ohlcv=False,
        )

        data_points = [(timestamp, state) async for timestamp, state in provider.iterate(config)]

        assert [timestamp for timestamp, _state in data_points] == [start, end]
        assert [state.get_price("ETH") for _timestamp, state in data_points] == [
            Decimal("2500"),
            Decimal("2550"),
        ]
        assert [state.metadata["data_source"] for _timestamp, state in data_points] == [
            "chainlink_historical",
            "chainlink_historical",
        ]
        assert data_points[-1][1].metadata["historical_price_hits"] == 2
        provider._fetch_historical_rounds.assert_awaited_once_with("ETH", start, end)

    @pytest.mark.asyncio
    async def test_iterate_does_not_refetch_tokens_already_cached(self):
        """Test archive mode leaves existing token cache intact."""
        from almanak.framework.backtesting.pnl.data_provider import HistoricalDataConfig

        provider = ChainlinkDataProvider(rpc_url="https://eth-mainnet.example.com")
        provider.set_historical_prices(
            "ETH",
            [(datetime(2024, 6, 15, 12, 0, tzinfo=UTC), Decimal("2500"))],
        )
        provider._verify_archive_access = AsyncMock(return_value=True)  # type: ignore[method-assign]
        provider._fetch_historical_rounds = AsyncMock()  # type: ignore[method-assign]
        config = HistoricalDataConfig(
            start_time=datetime(2024, 6, 15, 12, 0, tzinfo=UTC),
            end_time=datetime(2024, 6, 15, 13, 0, tzinfo=UTC),
            interval_seconds=3600,
            tokens=["ETH"],
        )

        data_points = [(timestamp, state) async for timestamp, state in provider.iterate(config)]

        assert data_points[0][1].get_price("ETH") == Decimal("2500")
        provider._fetch_historical_rounds.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_iterate_generates_pseudo_ohlcv_when_requested(self):
        """Test iteration emits pseudo-OHLCV bars from cached spot prices."""
        from almanak.framework.backtesting.pnl.data_provider import HistoricalDataConfig

        provider = ChainlinkDataProvider()
        timestamp = datetime(2024, 6, 15, 12, 0, tzinfo=UTC)
        provider.set_historical_prices("ETH", [(timestamp, Decimal("2500"))])
        config = HistoricalDataConfig(
            start_time=timestamp,
            end_time=timestamp + timedelta(hours=1),
            interval_seconds=3600,
            tokens=["ETH"],
            include_ohlcv=True,
        )

        _timestamp, state = await anext(provider.iterate(config))

        ohlcv = state.ohlcv["ETH"]
        assert ohlcv.open == Decimal("2500")
        assert ohlcv.high == Decimal("2500")
        assert ohlcv.low == Decimal("2500")
        assert ohlcv.close == Decimal("2500")
        assert ohlcv.volume is None

    @pytest.mark.asyncio
    async def test_iterate_normalizes_naive_archive_window(self):
        """Test archive preload receives UTC-aware timestamps for naive configs."""
        from almanak.framework.backtesting.pnl.data_provider import HistoricalDataConfig

        provider = ChainlinkDataProvider(rpc_url="https://eth-mainnet.example.com")
        provider._verify_archive_access = AsyncMock(return_value=True)  # type: ignore[method-assign]
        provider._fetch_historical_rounds = AsyncMock(  # type: ignore[method-assign]
            return_value=[
                (datetime(2024, 6, 15, 12, 0, tzinfo=UTC), Decimal("2500")),
                (datetime(2024, 6, 15, 13, 0, tzinfo=UTC), Decimal("2501")),
            ]
        )
        config = HistoricalDataConfig(
            start_time=datetime(2024, 6, 15, 12, 0),
            end_time=datetime(2024, 6, 15, 13, 0),
            interval_seconds=3600,
            tokens=["ETH"],
        )

        data_points = [(timestamp, state) async for timestamp, state in provider.iterate(config)]

        fetch_args = provider._fetch_historical_rounds.await_args.args
        assert fetch_args[1] == datetime(2024, 6, 15, 12, 0, tzinfo=UTC)
        assert fetch_args[2] == datetime(2024, 6, 15, 13, 0, tzinfo=UTC)
        assert [timestamp for timestamp, _state in data_points] == [
            datetime(2024, 6, 15, 12, 0, tzinfo=UTC),
            datetime(2024, 6, 15, 13, 0, tzinfo=UTC),
        ]

    @pytest.mark.asyncio
    async def test_iterate_empty_cache_raises_data_source_unavailable(self):
        """Test no-archive iteration raises instead of yielding empty market states."""
        from almanak.framework.backtesting.pnl.data_provider import HistoricalDataConfig

        provider = ChainlinkDataProvider()
        config = HistoricalDataConfig(
            start_time=datetime(2024, 6, 15, 12, 0, tzinfo=UTC),
            end_time=datetime(2024, 6, 15, 13, 0, tzinfo=UTC),
            interval_seconds=3600,
            tokens=["ETH"],
            include_ohlcv=False,
        )

        with pytest.raises(DataSourceUnavailable):
            _ = [(timestamp, state) async for timestamp, state in provider.iterate(config)]

    @pytest.mark.asyncio
    async def test_iterate_rejects_address_keyed_tokens(self):
        """Chainlink iteration rejects TokenRef inputs before yielding partial rows."""
        from almanak.framework.backtesting.pnl.data_provider import HistoricalDataConfig

        provider = ChainlinkDataProvider()
        config = HistoricalDataConfig(
            start_time=datetime(2024, 6, 15, 12, 0, tzinfo=UTC),
            end_time=datetime(2024, 6, 15, 13, 0, tzinfo=UTC),
            interval_seconds=3600,
            tokens=[("arbitrum", "0x5979D7b546E38E414F7E9822514be443A4800529")],
            include_ohlcv=False,
        )

        with pytest.raises(DataSourceUnavailable, match="only supports symbol tokens"):
            _ = [(timestamp, state) async for timestamp, state in provider.iterate(config)]


class TestGetPrice:
    """Tests for get_price method."""

    @pytest.mark.asyncio
    async def test_get_price_unknown_token(self):
        """Test get_price raises for unknown token."""
        provider = ChainlinkDataProvider()

        with pytest.raises(ValueError) as exc_info:
            await provider.get_price("UNKNOWN_TOKEN_XYZ")

        assert "Unknown token" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_price_no_feed(self):
        """Test get_price raises when no feed available."""
        provider = ChainlinkDataProvider(chain="base")

        with pytest.raises(ValueError) as exc_info:
            await provider.get_price("GMX")  # No GMX feed on Base

        assert "No Chainlink feed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_price_from_cache(self):
        """Test get_price returns cached data."""
        provider = ChainlinkDataProvider()

        # Pre-load historical data
        provider.set_historical_prices(
            "ETH",
            [(datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC), Decimal("2500"))],
        )

        price = await provider.get_price("ETH", datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC))

        assert price == Decimal("2500")

    @pytest.mark.asyncio
    async def test_get_price_live_request_prefers_latest_rpc_over_historical_cache(self):
        """Current-price requests must not be served from stale historical cache."""
        provider = ChainlinkDataProvider(rpc_url="https://eth-mainnet.example.com")
        provider.set_historical_prices(
            "ETH",
            [(datetime.now(UTC) - timedelta(hours=1), Decimal("2400"))],
        )
        provider._latest_price_or_raise = AsyncMock(return_value=Decimal("2600"))  # type: ignore[method-assign]

        price = await provider.get_price("ETH")

        assert price == Decimal("2600")
        provider._latest_price_or_raise.assert_awaited_once_with("ETH", True)

    @pytest.mark.asyncio
    async def test_get_price_historical_not_available(self):
        """Test get_price raises when historical data not available."""
        provider = ChainlinkDataProvider()

        with pytest.raises(ValueError) as exc_info:
            await provider.get_price("ETH", datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC))

        assert "not available" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_price_returns_archive_price_when_cache_disabled(self):
        """Historical archive fetch should return fetched prices even without cache."""
        provider = ChainlinkDataProvider(
            rpc_url="https://eth-mainnet.example.com",
            cache_ttl_seconds=0,
        )
        target = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)
        provider._verify_archive_access = AsyncMock(return_value=True)  # type: ignore[method-assign]
        provider._fetch_historical_rounds = AsyncMock(  # type: ignore[method-assign]
            return_value=[
                (target - timedelta(minutes=5), Decimal("2495")),
                (target, Decimal("2500")),
                (target + timedelta(minutes=5), Decimal("2505")),
            ]
        )

        price = await provider.get_price("ETH", target)

        assert price == Decimal("2500")


class TestGetOhlcv:
    """Tests for get_ohlcv method."""

    @pytest.mark.asyncio
    async def test_get_ohlcv_unknown_token(self):
        """Test get_ohlcv raises for unknown token."""
        provider = ChainlinkDataProvider()

        with pytest.raises(ValueError) as exc_info:
            await provider.get_ohlcv(
                "UNKNOWN_TOKEN",
                datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC),
                datetime(2024, 6, 15, 14, 0, 0, tzinfo=UTC),
            )

        assert "Unknown token" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_ohlcv_no_cache(self):
        """Test get_ohlcv raises when cache not available."""
        provider = ChainlinkDataProvider()

        with pytest.raises(ValueError) as exc_info:
            await provider.get_ohlcv(
                "ETH",
                datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC),
                datetime(2024, 6, 15, 14, 0, 0, tzinfo=UTC),
            )

        assert "not available" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_ohlcv_from_cache(self):
        """Test get_ohlcv generates pseudo-OHLCV from cached prices."""
        provider = ChainlinkDataProvider()

        # Pre-load historical data
        provider.set_historical_prices(
            "ETH",
            [
                (datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC), Decimal("2500")),
                (datetime(2024, 6, 15, 13, 0, 0, tzinfo=UTC), Decimal("2550")),
                (datetime(2024, 6, 15, 14, 0, 0, tzinfo=UTC), Decimal("2600")),
            ],
        )

        ohlcv = await provider.get_ohlcv(
            "ETH",
            datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC),
            datetime(2024, 6, 15, 14, 0, 0, tzinfo=UTC),
            interval_seconds=3600,
        )

        assert len(ohlcv) == 3
        # Chainlink provides spot prices, so O=H=L=C
        assert ohlcv[0].open == ohlcv[0].high == ohlcv[0].low == ohlcv[0].close
        assert ohlcv[0].close == Decimal("2500")
        assert ohlcv[1].close == Decimal("2550")
        assert ohlcv[2].close == Decimal("2600")


class TestProviderMetadata:
    """Tests for provider metadata properties."""

    def test_provider_name(self):
        """Test provider_name property."""
        provider = ChainlinkDataProvider(chain="arbitrum")
        assert provider.provider_name == "chainlink_arbitrum"

    def test_min_timestamp(self):
        """Test min_timestamp property."""
        provider = ChainlinkDataProvider()
        min_ts = provider.min_timestamp

        assert min_ts is not None
        assert min_ts.year == 2020

    def test_max_timestamp(self):
        """Test max_timestamp property."""
        provider = ChainlinkDataProvider()
        max_ts = provider.max_timestamp

        assert max_ts is not None
        # Max should be approximately now (within 1 minute)
        now = datetime.now(UTC)
        assert (now - max_ts).total_seconds() < 60


class TestAsyncContextManager:
    """Tests for async context manager support."""

    @pytest.mark.asyncio
    async def test_context_manager(self):
        """Test provider works as async context manager."""
        async with ChainlinkDataProvider() as provider:
            assert provider is not None
            assert provider._chain == "ethereum"


class TestConstants:
    """Tests for module-level constants."""

    def test_latest_round_data_selector(self):
        """Test latestRoundData function selector."""
        assert LATEST_ROUND_DATA_SELECTOR == "0xfeaf968c"

    def test_decimals_selector(self):
        """Test decimals function selector."""
        assert DECIMALS_SELECTOR == "0x313ce567"

    def test_chainlink_heartbeats_has_default(self):
        """Test CHAINLINK_HEARTBEATS has default entry."""
        assert "default" in CHAINLINK_HEARTBEATS
        assert CHAINLINK_HEARTBEATS["default"] == 3600

    def test_chainlink_price_feeds_all_chains(self):
        """Test CHAINLINK_PRICE_FEEDS has all expected chains."""
        expected_chains = ["ethereum", "arbitrum", "base", "optimism", "polygon", "avalanche"]
        for chain in expected_chains:
            assert chain in CHAINLINK_PRICE_FEEDS
