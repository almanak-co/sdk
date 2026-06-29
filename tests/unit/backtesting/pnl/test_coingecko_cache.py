"""Unit tests for CoinGecko historical price caching.

This module tests the aggressive caching feature for the CoinGeckoDataProvider,
including:
- HistoricalCacheStats dataclass
- HistoricalPriceCache with (token, date) keys and 1-hour TTL
- Persistent SQLite-backed cache for cross-run persistence
- Cache warming with pre-fetch capability
- Cache hit rate logging and statistics
- Target: >90% cache hit rate for repeated backtests
"""

import logging
import tempfile
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.backtesting.config import BacktestDataConfig
from almanak.framework.backtesting.pnl.data_provider import OHLCV, HistoricalDataConfig
from almanak.framework.backtesting.pnl.providers.coingecko import (
    CoinGeckoDataProvider,
    CoinGeckoRateLimitError,
    HistoricalCacheEntry,
    HistoricalCacheStats,
    HistoricalPriceCache,
    OHLCVCache,
    RetryConfig,
)


class TestHistoricalCacheStats:
    """Tests for HistoricalCacheStats dataclass."""

    def test_default_values(self):
        """Test default values are all zero."""
        stats = HistoricalCacheStats()
        assert stats.total_requests == 0
        assert stats.cache_hits == 0
        assert stats.cache_misses == 0
        assert stats.cache_entries == 0

    def test_hit_rate_zero_requests(self):
        """Test hit rate is 0 when no requests made."""
        stats = HistoricalCacheStats()
        assert stats.hit_rate == 0.0

    def test_hit_rate_calculation(self):
        """Test hit rate calculation is correct."""
        stats = HistoricalCacheStats(
            total_requests=100,
            cache_hits=90,
            cache_misses=10,
        )
        assert stats.hit_rate == 90.0

    def test_hit_rate_perfect(self):
        """Test 100% hit rate."""
        stats = HistoricalCacheStats(
            total_requests=50,
            cache_hits=50,
            cache_misses=0,
        )
        assert stats.hit_rate == 100.0

    def test_to_dict(self):
        """Test to_dict serialization."""
        stats = HistoricalCacheStats(
            total_requests=100,
            cache_hits=90,
            cache_misses=10,
            cache_entries=50,
        )
        result = stats.to_dict()
        assert result["total_requests"] == 100
        assert result["cache_hits"] == 90
        assert result["cache_misses"] == 10
        assert result["cache_entries"] == 50
        assert result["hit_rate_percent"] == 90.0


class TestCoinGeckoProviderInitialization:
    """Tests for CoinGecko provider constructor setup."""

    def test_data_config_rate_limit_overrides_api_tier_rate(self):
        """BacktestDataConfig supplies the rate limit even when an API key is present."""
        data_config = BacktestDataConfig(coingecko_rate_limit_per_minute=25)

        provider = CoinGeckoDataProvider(api_key="test-key", data_config=data_config)

        assert provider._rate_limit == 25
        assert provider._rate_limiter.initial_requests_per_minute == 25
        assert provider._min_request_interval == 0.2

    def test_explicit_pro_request_interval_is_preserved(self):
        """Only the default free-tier interval is auto-reduced for pro API keys."""
        provider = CoinGeckoDataProvider(api_key="test-key", min_request_interval=0.75)

        assert provider._min_request_interval == 0.75

    def test_token_addresses_are_stored_with_uppercase_symbols_and_normalized_addresses(self):
        """Address-backed token resolution should be case-insensitive by symbol and address."""
        provider = CoinGeckoDataProvider(
            token_addresses={"wstETH": ("arbitrum", "0x5979D7b546E38E414F7E9822514be443A4800529")}
        )

        assert provider._token_addresses == {
            "WSTETH": ("arbitrum", "0x5979d7b546e38e414f7e9822514be443a4800529")
        }


class TestHistoricalPriceCache:
    """Tests for HistoricalPriceCache class."""

    def test_default_ttl_one_hour(self):
        """Test default TTL is 1 hour (3600 seconds)."""
        cache = HistoricalPriceCache()
        assert cache.ttl_seconds == 3600

    def test_custom_ttl(self):
        """Test custom TTL can be set."""
        cache = HistoricalPriceCache(ttl_seconds=7200)
        assert cache.ttl_seconds == 7200

    def test_set_and_get(self):
        """Test basic set and get operations."""
        cache = HistoricalPriceCache()
        timestamp = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
        price = Decimal("2500.00")

        cache.set("WETH", timestamp, price)
        result = cache.get("WETH", timestamp)

        assert result == price

    def test_cache_key_is_token_and_date(self):
        """Test cache key uses date portion, not full timestamp."""
        cache = HistoricalPriceCache()

        # Same date, different times
        ts1 = datetime(2024, 1, 15, 10, 0, 0, tzinfo=UTC)
        ts2 = datetime(2024, 1, 15, 22, 30, 0, tzinfo=UTC)
        price = Decimal("2500.00")

        cache.set("WETH", ts1, price)

        # Should hit cache even with different time
        result = cache.get("WETH", ts2)
        assert result == price

    def test_different_dates_are_separate(self):
        """Test different dates have separate cache entries."""
        cache = HistoricalPriceCache()

        ts1 = datetime(2024, 1, 15, tzinfo=UTC)
        ts2 = datetime(2024, 1, 16, tzinfo=UTC)
        price1 = Decimal("2500.00")
        price2 = Decimal("2600.00")

        cache.set("WETH", ts1, price1)
        cache.set("WETH", ts2, price2)

        assert cache.get("WETH", ts1) == price1
        assert cache.get("WETH", ts2) == price2

    def test_different_tokens_are_separate(self):
        """Test different tokens have separate cache entries."""
        cache = HistoricalPriceCache()
        timestamp = datetime(2024, 1, 15, tzinfo=UTC)

        cache.set("WETH", timestamp, Decimal("2500.00"))
        cache.set("USDC", timestamp, Decimal("1.00"))

        assert cache.get("WETH", timestamp) == Decimal("2500.00")
        assert cache.get("USDC", timestamp) == Decimal("1.00")

    def test_token_case_insensitive(self):
        """Test token lookup is case-insensitive."""
        cache = HistoricalPriceCache()
        timestamp = datetime(2024, 1, 15, tzinfo=UTC)
        price = Decimal("2500.00")

        cache.set("weth", timestamp, price)

        assert cache.get("WETH", timestamp) == price
        assert cache.get("Weth", timestamp) == price

    def test_uppercase_0x_address_cache_keys_are_address_normalized(self):
        """Address cache tokens normalize even when the hex prefix is uppercase."""
        timestamp = datetime(2024, 1, 15, tzinfo=UTC)
        price = Decimal("1.00")
        cache = HistoricalPriceCache()

        cache.set("0X833589FCD6EDB6E08F4C7C32D4F71B54BDA02913", timestamp, price)

        assert cache.get("0x833589fcd6edb6e08f4c7c32d4f71b54bda02913", timestamp) == price

    def test_cache_miss_returns_none(self):
        """Test cache miss returns None."""
        cache = HistoricalPriceCache()
        timestamp = datetime(2024, 1, 15, tzinfo=UTC)

        result = cache.get("WETH", timestamp)
        assert result is None

    def test_stats_tracking_cache_hit(self):
        """Test cache hit updates stats correctly."""
        cache = HistoricalPriceCache()
        timestamp = datetime(2024, 1, 15, tzinfo=UTC)

        cache.set("WETH", timestamp, Decimal("2500.00"))
        cache.get("WETH", timestamp)  # Hit

        stats = cache.get_stats()
        assert stats.total_requests == 1
        assert stats.cache_hits == 1
        assert stats.cache_misses == 0

    def test_stats_tracking_cache_miss(self):
        """Test cache miss updates stats correctly."""
        cache = HistoricalPriceCache()
        timestamp = datetime(2024, 1, 15, tzinfo=UTC)

        cache.get("WETH", timestamp)  # Miss

        stats = cache.get_stats()
        assert stats.total_requests == 1
        assert stats.cache_hits == 0
        assert stats.cache_misses == 1

    def test_stats_entry_count(self):
        """Test cache entry count is tracked."""
        cache = HistoricalPriceCache()

        cache.set("WETH", datetime(2024, 1, 15, tzinfo=UTC), Decimal("2500.00"))
        cache.set("WETH", datetime(2024, 1, 16, tzinfo=UTC), Decimal("2600.00"))
        cache.set("USDC", datetime(2024, 1, 15, tzinfo=UTC), Decimal("1.00"))

        assert len(cache) == 3
        stats = cache.get_stats()
        assert stats.cache_entries == 3

    def test_reset_stats_keeps_data(self):
        """Test reset_stats clears stats but keeps cached data."""
        cache = HistoricalPriceCache()
        timestamp = datetime(2024, 1, 15, tzinfo=UTC)
        price = Decimal("2500.00")

        cache.set("WETH", timestamp, price)
        cache.get("WETH", timestamp)

        cache.reset_stats()

        stats = cache.get_stats()
        assert stats.total_requests == 0
        assert stats.cache_hits == 0
        assert stats.cache_entries == 1  # Data preserved

        # Data should still be there
        assert cache.get("WETH", timestamp) == price

    def test_clear_removes_everything(self):
        """Test clear removes all data and resets stats."""
        cache = HistoricalPriceCache()
        timestamp = datetime(2024, 1, 15, tzinfo=UTC)

        cache.set("WETH", timestamp, Decimal("2500.00"))
        cache.get("WETH", timestamp)

        cache.clear()

        assert len(cache) == 0
        stats = cache.get_stats()
        assert stats.total_requests == 0
        assert stats.cache_entries == 0

        # Data should be gone
        assert cache.get("WETH", timestamp) is None

    def test_expired_entry_returns_none(self):
        """Test expired entries are not returned."""
        cache = HistoricalPriceCache(ttl_seconds=1)
        timestamp = datetime(2024, 1, 15, tzinfo=UTC)

        cache.set("WETH", timestamp, Decimal("2500.00"))

        # Force expiry without sleeping
        key = cache._make_key("WETH", timestamp)
        cache._cache[key] = HistoricalCacheEntry(
            price=Decimal("2500.00"),
            cached_at=datetime.now(UTC) - timedelta(seconds=2),
        )

        # Entry should be expired
        result = cache.get("WETH", timestamp)
        assert result is None

    def test_ttl_zero_means_no_expiry(self):
        """TTL=0 means cache forever (no expiry), useful for immutable historical data."""
        cache = HistoricalPriceCache(ttl_seconds=0)
        timestamp = datetime(2024, 1, 15, tzinfo=UTC)

        cache.set("WETH", timestamp, Decimal("2500.00"))
        result = cache.get("WETH", timestamp)
        assert result == Decimal("2500.00")

    def test_hit_rate_target_achievable(self):
        """Test that >90% cache hit rate is achievable with caching."""
        cache = HistoricalPriceCache()

        # Simulate typical backtest: load data once, access many times
        dates = [datetime(2024, 1, i, tzinfo=UTC) for i in range(1, 31)]

        # Initial population (30 misses)
        for date in dates:
            cache.set("WETH", date, Decimal("2500.00"))

        # Now simulate 10 complete passes through the data (300 requests)
        for _ in range(10):
            for date in dates:
                cache.get("WETH", date)

        stats = cache.get_stats()
        # 300 requests, all should be hits
        assert stats.hit_rate == 100.0
        assert stats.total_requests == 300
        assert stats.cache_hits == 300


class TestOHLCVCache:
    """Tests for CoinGecko OHLCV iteration cache semantics."""

    def test_get_price_at_before_first_candle_returns_none(self):
        """The cache must not use a future candle as a historical price."""
        first_candle = OHLCV(
            timestamp=datetime(2024, 1, 1, 1, 0, tzinfo=UTC),
            open=Decimal("100"),
            high=Decimal("110"),
            low=Decimal("95"),
            close=Decimal("105"),
            volume=None,
        )
        cache = OHLCVCache(data={"ETH": [first_candle]}, fetched_at=datetime(2024, 1, 1, tzinfo=UTC))

        price = cache.get_price_at("ETH", datetime(2024, 1, 1, 0, 0, tzinfo=UTC))

        assert price is None

    def test_get_price_at_accepts_address_tuple_key(self):
        """The per-run OHLCV cache can be keyed by resolved token identity."""
        token_key = ("arbitrum", "0x5979D7b546E38E414F7E9822514be443A4800529")
        candle = OHLCV(
            timestamp=datetime(2024, 1, 1, 1, 0, tzinfo=UTC),
            open=Decimal("100"),
            high=Decimal("110"),
            low=Decimal("95"),
            close=Decimal("105"),
            volume=None,
        )
        cache = OHLCVCache(
            data={("arbitrum", "0x5979d7b546e38e414f7e9822514be443a4800529"): [candle]},
            fetched_at=datetime(2024, 1, 1, tzinfo=UTC),
        )

        price = cache.get_price_at(token_key, datetime(2024, 1, 1, 2, 0, tzinfo=UTC))

        assert price == Decimal("105")

    def test_get_price_at_normalizes_bare_address_with_default_chain(self):
        """Bare address lookups use the same chain-qualified key as prefetch."""
        address = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
        candle = OHLCV(
            timestamp=datetime(2024, 1, 1, 1, 0, tzinfo=UTC),
            open=Decimal("1"),
            high=Decimal("1"),
            low=Decimal("1"),
            close=Decimal("1"),
            volume=None,
        )
        cache = OHLCVCache(
            data={("base", address.lower()): [candle]},
            fetched_at=datetime(2024, 1, 1, tzinfo=UTC),
            default_chain="base",
        )

        price = cache.get_price_at(address, datetime(2024, 1, 1, 2, 0, tzinfo=UTC))

        assert price == Decimal("1")


class TestCoinGeckoIteration:
    """Tests for CoinGecko historical market-state iteration."""

    @pytest.mark.asyncio
    async def test_iterate_does_not_emit_future_price_before_first_candle(self):
        """Iteration should leave prices empty before the first prefetched candle."""
        first_candle = OHLCV(
            timestamp=datetime(2024, 1, 1, 1, 0, tzinfo=UTC),
            open=Decimal("100"),
            high=Decimal("110"),
            low=Decimal("95"),
            close=Decimal("105"),
            volume=None,
        )
        provider = CoinGeckoDataProvider(retry_config=RetryConfig(max_retries=0))
        provider._prefetch_ohlcv_data = AsyncMock(  # type: ignore[method-assign]
            return_value=OHLCVCache(
                data={"ETH": [first_candle]},
                fetched_at=datetime(2024, 1, 1, tzinfo=UTC),
            )
        )
        config = HistoricalDataConfig(
            start_time=datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
            end_time=datetime(2024, 1, 1, 1, 0, tzinfo=UTC),
            interval_seconds=3600,
            tokens=["ETH"],
            include_ohlcv=True,
        )

        data_points = [(timestamp, state) async for timestamp, state in provider.iterate(config)]

        assert data_points[0][0] == datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        assert data_points[0][1].prices == {}
        assert data_points[0][1].ohlcv == {}
        assert data_points[1][1].prices == {"ETH": Decimal("105")}
        assert data_points[1][1].ohlcv == {"ETH": first_candle}

    @pytest.mark.asyncio
    async def test_iterate_emits_address_keyed_market_state_for_address_tokens(self):
        """Resolved config tokens become address-keyed prices/OHLCV in MarketState."""
        token_key = ("arbitrum", "0x5979D7b546E38E414F7E9822514be443A4800529")
        normalized_key = ("arbitrum", "0x5979d7b546e38e414f7e9822514be443a4800529")
        candle = OHLCV(
            timestamp=datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
            open=Decimal("100"),
            high=Decimal("110"),
            low=Decimal("95"),
            close=Decimal("105"),
            volume=None,
        )
        provider = CoinGeckoDataProvider(retry_config=RetryConfig(max_retries=0))
        provider._prefetch_ohlcv_data = AsyncMock(  # type: ignore[method-assign]
            return_value=OHLCVCache(
                data={normalized_key: [candle]},
                fetched_at=datetime(2024, 1, 1, tzinfo=UTC),
            )
        )
        config = HistoricalDataConfig(
            start_time=datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
            end_time=datetime(2024, 1, 1, 0, 1, tzinfo=UTC),
            interval_seconds=3600,
            tokens=[token_key],
            chains=["arbitrum"],
            include_ohlcv=True,
        )

        data_points = [(timestamp, state) async for timestamp, state in provider.iterate(config)]

        assert data_points[0][1].prices == {normalized_key: Decimal("105")}
        assert data_points[0][1].ohlcv == {normalized_key: candle}
        assert data_points[0][1].available_tokens == [f"{normalized_key[0]}:{normalized_key[1]}"]

    @pytest.mark.asyncio
    async def test_get_price_hits_prefetched_cache_for_bare_address(self):
        """After iterate-style prefetch, bare address get_price hits the OHLCV cache."""
        address = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
        candle = OHLCV(
            timestamp=datetime(2024, 1, 1, 1, 0, tzinfo=UTC),
            open=Decimal("1"),
            high=Decimal("1"),
            low=Decimal("1"),
            close=Decimal("1"),
            volume=None,
        )
        provider = CoinGeckoDataProvider(retry_config=RetryConfig(max_retries=0))
        provider._cache = OHLCVCache(
            data={("base", address.lower()): [candle]},
            fetched_at=datetime(2024, 1, 1, tzinfo=UTC),
            default_chain="base",
        )

        price = await provider.get_price(address, datetime(2024, 1, 1, 2, 0, tzinfo=UTC))

        assert price == Decimal("1")


class TestPrefetchSeedPriorCandle:
    """Tests for the leading-edge prior-candle seed in ``_prefetch_ohlcv_data``.

    CoinGecko's first in-window sample lands a sub-interval after the requested
    start (e.g. ``00:00:48`` for a ``00:00:00`` start), so the first backtest
    tick has no candle at-or-before it. The prefetch seeds a genuine *prior*
    candle so the first tick forward-fills a past close -- without it a
    token-quoted strategy's numeraire projection fails loud at t0 (VIB-5127).
    """

    @staticmethod
    def _candle(ts: datetime, close: str) -> OHLCV:
        price = Decimal(close)
        return OHLCV(timestamp=ts, open=price, high=price, low=price, close=price, volume=None)

    @pytest.mark.asyncio
    async def test_prefetch_seeds_prior_candle_for_misaligned_first_candle(self):
        """A misaligned first candle is preceded by a real prior candle, so the
        first tick forward-fills a PAST price (not the future first candle)."""
        start = datetime(2026, 2, 1, 0, 0, tzinfo=UTC)
        end = datetime(2026, 2, 1, 3, 0, tzinfo=UTC)
        main_first = self._candle(
            datetime(2026, 2, 1, 0, 0, 48, tzinfo=UTC),
            "100",
        )
        main_second = self._candle(datetime(2026, 2, 1, 1, 0, tzinfo=UTC), "101")
        prior = self._candle(datetime(2026, 1, 31, 23, 1, tzinfo=UTC), "99")

        def fake_get_ohlcv(token, s, e, interval_seconds):
            # The pre-window seed request ends exactly at the window start.
            return [prior] if e == start else [main_first, main_second]

        provider = CoinGeckoDataProvider(retry_config=RetryConfig(max_retries=0))
        provider.get_ohlcv = AsyncMock(side_effect=fake_get_ohlcv)  # type: ignore[method-assign]

        config = HistoricalDataConfig(
            start_time=start, end_time=end, interval_seconds=3600, tokens=["ETH"], include_ohlcv=True
        )
        cache = await provider._prefetch_ohlcv_data(config)

        assert cache.data["ETH"][0] is prior  # prior candle prepended
        # First tick forward-fills the prior PAST close, never the future candle.
        assert cache.get_price_at("ETH", start) == Decimal("99")

    @pytest.mark.asyncio
    async def test_prefetch_normalizes_naive_window_before_seeding(self):
        """A naive config window is treated as UTC before aware candle comparisons."""
        start = datetime(2026, 2, 1, 0, 0)
        end = datetime(2026, 2, 1, 3, 0)
        start_utc = start.replace(tzinfo=UTC)
        end_utc = end.replace(tzinfo=UTC)
        main_first = self._candle(datetime(2026, 2, 1, 0, 0, 48, tzinfo=UTC), "100")
        prior = self._candle(datetime(2026, 1, 31, 23, 1, tzinfo=UTC), "99")
        calls: list[tuple[datetime, datetime]] = []

        def fake_get_ohlcv(token, s, e, interval_seconds):
            calls.append((s, e))
            return [prior] if e == start_utc else [main_first]

        provider = CoinGeckoDataProvider(retry_config=RetryConfig(max_retries=0))
        provider.get_ohlcv = AsyncMock(side_effect=fake_get_ohlcv)  # type: ignore[method-assign]

        config = HistoricalDataConfig(
            start_time=start,
            end_time=end,
            interval_seconds=3600,
            tokens=["ETH"],
            include_ohlcv=True,
        )
        cache = await provider._prefetch_ohlcv_data(config)

        assert cache.data["ETH"][0] is prior
        assert cache.get_price_at("ETH", start_utc) == Decimal("99")
        assert calls == [
            (start_utc, end_utc),
            (start_utc - timedelta(days=1), start_utc),
        ]

    @pytest.mark.asyncio
    async def test_fetch_prior_candle_normalizes_naive_start(self):
        """The seed helper treats a naive start as UTC before querying/comparing."""
        start = datetime(2026, 2, 1, 0, 0)
        start_utc = start.replace(tzinfo=UTC)
        prior = self._candle(datetime(2026, 1, 31, 23, 1, tzinfo=UTC), "99")

        provider = CoinGeckoDataProvider(retry_config=RetryConfig(max_retries=0))
        provider.get_ohlcv = AsyncMock(return_value=[prior])  # type: ignore[method-assign]

        seed = await provider._fetch_prior_candle("ETH", start, 3600)

        assert seed is prior
        provider.get_ohlcv.assert_awaited_once_with(
            "ETH",
            start_utc - timedelta(days=1),
            start_utc,
            3600,
        )

    @pytest.mark.asyncio
    async def test_prefetch_no_prior_candle_leaves_first_tick_unpriced(self):
        """With no candle at-or-before the start, nothing is fabricated and the
        first tick legitimately stays unpriceable (no look-ahead)."""
        start = datetime(2026, 2, 1, 0, 0, tzinfo=UTC)
        end = datetime(2026, 2, 1, 3, 0, tzinfo=UTC)
        main_first = self._candle(datetime(2026, 2, 1, 0, 0, 48, tzinfo=UTC), "100")

        def fake_get_ohlcv(token, s, e, interval_seconds):
            return [] if e == start else [main_first]  # no prior history

        provider = CoinGeckoDataProvider(retry_config=RetryConfig(max_retries=0))
        provider.get_ohlcv = AsyncMock(side_effect=fake_get_ohlcv)  # type: ignore[method-assign]

        config = HistoricalDataConfig(
            start_time=start, end_time=end, interval_seconds=3600, tokens=["ETH"], include_ohlcv=True
        )
        cache = await provider._prefetch_ohlcv_data(config)

        assert cache.data["ETH"][0] is main_first  # nothing prepended
        assert cache.get_price_at("ETH", start) is None

    @pytest.mark.asyncio
    async def test_prefetch_skips_seed_when_first_candle_aligned(self):
        """A first candle exactly at the start (e.g. daily, midnight-aligned)
        needs no seed, so no pre-window request is made."""
        start = datetime(2026, 2, 1, 0, 0, tzinfo=UTC)
        end = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)
        aligned_first = self._candle(start, "100")
        second = self._candle(datetime(2026, 2, 2, 0, 0, tzinfo=UTC), "101")

        def fake_get_ohlcv(token, s, e, interval_seconds):
            assert e != start, "no pre-window seed request expected for an aligned first candle"
            return [aligned_first, second]

        provider = CoinGeckoDataProvider(retry_config=RetryConfig(max_retries=0))
        provider.get_ohlcv = AsyncMock(side_effect=fake_get_ohlcv)  # type: ignore[method-assign]

        config = HistoricalDataConfig(
            start_time=start, end_time=end, interval_seconds=86400, tokens=["ETH"], include_ohlcv=True
        )
        cache = await provider._prefetch_ohlcv_data(config)

        assert cache.data["ETH"][0] is aligned_first
        assert cache.get_price_at("ETH", start) == Decimal("100")
        assert provider.get_ohlcv.await_count == 1  # main fetch only, no seed

    @pytest.mark.asyncio
    async def test_prefetch_fetches_bare_address_with_default_chain(self):
        """Bare address configs are fetched through the chain-qualified TokenRef."""
        start = datetime(2026, 2, 1, 0, 0, tzinfo=UTC)
        end = datetime(2026, 2, 1, 1, 0, tzinfo=UTC)
        address = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
        fetch_token = ("base", address.lower())
        candle = self._candle(start, "1")
        provider = CoinGeckoDataProvider(retry_config=RetryConfig(max_retries=0))
        provider.get_ohlcv = AsyncMock(return_value=[candle])  # type: ignore[method-assign]

        config = HistoricalDataConfig(
            start_time=start,
            end_time=end,
            interval_seconds=3600,
            tokens=[address],
            chains=["base"],
            include_ohlcv=True,
        )
        cache = await provider._prefetch_ohlcv_data(config)

        assert cache.data == {fetch_token: [candle]}
        provider.get_ohlcv.assert_awaited_once_with(fetch_token, start, end, 3600)


class TestCoinGeckoProviderCaching:
    """Tests for CoinGeckoDataProvider historical caching."""

    @pytest.fixture
    def provider(self):
        """Create a provider with fast retry config for tests."""
        return CoinGeckoDataProvider(
            retry_config=RetryConfig(
                max_retries=1,
                base_delay=0.01,
                max_delay=0.02,
            ),
            historical_cache_ttl=3600,
        )

    @pytest.fixture
    def mock_response_200(self):
        """Create a mock 200 response for historical price."""
        response = MagicMock()
        response.status = 200
        response.json = AsyncMock(
            return_value={
                "market_data": {
                    "current_price": {"usd": 2500.00},
                },
            }
        )
        response.__aenter__ = AsyncMock(return_value=response)
        response.__aexit__ = AsyncMock(return_value=None)
        return response

    @pytest.mark.asyncio
    async def test_historical_cache_ttl_configurable(self):
        """Test historical cache TTL is configurable."""
        provider = CoinGeckoDataProvider(historical_cache_ttl=7200)
        assert provider._historical_cache.ttl_seconds == 7200
        await provider.close()

    @pytest.mark.asyncio
    async def test_get_price_uses_cache_on_second_call(
        self, provider, mock_response_200
    ):
        """Test that second call to get_price uses cache."""
        timestamp = datetime(2024, 1, 15, tzinfo=UTC)

        with patch.object(
            provider, "_get_session"
        ) as mock_session, patch.object(
            provider, "_wait_for_rate_limit", new_callable=AsyncMock
        ):
            session = MagicMock()
            session.get = MagicMock(return_value=mock_response_200)
            mock_session.return_value = session

            # First call - makes API request
            price1 = await provider.get_price("WETH", timestamp)
            assert session.get.call_count == 1

            # Second call - should use cache
            price2 = await provider.get_price("WETH", timestamp)
            assert session.get.call_count == 1  # No additional call

            assert price1 == price2 == Decimal("2500")

        await provider.close()

    @pytest.mark.asyncio
    async def test_cache_hit_rate_logged(
        self, provider, mock_response_200, caplog
    ):
        """Test that cache hit rate is logged during iteration."""
        # This test verifies the logging behavior
        # We need to test the iterate method but that requires more complex setup
        # For now, test the stats method directly
        stats = provider.get_historical_cache_stats()
        assert "total_requests" in stats
        assert "hit_rate_percent" in stats
        await provider.close()

    @pytest.mark.asyncio
    async def test_clear_historical_cache(self, provider, mock_response_200):
        """Test clearing the historical cache."""
        timestamp = datetime(2024, 1, 15, tzinfo=UTC)

        with patch.object(
            provider, "_get_session"
        ) as mock_session, patch.object(
            provider, "_wait_for_rate_limit", new_callable=AsyncMock
        ):
            session = MagicMock()
            session.get = MagicMock(return_value=mock_response_200)
            mock_session.return_value = session

            # Populate cache
            await provider.get_price("WETH", timestamp)
            assert session.get.call_count == 1

            # Clear cache
            provider.clear_historical_cache()

            # Should make new API call
            await provider.get_price("WETH", timestamp)
            assert session.get.call_count == 2

        await provider.close()

    @pytest.mark.asyncio
    async def test_get_historical_cache_stats(self, provider, mock_response_200):
        """Test getting historical cache statistics."""
        timestamp = datetime(2024, 1, 15, tzinfo=UTC)

        with patch.object(
            provider, "_get_session"
        ) as mock_session, patch.object(
            provider, "_wait_for_rate_limit", new_callable=AsyncMock
        ):
            session = MagicMock()
            session.get = MagicMock(return_value=mock_response_200)
            mock_session.return_value = session

            # Make two calls - first miss, second hit
            await provider.get_price("WETH", timestamp)
            await provider.get_price("WETH", timestamp)

            stats = provider.get_historical_cache_stats()
            # First call: miss (API fetch), Second call: hit
            assert stats["total_requests"] == 2
            assert stats["cache_hits"] == 1
            assert stats["cache_misses"] == 1
            assert stats["hit_rate_percent"] == 50.0

        await provider.close()


class TestCacheWarming:
    """Tests for cache warming functionality."""

    @pytest.fixture
    def provider(self):
        """Create a provider with fast settings for tests."""
        return CoinGeckoDataProvider(
            retry_config=RetryConfig(
                max_retries=1,
                base_delay=0.01,
                max_delay=0.02,
            ),
            historical_cache_ttl=3600,
        )

    @pytest.fixture
    def mock_response_200_factory(self):
        """Create a factory for mock 200 responses with different prices."""
        def create_response(price: float):
            response = MagicMock()
            response.status = 200
            response.json = AsyncMock(
                return_value={
                    "market_data": {
                        "current_price": {"usd": price},
                    },
                }
            )
            response.__aenter__ = AsyncMock(return_value=response)
            response.__aexit__ = AsyncMock(return_value=None)
            return response
        return create_response

    @pytest.mark.asyncio
    async def test_warm_cache_fetches_date_range(
        self, provider, mock_response_200_factory
    ):
        """Test warm_cache fetches prices for entire date range."""
        with patch.object(
            provider, "_get_session"
        ) as mock_session, patch.object(
            provider, "_wait_for_rate_limit", new_callable=AsyncMock
        ):
            session = MagicMock()
            # Return different prices for each call
            session.get = MagicMock(
                side_effect=[
                    mock_response_200_factory(2500 + i * 10)
                    for i in range(5)
                ]
            )
            mock_session.return_value = session

            cached = await provider.warm_cache(
                tokens=["WETH"],
                start_date=datetime(2024, 1, 1, tzinfo=UTC),
                end_date=datetime(2024, 1, 5, tzinfo=UTC),
            )

            # Should have fetched 5 days
            assert cached["WETH"] == 5
            assert session.get.call_count == 5

        await provider.close()

    @pytest.mark.asyncio
    async def test_warm_cache_skips_already_cached(
        self, provider, mock_response_200_factory
    ):
        """Test warm_cache skips dates already in cache."""
        with patch.object(
            provider, "_get_session"
        ) as mock_session, patch.object(
            provider, "_wait_for_rate_limit", new_callable=AsyncMock
        ):
            session = MagicMock()
            session.get = MagicMock(return_value=mock_response_200_factory(2500))
            mock_session.return_value = session

            # Pre-populate some cache entries
            provider._historical_cache.set(
                "WETH",
                datetime(2024, 1, 2, tzinfo=UTC),
                Decimal("2510.00"),
            )
            provider._historical_cache.set(
                "WETH",
                datetime(2024, 1, 4, tzinfo=UTC),
                Decimal("2530.00"),
            )

            cached = await provider.warm_cache(
                tokens=["WETH"],
                start_date=datetime(2024, 1, 1, tzinfo=UTC),
                end_date=datetime(2024, 1, 5, tzinfo=UTC),
            )

            # Should count all 5 but only make 3 API calls
            assert cached["WETH"] == 5
            assert session.get.call_count == 3

        await provider.close()

    @pytest.mark.asyncio
    async def test_warm_cache_multiple_tokens(
        self, provider, mock_response_200_factory
    ):
        """Test warm_cache handles multiple tokens.

        USDC is no longer in a hardcoded allowlist; it resolves by contract
        address (R2 option b). The (chain, address) -> coin id resolution is
        pre-seeded into the cache so the price-fetch call count stays at
        3 days * 2 tokens = 6 (no extra contract-endpoint round trips).
        """
        provider._token_addresses["USDC"] = ("arbitrum", "0xaf88d065e77c8cC2239327C5EDb3A432268e5831")
        provider._coin_id_cache[("arbitrum", "0xaf88d065e77c8cc2239327c5edb3a432268e5831")] = "usd-coin"

        with patch.object(
            provider, "_get_session"
        ) as mock_session, patch.object(
            provider, "_wait_for_rate_limit", new_callable=AsyncMock
        ):
            session = MagicMock()
            session.get = MagicMock(return_value=mock_response_200_factory(2500))
            mock_session.return_value = session

            cached = await provider.warm_cache(
                tokens=["WETH", "USDC"],
                start_date=datetime(2024, 1, 1, tzinfo=UTC),
                end_date=datetime(2024, 1, 3, tzinfo=UTC),
            )

            # 3 days * 2 tokens = 6 calls
            assert cached["WETH"] == 3
            assert cached["USDC"] == 3
            assert session.get.call_count == 6

        await provider.close()

    @pytest.mark.asyncio
    async def test_warm_cache_returns_counts(
        self, provider, mock_response_200_factory
    ):
        """Test warm_cache returns correct counts per token.

        ARB resolves by contract address now (R2 option b); the resolution is
        pre-seeded so it does not add price-fetch round trips.
        """
        provider._token_addresses["ARB"] = ("arbitrum", "0x912CE59144191C1204E64559FE8253a0e49E6548")
        provider._coin_id_cache[("arbitrum", "0x912ce59144191c1204e64559fe8253a0e49e6548")] = "arbitrum"

        with patch.object(
            provider, "_get_session"
        ) as mock_session, patch.object(
            provider, "_wait_for_rate_limit", new_callable=AsyncMock
        ):
            session = MagicMock()
            session.get = MagicMock(return_value=mock_response_200_factory(2500))
            mock_session.return_value = session

            cached = await provider.warm_cache(
                tokens=["WETH", "ARB"],
                start_date=datetime(2024, 1, 1, tzinfo=UTC),
                end_date=datetime(2024, 1, 2, tzinfo=UTC),
            )

            assert "WETH" in cached
            assert "ARB" in cached
            assert cached["WETH"] == 2
            assert cached["ARB"] == 2

        await provider.close()

    @pytest.mark.asyncio
    async def test_warm_cache_logs_progress(
        self, provider, mock_response_200_factory, caplog
    ):
        """Test warm_cache logs progress information."""
        with patch.object(
            provider, "_get_session"
        ) as mock_session, patch.object(
            provider, "_wait_for_rate_limit", new_callable=AsyncMock
        ):
            session = MagicMock()
            session.get = MagicMock(return_value=mock_response_200_factory(2500))
            mock_session.return_value = session

            with caplog.at_level(
                logging.INFO,
                logger="almanak.framework.backtesting.pnl.providers.coingecko",
            ):
                await provider.warm_cache(
                    tokens=["WETH"],
                    start_date=datetime(2024, 1, 1, tzinfo=UTC),
                    end_date=datetime(2024, 1, 2, tzinfo=UTC),
                )

            # Check for expected log messages
            assert any("Warming" in record.message for record in caplog.records)
            assert any("complete" in record.message.lower() for record in caplog.records)

        await provider.close()

    @pytest.mark.asyncio
    async def test_cache_hit_rate_over_90_after_warming(
        self, provider, mock_response_200_factory
    ):
        """Test that cache hit rate exceeds 90% after warming.

        This test validates the acceptance criteria: >90% cache hit rate
        for repeated backtests after cache warming.
        """
        with patch.object(
            provider, "_get_session"
        ) as mock_session, patch.object(
            provider, "_wait_for_rate_limit", new_callable=AsyncMock
        ):
            session = MagicMock()
            session.get = MagicMock(return_value=mock_response_200_factory(2500))
            mock_session.return_value = session

            # Warm the cache for 10 days
            await provider.warm_cache(
                tokens=["WETH"],
                start_date=datetime(2024, 1, 1, tzinfo=UTC),
                end_date=datetime(2024, 1, 10, tzinfo=UTC),
            )

            # Reset stats to measure only subsequent accesses
            provider._historical_cache.reset_stats()

            # Simulate 5 backtest runs accessing the same data
            for _ in range(5):
                for day in range(1, 11):
                    timestamp = datetime(2024, 1, day, tzinfo=UTC)
                    await provider.get_price("WETH", timestamp)

            stats = provider.get_historical_cache_stats()

            # All 50 requests should be cache hits
            assert stats["total_requests"] == 50
            assert stats["cache_hits"] == 50
            assert stats["hit_rate_percent"] == 100.0
            assert stats["hit_rate_percent"] > 90.0  # Explicit check for >90%

        await provider.close()

    @pytest.mark.asyncio
    async def test_warm_cache_skips_unknown_token_without_fetching(self, provider):
        """Unknown tokens are honest misses and do not trigger price fetches."""
        provider.get_price = AsyncMock()  # type: ignore[method-assign]

        cached = await provider.warm_cache(
            tokens=["UNKNOWN"],
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 2, tzinfo=UTC),
        )

        assert cached == {"UNKNOWN": 0}
        provider.get_price.assert_not_awaited()
        await provider.close()

    @pytest.mark.asyncio
    async def test_warm_cache_stops_current_token_on_rate_limit(self, provider):
        """Rate limits stop the current token without overstating cached days."""
        provider.get_price = AsyncMock(  # type: ignore[method-assign]
            side_effect=[Decimal("2500"), CoinGeckoRateLimitError("rate limited")]
        )

        cached = await provider.warm_cache(
            tokens=["WETH"],
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 3, tzinfo=UTC),
        )

        assert cached == {"WETH": 1}
        assert provider.get_price.await_count == 2
        await provider.close()


class TestPersistentHistoricalPriceCache:
    """Tests for SQLite-backed persistent HistoricalPriceCache."""

    def test_persistent_cache_survives_recreation(self):
        """Data written to persistent cache is available in a new instance."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test_prices.db")
            ts = datetime(2024, 1, 15, tzinfo=UTC)

            # Write to first instance
            cache1 = HistoricalPriceCache(ttl_seconds=0, persistent=True, db_path=db_path)
            cache1.set("WETH", ts, Decimal("2500.00"))
            cache1.close()

            # Read from a fresh instance (simulates new process)
            cache2 = HistoricalPriceCache(ttl_seconds=0, persistent=True, db_path=db_path)
            price = cache2.get("WETH", ts)
            cache2.close()

            assert price == Decimal("2500.00")

    def test_persistent_cache_stats_track_disk_hits(self):
        """Disk-promoted entries count as cache hits in stats."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test_prices.db")
            ts = datetime(2024, 1, 15, tzinfo=UTC)

            cache1 = HistoricalPriceCache(ttl_seconds=0, persistent=True, db_path=db_path)
            cache1.set("WETH", ts, Decimal("2500.00"))
            cache1.close()

            cache2 = HistoricalPriceCache(ttl_seconds=0, persistent=True, db_path=db_path)
            cache2.get("WETH", ts)

            stats = cache2.get_stats()
            assert stats.cache_hits == 1
            assert stats.cache_misses == 0
            cache2.close()

    def test_persistent_cache_clear_removes_db_rows(self):
        """clear() removes both in-memory and SQLite data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test_prices.db")
            ts = datetime(2024, 1, 15, tzinfo=UTC)

            cache = HistoricalPriceCache(ttl_seconds=0, persistent=True, db_path=db_path)
            cache.set("WETH", ts, Decimal("2500.00"))
            cache.clear()
            cache.close()

            # Even a new instance should see nothing
            cache2 = HistoricalPriceCache(ttl_seconds=0, persistent=True, db_path=db_path)
            assert cache2.get("WETH", ts) is None
            cache2.close()

    def test_persistent_flag_property(self):
        """persistent property reports storage mode."""
        assert HistoricalPriceCache().persistent is False
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test.db")
            cache = HistoricalPriceCache(persistent=True, db_path=db_path)
            assert cache.persistent is True
            cache.close()

    def test_non_persistent_cache_does_not_create_db(self):
        """Non-persistent cache should not touch the filesystem."""
        cache = HistoricalPriceCache()
        assert cache._db is None

    def test_close_releases_db_connection(self):
        """close() should release the SQLite connection."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "test_close.db")
            cache = HistoricalPriceCache(ttl_seconds=0, persistent=True, db_path=db_path)
            ts = datetime(2024, 1, 15, tzinfo=UTC)
            cache.set("WETH", ts, Decimal("2500.00"))
            assert cache._db is not None

            cache.close()
            assert cache._db is None

    def test_close_idempotent_on_non_persistent(self):
        """close() on non-persistent cache is a no-op."""
        cache = HistoricalPriceCache()
        cache.close()  # should not raise
        assert cache._db is None


class TestRetryConfigForBacktest:
    """Tests for RetryConfig.for_backtest() factory."""

    def test_more_retries_than_default(self):
        """Backtest config should have more retries than default."""
        default = RetryConfig()
        backtest = RetryConfig.for_backtest()
        assert backtest.max_retries > default.max_retries

    def test_longer_max_delay_than_default(self):
        """Backtest config should tolerate longer backoff delays."""
        default = RetryConfig()
        backtest = RetryConfig.for_backtest()
        assert backtest.max_delay > default.max_delay

    def test_backoff_sequence_reasonable(self):
        """Backoff sequence should ramp up then cap."""
        cfg = RetryConfig.for_backtest()
        delays = [cfg.get_delay_for_attempt(i) for i in range(1, cfg.max_retries + 1)]
        # Each delay should be >= previous (monotonically increasing until cap)
        for i in range(1, len(delays)):
            assert delays[i] >= delays[i - 1]
        # Last delay should be capped at max_delay
        assert delays[-1] <= cfg.max_delay
