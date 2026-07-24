"""Unit tests for OHLCVCache filesystem fallback logic."""

from datetime import datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pytest

from almanak.framework.data.cache.ohlcv_cache import OHLCVCache
from almanak.framework.data.interfaces import OHLCVCandle


class TestOHLCVCacheFilesystemFallback:
    """Tests for OHLCVCache filesystem fallback when home dir is not writable."""

    def test_default_path_uses_home_dir(self, tmp_path):
        """Default (None) resolves to ~/.almanak/cache/ohlcv_cache.db."""
        fake_home = tmp_path / "home"
        expected = str(fake_home / ".almanak" / "cache" / "ohlcv_cache.db")
        with patch.object(Path, "home", return_value=fake_home):
            cache = OHLCVCache()
        assert cache.db_path == expected

    def test_fallback_to_tmp_when_home_not_writable(self):
        """Falls back to /tmp when home directory mkdir raises OSError."""
        original_mkdir = Path.mkdir

        def selective_mkdir(self, *args, **kwargs):
            if ".almanak" in str(self) and "/tmp" not in str(self):
                raise OSError("Read-only file system")
            return original_mkdir(self, *args, **kwargs)

        with patch.object(Path, "mkdir", selective_mkdir):
            cache = OHLCVCache()
        assert "/tmp/.almanak/cache/ohlcv_cache.db" in cache.db_path

    def test_explicit_path_bypasses_fallback(self):
        """Explicit db_path is used directly without fallback."""
        cache = OHLCVCache(db_path=":memory:")
        assert cache.db_path == ":memory:"


def _candle(hour: int) -> OHLCVCandle:
    return OHLCVCandle(
        timestamp=datetime(2024, 1, 1, hour),
        open=Decimal("100"),
        high=Decimal("110"),
        low=Decimal("90"),
        close=Decimal("105"),
        volume=Decimal("1000"),
    )


class TestOHLCVCacheCount:
    """Tests for OHLCVCache.count() filter combinations."""

    @pytest.fixture
    def cache(self, tmp_path):
        cache = OHLCVCache(db_path=str(tmp_path / "ohlcv_cache.db"))
        cache.store_candles([_candle(0), _candle(1)], "ETH", "USD", "1h", "ethereum")
        cache.store_candles([_candle(0)], "ETH", "USD", "1d", "ethereum")
        cache.store_candles([_candle(0)], "ETH", "USDT", "1h", "ethereum")
        cache.store_candles([_candle(0)], "ARB", "USD", "1h", "arbitrum")
        return cache

    def test_count_all(self, cache):
        assert cache.count() == 5

    def test_count_by_token(self, cache):
        assert cache.count(token="ETH") == 4
        assert cache.count(token="ARB") == 1

    def test_count_by_quote(self, cache):
        assert cache.count(quote="USD") == 4
        assert cache.count(quote="USDT") == 1

    def test_count_by_timeframe(self, cache):
        assert cache.count(timeframe="1h") == 4
        assert cache.count(timeframe="1d") == 1

    def test_count_by_chain(self, cache):
        assert cache.count(chain="ethereum") == 4
        assert cache.count(chain="arbitrum") == 1

    def test_count_with_all_filters(self, cache):
        assert cache.count(token="ETH", quote="USD", timeframe="1h", chain="ethereum") == 2

    def test_count_no_match_is_zero(self, cache):
        assert cache.count(token="ETH", quote="USD", timeframe="1h", chain="arbitrum") == 0

    def test_count_reflects_clear(self, cache):
        cache.clear(token="ETH", timeframe="1h")
        assert cache.count(token="ETH", timeframe="1h") == 0
        assert cache.count() == 2  # 1d ETH row + ARB row survive
