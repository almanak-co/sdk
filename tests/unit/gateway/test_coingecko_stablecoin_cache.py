"""Tests for CoinGecko stablecoin cache TTL optimization (VIB-431).

Stablecoins (USDC, USDT, DAI, etc.) use a longer cache TTL than volatile
tokens since their prices are ~$1.00 and rarely change. This reduces
unnecessary CoinGecko API calls and preserves rate limit budget.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest

from almanak.gateway.data.price.coingecko import CacheEntry, CoinGeckoPriceSource


@pytest.fixture
def source():
    """Create a CoinGecko source with 30s default TTL."""
    return CoinGeckoPriceSource(api_key="test", cache_ttl=30)


class TestStablecoinCacheTTL:
    """Test that stablecoins get a longer cache TTL."""

    def test_stablecoin_ttl_is_10x_default(self, source):
        """Stablecoins should use 10x the default TTL."""
        assert source._get_cache_ttl_for_token("USDC") == 300
        assert source._get_cache_ttl_for_token("USDT") == 300
        assert source._get_cache_ttl_for_token("DAI") == 300

    def test_volatile_token_ttl_is_default(self, source):
        """Non-stablecoin tokens should use the default TTL."""
        assert source._get_cache_ttl_for_token("WETH") == 30
        assert source._get_cache_ttl_for_token("WBTC") == 30
        assert source._get_cache_ttl_for_token("AVAX") == 30

    def test_stablecoin_check_is_case_insensitive(self, source):
        """Token matching should be case-insensitive."""
        assert source._get_cache_ttl_for_token("usdc") == 300
        assert source._get_cache_ttl_for_token("Usdt") == 300

    def test_all_known_stablecoins_get_long_ttl(self, source):
        """All stablecoins from core constants should get the longer TTL."""
        from almanak.core.constants import STABLECOINS

        for stablecoin in STABLECOINS:
            ttl = source._get_cache_ttl_for_token(stablecoin)
            assert ttl == 300, f"{stablecoin} should get stablecoin TTL (300s), got {ttl}s"

    def test_stablecoin_cache_survives_past_default_ttl(self, source):
        """Cached stablecoin entry at 60s old should still be valid (within 300s)."""
        from almanak.framework.data.interfaces import PriceResult

        fake_result = PriceResult(
            price=1.0,
            confidence=0.95,
            source="coingecko",
            timestamp=datetime.now(UTC),
        )
        # Manually insert a cache entry that is 60s old
        source._cache["USDC/USD"] = CacheEntry(
            result=fake_result,
            cached_at=datetime.now(UTC) - timedelta(seconds=60),
            fetch_latency_ms=50.0,
        )

        # Should still be cached (60s < 300s stablecoin TTL)
        cached = source._get_cached("USDC", "USD")
        assert cached is not None, "60s-old stablecoin cache should still be valid"

    def test_volatile_token_cache_expires_at_default_ttl(self, source):
        """Cached volatile token at 60s old should be expired (past 30s TTL)."""
        from almanak.framework.data.interfaces import PriceResult

        fake_result = PriceResult(
            price=2500.0,
            confidence=0.95,
            source="coingecko",
            timestamp=datetime.now(UTC),
        )
        source._cache["WETH/USD"] = CacheEntry(
            result=fake_result,
            cached_at=datetime.now(UTC) - timedelta(seconds=60),
            fetch_latency_ms=50.0,
        )

        # Should be expired (60s > 30s default TTL)
        cached = source._get_cached("WETH", "USD")
        assert cached is None, "60s-old volatile token cache should be expired"

    def test_custom_cache_ttl_scales_stablecoin_multiplier(self):
        """Custom cache_ttl should also scale for stablecoins."""
        source = CoinGeckoPriceSource(api_key="test", cache_ttl=60)
        assert source._get_cache_ttl_for_token("USDC") == 600  # 60 * 10
        assert source._get_cache_ttl_for_token("WETH") == 60

    def test_non_usd_quote_uses_default_ttl(self, source):
        """Stablecoin with non-USD quote should use default TTL.

        Cross-rates like USDC/ETH move with the quote leg, so the
        stablecoin multiplier should NOT apply.
        """
        assert source._get_cache_ttl_for_token("USDC", quote="ETH") == 30
        assert source._get_cache_ttl_for_token("USDT", quote="EUR") == 30
        # USD quote still gets the multiplier
        assert source._get_cache_ttl_for_token("USDC", quote="USD") == 300
