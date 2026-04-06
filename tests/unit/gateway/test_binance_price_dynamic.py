"""Tests for BinancePriceSource dynamic token resolution (VIB-645)."""

import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.gateway.data.price.binance import (
    BinancePriceSource,
    _NEGATIVE_CACHE_TTL,
    _TOKEN_TO_BINANCE_SYMBOL,
)


class TestBinanceDynamicResolution:
    """Test dynamic Binance symbol resolution for the gateway price source."""

    @pytest.fixture()
    def source(self):
        return BinancePriceSource(cache_ttl=30, request_timeout=5.0)

    def test_static_map_used_first(self, source):
        """Static map tokens should be used without dynamic resolution."""
        assert "ETH" in _TOKEN_TO_BINANCE_SYMBOL
        assert "WETH" in _TOKEN_TO_BINANCE_SYMBOL

    @pytest.mark.asyncio()
    async def test_dynamic_resolve_finds_usdt_pair(self, source):
        """Dynamic resolution should find {TOKEN}USDT pair."""
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={"price": "42.50"})
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_resp)

        with patch.object(source, "_get_session", return_value=mock_session):
            result = await source._resolve_binance_symbol("NEWTOKEN")

        assert result == "NEWTOKENUSDT"
        assert source._dynamic_symbol_cache["NEWTOKEN"] == "NEWTOKENUSDT"

    @pytest.mark.asyncio()
    async def test_dynamic_resolve_returns_none_when_not_found(self, source):
        """Dynamic resolution should return None when no pair exists."""
        mock_resp = AsyncMock()
        mock_resp.status = 400
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_resp)

        with patch.object(source, "_get_session", return_value=mock_session):
            result = await source._resolve_binance_symbol("DOESNOTEXIST")

        assert result is None

    @pytest.mark.asyncio()
    async def test_negative_cache_prevents_repeated_lookups(self, source):
        """Negative-cached tokens should not hit the API again."""
        source._negative_cache["BADTOKEN"] = time.time()

        from almanak.framework.data.interfaces import DataSourceUnavailable

        with pytest.raises(DataSourceUnavailable, match="negative-cached"):
            await source.get_price("BADTOKEN")

    @pytest.mark.asyncio()
    async def test_negative_cache_expires(self, source):
        """Expired negative cache entries should re-probe."""
        source._negative_cache["OLDTOKEN"] = time.time() - _NEGATIVE_CACHE_TTL - 100

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={"price": "10.0"})
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_resp)

        with patch.object(source, "_get_session", return_value=mock_session):
            result = await source.get_price("OLDTOKEN")

        assert result.price > 0
        assert source._dynamic_symbol_cache["OLDTOKEN"] == "OLDTOKENUSDT"

    @pytest.mark.asyncio()
    async def test_dynamic_cache_used_on_subsequent_calls(self, source):
        """Dynamically resolved symbols should be cached for future calls."""
        source._dynamic_symbol_cache["CACHED"] = "CACHEDUSDT"

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={"price": "5.0"})
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_resp)

        with patch.object(source, "_get_session", return_value=mock_session):
            result = await source.get_price("CACHED")

        assert result.price > 0
        assert result.confidence == 0.9  # Dynamic resolution gets lower confidence

    @pytest.mark.asyncio()
    async def test_stablecoins_bypass_dynamic_resolution(self, source):
        """Stablecoins should always return $1 without any API call."""
        result = await source.get_price("USDC")
        assert result.price == 1
        assert result.confidence == 1.0

    @pytest.mark.asyncio()
    async def test_static_tokens_get_full_confidence(self, source):
        """Tokens in the static map should have confidence=1.0."""
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={"price": "3000.0"})
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_resp)

        with patch.object(source, "_get_session", return_value=mock_session):
            result = await source.get_price("ETH")

        assert result.confidence == 1.0

    @pytest.mark.asyncio()
    async def test_evict_dynamic_cache_on_api_error(self, source):
        """Dynamic cache entries should be evicted if the API returns errors."""
        source._dynamic_symbol_cache["DELISTED"] = "DELISTEDUSDT"

        mock_resp = AsyncMock()
        mock_resp.status = 400
        mock_resp.text = AsyncMock(return_value="Invalid symbol")
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_resp)

        from almanak.framework.data.interfaces import DataSourceUnavailable

        with patch.object(source, "_get_session", return_value=mock_session):
            with pytest.raises(DataSourceUnavailable):
                await source.get_price("DELISTED")

        assert "DELISTED" not in source._dynamic_symbol_cache
