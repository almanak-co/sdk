"""Tests for BinancePriceSource dynamic token resolution (VIB-645)."""

import time
from unittest.mock import AsyncMock, MagicMock, patch
from urllib.parse import parse_qs, urlparse

import pytest

from almanak.gateway.data.ohlcv.binance_provider import BINANCE_SYMBOL_MAP
from almanak.gateway.data.price.binance import (
    _NEGATIVE_CACHE_TTL,
    _TOKEN_TO_BINANCE_SYMBOL,
    BinancePriceSource,
)
from almanak.integrations.binance.integration import BINANCE_UNSAFE_MARKET_BASES, INTEGRATION


class TestBinanceDynamicResolution:
    """Test dynamic Binance symbol resolution for the gateway price source."""

    @pytest.fixture()
    def source(self):
        return BinancePriceSource(cache_ttl=30, request_timeout=5.0)

    def test_static_map_used_first(self, source):
        """Static map tokens should be used without dynamic resolution."""
        assert "ETH" in _TOKEN_TO_BINANCE_SYMBOL
        assert "WETH" in _TOKEN_TO_BINANCE_SYMBOL

    @pytest.mark.parametrize("token", ["POL", "MATIC", "WMATIC", "WPOL"])
    def test_polygon_native_uses_pol_pair(self, token):
        """Polygon native must price off the live POLUSDT pair, not the dead
        MATICUSDT ghost listing (which returns ~4x the real price post-rebrand).
        """
        assert _TOKEN_TO_BINANCE_SYMBOL[token] == "POLUSDT"
        assert _TOKEN_TO_BINANCE_SYMBOL[token] != "MATICUSDT"

    @pytest.mark.parametrize("token", ["BTC", "WBTC", "BTCB"])
    def test_btc_family_uses_btcusdt_pair(self, token):
        """BSC's wrapper is BTCB (Binance-Peg BTC, 18 decimals — PR #2505).
        It MUST share the BTCUSDT spot pair with WBTC; a different mapping
        on BSC would silently fork BTC pricing per chain."""
        assert _TOKEN_TO_BINANCE_SYMBOL[token] == "BTCUSDT"

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

    # The execution-grade spot-price catalogue is deliberately independent of
    # the broader OHLCV/instrument map. Approved proxies never probe dynamically.
    @pytest.mark.asyncio()
    @pytest.mark.parametrize(
        ("token", "expected_pair"),
        [
            ("CBBTC", "BTCUSDT"),
            ("BTCB", "BTCUSDT"),  # static table BTC proxy
            ("GMX", "GMXUSDT"),  # static table
            ("PENDLE", "PENDLEUSDT"),  # static table
        ],
    )
    async def test_proxy_tokens_resolve_full_confidence_not_dynamic(self, source, token, expected_pair):
        """Curated tokens resolve to their Binance pair at full confidence via the
        reviewed spot-price table — never via dynamic probing."""
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={"price": "64000.0"})
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_resp)

        # Dynamic resolution must NOT be reached — static/CEX resolves first.
        source._resolve_binance_symbol = AsyncMock(
            side_effect=AssertionError(f"dynamic resolution should not run for {token}")
        )

        with patch.object(source, "_get_session", return_value=mock_session):
            result = await source.get_price(token)

        assert result.confidence == 1.0  # curated mapping → full confidence
        # Parse the ``symbol`` query param rather than a loose URL substring, so
        # the assertion can't false-pass on a longer pair that merely contains
        # ``expected_pair`` (e.g. "BTCUSDT" ⊂ "BTCUSDTX").
        called_url = mock_session.get.call_args[0][0]
        symbol_param = parse_qs(urlparse(called_url).query).get("symbol", [None])[0]
        assert symbol_param == expected_pair

    @pytest.mark.asyncio()
    async def test_cbbtc_is_explicitly_curated(self):
        assert _TOKEN_TO_BINANCE_SYMBOL["CBBTC"] == "BTCUSDT"

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


class TestPriceCatalogueIsolation:
    def test_execution_grade_catalogue_is_exact_and_independent(self):
        expected = {
            "ETH",
            "WETH",
            "BTC",
            "WBTC",
            "BTCB",
            "CBBTC",
            "SOL",
            "ARB",
            "AVAX",
            "WAVAX",
            "POL",
            "MATIC",
            "WMATIC",
            "WPOL",
            "BNB",
            "WBNB",
            "LINK",
            "UNI",
            "AAVE",
            "OP",
            "GMX",
            "CRV",
            "PENDLE",
            "LDO",
            "S",
            "WS",
            "DOGE",
            "CAKE",
            "JOE",
            "OKB",
            "WOKB",
            "XETH",
            "XBTC",
        }
        assert set(_TOKEN_TO_BINANCE_SYMBOL) == expected
        assert "FTM" not in _TOKEN_TO_BINANCE_SYMBOL
        assert "RNDR" not in _TOKEN_TO_BINANCE_SYMBOL

    def test_price_and_ohlcv_catalogues_share_safe_pairs_only(self):
        manifest_usdt = {
            base: symbol
            for (base, quote), symbol in (INTEGRATION.market_symbols or {}).items()
            if quote == "USDT" and base not in BINANCE_UNSAFE_MARKET_BASES
        }
        overlap = set(_TOKEN_TO_BINANCE_SYMBOL) & set(BINANCE_SYMBOL_MAP)

        assert BINANCE_UNSAFE_MARKET_BASES.isdisjoint(_TOKEN_TO_BINANCE_SYMBOL)
        assert BINANCE_UNSAFE_MARKET_BASES.isdisjoint(BINANCE_SYMBOL_MAP)
        assert {base: _TOKEN_TO_BINANCE_SYMBOL[base] for base in overlap} == {
            base: BINANCE_SYMBOL_MAP[base] for base in overlap
        }
        assert BINANCE_SYMBOL_MAP == manifest_usdt

    @pytest.mark.asyncio
    @pytest.mark.parametrize("token", ["FTM", "RNDR"])
    async def test_retired_symbols_never_probe_ghost_tickers(self, token):
        source = BinancePriceSource()
        source._resolve_binance_symbol = AsyncMock(side_effect=AssertionError("must not probe"))
        from almanak.framework.data.interfaces import DataSourceUnavailable

        with pytest.raises(DataSourceUnavailable, match="not approved"):
            await source.get_price(token)
        source._resolve_binance_symbol.assert_not_awaited()
