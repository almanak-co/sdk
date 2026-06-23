"""Tests for BinancePriceSource dynamic token resolution (VIB-645)."""

import time
from unittest.mock import AsyncMock, MagicMock, patch
from urllib.parse import parse_qs, urlparse

import pytest

from almanak.framework.data.models import CEX_SYMBOL_MAP
from almanak.gateway.data.price.binance import (
    BinancePriceSource,
    _NEGATIVE_CACHE_TTL,
    _TOKEN_TO_BINANCE_SYMBOL,
    _curated_binance_symbol,
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

    # NOTE: on the PRICE path only CBBTC routes through the new CEX_SYMBOL_MAP
    # lookup — DAI short-circuits as a stablecoin, and GMX/PENDLE/BTCB are
    # already in the local static table. (The OHLCV provider, where all 5 were
    # missing, is covered token-by-token in test_gateway_provider_tokens.py.)
    # This parametrization asserts each non-stablecoin BTC/proxy token resolves
    # to its expected pair at full confidence WITHOUT dynamic probing.
    @pytest.mark.asyncio()
    @pytest.mark.parametrize(
        ("token", "expected_pair"),
        [
            ("CBBTC", "BTCUSDT"),  # NOT in static table -> resolved via CEX_SYMBOL_MAP (the fix)
            ("BTCB", "BTCUSDT"),  # static table BTC proxy
            ("GMX", "GMXUSDT"),  # static table
            ("PENDLE", "PENDLEUSDT"),  # static table
        ],
    )
    async def test_proxy_tokens_resolve_full_confidence_not_dynamic(self, source, token, expected_pair):
        """Curated tokens resolve to their Binance pair at full confidence via the
        static table or CEX_SYMBOL_MAP — never via dynamic probing. CBBTC in
        particular (absent from the static table, no CBBTCUSDT pair) proves the
        CEX_SYMBOL_MAP fix, since dynamic resolution is asserted unreachable."""
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={"price": "64000.0"})
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_resp)

        # Dynamic resolution must NOT be reached — static/CEX resolves first.
        source._resolve_binance_symbol = AsyncMock(side_effect=AssertionError(f"dynamic resolution should not run for {token}"))

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
    async def test_cbbtc_absent_from_local_static_table(self):
        """Guards the exact gap the fix closes: CBBTC must be resolved via the
        canonical CEX_SYMBOL_MAP, not the local table (which lacks it)."""
        assert "CBBTC" not in _TOKEN_TO_BINANCE_SYMBOL

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


class TestPriceCanonicalParity:
    """``_curated_binance_symbol`` (the spot-price resolver) consults the
    canonical ``CEX_SYMBOL_MAP`` first, mirroring the OHLCV providers. These
    guards keep the price path from drifting from the canonical source and
    ensure it can never resolve a token to a different pair than the OHLCV
    path does."""

    @staticmethod
    def _cex_binance_bases() -> set[str]:
        return {base for (exch, base, _quote) in CEX_SYMBOL_MAP if exch == "binance"}

    @staticmethod
    def _cex_resolve(base: str) -> str | None:
        for quote in ("USDT", "USDC"):
            mapped = CEX_SYMBOL_MAP.get(("binance", base, quote))
            if mapped:
                return mapped
        return None

    def test_every_canonical_binance_base_is_resolvable(self):
        """Drift guard: every Binance base in ``CEX_SYMBOL_MAP`` must resolve via
        the price path too (the price-path analogue of the OHLCV drift guard)."""
        unresolved = sorted(b for b in self._cex_binance_bases() if _curated_binance_symbol(b) is None)
        assert unresolved == [], f"price-path Binance bases not resolvable: {unresolved}"

    def test_local_table_does_not_disagree_with_canonical(self):
        """Ordering-safety: the price local table may not map any token to a
        different pair than ``CEX_SYMBOL_MAP``, so canonical-first (price + OHLCV
        now share the ordering) is provably equivalent and the paths can't
        diverge."""
        conflicts = {
            base: (local, self._cex_resolve(base))
            for base, local in _TOKEN_TO_BINANCE_SYMBOL.items()
            if self._cex_resolve(base) is not None and self._cex_resolve(base) != local
        }
        assert conflicts == {}, f"price local table disagrees with CEX_SYMBOL_MAP: {conflicts}"

    def test_cbbtc_resolves_via_canonical(self):
        """CBBTC (absent from the local table, no CBBTCUSDT pair) resolves to the
        BTC proxy via the canonical map — the exact gap the fix closes."""
        assert _curated_binance_symbol("CBBTC") == "BTCUSDT"
