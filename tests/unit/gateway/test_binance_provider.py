"""Tests for BinanceOHLCVProvider symbol resolution and dynamic resolution."""

import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.data.models import CEX_SYMBOL_MAP
from almanak.gateway.data.ohlcv.binance_provider import (
    BINANCE_SYMBOL_MAP,
    BinanceOHLCVProvider,
)


def _cex_binance_bases() -> set[str]:
    return {base for (exch, base, _quote) in CEX_SYMBOL_MAP if exch == "binance"}


def _cex_resolve(base: str) -> str | None:
    for quote in ("USDT", "USDC"):
        mapped = CEX_SYMBOL_MAP.get(("binance", base, quote))
        if mapped:
            return mapped
    return None


class TestBinanceSymbolMap:
    """Test that BINANCE_SYMBOL_MAP contains all expected token mappings."""

    @pytest.fixture()
    def provider(self):
        return BinanceOHLCVProvider()

    @pytest.mark.parametrize(
        ("token", "expected_pair"),
        [
            # Wrapped native tokens -- the bug fix
            ("WBNB", "BNBUSDT"),
            ("BNB", "BNBUSDT"),
            ("WAVAX", "AVAXUSDT"),
            ("WMATIC", "POLUSDT"),  # MATIC->POL rebrand: POLUSDT is the live pair
            ("WPOL", "POLUSDT"),
            ("S", "SUSDT"),
            ("WS", "SUSDT"),
            # Pre-existing mappings (regression guard)
            ("WETH", "ETHUSDT"),
            ("ETH", "ETHUSDT"),
            ("AVAX", "AVAXUSDT"),
            ("MATIC", "POLUSDT"),  # MATIC->POL rebrand: POLUSDT is the live pair
            ("BTC", "BTCUSDT"),
            ("WBTC", "BTCUSDT"),
            # BSC's BTC wrapper is BTCB (Binance-Peg BTC, 18 decimals).
            # The OHLCV path must map it to the same BTCUSDT spot pair as
            # WBTC so price/OHLCV requests on BSC don't fall through to a
            # different feed. (PR #2505 / btcb-decimals-bsc.)
            ("BTCB", "BTCUSDT"),
        ],
    )
    def test_symbol_map_contains_token(self, token: str, expected_pair: str):
        """Each supported token must resolve to the correct Binance pair."""
        assert BINANCE_SYMBOL_MAP.get(token) == expected_pair

    @pytest.mark.parametrize(
        ("token", "expected_pair"),
        [
            ("wbnb", "BNBUSDT"),
            ("Wbnb", "BNBUSDT"),
            ("bnb", "BNBUSDT"),
            ("wavax", "AVAXUSDT"),
        ],
    )
    def test_resolve_symbol_case_insensitive(self, provider: BinanceOHLCVProvider, token: str, expected_pair: str):
        """_resolve_symbol should be case-insensitive."""
        assert provider._resolve_symbol(token) == expected_pair

    def test_resolve_symbol_returns_none_for_unknown(self, provider: BinanceOHLCVProvider):
        """Unknown tokens should return None, not raise."""
        assert provider._resolve_symbol("NONEXISTENT_TOKEN_XYZ") is None

    def test_wrapped_native_consistency(self):
        """Every wrapped native and its unwrapped form should map to the same pair."""
        pairs = [
            ("WETH", "ETH"),
            ("WBNB", "BNB"),
            ("WAVAX", "AVAX"),
            ("WMATIC", "MATIC"),
            ("WS", "S"),
        ]
        for wrapped, unwrapped in pairs:
            assert BINANCE_SYMBOL_MAP[wrapped] == BINANCE_SYMBOL_MAP[unwrapped], (
                f"{wrapped} and {unwrapped} should map to the same Binance pair"
            )


class TestDynamicSymbolResolution:
    """Test dynamic Binance symbol resolution (VIB-645)."""

    @pytest.fixture()
    def provider(self):
        return BinanceOHLCVProvider()

    @pytest.mark.asyncio()
    async def test_dynamic_resolve_finds_usdt_pair(self, provider):
        """Dynamic resolution should find a {TOKEN}USDT pair."""
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={"price": "1.5"})
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_resp)

        provider._session = mock_session
        provider._session_loop = None  # Skip loop check

        with patch.object(provider, "_get_session", return_value=mock_session):
            result = await provider._resolve_symbol_dynamic("NEWTOKEN")

        assert result == "NEWTOKENUSDT"
        assert provider._dynamic_symbol_cache["NEWTOKEN"] == "NEWTOKENUSDT"

    @pytest.mark.asyncio()
    async def test_dynamic_resolve_negative_cache(self, provider):
        """Failed dynamic resolution should be negative-cached."""
        mock_resp = AsyncMock()
        mock_resp.status = 400  # Not found
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_resp)

        with patch.object(provider, "_get_session", return_value=mock_session):
            result = await provider._resolve_symbol_dynamic("BADTOKEN")

        assert result is None
        assert "BADTOKEN" in provider._negative_cache

    @pytest.mark.asyncio()
    async def test_dynamic_resolve_uses_negative_cache(self, provider):
        """Negative-cached tokens should skip API calls."""
        provider._negative_cache["BADTOKEN"] = time.time()  # Just cached

        mock_session = MagicMock()
        mock_session.get = MagicMock()

        with patch.object(provider, "_get_session", return_value=mock_session):
            result = await provider._resolve_symbol_dynamic("BADTOKEN")

        assert result is None
        mock_session.get.assert_not_called()

    @pytest.mark.asyncio()
    async def test_dynamic_resolve_expired_negative_cache(self, provider):
        """Expired negative cache should re-probe."""
        provider._negative_cache["OLDTOKEN"] = time.time() - (5 * 3600)  # 5h ago (> 4h TTL)

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value={"price": "2.0"})
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_resp)

        with patch.object(provider, "_get_session", return_value=mock_session):
            result = await provider._resolve_symbol_dynamic("OLDTOKEN")

        assert result == "OLDTOKENUSDT"

    @pytest.mark.asyncio()
    async def test_dynamic_resolve_uses_cached_result(self, provider):
        """Already-resolved dynamic symbols should be returned from cache."""
        provider._dynamic_symbol_cache["CACHED"] = "CACHEDUSDT"

        mock_session = MagicMock()
        with patch.object(provider, "_get_session", return_value=mock_session):
            result = await provider._resolve_symbol_dynamic("CACHED")

        assert result == "CACHEDUSDT"
        mock_session.get.assert_not_called()


class TestThirdTableCanonicalParity:
    """``BinanceOHLCVProvider`` is the THIRD Binance symbol table (the
    backtesting / paper path). It now resolves through the canonical
    ``CEX_SYMBOL_MAP`` first, so it can never silently drift from the OHLCV /
    price providers again — the drift that left CBBTC/PENDLE unresolvable here
    (Binance skipped → sparse CoinGecko fallback → realized-vol failed)."""

    @pytest.fixture()
    def provider(self) -> BinanceOHLCVProvider:
        return BinanceOHLCVProvider()

    @pytest.mark.parametrize(
        "token,expected",
        [
            ("CBBTC", "BTCUSDT"),  # was MISSING from the local table → unresolvable
            ("PENDLE", "PENDLEUSDT"),  # was MISSING from the local table
            ("DAI", "DAIUSDT"),
            ("GMX", "GMXUSDT"),
            ("BTCB", "BTCUSDT"),
            ("WBTC", "BTCUSDT"),  # already local → still resolves (fallback path)
        ],
    )
    def test_resolves_via_canonical_then_local(self, provider, token, expected):
        assert provider._resolve_symbol(token) == expected
        assert provider._resolve_symbol(token.lower()) == expected  # case-insensitive

    def test_every_canonical_binance_base_is_resolvable(self, provider):
        """Drift guard: every Binance base in ``CEX_SYMBOL_MAP`` must resolve via
        this third table, so it can never silently diverge from the canonical
        source (the CBBTC/PENDLE gap)."""
        unresolved = sorted(b for b in _cex_binance_bases() if provider._resolve_symbol(b) is None)
        assert unresolved == [], f"third-table Binance bases not resolvable: {unresolved}"

    def test_local_table_does_not_disagree_with_canonical(self):
        """Ordering-safety: no token may map to a DIFFERENT pair in the local
        ``BINANCE_SYMBOL_MAP`` than in ``CEX_SYMBOL_MAP``. While they agree,
        canonical-first vs local-first ordering cannot change any result — so the
        three Binance tables stay coherent."""
        conflicts = {
            base: (local, _cex_resolve(base))
            for base, local in BINANCE_SYMBOL_MAP.items()
            if _cex_resolve(base) is not None and _cex_resolve(base) != local
        }
        assert conflicts == {}, f"local table disagrees with CEX_SYMBOL_MAP: {conflicts}"
