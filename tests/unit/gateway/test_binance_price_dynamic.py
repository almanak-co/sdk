"""Tests for BinancePriceSource dynamic token resolution (VIB-645, ALM-3185)."""

import time
from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock, patch
from urllib.parse import parse_qs, urlparse

import pytest

from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.framework.data.tokens.models import ResolvedToken
from almanak.gateway.data.ohlcv.binance_provider import BINANCE_SYMBOL_MAP
from almanak.gateway.data.price.binance import (
    _NEGATIVE_CACHE_TTL,
    _TOKEN_TO_BINANCE_SYMBOL,
    BinancePriceSource,
)
from almanak.integrations.binance.gateway import price_source as binance_price_source
from almanak.integrations.binance.integration import BINANCE_UNSAFE_MARKET_BASES, INTEGRATION


@dataclass(frozen=True)
class _StubIdentity:
    """Minimal stand-in for ``ResolvedToken`` — the gate reads only the CG id."""

    coingecko_id: str | None


USDC_RESOLVED = ResolvedToken(
    symbol="USDC",
    address="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
    decimals=6,
    chain="ethereum",
    chain_id=1,
    is_stablecoin=True,
)


def _mock_session(*, status: int = 200, price: str = "42.50") -> MagicMock:
    """A session whose every ticker request answers with ``price``.

    Deliberately permissive: it answers for ANY symbol, which is what makes the
    gate tests real. A probe that escapes the gate finds a "listed" pair and
    returns a confident price — exactly the same-ticker-stranger failure the
    gate exists to stop.
    """
    resp = AsyncMock()
    resp.status = status
    resp.json = AsyncMock(return_value={"price": price})
    resp.text = AsyncMock(return_value="Invalid symbol")
    resp.__aenter__ = AsyncMock(return_value=resp)
    resp.__aexit__ = AsyncMock(return_value=False)

    session = MagicMock()
    session.get = MagicMock(return_value=resp)
    return session


def _probed_symbols(session: MagicMock) -> list[str]:
    """Return the ``symbol`` query param of every ticker request made."""
    return [parse_qs(urlparse(call[0][0]).query).get("symbol", [None])[0] for call in session.get.call_args_list]


@pytest.fixture()
def corroborate(monkeypatch):
    """Install reviewed CoinGecko-id -> Binance-base links for one test."""

    def _install(links: dict[str, str]) -> None:
        monkeypatch.setattr(binance_price_source, "_COINGECKO_ID_TO_BINANCE_BASE", dict(links))

    return _install


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
    async def test_negative_cache_prevents_repeated_lookups(self, source, corroborate):
        """Negative-cached bases should not hit the API again.

        ALM-3185: the negative cache lives *behind* the corroboration gate, so
        the token needs a reviewed identity link to reach it at all.
        """
        corroborate({"bad-token": "BADTOKEN"})
        source._negative_cache["BADTOKEN"] = time.time()

        with pytest.raises(DataSourceUnavailable, match="negative-cached"):
            await source.get_price("BADTOKEN", resolved_token=_StubIdentity("bad-token"))

    @pytest.mark.asyncio()
    async def test_negative_cache_expires(self, source, corroborate):
        """Expired negative cache entries should re-probe."""
        corroborate({"old-token": "OLDTOKEN"})
        source._negative_cache["OLDTOKEN"] = time.time() - _NEGATIVE_CACHE_TTL - 100

        mock_session = _mock_session(price="10.0")

        with patch.object(source, "_get_session", return_value=mock_session):
            result = await source.get_price("OLDTOKEN", resolved_token=_StubIdentity("old-token"))

        assert result.price > 0
        assert source._dynamic_symbol_cache["OLDTOKEN"] == "OLDTOKENUSDT"

    @pytest.mark.asyncio()
    async def test_dynamic_cache_used_on_subsequent_calls(self, source, corroborate):
        """Dynamically resolved symbols should be cached for future calls."""
        corroborate({"cached-project": "CACHED"})
        source._dynamic_symbol_cache["CACHED"] = "CACHEDUSDT"

        mock_session = _mock_session(price="5.0")

        with patch.object(source, "_get_session", return_value=mock_session):
            result = await source.get_price("CACHED", resolved_token=_StubIdentity("cached-project"))

        assert result.price > 0
        assert result.confidence == 0.9  # Dynamic resolution gets lower confidence
        # The cached pair was used directly — no probing round-trip.
        assert _probed_symbols(mock_session) == ["CACHEDUSDT"]

    @pytest.mark.asyncio()
    async def test_stablecoins_bypass_dynamic_resolution(self, source):
        """Stablecoins should always return $1 without any API call."""
        result = await source.get_price("USDC", resolved_token=USDC_RESOLVED)
        assert result.price == 1
        assert result.confidence == 1.0
        assert result.peg_tokens == ("ethereum:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",)

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
    async def test_evict_dynamic_cache_on_api_error(self, source, corroborate):
        """Dynamic cache entries should be evicted if the API returns errors."""
        corroborate({"delisted-project": "DELISTED"})
        source._dynamic_symbol_cache["DELISTED"] = "DELISTEDUSDT"

        mock_session = _mock_session(status=400)

        with patch.object(source, "_get_session", return_value=mock_session):
            with pytest.raises(DataSourceUnavailable):
                await source.get_price("DELISTED", resolved_token=_StubIdentity("delisted-project"))

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

        with pytest.raises(DataSourceUnavailable, match="not approved"):
            await source.get_price(token)
        source._resolve_binance_symbol.assert_not_awaited()


class TestDynamicProbeCorroborationGate:
    """ALM-3185 — dynamic ticker probing is gated on a reviewed identity link.

    A ticker probe cannot tell two projects apart when they share a symbol:
    Velodrome's VELO on Optimism and Velo Labs' VELO on Binance are unrelated
    assets. Ungated, the collision produced a *confident* (0.9) wrong price, and
    ``PriceAggregator`` has no confidence-based eligibility filter — a single
    surviving source is returned as-is (``_fetch_all_sources``) and outlier
    detection is price-deviation-based — so whenever the address-based sources
    are rate-limited that invented price IS the returned price.

    Negative control: every test in this class fails if ``_gate_dynamic_base``
    is removed or made permissive. ``_mock_session`` answers 200 with a price
    for ANY symbol, so an escaping probe always "finds" a listed pair.
    """

    @pytest.fixture()
    def source(self):
        return BinancePriceSource(cache_ttl=30, request_timeout=5.0)

    @pytest.mark.asyncio()
    async def test_colliding_ticker_never_reaches_the_probe(self, source, corroborate):
        """Unknown token, ticker collides with a listed pair -> no price, no probe."""
        corroborate({})
        mock_session = _mock_session(price="0.0123")

        with patch.object(source, "_get_session", return_value=mock_session):
            with pytest.raises(DataSourceUnavailable, match="no corroborated Binance listing"):
                await source.get_price("VELO")

        mock_session.get.assert_not_called()

    @pytest.mark.asyncio()
    async def test_resolved_token_without_reviewed_link_is_rejected(self, source, corroborate):
        """The on-chain VELO (Velodrome) must not borrow Binance's VELO listing."""
        corroborate({"velo-labs": "VELO"})
        mock_session = _mock_session(price="0.0123")

        with patch.object(source, "_get_session", return_value=mock_session):
            with pytest.raises(DataSourceUnavailable, match="no corroborated Binance listing"):
                await source.get_price("VELO", resolved_token=_StubIdentity("velodrome-finance"))

        mock_session.get.assert_not_called()

    @pytest.mark.asyncio()
    async def test_corroborated_identity_resolves_through_the_probe(self, source, corroborate):
        """A token whose identity IS linked still resolves, at dynamic confidence."""
        corroborate({"velo-labs": "VELO"})
        mock_session = _mock_session(price="0.0123")

        with patch.object(source, "_get_session", return_value=mock_session):
            result = await source.get_price("VELO", resolved_token=_StubIdentity("velo-labs"))

        assert result.price > 0
        assert result.confidence == 0.9
        assert _probed_symbols(mock_session)[0] == "VELOUSDT"
        # Cache is keyed by the corroborated base, not by the requested symbol.
        assert source._dynamic_symbol_cache == {"VELO": "VELOUSDT"}

    @pytest.mark.asyncio()
    async def test_base_comes_from_the_identity_not_the_requested_symbol(self, source, corroborate):
        """The probed base is the reviewed venue ticker, never the caller's symbol."""
        corroborate({"wrapped-thing": "THING"})
        mock_session = _mock_session(price="7.5")

        with patch.object(source, "_get_session", return_value=mock_session):
            result = await source.get_price("WTHING", resolved_token=_StubIdentity("wrapped-thing"))

        assert result.price > 0
        assert _probed_symbols(mock_session)[0] == "THINGUSDT"
        assert "WTHINGUSDT" not in _probed_symbols(mock_session)

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("retired", sorted(BINANCE_UNSAFE_MARKET_BASES))
    async def test_corroborated_base_still_honours_the_retired_denylist(self, source, corroborate, retired):
        """A link pointing at a ghost ticker is as unsafe as a symbol that resolves to one."""
        corroborate({"some-project": retired})
        mock_session = _mock_session()

        with patch.object(source, "_get_session", return_value=mock_session):
            with pytest.raises(DataSourceUnavailable, match="not approved"):
                await source.get_price("SOMETOKEN", resolved_token=_StubIdentity("some-project"))

        mock_session.get.assert_not_called()

    @pytest.mark.asyncio()
    async def test_dynamic_symbol_cache_is_not_a_bypass(self, source, corroborate):
        """A cache row from a corroborated call must not serve an uncorroborated one."""
        corroborate({})
        source._dynamic_symbol_cache["VELO"] = "VELOUSDT"
        mock_session = _mock_session(price="0.0123")

        with patch.object(source, "_get_session", return_value=mock_session):
            with pytest.raises(DataSourceUnavailable, match="no corroborated Binance listing"):
                await source.get_price("VELO")

        mock_session.get.assert_not_called()

    @pytest.mark.asyncio()
    async def test_price_cache_is_not_a_bypass(self, source, corroborate):
        """The 30s price cache must not leak one project's price to a same-ticker stranger."""
        corroborate({"velo-labs": "VELO"})
        mock_session = _mock_session(price="0.0123")

        with patch.object(source, "_get_session", return_value=mock_session):
            warmed = await source.get_price("VELO", resolved_token=_StubIdentity("velo-labs"))
            assert warmed.price > 0
            calls_after_warm = mock_session.get.call_count

            with pytest.raises(DataSourceUnavailable, match="no corroborated Binance listing"):
                await source.get_price("VELO", resolved_token=_StubIdentity("velodrome-finance"))

        assert mock_session.get.call_count == calls_after_warm

    @pytest.mark.asyncio()
    @pytest.mark.parametrize("bogus", [None, {"coingecko_id": "velo-labs"}, "velo-labs", 42])
    async def test_gate_fails_closed_on_a_non_identity_resolved_token(self, source, corroborate, bogus):
        """Anything that is not a real identity closes the gate rather than raising."""
        corroborate({"velo-labs": "VELO"})
        mock_session = _mock_session()

        with patch.object(source, "_get_session", return_value=mock_session):
            with pytest.raises(DataSourceUnavailable, match="no corroborated Binance listing"):
                await source.get_price("VELO", resolved_token=bogus)

        mock_session.get.assert_not_called()

    @pytest.mark.asyncio()
    async def test_curated_tokens_are_unaffected_by_the_gate(self, source, corroborate):
        """Curated rows never consult the gate — full confidence, no identity needed."""
        corroborate({})
        mock_session = _mock_session(price="3000.0")
        source._gate_dynamic_base = MagicMock(side_effect=AssertionError("gate must not run for curated rows"))

        with patch.object(source, "_get_session", return_value=mock_session):
            result = await source.get_price("ETH", resolved_token=_StubIdentity("velodrome-finance"))

        assert result.confidence == 1.0
        assert _probed_symbols(mock_session) == ["ETHUSDT"]

    @pytest.mark.asyncio()
    async def test_stablecoin_lane_is_unaffected_by_the_gate(self, source, corroborate):
        """The $1 stablecoin lane short-circuits before the gate and makes no call."""
        corroborate({})
        mock_session = _mock_session()

        with patch.object(source, "_get_session", return_value=mock_session):
            result = await source.get_price("USDC", resolved_token=USDC_RESOLVED)

        assert result.price == 1
        assert result.confidence == 1.0
        mock_session.get.assert_not_called()

    def test_corroboration_table_shape_is_identity_keyed(self):
        """Structural guard on future rows: CoinGecko id -> Binance base, never a symbol join."""
        table = binance_price_source._COINGECKO_ID_TO_BINANCE_BASE
        for coingecko_id, base in table.items():
            assert coingecko_id and coingecko_id == coingecko_id.strip().lower(), (
                f"corroboration key {coingecko_id!r} must be a normalized CoinGecko id"
            )
            assert base and base.isalnum() and base == base.upper(), (
                f"corroboration value {base!r} must be an uppercase Binance base ticker"
            )
        assert BINANCE_UNSAFE_MARKET_BASES.isdisjoint(set(table.values()))
        # Curated rows own their symbols; a link must not shadow the reviewed table.
        assert set(table.values()).isdisjoint(set(_TOKEN_TO_BINANCE_SYMBOL))
