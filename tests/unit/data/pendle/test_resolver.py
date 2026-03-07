"""Tests for PendleMarketResolver -- dynamic market discovery."""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.data.pendle.api_client import CHAIN_ID_MAP, PendleAPIClient, PendleAPIError
from almanak.framework.data.pendle.models import PendleMarketData
from almanak.framework.data.pendle.resolver import PendleMarketResolver


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_market(
    market_address: str = "0xmarket1",
    pt_address: str = "0xpt1",
    pt_symbol: str = "PT-sUSDe-7MAY2026",
    yt_address: str = "0xyt1",
    yt_symbol: str = "YT-sUSDe-7MAY2026",
    sy_address: str = "0xsy1",
    underlying_address: str = "0xunderlying1",
    underlying_symbol: str = "sUSDe",
    expiry: int = 9999999999,  # far future
    is_expired: bool = False,
    implied_apy: str = "0.05",
    liquidity_usd: str = "1000000",
    chain_id: int = 1,
) -> PendleMarketData:
    return PendleMarketData(
        market_address=market_address,
        chain_id=chain_id,
        pt_address=pt_address,
        pt_symbol=pt_symbol,
        yt_address=yt_address,
        yt_symbol=yt_symbol,
        sy_address=sy_address,
        underlying_address=underlying_address,
        underlying_symbol=underlying_symbol,
        expiry=expiry,
        is_expired=is_expired,
        implied_apy=Decimal(implied_apy),
        liquidity_usd=Decimal(liquidity_usd),
    )


MOCK_MARKETS = [
    _make_market(
        market_address="0x8dae8ece668cf80d348873f23d456448e8694883",
        pt_address="0x3de0ff76e8b528c092d47b9dac775931cef80f49",
        pt_symbol="PT-sUSDe-7MAY2026",
        yt_address="0x30775b422b9c7415349855346352faa61fd97e41",
        yt_symbol="YT-sUSDe-7MAY2026",
        underlying_address="0x9d39a5de30e57443bff2a8307a4256c8797a3497",
        underlying_symbol="sUSDe",
        liquidity_usd="5000000",
    ),
    _make_market(
        market_address="0xmarket_wsteth",
        pt_address="0xpt_wsteth",
        pt_symbol="PT-wstETH-25JUN2026",
        yt_address="0xyt_wsteth",
        yt_symbol="YT-wstETH-25JUN2026",
        underlying_address="0x5979d7b546e38e414f7e9822514be443a4800529",
        underlying_symbol="wstETH",
        liquidity_usd="2000000",
    ),
    _make_market(
        market_address="0xmarket_expired",
        pt_address="0xpt_expired",
        pt_symbol="PT-sUSDe-5FEB2026",
        underlying_address="0x9d39a5de30e57443bff2a8307a4256c8797a3497",
        underlying_symbol="sUSDe",
        expiry=1000000000,  # past
        is_expired=True,
        liquidity_usd="100000",
    ),
]


@pytest.fixture
def mock_api_client():
    client = MagicMock(spec=PendleAPIClient)
    client.chain = "ethereum"
    client.chain_id = 1
    client.get_market_list.return_value = MOCK_MARKETS
    return client


@pytest.fixture
def resolver(mock_api_client):
    return PendleMarketResolver("ethereum", api_client=mock_api_client, cache_ttl=0)


# ---------------------------------------------------------------------------
# Tests: find_markets
# ---------------------------------------------------------------------------


class TestFindMarkets:
    def test_returns_all_active_markets(self, resolver):
        markets = resolver.find_markets(active_only=True)
        assert len(markets) == 2  # expired one is excluded
        assert all(not m.is_expired for m in markets)

    def test_returns_all_markets_including_expired(self, resolver):
        markets = resolver.find_markets(active_only=False)
        assert len(markets) == 3

    def test_filter_by_underlying_symbol(self, resolver):
        markets = resolver.find_markets(underlying="sUSDe", active_only=True)
        assert len(markets) == 1
        assert markets[0].underlying_symbol == "sUSDe"

    def test_filter_by_underlying_address(self, resolver):
        markets = resolver.find_markets(
            underlying="0x9d39a5de30e57443bff2a8307a4256c8797a3497",
            active_only=True,
        )
        assert len(markets) == 1

    def test_filter_by_wsteth(self, resolver):
        markets = resolver.find_markets(underlying="wstETH", active_only=True)
        assert len(markets) == 1
        assert markets[0].underlying_symbol == "wstETH"

    def test_sorted_by_liquidity_desc(self, resolver):
        markets = resolver.find_markets(active_only=True)
        assert markets[0].liquidity_usd >= markets[1].liquidity_usd

    def test_no_matches_returns_empty(self, resolver):
        markets = resolver.find_markets(underlying="NONEXISTENT")
        assert markets == []


# ---------------------------------------------------------------------------
# Tests: get_best_market
# ---------------------------------------------------------------------------


class TestGetBestMarket:
    def test_returns_highest_liquidity(self, resolver):
        best = resolver.get_best_market("sUSDe")
        assert best is not None
        assert best.liquidity_usd == Decimal("5000000")

    def test_returns_none_for_no_match(self, resolver):
        assert resolver.get_best_market("NONEXISTENT") is None

    def test_excludes_expired(self, resolver):
        # sUSDe has both active and expired markets
        best = resolver.get_best_market("sUSDe")
        assert best is not None
        assert not best.is_expired


# ---------------------------------------------------------------------------
# Tests: resolve_by_pt_symbol
# ---------------------------------------------------------------------------


class TestResolveByPtSymbol:
    def test_resolve_by_api_symbol(self, resolver):
        market = resolver.resolve_by_pt_symbol("PT-sUSDe-7MAY2026")
        assert market is not None
        assert market.market_address == "0x8dae8ece668cf80d348873f23d456448e8694883"

    def test_case_insensitive_resolve(self, resolver):
        market = resolver.resolve_by_pt_symbol("pt-susde-7may2026")
        assert market is not None

    def test_resolve_unknown_symbol_returns_none(self, resolver, mock_api_client):
        # No static dict match either -- mock the import
        mock_api_client.get_market_data.side_effect = PendleAPIError("not found")
        market = resolver.resolve_by_pt_symbol("PT-NONEXISTENT-2099")
        assert market is None


# ---------------------------------------------------------------------------
# Tests: resolve_by_market_address
# ---------------------------------------------------------------------------


class TestResolveByMarketAddress:
    def test_resolve_known_address(self, resolver):
        market = resolver.resolve_by_market_address("0x8dae8ece668cf80d348873f23d456448e8694883")
        assert market is not None
        assert market.pt_symbol == "PT-sUSDe-7MAY2026"

    def test_case_insensitive_address(self, resolver):
        market = resolver.resolve_by_market_address("0x8DAE8ECE668CF80D348873F23D456448E8694883")
        assert market is not None

    def test_unknown_address_tries_api(self, resolver, mock_api_client):
        unknown_market = _make_market(market_address="0xnewmarket")
        mock_api_client.get_market_data.return_value = unknown_market
        market = resolver.resolve_by_market_address("0xnewmarket_not_in_list")
        # Should try API fetch for unknown addresses
        mock_api_client.get_market_data.assert_called_once()


# ---------------------------------------------------------------------------
# Tests: resolve_pt_token_info
# ---------------------------------------------------------------------------


class TestResolvePtTokenInfo:
    def test_resolve_from_api(self, resolver):
        info = resolver.resolve_pt_token_info("PT-sUSDe-7MAY2026")
        assert info is not None
        addr, decimals = info
        assert addr == "0x3de0ff76e8b528c092d47b9dac775931cef80f49"
        assert decimals == 18


# ---------------------------------------------------------------------------
# Tests: resolve_yt_token_info
# ---------------------------------------------------------------------------


class TestResolveYtTokenInfo:
    def test_resolve_via_pt_crossref(self, resolver):
        info = resolver.resolve_yt_token_info("YT-sUSDe-7MAY2026")
        assert info is not None
        addr, decimals = info
        assert addr == "0x30775b422b9c7415349855346352faa61fd97e41"


# ---------------------------------------------------------------------------
# Tests: resolve_market_address_from_pt_symbol
# ---------------------------------------------------------------------------


class TestResolveMarketAddressFromPtSymbol:
    def test_resolve_from_api(self, resolver):
        addr = resolver.resolve_market_address_from_pt_symbol("PT-sUSDe-7MAY2026")
        assert addr == "0x8dae8ece668cf80d348873f23d456448e8694883"


# ---------------------------------------------------------------------------
# Tests: resolve_mint_sy_token
# ---------------------------------------------------------------------------


class TestResolveMintSyToken:
    def test_falls_back_to_api_underlying(self, resolver):
        # No static dict entry for this market, so falls back to API underlying
        token = resolver.resolve_mint_sy_token("0xmarket_wsteth")
        assert token == "0x5979d7b546e38e414f7e9822514be443a4800529"


# ---------------------------------------------------------------------------
# Tests: API failure fallback
# ---------------------------------------------------------------------------


class TestApiFallback:
    def test_api_failure_uses_cached_markets(self, mock_api_client):
        resolver = PendleMarketResolver("ethereum", api_client=mock_api_client, cache_ttl=9999)

        # First call succeeds and caches
        markets1 = resolver.find_markets(active_only=False)
        assert len(markets1) == 3

        # Simulate API failure
        mock_api_client.get_market_list.side_effect = PendleAPIError("timeout")
        resolver._cache_expiry = 0  # force cache miss

        # Should fall back to cached data
        markets2 = resolver.find_markets(active_only=False)
        assert len(markets2) == 3

    def test_api_failure_no_cache_returns_empty(self, mock_api_client):
        mock_api_client.get_market_list.side_effect = PendleAPIError("down")
        resolver = PendleMarketResolver("ethereum", api_client=mock_api_client, cache_ttl=0)
        markets = resolver.find_markets()
        assert markets == []


# ---------------------------------------------------------------------------
# Tests: clear_cache
# ---------------------------------------------------------------------------


class TestClearCache:
    def test_clear_cache_resets(self, resolver, mock_api_client):
        # Populate cache
        resolver.find_markets()
        assert mock_api_client.get_market_list.call_count == 1

        # Clear and refetch
        resolver.clear_cache()
        resolver.find_markets()
        assert mock_api_client.get_market_list.call_count == 2


# ---------------------------------------------------------------------------
# Tests: constructor validation
# ---------------------------------------------------------------------------


class TestConstructor:
    def test_rejects_unsupported_chain(self):
        with pytest.raises(ValueError, match="Unsupported chain"):
            PendleMarketResolver("solana")

    def test_accepts_valid_chains(self):
        for chain in ["ethereum", "arbitrum", "optimism", "base", "bsc"]:
            # Should not raise -- we mock the API client so no real HTTP calls
            mock_client = MagicMock(spec=PendleAPIClient)
            mock_client.chain = chain
            mock_client.chain_id = CHAIN_ID_MAP[chain]
            resolver = PendleMarketResolver(chain, api_client=mock_client)
            assert resolver.chain == chain
