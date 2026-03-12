"""Tests for DexScreener HTTP client."""

import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.data.dexscreener.client import (
    DexScreenerClient,
    DexScreenerError,
    DexScreenerRateLimited,
)
from almanak.framework.data.dexscreener.models import DexPair


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _mock_response(status=200, json_data=None):
    """Create a mock aiohttp response."""
    resp = MagicMock()
    resp.status = status
    resp.json = AsyncMock(return_value=json_data or {})
    resp.text = AsyncMock(return_value="error text")
    cm = AsyncMock()
    cm.__aenter__ = AsyncMock(return_value=resp)
    cm.__aexit__ = AsyncMock(return_value=False)
    return cm


def _sample_pair_json(symbol="BONK", price="0.00001234", liq_usd=500000, vol_h24=300000):
    return {
        "chainId": "solana",
        "dexId": "raydium",
        "pairAddress": f"pair_{symbol}",
        "baseToken": {"address": f"mint_{symbol}", "name": symbol, "symbol": symbol},
        "quoteToken": {"address": "usdc_mint", "name": "USD Coin", "symbol": "USDC"},
        "priceUsd": price,
        "liquidity": {"usd": liq_usd, "base": 0, "quote": 0},
        "volume": {"m5": 0, "h1": 0, "h6": 0, "h24": vol_h24},
        "priceChange": {"m5": 0, "h1": 5.0, "h6": 2.0, "h24": 10.0},
        "txns": {
            "m5": {"buys": 5, "sells": 3},
            "h1": {"buys": 80, "sells": 40},
            "h6": {"buys": 400, "sells": 300},
            "h24": {"buys": 2000, "sells": 1500},
        },
        "pairCreatedAt": int((time.time() - 3600 * 24) * 1000),
    }


# ---------------------------------------------------------------------------
# Tests: search_pairs
# ---------------------------------------------------------------------------


class TestSearchPairs:
    @pytest.mark.asyncio
    async def test_search_returns_pairs(self):
        client = DexScreenerClient()
        pairs_json = [_sample_pair_json("BONK"), _sample_pair_json("WIF")]
        response_data = {"pairs": pairs_json}

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=_mock_response(200, response_data))

        with patch.object(client, "_get_session", new_callable=AsyncMock, return_value=mock_session):
            pairs = await client.search_pairs("BONK")

        assert len(pairs) == 2
        assert all(isinstance(p, DexPair) for p in pairs)
        assert pairs[0].base_token.symbol == "BONK"
        await client.close()

    @pytest.mark.asyncio
    async def test_search_empty_results(self):
        client = DexScreenerClient()
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=_mock_response(200, {"pairs": []}))

        with patch.object(client, "_get_session", new_callable=AsyncMock, return_value=mock_session):
            pairs = await client.search_pairs("NONEXISTENT")

        assert pairs == []
        await client.close()


# ---------------------------------------------------------------------------
# Tests: get_token_pairs
# ---------------------------------------------------------------------------


class TestGetTokenPairs:
    @pytest.mark.asyncio
    async def test_get_token_pairs_array_response(self):
        """v1 endpoint returns a direct array."""
        client = DexScreenerClient()
        pairs_json = [_sample_pair_json("BONK")]

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=_mock_response(200, pairs_json))

        with patch.object(client, "_get_session", new_callable=AsyncMock, return_value=mock_session):
            pairs = await client.get_token_pairs("solana", "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263")

        assert len(pairs) == 1
        assert pairs[0].base_token.symbol == "BONK"
        await client.close()


# ---------------------------------------------------------------------------
# Tests: get_pair
# ---------------------------------------------------------------------------


class TestGetPair:
    @pytest.mark.asyncio
    async def test_get_pair_found(self):
        client = DexScreenerClient()
        pair_json = _sample_pair_json("BONK")

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=_mock_response(200, {"pair": pair_json}))

        with patch.object(client, "_get_session", new_callable=AsyncMock, return_value=mock_session):
            pair = await client.get_pair("solana", "pair_address")

        assert pair is not None
        assert pair.base_token.symbol == "BONK"
        await client.close()

    @pytest.mark.asyncio
    async def test_get_pair_not_found(self):
        client = DexScreenerClient()
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=_mock_response(200, {"pair": None, "pairs": None}))

        with patch.object(client, "_get_session", new_callable=AsyncMock, return_value=mock_session):
            pair = await client.get_pair("solana", "nonexistent")

        assert pair is None
        await client.close()


# ---------------------------------------------------------------------------
# Tests: get_top_boosts
# ---------------------------------------------------------------------------


class TestGetTopBoosts:
    @pytest.mark.asyncio
    async def test_get_top_boosts(self):
        client = DexScreenerClient()
        boosts = [
            {"chainId": "solana", "tokenAddress": "addr1", "amount": 100, "totalAmount": 500},
            {"chainId": "ethereum", "tokenAddress": "addr2", "amount": 50, "totalAmount": 200},
        ]

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=_mock_response(200, boosts))

        with patch.object(client, "_get_session", new_callable=AsyncMock, return_value=mock_session):
            result = await client.get_top_boosts()

        assert len(result) == 2
        assert result[0].chain_id == "solana"
        assert result[0].amount == 100
        await client.close()


# ---------------------------------------------------------------------------
# Tests: caching
# ---------------------------------------------------------------------------


class TestCaching:
    @pytest.mark.asyncio
    async def test_cache_hit(self):
        """Second call returns cached result without API call."""
        client = DexScreenerClient(cache_ttl=60)
        pairs_json = [_sample_pair_json("BONK")]
        response_data = {"pairs": pairs_json}

        mock_session = MagicMock()
        mock_get = MagicMock(return_value=_mock_response(200, response_data))
        mock_session.get = mock_get

        with patch.object(client, "_get_session", new_callable=AsyncMock, return_value=mock_session):
            result1 = await client.search_pairs("BONK")
            result2 = await client.search_pairs("BONK")

        assert len(result1) == 1
        assert len(result2) == 1
        # Only one HTTP call should have been made
        assert mock_get.call_count == 1
        await client.close()


# ---------------------------------------------------------------------------
# Tests: error handling
# ---------------------------------------------------------------------------


class TestErrorHandling:
    @pytest.mark.asyncio
    async def test_rate_limit_429(self):
        client = DexScreenerClient()
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=_mock_response(429))

        with patch.object(client, "_get_session", new_callable=AsyncMock, return_value=mock_session):
            with pytest.raises(DexScreenerRateLimited):
                await client.search_pairs("BONK")
        await client.close()

    @pytest.mark.asyncio
    async def test_server_error_500(self):
        client = DexScreenerClient()
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=_mock_response(500))

        with patch.object(client, "_get_session", new_callable=AsyncMock, return_value=mock_session):
            with pytest.raises(DexScreenerError, match="HTTP 500"):
                await client.search_pairs("BONK")
        await client.close()


# ---------------------------------------------------------------------------
# Tests: get_best_solana_pair
# ---------------------------------------------------------------------------


class TestGetBestSolanaPair:
    @pytest.mark.asyncio
    async def test_picks_highest_liquidity(self):
        client = DexScreenerClient()
        pairs_json = [
            _sample_pair_json("BONK", liq_usd=100_000),
            _sample_pair_json("BONK", liq_usd=500_000),
            {**_sample_pair_json("BONK", liq_usd=1_000_000), "chainId": "ethereum"},
        ]

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=_mock_response(200, {"pairs": pairs_json}))

        with patch.object(client, "_get_session", new_callable=AsyncMock, return_value=mock_session):
            best = await client.get_best_solana_pair("BONK")

        assert best is not None
        assert best.liquidity.usd == 500_000  # Highest Solana pair
        await client.close()

    @pytest.mark.asyncio
    async def test_no_solana_pairs(self):
        client = DexScreenerClient()
        pairs_json = [{**_sample_pair_json("BONK"), "chainId": "ethereum"}]

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=_mock_response(200, {"pairs": pairs_json}))

        with patch.object(client, "_get_session", new_callable=AsyncMock, return_value=mock_session):
            best = await client.get_best_solana_pair("BONK")

        assert best is None
        await client.close()


# ---------------------------------------------------------------------------
# Tests: get_solana_meme_candidates
# ---------------------------------------------------------------------------


class TestGetSolanaMemesCandidates:
    @pytest.mark.asyncio
    async def test_filters_by_liquidity_and_volume(self):
        client = DexScreenerClient()

        # Mock get_top_boosts -> empty (simplify)
        # Mock search_pairs -> returns some pairs
        low_liq = _sample_pair_json("LOW", liq_usd=1000, vol_h24=500)
        good = _sample_pair_json("GOOD", liq_usd=200_000, vol_h24=500_000)

        async def mock_get_top_boosts():
            return []

        async def mock_search_pairs(query):
            return [
                DexPair(
                    chain_id="solana",
                    base_token=DexPair.__dataclass_fields__["base_token"].default_factory(),
                    liquidity=DexPair.__dataclass_fields__["liquidity"].default_factory(),
                ),
            ]

        # Use _request mock instead
        call_count = 0

        async def mock_request(path, *, rate_attr, rate_limit):
            nonlocal call_count
            call_count += 1
            if "token-boosts" in path:
                return []
            if "search" in path:
                return {"pairs": [good, low_liq]}
            return {"pairs": []}

        with patch.object(client, "_request", side_effect=mock_request):
            candidates = await client.get_solana_meme_candidates(
                min_liquidity_usd=50_000,
                min_volume_h24=100_000,
            )

        # Only the good pair should pass
        good_candidates = [c for c in candidates if c.base_token.symbol == "GOOD"]
        low_candidates = [c for c in candidates if c.base_token.symbol == "LOW"]
        assert len(good_candidates) >= 1
        assert len(low_candidates) == 0
        await client.close()
