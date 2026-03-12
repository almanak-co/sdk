"""Tests for DexScreener price source."""

import time
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.data.interfaces import DataSourceUnavailable, PriceResult
from almanak.gateway.data.price.dexscreener import DexScreenerPriceSource


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def dexscreener_source():
    return DexScreenerPriceSource(chain_id="solana", cache_ttl=30, min_liquidity_usd=10_000)


def _make_price_result(price: Decimal = Decimal("0.00001234"), source: str = "dexscreener") -> PriceResult:
    from datetime import UTC, datetime

    return PriceResult(
        price=price,
        source=source,
        timestamp=datetime.now(UTC),
        confidence=0.9,
        stale=False,
    )


# ---------------------------------------------------------------------------
# Tests: source metadata
# ---------------------------------------------------------------------------


class TestDexScreenerMetadata:
    def test_source_name(self, dexscreener_source):
        assert dexscreener_source.source_name == "dexscreener"

    def test_supported_tokens(self, dexscreener_source):
        tokens = dexscreener_source.supported_tokens
        assert "SOL" in tokens
        assert "BONK" in tokens
        assert "WIF" in tokens
        assert "USDC" in tokens

    def test_cache_ttl(self, dexscreener_source):
        assert dexscreener_source.cache_ttl_seconds == 30


# ---------------------------------------------------------------------------
# Tests: get_price success
# ---------------------------------------------------------------------------


class TestDexScreenerGetPrice:
    @pytest.mark.asyncio
    async def test_get_price_sol(self, dexscreener_source):
        expected = _make_price_result(Decimal("84.50"))
        with patch.object(dexscreener_source, "_fetch_price", new_callable=AsyncMock, return_value=expected):
            result = await dexscreener_source.get_price("SOL")

        assert isinstance(result, PriceResult)
        assert result.source == "dexscreener"
        assert result.price == Decimal("84.50")

    @pytest.mark.asyncio
    async def test_get_price_meme_coin(self, dexscreener_source):
        expected = _make_price_result(Decimal("0.00001234"))
        with patch.object(dexscreener_source, "_fetch_price", new_callable=AsyncMock, return_value=expected):
            result = await dexscreener_source.get_price("BONK")

        assert result.price == Decimal("0.00001234")

    @pytest.mark.asyncio
    async def test_case_insensitive(self, dexscreener_source):
        expected = _make_price_result()
        with patch.object(dexscreener_source, "_fetch_price", new_callable=AsyncMock, return_value=expected):
            result = await dexscreener_source.get_price("bonk")
        assert result.source == "dexscreener"


# ---------------------------------------------------------------------------
# Tests: caching
# ---------------------------------------------------------------------------


class TestDexScreenerCaching:
    @pytest.mark.asyncio
    async def test_cache_hit(self, dexscreener_source):
        expected = _make_price_result()
        mock_fetch = AsyncMock(return_value=expected)
        with patch.object(dexscreener_source, "_fetch_price", mock_fetch):
            await dexscreener_source.get_price("BONK")
            await dexscreener_source.get_price("BONK")

        assert mock_fetch.call_count == 1

    @pytest.mark.asyncio
    async def test_stale_cache_on_error(self, dexscreener_source):
        expected = _make_price_result(Decimal("0.00001"))

        mock_fetch = AsyncMock(return_value=expected)
        with patch.object(dexscreener_source, "_fetch_price", mock_fetch):
            await dexscreener_source.get_price("BONK")

        # Expire cache
        dexscreener_source._cache["BONK/USD"].cached_at = time.time() - 1000

        mock_fetch_fail = AsyncMock(side_effect=Exception("Network error"))
        with patch.object(dexscreener_source, "_fetch_price", mock_fetch_fail):
            result = await dexscreener_source.get_price("BONK")

        assert result.stale is True
        assert result.confidence == 0.6
        assert result.price == Decimal("0.00001")


# ---------------------------------------------------------------------------
# Tests: error handling
# ---------------------------------------------------------------------------


class TestDexScreenerErrors:
    @pytest.mark.asyncio
    async def test_fetch_error_no_cache_raises(self, dexscreener_source):
        mock_fetch = AsyncMock(side_effect=Exception("Connection refused"))
        with patch.object(dexscreener_source, "_fetch_price", mock_fetch):
            with pytest.raises(DataSourceUnavailable, match="Fetch failed"):
                await dexscreener_source.get_price("BONK")

    @pytest.mark.asyncio
    async def test_data_source_unavailable_not_caught(self, dexscreener_source):
        mock_fetch = AsyncMock(side_effect=DataSourceUnavailable("dexscreener", "No pairs"))
        with patch.object(dexscreener_source, "_fetch_price", mock_fetch):
            with pytest.raises(DataSourceUnavailable, match="No pairs"):
                await dexscreener_source.get_price("BONK")


# ---------------------------------------------------------------------------
# Tests: _fetch_price parsing (mock session)
# ---------------------------------------------------------------------------


class TestDexScreenerFetchPrice:
    @pytest.mark.asyncio
    async def test_fetch_with_address_lookup(self, dexscreener_source):
        """SOL has a known address, should use token-pairs endpoint."""
        pair_json = [
            {
                "chainId": "solana",
                "priceUsd": "84.50",
                "liquidity": {"usd": 5000000},
                "volume": {"h24": 1000000},
            }
        ]

        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=pair_json)

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)

        with patch.object(dexscreener_source, "_get_session", new_callable=AsyncMock, return_value=mock_session):
            result = await dexscreener_source._fetch_price("SOL")

        assert result.price == Decimal("84.50")
        assert result.source == "dexscreener"

    @pytest.mark.asyncio
    async def test_fetch_no_pairs_raises(self, dexscreener_source):
        """Empty pair response raises DataSourceUnavailable."""
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[])

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)

        with patch.object(dexscreener_source, "_get_session", new_callable=AsyncMock, return_value=mock_session):
            with pytest.raises(DataSourceUnavailable, match="No pairs found"):
                await dexscreener_source._fetch_price("SOL")

    @pytest.mark.asyncio
    async def test_fetch_low_liquidity_raises(self, dexscreener_source):
        """Pairs below min_liquidity_usd are rejected."""
        pair_json = [
            {
                "chainId": "solana",
                "priceUsd": "0.001",
                "liquidity": {"usd": 100},  # Below 10k minimum
                "volume": {"h24": 100},
            }
        ]

        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=pair_json)

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)

        with patch.object(dexscreener_source, "_get_session", new_callable=AsyncMock, return_value=mock_session):
            with pytest.raises(DataSourceUnavailable, match="No liquid pair"):
                await dexscreener_source._fetch_price("SOL")


# ---------------------------------------------------------------------------
# Tests: confidence calculation
# ---------------------------------------------------------------------------


class TestDexScreenerConfidence:
    def test_high_liquidity_high_confidence(self, dexscreener_source):
        pair = {"liquidity": {"usd": 5_000_000}, "volume": {"h24": 500_000}}
        conf = dexscreener_source._calculate_confidence(pair)
        assert conf >= 0.9

    def test_low_liquidity_lower_confidence(self, dexscreener_source):
        pair = {"liquidity": {"usd": 50_000}, "volume": {"h24": 500_000}}
        conf = dexscreener_source._calculate_confidence(pair)
        assert conf == 0.85

    def test_low_volume_penalty(self, dexscreener_source):
        pair = {"liquidity": {"usd": 500_000}, "volume": {"h24": 5_000}}
        conf = dexscreener_source._calculate_confidence(pair)
        assert conf < 0.9  # Penalized for low volume


# ---------------------------------------------------------------------------
# Tests: health check
# ---------------------------------------------------------------------------


class TestDexScreenerHealthCheck:
    @pytest.mark.asyncio
    async def test_health_check_success(self, dexscreener_source):
        with patch.object(dexscreener_source, "get_price", new_callable=AsyncMock, return_value=_make_price_result()):
            assert await dexscreener_source.health_check() is True

    @pytest.mark.asyncio
    async def test_health_check_failure(self, dexscreener_source):
        with patch.object(dexscreener_source, "get_price", new_callable=AsyncMock, side_effect=Exception("down")):
            assert await dexscreener_source.health_check() is False
