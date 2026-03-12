"""Tests for Pyth Network price source."""

import time
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.data.interfaces import DataSourceUnavailable, PriceResult
from almanak.gateway.data.price.pyth import PYTH_FEED_IDS, PythPriceSource, _CacheEntry


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def pyth_source():
    return PythPriceSource(cache_ttl=15, request_timeout=5.0)


def _make_price_result(price: Decimal = Decimal("84.175101"), confidence: float = 1.0) -> PriceResult:
    """Build a PriceResult for testing."""
    from datetime import UTC, datetime

    return PriceResult(
        price=price,
        source="pyth",
        timestamp=datetime.now(UTC),
        confidence=confidence,
        stale=False,
    )


# ---------------------------------------------------------------------------
# Tests: source metadata
# ---------------------------------------------------------------------------

class TestPythSourceMetadata:
    def test_source_name(self, pyth_source):
        assert pyth_source.source_name == "pyth"

    def test_supported_tokens(self, pyth_source):
        tokens = pyth_source.supported_tokens
        assert "SOL" in tokens
        assert "ETH" in tokens
        assert "USDC" in tokens
        assert "BONK" in tokens

    def test_cache_ttl(self, pyth_source):
        assert pyth_source.cache_ttl_seconds == 15


# ---------------------------------------------------------------------------
# Tests: get_price success (mocking _fetch_price)
# ---------------------------------------------------------------------------

class TestPythGetPrice:
    @pytest.mark.asyncio
    async def test_get_price_sol(self, pyth_source):
        """SOL price fetched and returned."""
        expected = _make_price_result(Decimal("84.175101"))
        with patch.object(pyth_source, "_fetch_price", new_callable=AsyncMock, return_value=expected):
            result = await pyth_source.get_price("SOL")

        assert isinstance(result, PriceResult)
        assert result.source == "pyth"
        assert not result.stale
        assert result.price == Decimal("84.175101")

    @pytest.mark.asyncio
    async def test_get_price_usdc(self, pyth_source):
        """USDC price should be ~1.0."""
        expected = _make_price_result(Decimal("0.99997"))
        with patch.object(pyth_source, "_fetch_price", new_callable=AsyncMock, return_value=expected):
            result = await pyth_source.get_price("USDC")

        assert abs(result.price - Decimal("1.0")) < Decimal("0.01")

    @pytest.mark.asyncio
    async def test_case_insensitive_token(self, pyth_source):
        """Token lookup should be case-insensitive."""
        expected = _make_price_result()
        with patch.object(pyth_source, "_fetch_price", new_callable=AsyncMock, return_value=expected):
            result = await pyth_source.get_price("sol")
        assert result.source == "pyth"


# ---------------------------------------------------------------------------
# Tests: caching
# ---------------------------------------------------------------------------

class TestPythCaching:
    @pytest.mark.asyncio
    async def test_cache_hit(self, pyth_source):
        """Second call returns cached result without API call."""
        expected = _make_price_result()
        mock_fetch = AsyncMock(return_value=expected)
        with patch.object(pyth_source, "_fetch_price", mock_fetch):
            result1 = await pyth_source.get_price("SOL")
            result2 = await pyth_source.get_price("SOL")

        assert result1.price == result2.price
        # Only one fetch call should have been made
        assert mock_fetch.call_count == 1

    @pytest.mark.asyncio
    async def test_stale_cache_on_error(self, pyth_source):
        """On fetch error, stale cache is returned with reduced confidence."""
        expected = _make_price_result(Decimal("84.0"))

        # First call succeeds and populates cache
        mock_fetch = AsyncMock(return_value=expected)
        with patch.object(pyth_source, "_fetch_price", mock_fetch):
            await pyth_source.get_price("SOL")

        # Expire the cache
        pyth_source._cache["SOL/USD"].cached_at = time.time() - 1000

        # Second call fails, should fall back to stale cache
        mock_fetch_fail = AsyncMock(side_effect=Exception("Network error"))
        with patch.object(pyth_source, "_fetch_price", mock_fetch_fail):
            result = await pyth_source.get_price("SOL")

        assert result.stale is True
        assert result.confidence == 0.7
        assert result.price == Decimal("84.0")


# ---------------------------------------------------------------------------
# Tests: error handling
# ---------------------------------------------------------------------------

class TestPythErrors:
    @pytest.mark.asyncio
    async def test_unsupported_token_raises(self, pyth_source):
        """Unknown token raises DataSourceUnavailable."""
        with pytest.raises(DataSourceUnavailable, match="No Pyth feed"):
            await pyth_source.get_price("UNKNOWN_TOKEN_XYZ")

    @pytest.mark.asyncio
    async def test_fetch_error_no_cache_raises(self, pyth_source):
        """Fetch error without cache raises DataSourceUnavailable."""
        mock_fetch = AsyncMock(side_effect=Exception("Connection refused"))
        with patch.object(pyth_source, "_fetch_price", mock_fetch):
            with pytest.raises(DataSourceUnavailable, match="Fetch failed"):
                await pyth_source.get_price("SOL")

    @pytest.mark.asyncio
    async def test_data_source_unavailable_not_caught(self, pyth_source):
        """DataSourceUnavailable from _fetch_price re-raised directly."""
        mock_fetch = AsyncMock(side_effect=DataSourceUnavailable("pyth", "Zero price"))
        with patch.object(pyth_source, "_fetch_price", mock_fetch):
            with pytest.raises(DataSourceUnavailable, match="Zero price"):
                await pyth_source.get_price("SOL")


# ---------------------------------------------------------------------------
# Tests: _fetch_price parsing (unit test the parser logic)
# ---------------------------------------------------------------------------

class TestPythFetchPriceParsing:
    @pytest.mark.asyncio
    async def test_parse_sol_price(self, pyth_source):
        """Test that Hermes response is parsed correctly."""
        response_data = {
            "parsed": [
                {
                    "id": PYTH_FEED_IDS["SOL"],
                    "price": {
                        "price": "8417510100",
                        "conf": "5455971",
                        "expo": -8,
                        "publish_time": int(time.time()),
                    },
                }
            ]
        }

        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=response_data)

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)

        with patch.object(pyth_source, "_get_session", new_callable=AsyncMock, return_value=mock_session):
            result = await pyth_source._fetch_price(PYTH_FEED_IDS["SOL"], "SOL")

        # price = 8417510100 * 10^-8 = 84.17510100
        assert result.price == Decimal("8417510100") * Decimal("1E-8")
        assert result.source == "pyth"
        assert not result.stale

    @pytest.mark.asyncio
    async def test_parse_zero_price_raises(self, pyth_source):
        """Zero price in response raises DataSourceUnavailable."""
        response_data = {
            "parsed": [
                {
                    "id": PYTH_FEED_IDS["SOL"],
                    "price": {
                        "price": "0",
                        "conf": "0",
                        "expo": -8,
                        "publish_time": int(time.time()),
                    },
                }
            ]
        }

        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=response_data)

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)

        with patch.object(pyth_source, "_get_session", new_callable=AsyncMock, return_value=mock_session):
            with pytest.raises(DataSourceUnavailable, match="Zero price"):
                await pyth_source._fetch_price(PYTH_FEED_IDS["SOL"], "SOL")

    @pytest.mark.asyncio
    async def test_http_error_raises(self, pyth_source):
        """HTTP error raises DataSourceUnavailable."""
        mock_resp = MagicMock()
        mock_resp.status = 500
        mock_resp.text = AsyncMock(return_value="Internal Server Error")

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)

        with patch.object(pyth_source, "_get_session", new_callable=AsyncMock, return_value=mock_session):
            with pytest.raises(DataSourceUnavailable, match="HTTP 500"):
                await pyth_source._fetch_price(PYTH_FEED_IDS["SOL"], "SOL")

    @pytest.mark.asyncio
    async def test_empty_parsed_raises(self, pyth_source):
        """Empty parsed array raises DataSourceUnavailable."""
        response_data = {"parsed": []}

        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=response_data)

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=False)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)

        with patch.object(pyth_source, "_get_session", new_callable=AsyncMock, return_value=mock_session):
            with pytest.raises(DataSourceUnavailable, match="No parsed data"):
                await pyth_source._fetch_price(PYTH_FEED_IDS["SOL"], "SOL")


# ---------------------------------------------------------------------------
# Tests: confidence calculation
# ---------------------------------------------------------------------------

class TestPythConfidence:
    def test_tight_spread_high_confidence(self, pyth_source):
        """Tight confidence interval -> high confidence."""
        # 0.06% spread
        conf = pyth_source._calculate_confidence(
            price_int=8417510100, conf_int=5455971, publish_time=int(time.time())
        )
        assert conf >= 0.9

    def test_wide_spread_lower_confidence(self, pyth_source):
        """Wide confidence interval -> lower confidence."""
        # >1% spread
        conf = pyth_source._calculate_confidence(
            price_int=100000, conf_int=2000, publish_time=int(time.time())
        )
        assert conf == 0.8

    def test_stale_publish_time_reduces_confidence(self, pyth_source):
        """Old publish_time reduces confidence."""
        conf = pyth_source._calculate_confidence(
            price_int=8417510100, conf_int=5455971,
            publish_time=int(time.time()) - 120,  # 2 minutes old
        )
        assert conf == 0.85

    def test_very_stale_publish_time(self, pyth_source):
        """Very old publish_time reduces confidence further."""
        conf = pyth_source._calculate_confidence(
            price_int=8417510100, conf_int=5455971,
            publish_time=int(time.time()) - 400,  # >5 minutes old
        )
        assert conf == 0.5


# ---------------------------------------------------------------------------
# Tests: feed ID coverage
# ---------------------------------------------------------------------------

class TestPythFeedIDs:
    def test_all_solana_tokens_have_feeds(self):
        """Key Solana tokens have Pyth feed IDs."""
        expected = {"SOL", "WSOL", "USDC", "USDT", "JUP", "RAY", "BONK"}
        assert expected.issubset(set(PYTH_FEED_IDS.keys()))

    def test_feed_ids_are_64_hex_chars(self):
        """All feed IDs are 64-character hex strings."""
        for token, feed_id in PYTH_FEED_IDS.items():
            assert len(feed_id) == 64, f"{token} feed ID has wrong length: {len(feed_id)}"
            int(feed_id, 16)  # Should not raise


# ---------------------------------------------------------------------------
# Tests: health check
# ---------------------------------------------------------------------------

class TestPythHealthCheck:
    @pytest.mark.asyncio
    async def test_health_check_success(self, pyth_source):
        """Health check returns True on successful price fetch."""
        with patch.object(pyth_source, "get_price", new_callable=AsyncMock, return_value=_make_price_result()):
            assert await pyth_source.health_check() is True

    @pytest.mark.asyncio
    async def test_health_check_failure(self, pyth_source):
        """Health check returns False on fetch failure."""
        with patch.object(pyth_source, "get_price", new_callable=AsyncMock, side_effect=Exception("down")):
            assert await pyth_source.health_check() is False
