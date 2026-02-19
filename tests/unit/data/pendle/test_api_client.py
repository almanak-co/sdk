"""Unit tests for PendleAPIClient with mocked HTTP responses."""

import json
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.data.pendle.api_client import (
    CHAIN_ID_MAP,
    PENDLE_API_BASE,
    PendleAPIClient,
    PendleAPIError,
)
from almanak.framework.data.pendle.models import PendleMarketData, PendleSwapQuote


# =========================================================================
# Fixtures
# =========================================================================


@pytest.fixture
def client():
    """Create a PendleAPIClient with no rate limiting delay for tests."""
    c = PendleAPIClient(chain="ethereum", cache_ttl_seconds=0.01)
    c._min_interval = 0  # Disable rate limiting for tests
    return c


@pytest.fixture
def mock_market_response():
    """Mock API response for market data."""
    return {
        "pt": "0xpt_address",
        "yt": "0xyt_address",
        "sy": "0xsy_address",
        "underlyingAsset": "0xunderlying",
        "expiry": 1750000000,
        "impliedApy": 0.05,
        "underlyingApy": 0.03,
        "ptDiscount": 0.97,
        "ytDiscount": 0.03,
        "liquidity": 5000000,
        "tradingVolume": 200000,
        "isExpired": False,
    }


@pytest.fixture
def mock_swap_response():
    """Mock API response for swap quote."""
    return {
        "amountOut": 1020000,
        "tokenOut": "0xpt_address",
        "priceImpact": 0.0005,
    }


# =========================================================================
# Initialization Tests
# =========================================================================


class TestPendleAPIClientInit:
    """Test client initialization."""

    def test_valid_chain(self):
        client = PendleAPIClient(chain="ethereum")
        assert client.chain == "ethereum"
        assert client.chain_id == 1

    def test_arbitrum_chain(self):
        client = PendleAPIClient(chain="arbitrum")
        assert client.chain_id == 42161

    def test_unsupported_chain_raises(self):
        with pytest.raises(ValueError, match="Unsupported chain"):
            PendleAPIClient(chain="solana")

    def test_custom_cache_ttl(self):
        client = PendleAPIClient(chain="ethereum", cache_ttl_seconds=60.0)
        assert client.cache_ttl == 60.0

    def test_api_key_stored(self):
        client = PendleAPIClient(chain="ethereum", api_key="test-key")
        assert client.api_key == "test-key"

    def test_health_metrics_initial(self):
        client = PendleAPIClient(chain="ethereum")
        health = client.health
        assert health["request_count"] == 0
        assert health["error_count"] == 0
        assert health["chain"] == "ethereum"


# =========================================================================
# Market Data Tests
# =========================================================================


class TestGetMarketData:
    """Test get_market_data method."""

    def test_returns_pendle_market_data(self, client, mock_market_response):
        with patch.object(client, "_request", return_value=mock_market_response):
            result = client.get_market_data("0xmarket123")

        assert isinstance(result, PendleMarketData)
        assert result.market_address == "0xmarket123"
        assert result.implied_apy == Decimal("0.05")
        assert result.pt_address == "0xpt_address"

    def test_caches_result(self, client, mock_market_response):
        mock_req = MagicMock(return_value=mock_market_response)
        with patch.object(client, "_request", mock_req):
            # First call hits API
            client.get_market_data("0xmarket123")
            # Second call should use cache
            client.get_market_data("0xmarket123")

        assert mock_req.call_count == 1

    def test_handles_nested_data_wrapper(self, client):
        wrapped = {"data": {"impliedApy": 0.08, "liquidity": 1000000}}
        with patch.object(client, "_request", return_value=wrapped):
            result = client.get_market_data("0xmarket")

        assert result.implied_apy == Decimal("0.08")

    def test_api_error_propagates(self, client):
        with patch.object(client, "_request", side_effect=PendleAPIError("timeout")):
            with pytest.raises(PendleAPIError, match="timeout"):
                client.get_market_data("0xmarket")


# =========================================================================
# Swap Quote Tests
# =========================================================================


class TestGetSwapQuote:
    """Test get_swap_quote method."""

    def test_returns_swap_quote(self, client, mock_swap_response):
        with patch.object(client, "_request", return_value=mock_swap_response):
            result = client.get_swap_quote(
                market="0xmarket",
                token_in="0xusdc",
                amount_in=1000000,
                swap_type="token_to_pt",
            )

        assert isinstance(result, PendleSwapQuote)
        assert result.amount_out == 1020000
        assert result.amount_in == 1000000

    def test_invalid_swap_type_raises(self, client):
        with pytest.raises(PendleAPIError, match="Invalid swap_type"):
            client.get_swap_quote(
                market="0xmarket",
                token_in="0xusdc",
                amount_in=1000000,
                swap_type="invalid",
            )

    def test_all_swap_types_accepted(self, client, mock_swap_response):
        for swap_type in ["token_to_pt", "pt_to_token", "token_to_yt", "yt_to_token"]:
            with patch.object(client, "_request", return_value=mock_swap_response):
                result = client.get_swap_quote(
                    market="0xmarket",
                    token_in="0xtoken",
                    amount_in=100,
                    swap_type=swap_type,
                )
                assert result.amount_out == 1020000

    def test_swap_quotes_not_cached(self, client, mock_swap_response):
        """Swap quotes should never be cached (amount-dependent)."""
        mock_req = MagicMock(return_value=mock_swap_response)
        with patch.object(client, "_request", mock_req):
            client.get_swap_quote("0xm", "0xt", 100, "token_to_pt")
            client.get_swap_quote("0xm", "0xt", 100, "token_to_pt")

        assert mock_req.call_count == 2

    def test_price_impact_conversion_from_decimal(self, client):
        response = {"amountOut": 100, "tokenOut": "0x1", "priceImpact": 0.003}
        with patch.object(client, "_request", return_value=response):
            result = client.get_swap_quote("0xm", "0xt", 100, "token_to_pt")
        assert result.price_impact_bps == 30

    def test_price_impact_already_bps(self, client):
        response = {"amountOut": 100, "tokenOut": "0x1", "priceImpactBps": 50}
        with patch.object(client, "_request", return_value=response):
            result = client.get_swap_quote("0xm", "0xt", 100, "token_to_pt")
        assert result.price_impact_bps == 50


# =========================================================================
# Convenience Methods Tests
# =========================================================================


class TestConvenienceMethods:
    """Test get_pt_price and get_implied_apy."""

    def test_get_pt_price(self, client, mock_market_response):
        with patch.object(client, "_request", return_value=mock_market_response):
            price = client.get_pt_price("0xmarket")
        assert price == Decimal("0.97")

    def test_get_pt_price_invalid_raises(self, client):
        bad_response = {"ptDiscount": 0, "liquidity": 0}
        with patch.object(client, "_request", return_value=bad_response):
            with pytest.raises(PendleAPIError, match="Invalid PT price"):
                client.get_pt_price("0xmarket")

    def test_get_implied_apy(self, client, mock_market_response):
        with patch.object(client, "_request", return_value=mock_market_response):
            apy = client.get_implied_apy("0xmarket")
        assert apy == Decimal("0.05")


# =========================================================================
# Cache Tests
# =========================================================================


class TestCache:
    """Test caching behavior."""

    def test_clear_cache(self, client, mock_market_response):
        with patch.object(client, "_request", return_value=mock_market_response) as mock_req:
            client.get_market_data("0xmarket")
            client.clear_cache()
            client.get_market_data("0xmarket")
        assert mock_req.call_count == 2

    def test_max_cache_entries_eviction(self):
        client = PendleAPIClient(chain="ethereum", max_cache_entries=2, cache_ttl_seconds=100)
        client._min_interval = 0
        client._set_cached("key1", "val1")
        client._set_cached("key2", "val2")
        client._set_cached("key3", "val3")
        assert len(client._cache) == 2


# =========================================================================
# Health Metrics Tests
# =========================================================================


class TestHealthMetrics:
    """Test health metric tracking."""

    def test_request_count_increments(self, client, mock_market_response):
        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_response = MagicMock()
            mock_response.read.return_value = json.dumps(mock_market_response).encode()
            mock_response.__enter__ = lambda s: s
            mock_response.__exit__ = MagicMock(return_value=False)
            mock_urlopen.return_value = mock_response

            client.get_market_data("0xmarket")

        assert client.health["request_count"] == 1

    def test_error_count_increments(self, client):
        with patch("urllib.request.urlopen", side_effect=Exception("network error")):
            with pytest.raises(PendleAPIError):
                client.get_market_data("0xmarket")

        assert client.health["error_count"] == 1
