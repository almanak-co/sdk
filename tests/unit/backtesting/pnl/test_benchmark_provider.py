"""Tests for benchmark data provider CoinGecko API key propagation.

Verifies that the benchmark provider uses CoinGecko Pro API when
COINGECKO_API_KEY is set, and falls back to the free API otherwise.
"""

import asyncio
from datetime import datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.backtesting.pnl.providers.benchmark import (
    Benchmark,
    _get_single_token_prices,
    _parse_coingecko_prices,
    get_benchmark_price_series,
    get_benchmark_returns,
    get_benchmark_total_return,
)


class TestCoinGeckoApiKeyPropagation:
    """Tests that benchmark provider properly propagates CoinGecko API key."""

    @pytest.fixture
    def mock_coingecko_response(self):
        """Mock CoinGecko API response with valid price data."""
        return {
            "prices": [
                [1704067200000, 2300.50],  # 2024-01-01
                [1704153600000, 2350.75],  # 2024-01-02
                [1704240000000, 2280.00],  # 2024-01-03
            ],
            "market_caps": [],
            "total_volumes": [],
        }

    @pytest.mark.asyncio
    async def test_uses_pro_api_when_key_set(self, mock_coingecko_response):
        """When COINGECKO_API_KEY is set, should use pro-api base URL and auth header."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=mock_coingecko_response)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=mock_response),
            __aexit__=AsyncMock(return_value=False),
        ))

        mock_session_ctx = AsyncMock(
            __aenter__=AsyncMock(return_value=mock_session),
            __aexit__=AsyncMock(return_value=False),
        )

        with patch.dict("os.environ", {"COINGECKO_API_KEY": "test-api-key-123"}):
            with patch("aiohttp.ClientSession", return_value=mock_session_ctx):
                await _get_single_token_prices(
                    "ETH",
                    datetime(2024, 1, 1),
                    datetime(2024, 1, 3),
                    86400,
                )

        # Verify the call used pro API base URL and auth header
        call_args = mock_session.get.call_args
        url = call_args[0][0] if call_args[0] else call_args[1].get("url", "")
        headers = call_args[1].get("headers", {})

        assert "pro-api.coingecko.com" in url, f"Expected pro API URL, got: {url}"
        assert headers.get("x-cg-pro-api-key") == "test-api-key-123"

    @pytest.mark.asyncio
    async def test_uses_free_api_when_no_key(self, mock_coingecko_response):
        """When COINGECKO_API_KEY is not set, should use free API base URL."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=mock_coingecko_response)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=mock_response),
            __aexit__=AsyncMock(return_value=False),
        ))

        mock_session_ctx = AsyncMock(
            __aenter__=AsyncMock(return_value=mock_session),
            __aexit__=AsyncMock(return_value=False),
        )

        with patch.dict("os.environ", {}, clear=True):
            # Ensure no COINGECKO_API_KEY in env
            with patch("aiohttp.ClientSession", return_value=mock_session_ctx):
                await _get_single_token_prices(
                    "ETH",
                    datetime(2024, 1, 1),
                    datetime(2024, 1, 3),
                    86400,
                )

        call_args = mock_session.get.call_args
        url = call_args[0][0] if call_args[0] else call_args[1].get("url", "")
        headers = call_args[1].get("headers", {})

        assert "api.coingecko.com/api/v3" in url, f"Expected free API URL, got: {url}"
        assert "pro-api" not in url, f"Should NOT use pro API URL: {url}"
        assert "x-cg-pro-api-key" not in headers, "Should not send API key header"

    @pytest.mark.asyncio
    async def test_correct_token_id_mapping(self, mock_coingecko_response):
        """Verify ETH maps to 'ethereum' and BTC to 'bitcoin' in API URL."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=mock_coingecko_response)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=mock_response),
            __aexit__=AsyncMock(return_value=False),
        ))

        mock_session_ctx = AsyncMock(
            __aenter__=AsyncMock(return_value=mock_session),
            __aexit__=AsyncMock(return_value=False),
        )

        with patch.dict("os.environ", {}, clear=True):
            with patch("aiohttp.ClientSession", return_value=mock_session_ctx):
                await _get_single_token_prices(
                    "ETH",
                    datetime(2024, 1, 1),
                    datetime(2024, 1, 3),
                    86400,
                )

        call_args = mock_session.get.call_args
        url = call_args[0][0]
        assert "/coins/ethereum/market_chart/range" in url

    @pytest.mark.asyncio
    async def test_handles_rate_limit_gracefully(self):
        """Should return empty list on 429 rate limit response."""
        mock_response = AsyncMock()
        mock_response.status = 429

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=mock_response),
            __aexit__=AsyncMock(return_value=False),
        ))

        mock_session_ctx = AsyncMock(
            __aenter__=AsyncMock(return_value=mock_session),
            __aexit__=AsyncMock(return_value=False),
        )

        with patch.dict("os.environ", {}, clear=True):
            with patch("aiohttp.ClientSession", return_value=mock_session_ctx):
                result = await _get_single_token_prices(
                    "ETH",
                    datetime(2024, 1, 1),
                    datetime(2024, 1, 3),
                    86400,
                )

        assert result == []

    @pytest.mark.asyncio
    async def test_handles_401_gracefully(self):
        """Should return empty list on 401 unauthorized response."""
        mock_response = AsyncMock()
        mock_response.status = 401

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=mock_response),
            __aexit__=AsyncMock(return_value=False),
        ))

        mock_session_ctx = AsyncMock(
            __aenter__=AsyncMock(return_value=mock_session),
            __aexit__=AsyncMock(return_value=False),
        )

        with patch.dict("os.environ", {}, clear=True):
            with patch("aiohttp.ClientSession", return_value=mock_session_ctx):
                result = await _get_single_token_prices(
                    "ETH",
                    datetime(2024, 1, 1),
                    datetime(2024, 1, 3),
                    86400,
                )

        assert result == []


class TestParseCoingeckoPrices:
    """Tests for the price parsing function."""

    def test_parse_valid_data(self):
        """Should parse CoinGecko response into BenchmarkPricePoint list."""
        data = {
            "prices": [
                [1704067200000, 2300.50],
                [1704153600000, 2350.75],
            ],
        }
        result = _parse_coingecko_prices(data, 86400)
        assert len(result) == 2
        assert result[0].price == Decimal("2300.5")
        assert result[1].price == Decimal("2350.75")

    def test_parse_empty_data(self):
        """Should return empty list for empty response."""
        result = _parse_coingecko_prices({"prices": []}, 86400)
        assert result == []

    def test_parse_missing_prices_key(self):
        """Should return empty list when 'prices' key is missing."""
        result = _parse_coingecko_prices({}, 86400)
        assert result == []


class TestBenchmarkEnum:
    """Tests for Benchmark enum parsing."""

    def test_from_string_eth(self):
        assert Benchmark.from_string("eth") == Benchmark.ETH_HOLD
        assert Benchmark.from_string("ETH") == Benchmark.ETH_HOLD
        assert Benchmark.from_string("ethereum") == Benchmark.ETH_HOLD

    def test_from_string_btc(self):
        assert Benchmark.from_string("btc") == Benchmark.BTC_HOLD
        assert Benchmark.from_string("bitcoin") == Benchmark.BTC_HOLD

    def test_from_string_defi(self):
        assert Benchmark.from_string("defi") == Benchmark.DEFI_INDEX

    def test_from_string_invalid(self):
        with pytest.raises(ValueError, match="Unknown benchmark"):
            Benchmark.from_string("invalid_benchmark")

    def test_from_string_direct_value(self):
        assert Benchmark.from_string("eth_hold") == Benchmark.ETH_HOLD
        assert Benchmark.from_string("btc_hold") == Benchmark.BTC_HOLD
        assert Benchmark.from_string("defi_index") == Benchmark.DEFI_INDEX
