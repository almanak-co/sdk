"""Tests for DefiLlamaProvider.

Tests cover:
- DataProvider protocol: name, data_class, fetch, health
- Historical prices endpoint (/prices/historical, /prices/current)
- Yield pools endpoint (/yields/pools) with chain and project filters
- TVL endpoint (/protocol/{protocol})
- to_llama_coin_id mapping
- Rate limiting with token bucket
- Caching behavior
- Error handling (HTTP errors, rate limits, missing data)
- OHLCV convenience method (pseudo-candles from prices)
"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.data.interfaces import DataSourceUnavailable, OHLCVCandle
from almanak.framework.data.models import DataClassification, DataEnvelope
from almanak.framework.data.providers.defillama_provider import (
    _CHAIN_TO_LLAMA,
    DefiLlamaProvider,
    LlamaPrice,
    LlamaTvl,
    LlamaYieldPool,
    _TokenBucket,
    to_llama_coin_id,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def provider() -> DefiLlamaProvider:
    """Create a fresh provider for each test."""
    return DefiLlamaProvider(cache_ttl=60, request_timeout=5.0)


# ---------------------------------------------------------------------------
# Mock response builders
# ---------------------------------------------------------------------------


def _make_price_response(coin_id: str = "arbitrum:0xABCD", price: float = 1800.0, ts: int = 1700000000) -> dict:
    """Build a mock DeFi Llama historical price response."""
    return {
        "coins": {
            coin_id: {
                "price": price,
                "timestamp": ts,
                "confidence": 0.99,
                "decimals": 18,
                "symbol": "WETH",
            }
        }
    }


def _make_current_price_response(coin_id: str = "arbitrum:0xABCD", price: float = 1850.0) -> dict:
    """Build a mock DeFi Llama current price response."""
    return {
        "coins": {
            coin_id: {
                "price": price,
                "timestamp": int(time.time()),
                "confidence": 0.99,
                "decimals": 18,
                "symbol": "WETH",
            }
        }
    }


def _make_yields_response(pools: list[dict] | None = None) -> dict:
    """Build a mock DeFi Llama yields API response."""
    if pools is None:
        pools = [
            {
                "pool": "pool-id-1",
                "chain": "Arbitrum",
                "project": "uniswap-v3",
                "symbol": "USDC-WETH",
                "tvlUsd": 5000000,
                "apy": 12.5,
                "apyBase": 8.2,
                "apyReward": 4.3,
                "ilRisk": True,
                "exposure": "multi",
            },
            {
                "pool": "pool-id-2",
                "chain": "Ethereum",
                "project": "aave-v3",
                "symbol": "USDC",
                "tvlUsd": 10000000,
                "apy": 3.5,
                "apyBase": 3.5,
                "apyReward": None,
                "ilRisk": False,
                "exposure": "single",
            },
            {
                "pool": "pool-id-3",
                "chain": "Arbitrum",
                "project": "aave-v3",
                "symbol": "WETH",
                "tvlUsd": 2000000,
                "apy": 2.1,
                "apyBase": 2.1,
                "apyReward": None,
                "ilRisk": False,
                "exposure": "single",
            },
        ]
    return {"status": "success", "data": pools}


def _make_tvl_response(
    protocol: str = "uniswap",
    chain_tvls: dict[str, float] | None = None,
) -> dict:
    """Build a mock DeFi Llama protocol TVL response."""
    if chain_tvls is None:
        chain_tvls = {
            "Ethereum": 1500000000,
            "Arbitrum": 800000000,
            "Base": 300000000,
            "Ethereum-staking": 50000000,
            "Arbitrum-borrowed": 20000000,
        }
    return {
        "name": protocol.title(),
        "currentChainTvls": chain_tvls,
    }


def _mock_response(status: int = 200, json_data: dict | None = None, text: str = "") -> AsyncMock:
    """Build a mock aiohttp response that works as an async context manager.

    The mock supports `async with session.get(url) as response:` usage.
    """
    resp = AsyncMock()
    resp.status = status
    resp.json = AsyncMock(return_value=json_data or {})
    resp.text = AsyncMock(return_value=text)
    # Support async context manager: `async with session.get(...) as response:`
    resp.__aenter__ = AsyncMock(return_value=resp)
    resp.__aexit__ = AsyncMock(return_value=False)
    return resp


# ---------------------------------------------------------------------------
# to_llama_coin_id tests
# ---------------------------------------------------------------------------


class TestToLlamaCoinId:
    def test_arbitrum_mapping(self):
        result = to_llama_coin_id("0xABCD", "arbitrum")
        assert result == "arbitrum:0xABCD"

    def test_ethereum_mapping(self):
        result = to_llama_coin_id("0x1234", "ethereum")
        assert result == "ethereum:0x1234"

    def test_avalanche_mapping(self):
        result = to_llama_coin_id("0x5678", "avalanche")
        assert result == "avax:0x5678"

    def test_bsc_mapping(self):
        result = to_llama_coin_id("0xDEAD", "bsc")
        assert result == "bsc:0xDEAD"

    def test_case_insensitive(self):
        result = to_llama_coin_id("0xABCD", "Arbitrum")
        assert result == "arbitrum:0xABCD"

    def test_unsupported_chain_raises(self):
        with pytest.raises(DataSourceUnavailable, match="Unsupported chain"):
            to_llama_coin_id("0xABCD", "solana")

    def test_all_chains_mapped(self):
        """Verify all chains in _CHAIN_TO_LLAMA produce valid coin IDs."""
        for chain in _CHAIN_TO_LLAMA:
            result = to_llama_coin_id("0xTEST", chain)
            assert ":" in result
            assert result.endswith("0xTEST")


# ---------------------------------------------------------------------------
# DataProvider protocol tests
# ---------------------------------------------------------------------------


class TestDataProviderProtocol:
    def test_name(self, provider: DefiLlamaProvider):
        assert provider.name == "defillama"

    def test_data_class(self, provider: DefiLlamaProvider):
        assert provider.data_class == DataClassification.INFORMATIONAL

    def test_health_initial(self, provider: DefiLlamaProvider):
        h = provider.health()
        assert h["status"] == "healthy"
        assert h["total_requests"] == 0
        assert h["errors"] == 0
        assert h["success_rate"] == 100.0

    def test_health_after_errors(self, provider: DefiLlamaProvider):
        provider._metrics.total_requests = 10
        provider._metrics.errors = 6
        provider._metrics.successful_requests = 4
        h = provider.health()
        assert h["status"] == "degraded"

    def test_health_avg_latency(self, provider: DefiLlamaProvider):
        provider._metrics.successful_requests = 5
        provider._metrics.total_latency_ms = 500.0
        h = provider.health()
        assert h["average_latency_ms"] == 100.0


# ---------------------------------------------------------------------------
# Rate limiter tests
# ---------------------------------------------------------------------------


class TestTokenBucket:
    def test_initial_tokens(self):
        bucket = _TokenBucket(rate=10, period=1.0)
        # Should allow first 10 requests
        for _ in range(10):
            assert bucket.acquire() is True
        # 11th should fail
        assert bucket.acquire() is False

    def test_refill(self):
        bucket = _TokenBucket(rate=10, period=1.0)
        # Drain all tokens
        for _ in range(10):
            bucket.acquire()
        assert bucket.acquire() is False

        # Simulate time passing
        with patch("almanak.framework.data.providers.defillama_provider.time") as mock_time:
            mock_time.monotonic.return_value = bucket._last_refill + 1.0
            assert bucket.acquire() is True


# ---------------------------------------------------------------------------
# Historical prices tests
# ---------------------------------------------------------------------------


class TestHistoricalPrices:
    def test_historical_prices_with_timestamps(self, provider: DefiLlamaProvider):
        """Test fetching historical prices for specific timestamps."""
        coin_id = "arbitrum:0xABCD"
        mock_resp = _mock_response(200, _make_price_response(coin_id, 1800.0, 1700000000))

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.closed = False

        provider._session = mock_session

        result = asyncio.run(
            provider.get_historical_prices(
                token_address="0xABCD",
                chain="arbitrum",
                timestamps=[1700000000],
            )
        )

        assert len(result) == 1
        assert result[0].price == Decimal("1800.0")
        assert result[0].coin_id == coin_id

    def test_current_price_no_timestamps(self, provider: DefiLlamaProvider):
        """Test fetching current price when no timestamps provided."""
        coin_id = "ethereum:0x1234"
        mock_resp = _mock_response(200, _make_current_price_response(coin_id, 1850.0))

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.closed = False

        provider._session = mock_session

        result = asyncio.run(
            provider.get_historical_prices(
                token_address="0x1234",
                chain="ethereum",
            )
        )

        assert len(result) == 1
        assert result[0].price == Decimal("1850.0")
        assert result[0].coin_id == coin_id

    def test_no_price_data_raises(self, provider: DefiLlamaProvider):
        """Test that missing price data raises DataSourceUnavailable."""
        mock_resp = _mock_response(200, {"coins": {}})

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.closed = False

        provider._session = mock_session

        with pytest.raises(DataSourceUnavailable, match="No price data"):
            asyncio.run(
                provider.get_historical_prices(
                    token_address="0xDEAD",
                    chain="arbitrum",
                )
            )

    def test_rate_limited(self, provider: DefiLlamaProvider):
        """Test rate limiting raises DataSourceUnavailable."""
        # Drain the rate limiter
        for _ in range(20):
            provider._rate_limiter.acquire()

        with pytest.raises(DataSourceUnavailable, match="Rate limited"):
            asyncio.run(
                provider.get_historical_prices(
                    token_address="0xABCD",
                    chain="arbitrum",
                    timestamps=[1700000000],
                )
            )

    def test_cache_hit(self, provider: DefiLlamaProvider):
        """Test that cached results are returned without API call."""
        coin_id = "arbitrum:0xABCD"
        mock_resp = _mock_response(200, _make_price_response(coin_id, 1800.0, 1700000000))

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.closed = False

        provider._session = mock_session

        # First call populates cache
        result1 = asyncio.run(
            provider.get_historical_prices(
                token_address="0xABCD",
                chain="arbitrum",
                timestamps=[1700000000],
            )
        )

        # Second call should use cache
        result2 = asyncio.run(
            provider.get_historical_prices(
                token_address="0xABCD",
                chain="arbitrum",
                timestamps=[1700000000],
            )
        )

        assert result1 == result2
        assert provider._metrics.cache_hits == 1

    def test_unsupported_chain(self, provider: DefiLlamaProvider):
        """Test that unsupported chain raises."""
        with pytest.raises(DataSourceUnavailable, match="Unsupported chain"):
            asyncio.run(
                provider.get_historical_prices(
                    token_address="0xABCD",
                    chain="solana",
                )
            )

    def test_multiple_timestamps(self, provider: DefiLlamaProvider):
        """Test fetching prices for multiple timestamps."""
        coin_id = "arbitrum:0xABCD"

        # Create responses for two timestamps
        resp1 = _mock_response(200, _make_price_response(coin_id, 1800.0, 1700000000))
        resp2 = _mock_response(200, _make_price_response(coin_id, 1810.0, 1700003600))

        mock_session = AsyncMock()
        mock_session.get = MagicMock(side_effect=[resp1, resp2])
        mock_session.closed = False

        provider._session = mock_session

        result = asyncio.run(
            provider.get_historical_prices(
                token_address="0xABCD",
                chain="arbitrum",
                timestamps=[1700000000, 1700003600],
            )
        )

        assert len(result) == 2
        assert result[0].price == Decimal("1800.0")
        assert result[1].price == Decimal("1810.0")


# ---------------------------------------------------------------------------
# Yield pools tests
# ---------------------------------------------------------------------------


class TestYieldPools:
    def test_get_all_pools(self, provider: DefiLlamaProvider):
        """Test fetching all yield pools without filters."""
        mock_resp = _mock_response(200, _make_yields_response())

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.closed = False

        provider._session = mock_session

        result = asyncio.run(provider.get_yield_pools())

        assert len(result) == 3
        assert all(isinstance(p, LlamaYieldPool) for p in result)

    def test_filter_by_chain(self, provider: DefiLlamaProvider):
        """Test filtering yield pools by chain."""
        mock_resp = _mock_response(200, _make_yields_response())

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.closed = False

        provider._session = mock_session

        result = asyncio.run(provider.get_yield_pools(chain="arbitrum"))

        # Only "Arbitrum" pools should be returned
        assert len(result) == 2
        assert all(p.chain == "arbitrum" for p in result)

    def test_filter_by_project(self, provider: DefiLlamaProvider):
        """Test filtering yield pools by project."""
        mock_resp = _mock_response(200, _make_yields_response())

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.closed = False

        provider._session = mock_session

        result = asyncio.run(provider.get_yield_pools(project="aave-v3"))

        assert len(result) == 2
        assert all(p.project == "aave-v3" for p in result)

    def test_filter_by_chain_and_project(self, provider: DefiLlamaProvider):
        """Test filtering by both chain and project."""
        mock_resp = _mock_response(200, _make_yields_response())

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.closed = False

        provider._session = mock_session

        result = asyncio.run(provider.get_yield_pools(chain="arbitrum", project="aave-v3"))

        assert len(result) == 1
        assert result[0].pool_id == "pool-id-3"
        assert result[0].chain == "arbitrum"
        assert result[0].project == "aave-v3"

    def test_pool_fields(self, provider: DefiLlamaProvider):
        """Test that all pool fields are correctly parsed."""
        mock_resp = _mock_response(200, _make_yields_response())

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.closed = False

        provider._session = mock_session

        result = asyncio.run(provider.get_yield_pools())

        pool = result[0]
        assert pool.pool_id == "pool-id-1"
        assert pool.chain == "arbitrum"
        assert pool.project == "uniswap-v3"
        assert pool.symbol == "USDC-WETH"
        assert pool.tvl_usd == Decimal("5000000")
        assert pool.apy == 12.5
        assert pool.apy_base == 8.2
        assert pool.apy_reward == 4.3
        assert pool.il_risk is True
        assert pool.exposure == "multi"

    def test_yields_http_error(self, provider: DefiLlamaProvider):
        """Test HTTP error on yields endpoint."""
        mock_resp = _mock_response(500, text="Internal Server Error")

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.closed = False

        provider._session = mock_session

        with pytest.raises(DataSourceUnavailable, match="Yields API HTTP 500"):
            asyncio.run(provider.get_yield_pools())

    def test_yields_cache(self, provider: DefiLlamaProvider):
        """Test that yields results are cached."""
        mock_resp = _mock_response(200, _make_yields_response())

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.closed = False

        provider._session = mock_session

        # First call
        asyncio.run(provider.get_yield_pools())
        # Second call should hit cache
        asyncio.run(provider.get_yield_pools())

        assert provider._metrics.cache_hits == 1

    def test_malformed_pool_skipped(self, provider: DefiLlamaProvider):
        """Test that malformed pool entries are silently skipped."""
        pools = [
            {
                "pool": "good-pool",
                "chain": "Arbitrum",
                "project": "uniswap-v3",
                "symbol": "USDC-WETH",
                "tvlUsd": 5000000,
                "apy": 12.5,
            },
            {
                "pool": "bad-pool",
                "chain": "Arbitrum",
                "project": "uniswap-v3",
                "symbol": "BAD",
                "tvlUsd": "not-a-number",
                "apy": "invalid",
            },
        ]
        mock_resp = _mock_response(200, _make_yields_response(pools))

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.closed = False

        provider._session = mock_session

        result = asyncio.run(provider.get_yield_pools())
        # Only the good pool should be returned
        assert len(result) == 1
        assert result[0].pool_id == "good-pool"


# ---------------------------------------------------------------------------
# TVL tests
# ---------------------------------------------------------------------------


class TestTvl:
    def test_get_tvl(self, provider: DefiLlamaProvider):
        """Test fetching protocol TVL."""
        mock_resp = _mock_response(200, _make_tvl_response("uniswap"))

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.closed = False

        provider._session = mock_session

        result = asyncio.run(provider.get_tvl("uniswap"))

        assert isinstance(result, LlamaTvl)
        assert result.protocol == "uniswap"
        # Should sum chain TVLs (excluding staking/borrowed variants)
        assert result.tvl_usd == Decimal("2600000000")  # 1.5B + 800M + 300M
        assert "ethereum" in result.chain_tvls
        assert "arbitrum" in result.chain_tvls
        assert "base" in result.chain_tvls

    def test_tvl_excludes_derived_categories(self, provider: DefiLlamaProvider):
        """Test that staking/borrowed variants are excluded from chain breakdown."""
        mock_resp = _mock_response(200, _make_tvl_response("uniswap"))

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.closed = False

        provider._session = mock_session

        result = asyncio.run(provider.get_tvl("uniswap"))

        # "Ethereum-staking" and "Arbitrum-borrowed" should be excluded
        assert "ethereum-staking" not in result.chain_tvls
        assert "arbitrum-borrowed" not in result.chain_tvls

    def test_tvl_with_total_key(self, provider: DefiLlamaProvider):
        """Test TVL parsing when 'total' key is present."""
        tvl_data = {
            "name": "Aave V3",
            "currentChainTvls": {
                "total": 5000000000,
                "Ethereum": 3000000000,
                "Arbitrum": 2000000000,
            },
        }
        mock_resp = _mock_response(200, tvl_data)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.closed = False

        provider._session = mock_session

        result = asyncio.run(provider.get_tvl("aave-v3"))

        assert result.tvl_usd == Decimal("5000000000")

    def test_tvl_empty_protocol_raises(self, provider: DefiLlamaProvider):
        """Test that empty protocol name raises."""
        with pytest.raises(DataSourceUnavailable, match="Protocol name is required"):
            asyncio.run(provider.get_tvl(""))

    def test_tvl_http_error(self, provider: DefiLlamaProvider):
        """Test HTTP error on TVL endpoint."""
        mock_resp = _mock_response(404, text="Not Found")

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.closed = False

        provider._session = mock_session

        with pytest.raises(DataSourceUnavailable, match="TVL API HTTP 404"):
            asyncio.run(provider.get_tvl("nonexistent-protocol"))

    def test_tvl_cache(self, provider: DefiLlamaProvider):
        """Test that TVL results are cached."""
        mock_resp = _mock_response(200, _make_tvl_response("uniswap"))

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.closed = False

        provider._session = mock_session

        asyncio.run(provider.get_tvl("uniswap"))
        asyncio.run(provider.get_tvl("uniswap"))

        assert provider._metrics.cache_hits == 1


# ---------------------------------------------------------------------------
# OHLCV convenience tests
# ---------------------------------------------------------------------------


class TestOhlcvFromPrices:
    def test_pseudo_ohlcv_candles(self, provider: DefiLlamaProvider):
        """Test building pseudo-OHLCV candles from historical prices."""
        coin_id = "arbitrum:0xABCD"

        resp1 = _mock_response(200, _make_price_response(coin_id, 1800.0, 1700000000))
        resp2 = _mock_response(200, _make_price_response(coin_id, 1810.0, 1700003600))

        mock_session = AsyncMock()
        mock_session.get = MagicMock(side_effect=[resp1, resp2])
        mock_session.closed = False

        provider._session = mock_session

        result = asyncio.run(
            provider.get_ohlcv_from_prices(
                token_address="0xABCD",
                chain="arbitrum",
                timestamps=[1700000000, 1700003600],
            )
        )

        assert len(result) == 2
        assert all(isinstance(c, OHLCVCandle) for c in result)
        # Each candle has open=high=low=close=price, volume=None
        assert result[0].open == result[0].close == Decimal("1800.0")
        assert result[0].volume is None
        # Sorted ascending
        assert result[0].timestamp < result[1].timestamp


# ---------------------------------------------------------------------------
# fetch() dispatch tests
# ---------------------------------------------------------------------------


class TestFetchDispatch:
    def test_fetch_prices(self, provider: DefiLlamaProvider):
        """Test fetch() dispatches to prices endpoint."""
        coin_id = "ethereum:0x1234"
        mock_resp = _mock_response(200, _make_current_price_response(coin_id, 1850.0))

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.closed = False

        provider._session = mock_session

        envelope = provider.fetch(
            endpoint="prices",
            token_address="0x1234",
            chain="ethereum",
        )

        assert isinstance(envelope, DataEnvelope)
        assert envelope.meta.source == "defillama"
        assert isinstance(envelope.value, list)
        assert len(envelope.value) == 1
        assert envelope.value[0].price == Decimal("1850.0")

    def test_fetch_yields(self, provider: DefiLlamaProvider):
        """Test fetch() dispatches to yields endpoint."""
        mock_resp = _mock_response(200, _make_yields_response())

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.closed = False

        provider._session = mock_session

        envelope = provider.fetch(endpoint="yields")

        assert isinstance(envelope, DataEnvelope)
        assert envelope.meta.source == "defillama"
        assert isinstance(envelope.value, list)

    def test_fetch_tvl(self, provider: DefiLlamaProvider):
        """Test fetch() dispatches to TVL endpoint."""
        mock_resp = _mock_response(200, _make_tvl_response("uniswap"))

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.closed = False

        provider._session = mock_session

        envelope = provider.fetch(endpoint="tvl", protocol="uniswap")

        assert isinstance(envelope, DataEnvelope)
        assert envelope.meta.source == "defillama"
        assert isinstance(envelope.value, LlamaTvl)

    def test_fetch_unknown_endpoint(self, provider: DefiLlamaProvider):
        """Test fetch() raises for unknown endpoint."""
        with pytest.raises(DataSourceUnavailable, match="Unknown endpoint"):
            provider.fetch(endpoint="unknown")


# ---------------------------------------------------------------------------
# Cache and clear tests
# ---------------------------------------------------------------------------


class TestCaching:
    def test_clear_cache(self, provider: DefiLlamaProvider):
        """Test cache clearing."""
        provider._cache["test_key"] = (["data"], time.monotonic())
        assert len(provider._cache) == 1

        provider.clear_cache()
        assert len(provider._cache) == 0

    def test_cache_expiry(self, provider: DefiLlamaProvider):
        """Test that expired cache entries are ignored."""
        # Set cache TTL to 1 second
        provider._cache_ttl = 1

        # Store a value
        provider._cache["test_key"] = (["old_data"], time.monotonic() - 2.0)

        # Should return None (expired)
        assert provider._get_cached("test_key") is None

    def test_cache_fresh(self, provider: DefiLlamaProvider):
        """Test that fresh cache entries are returned."""
        provider._cache["test_key"] = (["fresh_data"], time.monotonic())

        assert provider._get_cached("test_key") == ["fresh_data"]


# ---------------------------------------------------------------------------
# LlamaPrice / LlamaYieldPool / LlamaTvl dataclass tests
# ---------------------------------------------------------------------------


class TestDataclasses:
    def test_llama_price(self):
        p = LlamaPrice(
            price=Decimal("1800.0"),
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            coin_id="arbitrum:0xABCD",
        )
        assert p.price == Decimal("1800.0")
        assert p.confidence == 1.0

    def test_llama_yield_pool(self):
        pool = LlamaYieldPool(
            pool_id="test",
            chain="arbitrum",
            project="uniswap-v3",
            symbol="USDC-WETH",
            tvl_usd=Decimal("5000000"),
            apy=12.5,
        )
        assert pool.apy_base is None
        assert pool.il_risk is False

    def test_llama_tvl(self):
        tvl = LlamaTvl(
            protocol="uniswap",
            tvl_usd=Decimal("1000000000"),
            chain_tvls={"ethereum": Decimal("500000000")},
        )
        assert tvl.chain_tvls["ethereum"] == Decimal("500000000")

    def test_llama_tvl_default_chain_tvls(self):
        tvl = LlamaTvl(
            protocol="test",
            tvl_usd=Decimal("100"),
        )
        assert tvl.chain_tvls == {}
