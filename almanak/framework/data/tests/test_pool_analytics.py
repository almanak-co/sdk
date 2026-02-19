"""Tests for PoolAnalyticsReader and PoolAnalytics dataclass."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.framework.data.models import DataEnvelope
from almanak.framework.data.pools.analytics import (
    PoolAnalytics,
    PoolAnalyticsReader,
    PoolAnalyticsResult,
    _safe_decimal,
    _safe_float,
)

# =============================================================================
# Helper: mock aiohttp response
# =============================================================================


def _mock_aiohttp_response(json_data: dict | list, status: int = 200) -> MagicMock:
    response = MagicMock()
    response.status = status
    response.json = AsyncMock(return_value=json_data)
    response.text = AsyncMock(return_value="error")
    response.__aenter__ = AsyncMock(return_value=response)
    response.__aexit__ = AsyncMock(return_value=False)
    return response


def _mock_session(response: MagicMock) -> MagicMock:
    session = MagicMock()
    session.get = MagicMock(return_value=response)
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    return session


# =============================================================================
# PoolAnalytics dataclass tests
# =============================================================================


class TestPoolAnalytics:
    def test_construction(self):
        pa = PoolAnalytics(
            pool_address="0xabc",
            chain="arbitrum",
            protocol="uniswap-v3",
            tvl_usd=Decimal("1000000"),
            volume_24h_usd=Decimal("500000"),
            volume_7d_usd=Decimal("3000000"),
            fee_apr=12.5,
            fee_apy=13.2,
        )
        assert pa.pool_address == "0xabc"
        assert pa.chain == "arbitrum"
        assert pa.tvl_usd == Decimal("1000000")
        assert pa.fee_apr == 12.5
        assert pa.utilization_rate is None
        assert pa.token0_weight == 0.5
        assert pa.token1_weight == 0.5

    def test_frozen(self):
        pa = PoolAnalytics(
            pool_address="0xabc",
            chain="arbitrum",
            protocol="uniswap-v3",
            tvl_usd=Decimal("1000000"),
            volume_24h_usd=Decimal("500000"),
            volume_7d_usd=Decimal("3000000"),
            fee_apr=12.5,
            fee_apy=13.2,
        )
        with pytest.raises(AttributeError):
            pa.tvl_usd = Decimal("0")  # type: ignore[misc]

    def test_with_utilization_rate(self):
        pa = PoolAnalytics(
            pool_address="0xabc",
            chain="ethereum",
            protocol="aave-v3",
            tvl_usd=Decimal("5000000"),
            volume_24h_usd=Decimal("0"),
            volume_7d_usd=Decimal("0"),
            fee_apr=4.5,
            fee_apy=4.6,
            utilization_rate=0.78,
        )
        assert pa.utilization_rate == 0.78


class TestPoolAnalyticsResult:
    def test_construction(self):
        pa = PoolAnalytics(
            pool_address="0xabc",
            chain="arbitrum",
            protocol="uniswap-v3",
            tvl_usd=Decimal("1000000"),
            volume_24h_usd=Decimal("500000"),
            volume_7d_usd=Decimal("3000000"),
            fee_apr=12.5,
            fee_apy=13.2,
        )
        result = PoolAnalyticsResult(
            pool_address="0xabc",
            analytics=pa,
            metric_value=12.5,
            metric_name="fee_apr",
        )
        assert result.pool_address == "0xabc"
        assert result.metric_value == 12.5
        assert result.metric_name == "fee_apr"


# =============================================================================
# Helper function tests
# =============================================================================


class TestSafeDecimal:
    def test_valid(self):
        assert _safe_decimal("123.45") == Decimal("123.45")

    def test_none(self):
        assert _safe_decimal(None) == Decimal(0)

    def test_invalid(self):
        assert _safe_decimal("abc") == Decimal(0)

    def test_int(self):
        assert _safe_decimal(42) == Decimal("42")


class TestSafeFloat:
    def test_valid(self):
        assert _safe_float("12.5") == 12.5

    def test_none(self):
        assert _safe_float(None) == 0.0

    def test_nan(self):
        assert _safe_float(float("nan")) == 0.0

    def test_inf(self):
        assert _safe_float(float("inf")) == 0.0

    def test_invalid(self):
        assert _safe_float("abc") == 0.0


# =============================================================================
# PoolAnalyticsReader tests
# =============================================================================

# Sample DeFi Llama pool data
_SAMPLE_LLAMA_POOL = {
    "pool": "abc123-0xabc",
    "chain": "Arbitrum",
    "project": "uniswap-v3",
    "symbol": "USDC-WETH",
    "tvlUsd": 5000000,
    "apy": 15.2,
    "apyBase": 12.5,
    "apyReward": 2.7,
    "volumeUsd1d": 1200000,
    "volumeUsd7d": 7500000,
    "ilRisk": True,
}

_SAMPLE_LLAMA_POOL_2 = {
    "pool": "def456-0xdef",
    "chain": "Arbitrum",
    "project": "aerodrome-v2",
    "symbol": "USDC-WETH",
    "tvlUsd": 3000000,
    "apy": 20.1,
    "apyBase": 18.0,
    "apyReward": 2.1,
    "volumeUsd1d": 800000,
    "volumeUsd7d": 5000000,
    "ilRisk": True,
}

# Sample GeckoTerminal pool data
_SAMPLE_GT_POOL = {
    "data": {
        "attributes": {
            "reserve_in_usd": "4500000",
            "volume_usd": {"h24": "900000"},
            "pool_fee": 0.003,
            "dex_id": "uniswap_v3",
        }
    }
}


class TestPoolAnalyticsReaderDefiLlama:
    def test_get_pool_analytics_defillama_success(self):
        reader = PoolAnalyticsReader()
        response = _mock_aiohttp_response({"data": [_SAMPLE_LLAMA_POOL]})
        session = _mock_session(response)

        with patch("aiohttp.ClientSession", return_value=session):
            result = reader.get_pool_analytics("0xabc", "arbitrum")

        assert isinstance(result, DataEnvelope)
        assert isinstance(result.value, PoolAnalytics)
        assert result.value.tvl_usd == Decimal("5000000")
        assert result.value.fee_apr == 12.5
        assert result.meta.source == "defillama"
        assert result.meta.confidence == 0.85

    def test_get_pool_analytics_defillama_not_found_falls_to_gt(self):
        reader = PoolAnalyticsReader()

        # DeFi Llama returns no matching pool
        llama_response = _mock_aiohttp_response({"data": []})
        llama_session = _mock_session(llama_response)

        # GeckoTerminal returns data
        gt_response = _mock_aiohttp_response(_SAMPLE_GT_POOL)
        gt_session = _mock_session(gt_response)

        call_count = [0]

        def session_factory(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return llama_session
            return gt_session

        with patch("aiohttp.ClientSession", side_effect=session_factory):
            result = reader.get_pool_analytics("0xabc", "arbitrum")

        assert isinstance(result, DataEnvelope)
        assert result.value.tvl_usd == Decimal("4500000")
        assert result.meta.source == "geckoterminal"

    def test_get_pool_analytics_all_fail(self):
        reader = PoolAnalyticsReader()

        # Both fail
        fail_response = _mock_aiohttp_response({}, status=500)
        fail_session = _mock_session(fail_response)

        with patch("aiohttp.ClientSession", return_value=fail_session):
            with pytest.raises(DataSourceUnavailable, match="All providers failed"):
                reader.get_pool_analytics("0xabc", "arbitrum")

    def test_get_pool_analytics_caching(self):
        reader = PoolAnalyticsReader(cache_ttl=300)
        response = _mock_aiohttp_response({"data": [_SAMPLE_LLAMA_POOL]})
        session = _mock_session(response)

        with patch("aiohttp.ClientSession", return_value=session):
            result1 = reader.get_pool_analytics("0xabc", "arbitrum")
            result2 = reader.get_pool_analytics("0xabc", "arbitrum")

        assert result1.meta.cache_hit is False
        assert result2.meta.cache_hit is True

    def test_get_pool_analytics_unsupported_chain(self):
        reader = PoolAnalyticsReader()
        # DeFi Llama doesn't support "fantom" in our mapping
        with pytest.raises(DataSourceUnavailable):
            reader.get_pool_analytics("0xabc", "fantom")


class TestPoolAnalyticsReaderGeckoTerminal:
    def test_parse_gt_pool(self):
        reader = PoolAnalyticsReader()
        analytics = reader._parse_gt_pool_to_analytics(
            _SAMPLE_GT_POOL,
            "0xpool",
            "arbitrum",
            None,
        )
        assert analytics.tvl_usd == Decimal("4500000")
        assert analytics.volume_24h_usd == Decimal("900000")
        assert analytics.protocol == "uniswap_v3"
        # fee_apr should be calculated: (900000 * 0.003 * 365) / 4500000 * 100
        expected_apr = (900000 * 0.003 * 365) / 4500000 * 100
        assert abs(analytics.fee_apr - expected_apr) < 0.01

    def test_parse_gt_pool_zero_tvl(self):
        data = {
            "data": {
                "attributes": {
                    "reserve_in_usd": "0",
                    "volume_usd": {"h24": "100"},
                    "pool_fee": 0.003,
                    "dex_id": "test",
                }
            }
        }
        reader = PoolAnalyticsReader()
        analytics = reader._parse_gt_pool_to_analytics(data, "0x", "base", None)
        assert analytics.tvl_usd == Decimal("0")
        assert analytics.fee_apr == 0.0


class TestBestPool:
    def test_best_pool_by_fee_apr(self):
        reader = PoolAnalyticsReader()
        # Pool 2 has higher APY base (18.0 vs 12.5)
        response = _mock_aiohttp_response({"data": [_SAMPLE_LLAMA_POOL, _SAMPLE_LLAMA_POOL_2]})
        session = _mock_session(response)

        with patch("aiohttp.ClientSession", return_value=session):
            result = reader.best_pool("WETH", "USDC", "arbitrum", metric="fee_apr")

        assert isinstance(result, DataEnvelope)
        assert isinstance(result.value, PoolAnalyticsResult)
        assert result.value.metric_name == "fee_apr"
        # aerodrome pool has higher fee_apr (18.0 > 12.5)
        assert result.value.metric_value == 18.0

    def test_best_pool_by_tvl(self):
        reader = PoolAnalyticsReader()
        response = _mock_aiohttp_response({"data": [_SAMPLE_LLAMA_POOL, _SAMPLE_LLAMA_POOL_2]})
        session = _mock_session(response)

        with patch("aiohttp.ClientSession", return_value=session):
            result = reader.best_pool("WETH", "USDC", "arbitrum", metric="tvl_usd")

        # Pool 1 has higher TVL (5M > 3M)
        assert float(result.value.metric_value) == 5000000.0

    def test_best_pool_no_match(self):
        reader = PoolAnalyticsReader()
        # No matching pool for this pair
        response = _mock_aiohttp_response({"data": [_SAMPLE_LLAMA_POOL]})
        session = _mock_session(response)

        with patch("aiohttp.ClientSession", return_value=session):
            with pytest.raises(DataSourceUnavailable, match="No pools found"):
                reader.best_pool("ARB", "DAI", "arbitrum")

    def test_best_pool_invalid_metric(self):
        reader = PoolAnalyticsReader()
        response = _mock_aiohttp_response({"data": [_SAMPLE_LLAMA_POOL]})
        session = _mock_session(response)

        with patch("aiohttp.ClientSession", return_value=session):
            with pytest.raises(ValueError, match="Invalid metric"):
                reader.best_pool("WETH", "USDC", "arbitrum", metric="invalid")

    def test_best_pool_with_protocol_filter(self):
        reader = PoolAnalyticsReader()
        response = _mock_aiohttp_response({"data": [_SAMPLE_LLAMA_POOL, _SAMPLE_LLAMA_POOL_2]})
        session = _mock_session(response)

        with patch("aiohttp.ClientSession", return_value=session):
            result = reader.best_pool(
                "WETH",
                "USDC",
                "arbitrum",
                protocols=["uniswap_v3"],
            )

        # Only uniswap pool matches
        assert result.value.analytics.protocol == "uniswap-v3"


class TestPoolAnalyticsReaderHealth:
    def test_health_initial(self):
        reader = PoolAnalyticsReader()
        h = reader.health()
        assert h["defillama"]["successes"] == 0
        assert h["defillama"]["failures"] == 0


class TestPoolAnalyticsReaderEdgeCases:
    def test_case_insensitive_chain(self):
        reader = PoolAnalyticsReader()
        response = _mock_aiohttp_response({"data": [_SAMPLE_LLAMA_POOL]})
        session = _mock_session(response)

        with patch("aiohttp.ClientSession", return_value=session):
            result = reader.get_pool_analytics("0xABC", "ARBITRUM")

        assert result.value.chain == "arbitrum"
        assert result.value.pool_address == "0xabc"

    def test_pool_with_missing_volume(self):
        pool = {**_SAMPLE_LLAMA_POOL, "volumeUsd1d": None, "volumeUsd7d": None}
        reader = PoolAnalyticsReader()
        analytics = reader._parse_llama_pool_to_analytics(pool, "0xabc", "arbitrum", None)
        assert analytics.volume_24h_usd == Decimal(0)
        assert analytics.volume_7d_usd == Decimal(0)
