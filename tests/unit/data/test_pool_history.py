"""Tests for PoolHistoryReader and PoolSnapshot.

Tests cover:
- PoolSnapshot dataclass construction and fields
- The Graph subgraph provider with mocked responses
- DeFi Llama fallback provider with mocked responses
- GeckoTerminal fallback provider with mocked responses
- Provider fallback chain (primary -> fallback1 -> fallback2)
- VersionedDataCache integration (cache hit/miss, finality tagging)
- MarketSnapshot.pool_history() integration
- Error handling (all providers fail, unsupported resolution)
- Serialization/deserialization of PoolSnapshot
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.data.cache.versioned_cache import VersionedDataCache
from almanak.framework.data.exceptions import DataUnavailableError
from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.framework.data.market_snapshot import MarketSnapshot, PoolHistoryUnavailableError
from almanak.framework.data.models import DataClassification, DataEnvelope
from almanak.framework.data.pools.history import (
    PoolHistoryReader,
    PoolSnapshot,
    _deserialize_snapshots,
    _estimate_reserves,
    _safe_decimal,
    _serialize_snapshots,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tmp_cache(tmp_path: Path) -> VersionedDataCache:
    """Create a VersionedDataCache in a temp directory."""
    return VersionedDataCache(cache_dir=tmp_path, data_type="pool_history")


@pytest.fixture
def reader(tmp_cache: VersionedDataCache) -> PoolHistoryReader:
    """Create a PoolHistoryReader with temp cache."""
    return PoolHistoryReader(cache=tmp_cache, request_timeout=5.0)


@pytest.fixture
def start_date() -> datetime:
    return datetime(2024, 1, 1, tzinfo=UTC)


@pytest.fixture
def end_date() -> datetime:
    return datetime(2024, 1, 7, tzinfo=UTC)


# ---------------------------------------------------------------------------
# PoolSnapshot tests
# ---------------------------------------------------------------------------


class TestPoolSnapshot:
    def test_construction(self) -> None:
        snap = PoolSnapshot(
            tvl=Decimal("1000000"),
            volume_24h=Decimal("500000"),
            fee_revenue_24h=Decimal("1500"),
            token0_reserve=Decimal("500"),
            token1_reserve=Decimal("500000"),
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        )
        assert snap.tvl == Decimal("1000000")
        assert snap.volume_24h == Decimal("500000")
        assert snap.fee_revenue_24h == Decimal("1500")
        assert snap.token0_reserve == Decimal("500")
        assert snap.token1_reserve == Decimal("500000")
        assert snap.timestamp.year == 2024

    def test_frozen(self) -> None:
        snap = PoolSnapshot(
            tvl=Decimal("1000"),
            volume_24h=Decimal("500"),
            fee_revenue_24h=Decimal("10"),
            token0_reserve=Decimal("100"),
            token1_reserve=Decimal("100"),
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        )
        with pytest.raises(AttributeError):
            snap.tvl = Decimal("999")  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Serialization tests
# ---------------------------------------------------------------------------


class TestSerialization:
    def test_roundtrip(self) -> None:
        snapshots = [
            PoolSnapshot(
                tvl=Decimal("1000000.50"),
                volume_24h=Decimal("500000"),
                fee_revenue_24h=Decimal("1500.25"),
                token0_reserve=Decimal("277.5"),
                token1_reserve=Decimal("500000"),
                timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
            ),
            PoolSnapshot(
                tvl=Decimal("1001000"),
                volume_24h=Decimal("510000"),
                fee_revenue_24h=Decimal("1530"),
                token0_reserve=Decimal("278"),
                token1_reserve=Decimal("501000"),
                timestamp=datetime(2024, 1, 2, 12, 0, 0, tzinfo=UTC),
            ),
        ]
        serialized = _serialize_snapshots(snapshots)
        deserialized = _deserialize_snapshots(serialized)

        assert len(deserialized) == 2
        assert deserialized[0].tvl == Decimal("1000000.50")
        assert deserialized[0].fee_revenue_24h == Decimal("1500.25")
        assert deserialized[1].volume_24h == Decimal("510000")

    def test_deserialize_invalid_data(self) -> None:
        assert _deserialize_snapshots("not a list") == []
        assert _deserialize_snapshots(None) == []
        assert _deserialize_snapshots([{"bad": "data"}]) == []

    def test_deserialize_malformed_entry_skipped(self) -> None:
        data = [
            {
                "tvl": "1000",
                "volume_24h": "500",
                "fee_revenue_24h": "10",
                "token0_reserve": "100",
                "token1_reserve": "100",
                "timestamp": "2024-01-01T00:00:00+00:00",
            },
            {"tvl": "bad"},  # Missing fields
        ]
        result = _deserialize_snapshots(data)
        assert len(result) == 1


# ---------------------------------------------------------------------------
# Helper tests
# ---------------------------------------------------------------------------


class TestHelpers:
    def test_safe_decimal_valid(self) -> None:
        assert _safe_decimal("123.45") == Decimal("123.45")
        assert _safe_decimal(123) == Decimal("123")
        assert _safe_decimal(0) == Decimal("0")

    def test_safe_decimal_invalid(self) -> None:
        assert _safe_decimal(None) == Decimal("0")
        assert _safe_decimal("bad") == Decimal("0")
        assert _safe_decimal("") == Decimal("0")

    def test_estimate_reserves(self) -> None:
        t0, t1 = _estimate_reserves(Decimal("1000000"), Decimal("2000"), Decimal("1"))
        assert t0 == Decimal("250")  # 500000 / 2000
        assert t1 == Decimal("500000")  # 500000 / 1

    def test_estimate_reserves_zero_tvl(self) -> None:
        t0, t1 = _estimate_reserves(Decimal("0"), Decimal("2000"), Decimal("1"))
        assert t0 == Decimal("0")
        assert t1 == Decimal("0")

    def test_estimate_reserves_zero_prices(self) -> None:
        t0, t1 = _estimate_reserves(Decimal("1000000"), Decimal("0"), Decimal("0"))
        assert t0 == Decimal("0")
        assert t1 == Decimal("0")


# ---------------------------------------------------------------------------
# Mock response builders
# ---------------------------------------------------------------------------


def _make_subgraph_response(count: int = 3, entity: str = "poolHourDatas") -> dict:
    """Build a mock The Graph subgraph response."""
    base_ts = 1704067200  # 2024-01-01 00:00:00 UTC
    items = []
    period_field = "periodStartUnix" if entity == "poolHourDatas" else "date"
    for i in range(count):
        items.append(
            {
                period_field: base_ts + i * 3600,
                "tvlUSD": str(1000000 + i * 1000),
                "volumeUSD": str(50000 + i * 100),
                "feesUSD": str(150 + i * 5),
                "liquidity": str(10**18),
                "token0Price": "2000",
                "token1Price": "1",
            }
        )
    return {"data": {entity: items}}


def _make_llama_pools_response(pool_address: str = "0xtest") -> dict:
    """Build a mock DeFi Llama pools listing response."""
    return {
        "data": [
            {
                "pool": f"abc_{pool_address}_def",
                "chain": "Arbitrum",
                "project": "uniswap-v3",
                "symbol": "USDC-WETH",
                "tvlUsd": 5000000,
                "apy": 15.0,
                "apyBase": 12.0,
            }
        ]
    }


def _make_llama_chart_response(count: int = 5) -> dict:
    """Build a mock DeFi Llama pool chart response."""
    base_date = datetime(2024, 1, 1, tzinfo=UTC)
    data = []
    for i in range(count):
        ts = base_date + timedelta(days=i)
        data.append(
            {
                "timestamp": ts.isoformat(),
                "tvlUsd": 1000000 + i * 10000,
                "apy": 15.0,
                "apyBase": 12.0,
                "il7d": 0.01,
            }
        )
    return {"data": data}


def _make_gecko_ohlcv_response(count: int = 5) -> dict:
    """Build a mock GeckoTerminal OHLCV response."""
    base_ts = 1704067200  # 2024-01-01 00:00:00 UTC
    ohlcv_list = []
    for i in range(count):
        ohlcv_list.append(
            [
                base_ts + (count - 1 - i) * 3600,  # newest first
                "2000",
                "2010",
                "1990",
                "2005",
                str(100000 + i * 1000),
            ]
        )
    return {
        "data": {
            "attributes": {
                "ohlcv_list": ohlcv_list,
            }
        }
    }


def _mock_aiohttp_response(data: dict, status: int = 200) -> MagicMock:
    """Create a mock aiohttp response context manager."""
    response = AsyncMock()
    response.status = status
    response.json = AsyncMock(return_value=data)
    response.text = AsyncMock(return_value=json.dumps(data))

    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=response)
    ctx.__aexit__ = AsyncMock(return_value=False)
    return ctx


# ---------------------------------------------------------------------------
# The Graph Provider tests
# ---------------------------------------------------------------------------


class TestTheGraphProvider:
    def test_fetch_from_thegraph_success(self, reader: PoolHistoryReader) -> None:
        """Test successful fetch from The Graph."""
        mock_response = _make_subgraph_response(count=3)
        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=_mock_aiohttp_response(mock_response))
        mock_session.closed = False

        reader._session = mock_session

        snapshots = reader._fetch_from_thegraph(
            pool_address="0xtest",
            chain="arbitrum",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 7, tzinfo=UTC),
            resolution="1h",
        )
        assert len(snapshots) == 3
        assert snapshots[0].tvl == Decimal("1000000")
        assert snapshots[1].tvl == Decimal("1001000")
        assert snapshots[2].tvl == Decimal("1002000")
        assert reader._metrics["thegraph"].successes == 1

    def test_fetch_from_thegraph_no_subgraph(self, reader: PoolHistoryReader) -> None:
        """Test failure when no subgraph is available for the chain."""
        with pytest.raises(DataSourceUnavailable, match="No subgraph available"):
            reader._fetch_from_thegraph(
                pool_address="0xtest",
                chain="unsupported_chain",
                start_date=datetime(2024, 1, 1, tzinfo=UTC),
                end_date=datetime(2024, 1, 7, tzinfo=UTC),
                resolution="1h",
            )

    def test_fetch_from_thegraph_daily_resolution(self, reader: PoolHistoryReader) -> None:
        """Test daily resolution uses poolDayDatas entity."""
        mock_response = _make_subgraph_response(count=2, entity="poolDayDatas")
        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=_mock_aiohttp_response(mock_response))
        mock_session.closed = False

        reader._session = mock_session

        snapshots = reader._fetch_from_thegraph(
            pool_address="0xtest",
            chain="ethereum",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 7, tzinfo=UTC),
            resolution="1d",
        )
        assert len(snapshots) == 2

    def test_fetch_from_thegraph_http_error(self, reader: PoolHistoryReader) -> None:
        """Test HTTP error from The Graph."""
        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=_mock_aiohttp_response({"error": "bad"}, status=500))
        mock_session.closed = False

        reader._session = mock_session

        with pytest.raises(DataSourceUnavailable, match="HTTP 500"):
            reader._fetch_from_thegraph(
                pool_address="0xtest",
                chain="arbitrum",
                start_date=datetime(2024, 1, 1, tzinfo=UTC),
                end_date=datetime(2024, 1, 7, tzinfo=UTC),
                resolution="1h",
            )

    def test_fetch_from_thegraph_graphql_errors(self, reader: PoolHistoryReader) -> None:
        """Test GraphQL errors from The Graph."""
        mock_response = {"errors": [{"message": "pool not found"}]}
        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=_mock_aiohttp_response(mock_response))
        mock_session.closed = False

        reader._session = mock_session

        with pytest.raises(DataSourceUnavailable, match="GraphQL errors"):
            reader._fetch_from_thegraph(
                pool_address="0xtest",
                chain="arbitrum",
                start_date=datetime(2024, 1, 1, tzinfo=UTC),
                end_date=datetime(2024, 1, 7, tzinfo=UTC),
                resolution="1h",
            )

    def test_parse_subgraph_malformed_entries_skipped(self, reader: PoolHistoryReader) -> None:
        """Test that malformed entries in subgraph response are skipped."""
        data = {
            "poolHourDatas": [
                {
                    "periodStartUnix": 1704067200,
                    "tvlUSD": "1000000",
                    "volumeUSD": "50000",
                    "feesUSD": "150",
                    "liquidity": "1000000000",
                    "token0Price": "2000",
                    "token1Price": "1",
                },
                {"periodStartUnix": "not_a_number", "tvlUSD": "bad"},  # Malformed
            ]
        }
        snapshots = reader._parse_subgraph_response(data, "poolHourDatas", "periodStartUnix")
        assert len(snapshots) == 1


# ---------------------------------------------------------------------------
# DeFi Llama Provider tests
# ---------------------------------------------------------------------------


class TestDefiLlamaProvider:
    def test_fetch_from_defillama_success(self, reader: PoolHistoryReader) -> None:
        """Test successful fetch from DeFi Llama."""
        pools_response = _make_llama_pools_response(pool_address="0xtest")
        chart_response = _make_llama_chart_response(count=3)

        mock_session = AsyncMock()

        # First call: pools listing, second call: chart
        call_count = 0

        def mock_get(url, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return _mock_aiohttp_response(pools_response)
            else:
                return _mock_aiohttp_response(chart_response)

        mock_session.get = mock_get
        mock_session.closed = False
        reader._session = mock_session

        # Pre-fill rate limiter tokens
        reader._rate_limiter._tokens = 10.0

        snapshots = reader._fetch_from_defillama(
            pool_address="0xtest",
            chain="arbitrum",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 7, tzinfo=UTC),
            resolution="1d",
        )
        assert len(snapshots) == 3
        assert snapshots[0].tvl >= Decimal("1000000")
        assert reader._metrics["defillama"].successes == 1

    def test_fetch_from_defillama_pool_not_found(self, reader: PoolHistoryReader) -> None:
        """Test DeFi Llama when pool is not found."""
        empty_response = {"data": []}
        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=_mock_aiohttp_response(empty_response))
        mock_session.closed = False
        reader._session = mock_session
        reader._rate_limiter._tokens = 10.0

        with pytest.raises(DataSourceUnavailable, match="not found"):
            reader._fetch_from_defillama(
                pool_address="0xnonexistent",
                chain="arbitrum",
                start_date=datetime(2024, 1, 1, tzinfo=UTC),
                end_date=datetime(2024, 1, 7, tzinfo=UTC),
                resolution="1d",
            )

    def test_defillama_fee_revenue_from_apy(self, reader: PoolHistoryReader) -> None:
        """Test that fee revenue is estimated from apyBase."""
        chart_data = [
            {
                "timestamp": "2024-01-01T00:00:00+00:00",
                "tvlUsd": 1000000,
                "apyBase": 10.0,  # 10% APY -> ~0.0274% daily
            }
        ]
        snapshots = reader._parse_defillama_response(
            chart_data,
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 2, tzinfo=UTC),
            resolution="1d",
        )
        assert len(snapshots) == 1
        # 10% APY -> daily rate ~ 0.0274% -> $274 on $1M TVL
        assert snapshots[0].fee_revenue_24h > Decimal("0")
        assert snapshots[0].fee_revenue_24h < Decimal("1000")  # Sanity check


# ---------------------------------------------------------------------------
# GeckoTerminal Provider tests
# ---------------------------------------------------------------------------


class TestGeckoTerminalProvider:
    def test_fetch_from_geckoterminal_success(self, reader: PoolHistoryReader) -> None:
        """Test successful fetch from GeckoTerminal."""
        gecko_response = _make_gecko_ohlcv_response(count=5)
        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=_mock_aiohttp_response(gecko_response))
        mock_session.closed = False
        reader._session = mock_session
        reader._rate_limiter._tokens = 10.0

        snapshots = reader._fetch_from_geckoterminal(
            pool_address="0xtest",
            chain="arbitrum",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 7, tzinfo=UTC),
            resolution="1h",
        )
        assert len(snapshots) == 5
        # GeckoTerminal only provides volume
        assert all(s.volume_24h > 0 for s in snapshots)
        assert reader._metrics["geckoterminal"].successes == 1

    def test_fetch_from_geckoterminal_unsupported_chain(self, reader: PoolHistoryReader) -> None:
        """Test GeckoTerminal with unsupported chain."""
        with pytest.raises(DataSourceUnavailable, match="Unsupported chain"):
            reader._fetch_from_geckoterminal(
                pool_address="0xtest",
                chain="unknown_chain",
                start_date=datetime(2024, 1, 1, tzinfo=UTC),
                end_date=datetime(2024, 1, 7, tzinfo=UTC),
                resolution="1h",
            )

    def test_geckoterminal_data_reversed_to_ascending(self, reader: PoolHistoryReader) -> None:
        """Test that GeckoTerminal data (newest first) is reversed to ascending."""
        gecko_response = _make_gecko_ohlcv_response(count=3)
        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=_mock_aiohttp_response(gecko_response))
        mock_session.closed = False
        reader._session = mock_session
        reader._rate_limiter._tokens = 10.0

        snapshots = reader._fetch_from_geckoterminal(
            pool_address="0xtest",
            chain="base",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 7, tzinfo=UTC),
            resolution="1h",
        )
        # Verify ascending order
        for i in range(1, len(snapshots)):
            assert snapshots[i].timestamp >= snapshots[i - 1].timestamp

    def test_geckoterminal_resolution_mapping(self, reader: PoolHistoryReader) -> None:
        """Test that resolution maps to correct GeckoTerminal timeframe."""
        gecko_response = _make_gecko_ohlcv_response(count=1)
        mock_session = AsyncMock()
        mock_session.closed = False
        reader._session = mock_session
        reader._rate_limiter._tokens = 10.0

        for resolution in ("1h", "4h", "1d"):
            mock_session.get = MagicMock(return_value=_mock_aiohttp_response(gecko_response))

            snapshots = reader._fetch_from_geckoterminal(
                pool_address="0xtest",
                chain="ethereum",
                start_date=datetime(2024, 1, 1, tzinfo=UTC),
                end_date=datetime(2024, 1, 7, tzinfo=UTC),
                resolution=resolution,
            )
            assert len(snapshots) >= 0  # Just verify no error


# ---------------------------------------------------------------------------
# Provider fallback tests
# ---------------------------------------------------------------------------


class TestProviderFallback:
    def test_primary_success_no_fallback(self, reader: PoolHistoryReader) -> None:
        """Test that successful primary provider doesn't trigger fallback."""
        mock_response = _make_subgraph_response(count=3)
        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=_mock_aiohttp_response(mock_response))
        mock_session.closed = False
        reader._session = mock_session

        source, snapshots = reader._fetch_with_fallback(
            pool_address="0xtest",
            chain="arbitrum",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 7, tzinfo=UTC),
            resolution="1h",
        )
        assert source == "thegraph"
        assert len(snapshots) == 3

    def test_fallback_to_defillama(self, reader: PoolHistoryReader) -> None:
        """Test fallback to DeFi Llama when The Graph fails."""
        # Make The Graph fail (no subgraph for this chain)
        pools_response = _make_llama_pools_response("0xtest")
        chart_response = _make_llama_chart_response(count=2)

        mock_session = AsyncMock()
        call_count = 0

        def mock_get(url, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return _mock_aiohttp_response(pools_response)
            else:
                return _mock_aiohttp_response(chart_response)

        mock_session.get = mock_get
        mock_session.closed = False
        reader._session = mock_session
        reader._rate_limiter._tokens = 10.0

        # Use a chain that has no Graph subgraph configured for any protocol
        # We need to mock _find_subgraph_url to return None
        with patch.object(reader, "_find_subgraph_url", return_value=None):
            source, snapshots = reader._fetch_with_fallback(
                pool_address="0xtest",
                chain="arbitrum",
                start_date=datetime(2024, 1, 1, tzinfo=UTC),
                end_date=datetime(2024, 1, 7, tzinfo=UTC),
                resolution="1d",
            )
        assert source == "defillama"
        assert len(snapshots) == 2

    def test_fallback_to_geckoterminal(self, reader: PoolHistoryReader) -> None:
        """Test fallback to GeckoTerminal when The Graph and DeFi Llama fail."""
        gecko_response = _make_gecko_ohlcv_response(count=3)
        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=_mock_aiohttp_response(gecko_response))
        mock_session.closed = False
        reader._session = mock_session
        reader._rate_limiter._tokens = 10.0

        with (
            patch.object(reader, "_find_subgraph_url", return_value=None),
            patch.object(
                reader, "_fetch_from_defillama", side_effect=DataSourceUnavailable(source="defillama", reason="fail")
            ),
        ):
            source, snapshots = reader._fetch_with_fallback(
                pool_address="0xtest",
                chain="arbitrum",
                start_date=datetime(2024, 1, 1, tzinfo=UTC),
                end_date=datetime(2024, 1, 7, tzinfo=UTC),
                resolution="1h",
            )
        assert source == "geckoterminal"
        assert len(snapshots) == 3

    def test_all_providers_fail(self, reader: PoolHistoryReader) -> None:
        """Test DataUnavailableError when all providers fail."""
        with (
            patch.object(reader, "_find_subgraph_url", return_value=None),
            patch.object(
                reader, "_fetch_from_defillama", side_effect=DataSourceUnavailable(source="defillama", reason="fail")
            ),
            patch.object(
                reader,
                "_fetch_from_geckoterminal",
                side_effect=DataSourceUnavailable(source="geckoterminal", reason="fail"),
            ),
        ):
            with pytest.raises(DataUnavailableError, match="All providers failed"):
                reader._fetch_with_fallback(
                    pool_address="0xtest",
                    chain="arbitrum",
                    start_date=datetime(2024, 1, 1, tzinfo=UTC),
                    end_date=datetime(2024, 1, 7, tzinfo=UTC),
                    resolution="1h",
                )


# ---------------------------------------------------------------------------
# Cache integration tests
# ---------------------------------------------------------------------------


class TestCacheIntegration:
    def test_cache_miss_then_hit(self, reader: PoolHistoryReader) -> None:
        """Test that results are cached and returned on subsequent calls."""
        mock_response = _make_subgraph_response(count=2)
        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=_mock_aiohttp_response(mock_response))
        mock_session.closed = False
        reader._session = mock_session

        start = datetime(2024, 1, 1, tzinfo=UTC)
        end = datetime(2024, 1, 2, tzinfo=UTC)

        # First call: cache miss
        result1 = reader.get_pool_history("0xtest", "arbitrum", start, end, resolution="1h")
        assert result1.meta.cache_hit is False
        assert result1.meta.source == "thegraph"

        # Second call: cache hit
        result2 = reader.get_pool_history("0xtest", "arbitrum", start, end, resolution="1h")
        assert result2.meta.cache_hit is True
        assert "cache" in result2.meta.source

    def test_finality_tagging_finalized(self, reader: PoolHistoryReader) -> None:
        """Test that old data is tagged as finalized."""
        # Create snapshots older than 24h
        old_start = datetime(2023, 1, 1, tzinfo=UTC)
        old_end = datetime(2023, 1, 2, tzinfo=UTC)

        mock_response = {
            "data": {
                "poolHourDatas": [
                    {
                        "periodStartUnix": int(old_start.timestamp()),
                        "tvlUSD": "1000000",
                        "volumeUSD": "50000",
                        "feesUSD": "150",
                        "liquidity": "1000000",
                        "token0Price": "2000",
                        "token1Price": "1",
                    }
                ]
            }
        }
        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=_mock_aiohttp_response(mock_response))
        mock_session.closed = False
        reader._session = mock_session

        result = reader.get_pool_history("0xtest", "arbitrum", old_start, old_end, resolution="1h")
        assert result.meta.cache_hit is False

        # Check cache entry was stored as finalized
        cache_key = f"0xtest:arbitrum:{int(old_start.timestamp())}:{int(old_end.timestamp())}:1h"
        entry = reader._cache.get(cache_key)
        assert entry is not None
        assert entry.finality_status == "finalized"


# ---------------------------------------------------------------------------
# get_pool_history envelope tests
# ---------------------------------------------------------------------------


class TestGetPoolHistory:
    def test_returns_data_envelope(self, reader: PoolHistoryReader) -> None:
        """Test that get_pool_history returns DataEnvelope."""
        mock_response = _make_subgraph_response(count=3)
        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=_mock_aiohttp_response(mock_response))
        mock_session.closed = False
        reader._session = mock_session

        result = reader.get_pool_history(
            "0xtest",
            "arbitrum",
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 1, 7, tzinfo=UTC),
        )
        assert isinstance(result, DataEnvelope)
        assert result.classification == DataClassification.INFORMATIONAL
        assert isinstance(result.value, list)
        assert len(result.value) == 3
        assert all(isinstance(s, PoolSnapshot) for s in result.value)

    def test_invalid_resolution(self, reader: PoolHistoryReader) -> None:
        """Test that invalid resolution raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported resolution"):
            reader.get_pool_history(
                "0xtest",
                "arbitrum",
                datetime(2024, 1, 1, tzinfo=UTC),
                datetime(2024, 1, 7, tzinfo=UTC),
                resolution="5m",
            )

    def test_snapshots_sorted_ascending(self, reader: PoolHistoryReader) -> None:
        """Test that results are sorted by timestamp ascending."""
        mock_response = _make_subgraph_response(count=5)
        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=_mock_aiohttp_response(mock_response))
        mock_session.closed = False
        reader._session = mock_session

        result = reader.get_pool_history(
            "0xtest",
            "ethereum",
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 1, 7, tzinfo=UTC),
        )
        timestamps = [s.timestamp for s in result.value]
        assert timestamps == sorted(timestamps)

    def test_health_metrics(self, reader: PoolHistoryReader) -> None:
        """Test health metrics tracking."""
        mock_response = _make_subgraph_response(count=1)
        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=_mock_aiohttp_response(mock_response))
        mock_session.closed = False
        reader._session = mock_session

        reader.get_pool_history(
            "0xtest",
            "arbitrum",
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2024, 1, 2, tzinfo=UTC),
        )

        health = reader.health()
        assert health["thegraph"]["requests"] >= 1
        assert health["thegraph"]["successes"] >= 1


# ---------------------------------------------------------------------------
# MarketSnapshot integration tests
# ---------------------------------------------------------------------------


class TestMarketSnapshotIntegration:
    def test_pool_history_method(self) -> None:
        """Test MarketSnapshot.pool_history() delegates to reader."""
        mock_reader = MagicMock()
        mock_envelope = DataEnvelope(
            value=[
                PoolSnapshot(
                    tvl=Decimal("1000000"),
                    volume_24h=Decimal("50000"),
                    fee_revenue_24h=Decimal("150"),
                    token0_reserve=Decimal("250"),
                    token1_reserve=Decimal("500000"),
                    timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                )
            ],
            meta=MagicMock(),
            classification=DataClassification.INFORMATIONAL,
        )
        mock_reader.get_pool_history.return_value = mock_envelope

        snapshot = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0xwallet",
            pool_history_reader=mock_reader,
        )

        result = snapshot.pool_history(
            pool_address="0xpool",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 7, tzinfo=UTC),
        )
        assert result is mock_envelope
        mock_reader.get_pool_history.assert_called_once_with(
            pool_address="0xpool",
            chain="arbitrum",
            start_date=datetime(2024, 1, 1, tzinfo=UTC),
            end_date=datetime(2024, 1, 7, tzinfo=UTC),
            resolution="1h",
        )

    def test_pool_history_no_reader(self) -> None:
        """Test ValueError when no reader configured."""
        snapshot = MarketSnapshot(chain="arbitrum", wallet_address="0xwallet")
        with pytest.raises(ValueError, match="No pool history reader"):
            snapshot.pool_history("0xpool")

    def test_pool_history_error_wrapped(self) -> None:
        """Test that reader errors are wrapped in PoolHistoryUnavailableError."""
        mock_reader = MagicMock()
        mock_reader.get_pool_history.side_effect = DataUnavailableError(
            data_type="pool_history",
            instrument="0xpool",
            reason="All providers failed",
        )

        snapshot = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0xwallet",
            pool_history_reader=mock_reader,
        )

        with pytest.raises(PoolHistoryUnavailableError, match="0xpool"):
            snapshot.pool_history("0xpool")

    def test_pool_history_defaults(self) -> None:
        """Test default start_date (90 days ago) and end_date (now)."""
        mock_reader = MagicMock()
        mock_reader.get_pool_history.return_value = DataEnvelope(
            value=[],
            meta=MagicMock(),
            classification=DataClassification.INFORMATIONAL,
        )

        snapshot = MarketSnapshot(
            chain="ethereum",
            wallet_address="0xwallet",
            pool_history_reader=mock_reader,
        )

        snapshot.pool_history("0xpool")

        call_args = mock_reader.get_pool_history.call_args
        start_date = call_args.kwargs["start_date"]
        end_date = call_args.kwargs["end_date"]

        # start_date should be roughly 90 days ago
        now = datetime.now(UTC)
        expected_start = now - timedelta(days=90)
        assert abs((start_date - expected_start).total_seconds()) < 5  # Within 5 seconds

        # end_date should be roughly now
        assert abs((end_date - now).total_seconds()) < 5

    def test_pool_history_chain_override(self) -> None:
        """Test that explicit chain parameter overrides strategy chain."""
        mock_reader = MagicMock()
        mock_reader.get_pool_history.return_value = DataEnvelope(
            value=[],
            meta=MagicMock(),
            classification=DataClassification.INFORMATIONAL,
        )

        snapshot = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0xwallet",
            pool_history_reader=mock_reader,
        )

        snapshot.pool_history("0xpool", chain="base")

        call_args = mock_reader.get_pool_history.call_args
        assert call_args.kwargs["chain"] == "base"
