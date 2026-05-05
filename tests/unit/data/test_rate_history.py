"""Tests for RateHistoryReader, LendingRateSnapshot, and FundingRateSnapshot.

Tests cover:
- LendingRateSnapshot and FundingRateSnapshot dataclass construction
- The Graph subgraph provider for lending rates (Aave, Compound) with mocked responses
- DeFi Llama fallback provider for lending rates with mocked responses
- Hyperliquid provider for funding rates with mocked responses
- DeFi Llama fallback for funding rates
- Provider fallback chain (primary -> fallback)
- VersionedDataCache integration (cache hit/miss, finality tagging)
- MarketSnapshot.lending_rate_history() and funding_rate_history() integration
- Error handling (all providers fail, invalid inputs)
- Serialization/deserialization of snapshots
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.data.cache.versioned_cache import VersionedDataCache
from almanak.framework.data.exceptions import DataUnavailableError
from almanak.framework.data.market_snapshot import (
    FundingRateHistoryUnavailableError,
    LendingRateHistoryUnavailableError,
    MarketSnapshot,
)
from almanak.framework.data.models import DataClassification, DataEnvelope
from almanak.framework.data.rates.history import (
    FundingRateSnapshot,
    LendingRateSnapshot,
    RateHistoryReader,
    _deserialize_funding_snapshots,
    _deserialize_lending_snapshots,
    _safe_decimal,
    _serialize_funding_snapshots,
    _serialize_lending_snapshots,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tmp_cache(tmp_path: Path) -> VersionedDataCache:
    """Create a VersionedDataCache in a temp directory."""
    return VersionedDataCache(cache_dir=tmp_path, data_type="rate_history")


@pytest.fixture
def reader(tmp_cache: VersionedDataCache) -> RateHistoryReader:
    """Create a RateHistoryReader with temp cache."""
    return RateHistoryReader(cache=tmp_cache, request_timeout=5.0)


@pytest.fixture
def start_date() -> datetime:
    return datetime(2024, 1, 1, tzinfo=UTC)


@pytest.fixture
def end_date() -> datetime:
    return datetime(2024, 1, 7, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Helper: mock aiohttp response
# ---------------------------------------------------------------------------


def _mock_aiohttp_response(data: dict | list, status: int = 200) -> MagicMock:
    """Create a mock aiohttp response context manager."""
    response = AsyncMock()
    response.status = status
    response.json = AsyncMock(return_value=data)
    response.text = AsyncMock(return_value=str(data))

    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=response)
    ctx.__aexit__ = AsyncMock(return_value=False)
    return ctx


# ---------------------------------------------------------------------------
# LendingRateSnapshot tests
# ---------------------------------------------------------------------------


class TestLendingRateSnapshot:
    def test_construction(self) -> None:
        snap = LendingRateSnapshot(
            supply_apy=Decimal("5.25"),
            borrow_apy=Decimal("7.50"),
            utilization=Decimal("82.3"),
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        )
        assert snap.supply_apy == Decimal("5.25")
        assert snap.borrow_apy == Decimal("7.50")
        assert snap.utilization == Decimal("82.3")
        assert snap.timestamp.year == 2024

    def test_construction_no_utilization(self) -> None:
        snap = LendingRateSnapshot(
            supply_apy=Decimal("3.0"),
            borrow_apy=Decimal("5.0"),
            utilization=None,
            timestamp=datetime(2024, 6, 15, tzinfo=UTC),
        )
        assert snap.utilization is None

    def test_frozen(self) -> None:
        snap = LendingRateSnapshot(
            supply_apy=Decimal("5.0"),
            borrow_apy=Decimal("7.0"),
            utilization=Decimal("80.0"),
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        )
        with pytest.raises(AttributeError):
            snap.supply_apy = Decimal("999")  # type: ignore[misc]


# ---------------------------------------------------------------------------
# FundingRateSnapshot tests
# ---------------------------------------------------------------------------


class TestFundingRateSnapshot:
    def test_construction(self) -> None:
        snap = FundingRateSnapshot(
            rate=Decimal("0.0001"),
            annualized_rate=Decimal("0.876"),
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        )
        assert snap.rate == Decimal("0.0001")
        assert snap.annualized_rate == Decimal("0.876")

    def test_frozen(self) -> None:
        snap = FundingRateSnapshot(
            rate=Decimal("0.0001"),
            annualized_rate=Decimal("0.876"),
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        )
        with pytest.raises(AttributeError):
            snap.rate = Decimal("999")  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Serialization tests
# ---------------------------------------------------------------------------


class TestLendingSerialization:
    def test_round_trip(self) -> None:
        snapshots = [
            LendingRateSnapshot(
                supply_apy=Decimal("5.25"),
                borrow_apy=Decimal("7.50"),
                utilization=Decimal("82.3"),
                timestamp=datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
            ),
            LendingRateSnapshot(
                supply_apy=Decimal("5.30"),
                borrow_apy=Decimal("7.45"),
                utilization=None,
                timestamp=datetime(2024, 1, 2, 12, 0, tzinfo=UTC),
            ),
        ]
        serialized = _serialize_lending_snapshots(snapshots)
        deserialized = _deserialize_lending_snapshots(serialized)

        assert len(deserialized) == 2
        assert deserialized[0].supply_apy == Decimal("5.25")
        assert deserialized[0].borrow_apy == Decimal("7.50")
        assert deserialized[0].utilization == Decimal("82.3")
        assert deserialized[1].utilization is None

    def test_deserialize_malformed_skips(self) -> None:
        data = [
            {"supply_apy": "5.0", "borrow_apy": "7.0", "timestamp": "2024-01-01T00:00:00+00:00"},
            {"bad_field": "nope"},
        ]
        result = _deserialize_lending_snapshots(data)
        assert len(result) == 1

    def test_deserialize_non_list(self) -> None:
        assert _deserialize_lending_snapshots("not a list") == []
        assert _deserialize_lending_snapshots(None) == []


class TestFundingSerialization:
    def test_round_trip(self) -> None:
        snapshots = [
            FundingRateSnapshot(
                rate=Decimal("0.0001"),
                annualized_rate=Decimal("0.876"),
                timestamp=datetime(2024, 1, 1, 8, 0, tzinfo=UTC),
            ),
        ]
        serialized = _serialize_funding_snapshots(snapshots)
        deserialized = _deserialize_funding_snapshots(serialized)

        assert len(deserialized) == 1
        assert deserialized[0].rate == Decimal("0.0001")
        assert deserialized[0].annualized_rate == Decimal("0.876")

    def test_deserialize_malformed_skips(self) -> None:
        data = [{"rate": "0.0001", "annualized_rate": "0.876", "timestamp": "2024-01-01T00:00:00+00:00"}, {"bad": 1}]
        result = _deserialize_funding_snapshots(data)
        assert len(result) == 1

    def test_deserialize_non_list(self) -> None:
        assert _deserialize_funding_snapshots("not a list") == []


# ---------------------------------------------------------------------------
# Utility tests
# ---------------------------------------------------------------------------


class TestSafeDecimal:
    def test_valid(self) -> None:
        assert _safe_decimal("5.25") == Decimal("5.25")
        assert _safe_decimal(42) == Decimal("42")
        assert _safe_decimal(3.14) == Decimal("3.14")

    def test_none(self) -> None:
        assert _safe_decimal(None) == Decimal("0")

    def test_invalid(self) -> None:
        assert _safe_decimal("not-a-number") == Decimal("0")
        assert _safe_decimal(object()) == Decimal("0")


# ---------------------------------------------------------------------------
# RateHistoryReader: Lending rate tests
# ---------------------------------------------------------------------------


class TestLendingRateFromTheGraph:
    def test_aave_v3_success(self, reader: RateHistoryReader) -> None:
        """Test fetching Aave V3 lending rates from The Graph."""
        mock_response = {
            "data": {
                "reserveParamsHistoryItems": [
                    {
                        "timestamp": "1704067200",  # 2024-01-01
                        "liquidityRate": "52500000000000000000000000",  # 5.25% in RAY
                        "variableBorrowRate": "75000000000000000000000000",  # 7.5% in RAY
                        "utilizationRate": "823000000000000000000000000",  # 82.3% in RAY
                    },
                    {
                        "timestamp": "1704153600",  # 2024-01-02
                        "liquidityRate": "53000000000000000000000000",  # 5.3%
                        "variableBorrowRate": "74500000000000000000000000",  # 7.45%
                        "utilizationRate": "810000000000000000000000000",  # 81%
                    },
                ]
            }
        }

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=_mock_aiohttp_response(mock_response))
        mock_session.closed = False
        reader._session = mock_session
        reader._rate_limiter._tokens = 10.0

        envelope = reader.get_lending_rate_history(
            protocol="aave_v3",
            token="USDC",
            chain="arbitrum",
            days=90,
        )

        assert isinstance(envelope, DataEnvelope)
        assert envelope.classification == DataClassification.INFORMATIONAL
        assert envelope.meta.source == "thegraph"
        assert envelope.meta.cache_hit is False
        assert len(envelope.value) == 2

        snap = envelope.value[0]
        assert isinstance(snap, LendingRateSnapshot)
        # 52500000000000000000000000 / 1e27 * 100 = 5.25
        assert float(snap.supply_apy) == pytest.approx(5.25, rel=1e-4)
        assert float(snap.borrow_apy) == pytest.approx(7.5, rel=1e-4)
        assert snap.utilization is not None
        assert float(snap.utilization) == pytest.approx(82.3, rel=1e-4)

    def test_compound_v3_success(self, reader: RateHistoryReader) -> None:
        """Test fetching Compound V3 lending rates from The Graph."""
        mock_response = {
            "data": {
                "marketHourlySnapshots": [
                    {
                        "timestamp": "1704067200",
                        "rates": [
                            {"rate": "4.50", "side": "LENDER"},
                            {"rate": "6.25", "side": "BORROWER"},
                        ],
                    },
                ]
            }
        }

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=_mock_aiohttp_response(mock_response))
        mock_session.closed = False
        reader._session = mock_session
        reader._rate_limiter._tokens = 10.0

        envelope = reader.get_lending_rate_history(
            protocol="compound_v3",
            token="USDC",
            chain="ethereum",
            days=30,
        )

        assert len(envelope.value) == 1
        snap = envelope.value[0]
        assert snap.supply_apy == Decimal("4.50")
        assert snap.borrow_apy == Decimal("6.25")
        assert snap.utilization is None

    def test_no_subgraph_falls_to_defillama(self, reader: RateHistoryReader) -> None:
        """Test fallback to DeFi Llama when no subgraph available."""
        # Use relative dates within the reader's computed window (days=7)
        now = datetime.now(UTC)
        ts1 = (now - timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%S+00:00")
        ts2 = (now - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%S+00:00")

        # morpho_blue has no subgraph URL configured
        pools_response = {
            "data": [
                {
                    "pool": "morpho-usdc-pool-123",
                    "project": "morpho-blue",
                    "chain": "Ethereum",
                    "symbol": "USDC",
                    "apy": 4.2,
                    "apyBase": 4.2,
                    "apyBorrow": 5.8,
                },
            ]
        }
        chart_response = {
            "data": [
                {
                    "timestamp": ts1,
                    "tvlUsd": 50000000,
                    "apy": 4.2,
                    "apyBase": 4.2,
                    "apyBorrow": 5.8,
                },
                {
                    "timestamp": ts2,
                    "tvlUsd": 51000000,
                    "apy": 4.3,
                    "apyBase": 4.3,
                    "apyBorrow": 5.7,
                },
            ]
        }

        call_count = 0

        def mock_get(url, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return _mock_aiohttp_response(pools_response)
            else:
                return _mock_aiohttp_response(chart_response)

        mock_session = AsyncMock()
        mock_session.get = mock_get
        mock_session.closed = False
        reader._session = mock_session
        reader._rate_limiter._tokens = 10.0

        envelope = reader.get_lending_rate_history(
            protocol="morpho_blue",
            token="USDC",
            chain="ethereum",
            days=7,
        )

        assert envelope.meta.source == "defillama"
        assert len(envelope.value) == 2
        assert envelope.value[0].supply_apy == Decimal("4.2")
        assert envelope.value[0].borrow_apy == Decimal("5.8")

    def test_all_providers_fail(self, reader: RateHistoryReader) -> None:
        """Test that DataUnavailableError is raised when all providers fail."""
        # Make The Graph fail (no subgraph for this protocol/chain combo)
        # and DeFi Llama return no matching pools
        pools_response = {"data": []}

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=_mock_aiohttp_response(pools_response))
        mock_session.closed = False
        reader._session = mock_session
        reader._rate_limiter._tokens = 10.0

        with pytest.raises(DataUnavailableError) as exc_info:
            reader.get_lending_rate_history(
                protocol="morpho_blue",
                token="USDC",
                chain="sonic",  # No subgraph for sonic
                days=30,
            )

        assert "All providers failed" in exc_info.value.reason

    def test_invalid_days(self, reader: RateHistoryReader) -> None:
        """Test that invalid days parameter raises ValueError."""
        with pytest.raises(ValueError, match="days must be >= 1"):
            reader.get_lending_rate_history("aave_v3", "USDC", "ethereum", days=0)


class TestLendingRateFromDefillama:
    def test_defillama_date_filtering(self, reader: RateHistoryReader) -> None:
        """Test that DeFi Llama results are filtered by date range."""
        now = datetime.now(UTC)
        ts_out_before = (now - timedelta(days=10)).strftime("%Y-%m-%dT%H:%M:%S+00:00")
        ts_in_range = (now - timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%S+00:00")
        ts_out_after = (now + timedelta(days=5)).strftime("%Y-%m-%dT%H:%M:%S+00:00")

        pools_response = {
            "data": [
                {
                    "pool": "aave-usdc-arb",
                    "project": "aave-v3",
                    "chain": "Arbitrum",
                    "symbol": "USDC",
                },
            ]
        }
        chart_response = {
            "data": [
                {"timestamp": ts_out_before, "apy": 3.0, "apyBorrow": 5.0},  # Out of range
                {"timestamp": ts_in_range, "apy": 4.0, "apyBorrow": 6.0},  # In range
                {"timestamp": ts_out_after, "apy": 4.5, "apyBorrow": 6.5},  # Out of range (future)
            ]
        }

        call_count = 0

        def mock_get(url, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return _mock_aiohttp_response(pools_response)
            else:
                return _mock_aiohttp_response(chart_response)

        mock_session = AsyncMock()
        mock_session.get = mock_get
        mock_session.closed = False
        reader._session = mock_session
        reader._rate_limiter._tokens = 10.0

        # The Graph must fail first so DeFi Llama is used (aave_v3 on arbitrum has subgraph)
        # Force The Graph to fail by making post also fail
        mock_session.post = MagicMock(return_value=_mock_aiohttp_response({"errors": [{"message": "fail"}]}))

        envelope = reader.get_lending_rate_history(
            protocol="aave_v3",
            token="USDC",
            chain="arbitrum",
            days=7,
        )

        assert isinstance(envelope, DataEnvelope)
        assert envelope.meta.source == "defillama"
        # Only 1 entry should be in range
        assert len(envelope.value) == 1
        assert envelope.value[0].supply_apy == Decimal("4.0")


# ---------------------------------------------------------------------------
# RateHistoryReader: Funding rate tests
# ---------------------------------------------------------------------------


class TestFundingRateFromHyperliquid:
    def test_hyperliquid_success(self, reader: RateHistoryReader) -> None:
        """Test fetching funding rates from Hyperliquid."""
        now = datetime.now(UTC)
        t1 = (now - timedelta(hours=48)).strftime("%Y-%m-%dT%H:%M:%S+00:00")
        t2 = (now - timedelta(hours=40)).strftime("%Y-%m-%dT%H:%M:%S+00:00")
        t3 = (now - timedelta(hours=32)).strftime("%Y-%m-%dT%H:%M:%S+00:00")

        mock_response = [
            {
                "coin": "ETH",
                "fundingRate": "0.0001",
                "premium": "0.00005",
                "time": t1,
            },
            {
                "coin": "ETH",
                "fundingRate": "0.00012",
                "premium": "0.00006",
                "time": t2,
            },
            {
                "coin": "ETH",
                "fundingRate": "0.00008",
                "premium": "0.00004",
                "time": t3,
            },
        ]

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=_mock_aiohttp_response(mock_response))
        mock_session.closed = False
        reader._session = mock_session
        reader._rate_limiter._tokens = 10.0

        envelope = reader.get_funding_rate_history(
            venue="hyperliquid",
            market_symbol="ETH-USD",
            hours=168,
        )

        assert isinstance(envelope, DataEnvelope)
        assert envelope.classification == DataClassification.INFORMATIONAL
        assert envelope.meta.source == "hyperliquid"
        assert envelope.meta.cache_hit is False
        assert len(envelope.value) == 3

        snap = envelope.value[0]
        assert isinstance(snap, FundingRateSnapshot)
        assert snap.rate == Decimal("0.0001")
        # Annualized: 0.0001 * 8760
        assert snap.annualized_rate == Decimal("0.0001") * Decimal("8760")

    def test_unsupported_market(self, reader: RateHistoryReader) -> None:
        """Test that unsupported market falls through to next provider."""
        # No DeFi Llama fallback will work either for unknown market
        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=_mock_aiohttp_response({"data": []}))
        mock_session.closed = False
        reader._session = mock_session
        reader._rate_limiter._tokens = 10.0

        with pytest.raises(DataUnavailableError) as exc_info:
            reader.get_funding_rate_history(
                venue="hyperliquid",
                market_symbol="UNKNOWN-USD",
                hours=24,
            )

        assert "All providers failed" in exc_info.value.reason

    def test_invalid_hours(self, reader: RateHistoryReader) -> None:
        """Test that invalid hours parameter raises ValueError."""
        with pytest.raises(ValueError, match="hours must be >= 1"):
            reader.get_funding_rate_history("hyperliquid", "ETH-USD", hours=0)

    def test_gmx_v2_routes_to_hyperliquid(self, reader: RateHistoryReader) -> None:
        """Test that GMX V2 venue also tries Hyperliquid as primary."""
        now = datetime.now(UTC)
        t1 = (now - timedelta(hours=8)).strftime("%Y-%m-%dT%H:%M:%S+00:00")

        mock_response = [
            {
                "coin": "BTC",
                "fundingRate": "0.00005",
                "premium": "0.00002",
                "time": t1,
            },
        ]

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=_mock_aiohttp_response(mock_response))
        mock_session.closed = False
        reader._session = mock_session
        reader._rate_limiter._tokens = 10.0

        envelope = reader.get_funding_rate_history(
            venue="gmx_v2",
            market_symbol="BTC-USD",
            hours=24,
        )

        assert envelope.meta.source == "hyperliquid"
        assert len(envelope.value) == 1

    def test_hyperliquid_timestamp_int(self, reader: RateHistoryReader) -> None:
        """Test parsing Hyperliquid response with integer timestamps (milliseconds)."""
        now = datetime.now(UTC)
        ts_ms = int((now - timedelta(hours=24)).timestamp() * 1000)

        mock_response = [
            {
                "coin": "ETH",
                "fundingRate": "0.0002",
                "premium": "0.0001",
                "time": ts_ms,
            },
        ]

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=_mock_aiohttp_response(mock_response))
        mock_session.closed = False
        reader._session = mock_session
        reader._rate_limiter._tokens = 10.0

        envelope = reader.get_funding_rate_history(
            venue="hyperliquid",
            market_symbol="ETH-USD",
            hours=168,
        )

        assert len(envelope.value) == 1
        assert envelope.value[0].rate == Decimal("0.0002")


# ---------------------------------------------------------------------------
# RateHistoryReader: Cache tests
# ---------------------------------------------------------------------------


class TestRateHistoryCache:
    def test_lending_cache_hit(self, reader: RateHistoryReader) -> None:
        """Test that second call returns cached data."""
        mock_response = {
            "data": {
                "reserveParamsHistoryItems": [
                    {
                        "timestamp": "1704067200",
                        "liquidityRate": "50000000000000000000000000",
                        "variableBorrowRate": "70000000000000000000000000",
                        "utilizationRate": "800000000000000000000000000",
                    },
                ]
            }
        }

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=_mock_aiohttp_response(mock_response))
        mock_session.closed = False
        reader._session = mock_session
        reader._rate_limiter._tokens = 10.0

        # First call: fresh fetch
        env1 = reader.get_lending_rate_history("aave_v3", "USDC", "ethereum", days=90)
        assert env1.meta.cache_hit is False
        assert env1.meta.source == "thegraph"

        # Second call: should hit cache
        env2 = reader.get_lending_rate_history("aave_v3", "USDC", "ethereum", days=90)
        assert env2.meta.cache_hit is True
        assert "cache" in env2.meta.source
        assert len(env2.value) == len(env1.value)

    def test_funding_cache_hit(self, reader: RateHistoryReader) -> None:
        """Test funding rate cache hit."""
        now = datetime.now(UTC)
        t1 = (now - timedelta(hours=8)).strftime("%Y-%m-%dT%H:%M:%S+00:00")

        mock_response = [
            {
                "coin": "ETH",
                "fundingRate": "0.0001",
                "premium": "0.00005",
                "time": t1,
            },
        ]

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=_mock_aiohttp_response(mock_response))
        mock_session.closed = False
        reader._session = mock_session
        reader._rate_limiter._tokens = 10.0

        # First call
        env1 = reader.get_funding_rate_history("hyperliquid", "ETH-USD", hours=168)
        assert env1.meta.cache_hit is False

        # Second call: should hit cache
        env2 = reader.get_funding_rate_history("hyperliquid", "ETH-USD", hours=168)
        assert env2.meta.cache_hit is True

    def test_lending_finality_tagging(self, reader: RateHistoryReader) -> None:
        """Test that old data is tagged as finalized, recent as provisional."""
        now = datetime.now(UTC)
        old_ts = int((now - timedelta(days=30)).timestamp())

        mock_response = {
            "data": {
                "reserveParamsHistoryItems": [
                    {
                        "timestamp": str(old_ts),
                        "liquidityRate": "50000000000000000000000000",
                        "variableBorrowRate": "70000000000000000000000000",
                        "utilizationRate": "800000000000000000000000000",
                    },
                ]
            }
        }

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=_mock_aiohttp_response(mock_response))
        mock_session.closed = False
        reader._session = mock_session
        reader._rate_limiter._tokens = 10.0

        envelope = reader.get_lending_rate_history("aave_v3", "USDC", "ethereum", days=90)

        # All data is >24h old, so should be finalized
        # Verify data was fetched and can be retrieved from cache
        assert len(envelope.value) == 1

        # Second call should be a cache hit (proves caching worked)
        env2 = reader.get_lending_rate_history("aave_v3", "USDC", "ethereum", days=90)
        assert env2.meta.cache_hit is True
        # Source should indicate finalized data
        assert "finalized" in env2.meta.source


# ---------------------------------------------------------------------------
# RateHistoryReader: Health metrics tests
# ---------------------------------------------------------------------------


class TestRateHistoryHealth:
    def test_health_metrics(self, reader: RateHistoryReader) -> None:
        """Test that health() returns metrics for all providers."""
        health = reader.health()
        assert "thegraph" in health
        assert "defillama" in health
        assert "hyperliquid" in health
        assert "gmx_v2" in health

        for _name, metrics in health.items():
            assert "requests" in metrics
            assert "successes" in metrics
            assert "failures" in metrics

    def test_metrics_increment_on_success(self, reader: RateHistoryReader) -> None:
        """Test that metrics increment on successful fetch."""
        mock_response = {
            "data": {
                "reserveParamsHistoryItems": [
                    {
                        "timestamp": "1704067200",
                        "liquidityRate": "50000000000000000000000000",
                        "variableBorrowRate": "70000000000000000000000000",
                        "utilizationRate": "800000000000000000000000000",
                    },
                ]
            }
        }

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=_mock_aiohttp_response(mock_response))
        mock_session.closed = False
        reader._session = mock_session
        reader._rate_limiter._tokens = 10.0

        reader.get_lending_rate_history("aave_v3", "USDC", "ethereum", days=90)

        health = reader.health()
        assert health["thegraph"]["requests"] == 1
        assert health["thegraph"]["successes"] == 1
        assert health["thegraph"]["failures"] == 0


# ---------------------------------------------------------------------------
# MarketSnapshot integration tests
# ---------------------------------------------------------------------------


class TestMarketSnapshotLendingRateHistory:
    def test_lending_rate_history_success(self) -> None:
        """Test MarketSnapshot.lending_rate_history() delegates correctly."""
        now = datetime.now(UTC)
        snap = LendingRateSnapshot(
            supply_apy=Decimal("5.0"),
            borrow_apy=Decimal("7.0"),
            utilization=Decimal("80.0"),
            timestamp=now - timedelta(days=1),
        )
        mock_envelope = DataEnvelope(
            value=[snap],
            meta=MagicMock(source="thegraph", cache_hit=False),
            classification=DataClassification.INFORMATIONAL,
        )

        mock_reader = MagicMock()
        mock_reader.get_lending_rate_history.return_value = mock_envelope

        snapshot = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x" + "1" * 40,
            rate_history_reader=mock_reader,
        )

        result = snapshot.lending_rate_history("aave_v3", "USDC", days=90)

        assert result is mock_envelope
        mock_reader.get_lending_rate_history.assert_called_once_with(
            protocol="aave_v3",
            token="USDC",
            chain="arbitrum",
            days=90,
        )

    def test_lending_rate_history_default_chain(self) -> None:
        """Test that chain defaults to snapshot's chain."""
        mock_reader = MagicMock()
        mock_reader.get_lending_rate_history.return_value = DataEnvelope(
            value=[],
            meta=MagicMock(),
            classification=DataClassification.INFORMATIONAL,
        )

        snapshot = MarketSnapshot(
            chain="optimism",
            wallet_address="0x" + "1" * 40,
            rate_history_reader=mock_reader,
        )

        snapshot.lending_rate_history("aave_v3", "USDC")

        mock_reader.get_lending_rate_history.assert_called_once_with(
            protocol="aave_v3",
            token="USDC",
            chain="optimism",
            days=90,
        )

    def test_lending_rate_history_no_reader(self) -> None:
        """Test that ValueError raised when no reader configured."""
        snapshot = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x" + "1" * 40,
        )

        with pytest.raises(ValueError, match="No rate history reader"):
            snapshot.lending_rate_history("aave_v3", "USDC")

    def test_lending_rate_history_error_wrapping(self) -> None:
        """Test that errors are wrapped in LendingRateHistoryUnavailableError."""
        mock_reader = MagicMock()
        mock_reader.get_lending_rate_history.side_effect = DataUnavailableError(
            data_type="lending_rate_history",
            instrument="aave_v3/USDC",
            reason="All providers failed",
        )

        snapshot = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x" + "1" * 40,
            rate_history_reader=mock_reader,
        )

        with pytest.raises(LendingRateHistoryUnavailableError) as exc_info:
            snapshot.lending_rate_history("aave_v3", "USDC")

        assert exc_info.value.protocol == "aave_v3"
        assert exc_info.value.token == "USDC"


class TestMarketSnapshotFundingRateHistory:
    def test_funding_rate_history_success(self) -> None:
        """Test MarketSnapshot.funding_rate_history() delegates correctly."""
        now = datetime.now(UTC)
        snap = FundingRateSnapshot(
            rate=Decimal("0.0001"),
            annualized_rate=Decimal("0.876"),
            timestamp=now - timedelta(hours=8),
        )
        mock_envelope = DataEnvelope(
            value=[snap],
            meta=MagicMock(source="hyperliquid", cache_hit=False),
            classification=DataClassification.INFORMATIONAL,
        )

        mock_reader = MagicMock()
        mock_reader.get_funding_rate_history.return_value = mock_envelope

        snapshot = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x" + "1" * 40,
            rate_history_reader=mock_reader,
        )

        result = snapshot.funding_rate_history("hyperliquid", "ETH-USD", hours=168)

        assert result is mock_envelope
        mock_reader.get_funding_rate_history.assert_called_once_with(
            venue="hyperliquid",
            market_symbol="ETH-USD",
            hours=168,
        )

    def test_funding_rate_history_no_reader(self) -> None:
        """Test that ValueError raised when no reader configured."""
        snapshot = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x" + "1" * 40,
        )

        with pytest.raises(ValueError, match="No rate history reader"):
            snapshot.funding_rate_history("hyperliquid", "ETH-USD")

    def test_funding_rate_history_error_wrapping(self) -> None:
        """Test that errors are wrapped in FundingRateHistoryUnavailableError."""
        mock_reader = MagicMock()
        mock_reader.get_funding_rate_history.side_effect = DataUnavailableError(
            data_type="funding_rate_history",
            instrument="hyperliquid/ETH-USD",
            reason="All providers failed",
        )

        snapshot = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x" + "1" * 40,
            rate_history_reader=mock_reader,
        )

        with pytest.raises(FundingRateHistoryUnavailableError) as exc_info:
            snapshot.funding_rate_history("hyperliquid", "ETH-USD")

        assert exc_info.value.venue == "hyperliquid"
        assert exc_info.value.market == "ETH-USD"


# ---------------------------------------------------------------------------
# DataEnvelope integration tests
# ---------------------------------------------------------------------------


class TestDataEnvelopeIntegration:
    def test_lending_envelope_classification(self, reader: RateHistoryReader) -> None:
        """Test that lending envelopes have INFORMATIONAL classification."""
        mock_response = {
            "data": {
                "reserveParamsHistoryItems": [
                    {
                        "timestamp": "1704067200",
                        "liquidityRate": "50000000000000000000000000",
                        "variableBorrowRate": "70000000000000000000000000",
                        "utilizationRate": "800000000000000000000000000",
                    },
                ]
            }
        }

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=_mock_aiohttp_response(mock_response))
        mock_session.closed = False
        reader._session = mock_session
        reader._rate_limiter._tokens = 10.0

        envelope = reader.get_lending_rate_history("aave_v3", "USDC", "ethereum", days=30)

        assert envelope.is_execution_grade is False
        assert envelope.classification == DataClassification.INFORMATIONAL

    def test_funding_envelope_metadata(self, reader: RateHistoryReader) -> None:
        """Test that funding envelopes carry correct metadata."""
        now = datetime.now(UTC)
        t1 = (now - timedelta(hours=8)).strftime("%Y-%m-%dT%H:%M:%S+00:00")

        mock_response = [
            {
                "coin": "ETH",
                "fundingRate": "0.0001",
                "premium": "0.00005",
                "time": t1,
            },
        ]

        mock_session = AsyncMock()
        mock_session.post = MagicMock(return_value=_mock_aiohttp_response(mock_response))
        mock_session.closed = False
        reader._session = mock_session
        reader._rate_limiter._tokens = 10.0

        envelope = reader.get_funding_rate_history("hyperliquid", "ETH-USD", hours=24)

        assert envelope.meta.source == "hyperliquid"
        assert envelope.meta.finality == "off_chain"
        assert envelope.meta.confidence == 0.85
        assert envelope.meta.latency_ms >= 0
