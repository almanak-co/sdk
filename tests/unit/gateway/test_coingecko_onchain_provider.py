"""Tests for CoinGeckoOnchainOHLCVProvider.

Tests cover:
- OHLCVProvider protocol: get_ohlcv, supported_timeframes
- DataProvider protocol: name, data_class, fetch, health
- CoinGecko Onchain API response parsing
- Rate limiting with token bucket
- Chain-to-network mapping
- Timeframe mapping
- Caching behavior
- Error handling (HTTP errors, rate limits, invalid data)
"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest

from almanak.framework.data.interfaces import DataSourceTimeout, DataSourceUnavailable, OHLCVCandle
from almanak.framework.data.models import DataClassification, DataEnvelope
from almanak.framework.data.timeframes import (
    CANONICAL_OHLCV_TIMEFRAMES,
    COINGECKO_ONCHAIN_OHLCV_TIMEFRAMES,
    CoinGeckoOnchainOHLCVParams,
    OHLCVTimeframe,
)
from almanak.gateway.data.ohlcv.coingecko_onchain_provider import (
    _CHAIN_TO_NETWORK,
    CoinGeckoOnchainOHLCVProvider,
    _TokenBucket,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def provider() -> CoinGeckoOnchainOHLCVProvider:
    """Create a fresh provider for each test."""
    return CoinGeckoOnchainOHLCVProvider(cache_ttl=60, request_timeout=5.0, api_key="test-key")


def _make_ohlcv_response(
    candles: list[list[float | int]] | None = None,
) -> dict:
    """Build a mock CoinGecko Onchain OHLCV JSON response.

    Default produces 3 candles in descending order (API default).
    """
    if candles is None:
        # Descending timestamp order (newest first, as CoinGecko Onchain returns)
        candles = [
            [1700003600, 1810.5, 1820.0, 1800.0, 1815.0, 50000.0],
            [1700000000, 1800.0, 1812.0, 1795.0, 1810.0, 45000.0],
            [1699996400, 1790.0, 1805.0, 1785.0, 1800.0, 42000.0],
        ]
    return {
        "data": {
            "id": "eth_pool_123",
            "type": "ohlcv",
            "attributes": {
                "ohlcv_list": candles,
            },
        },
    }


def _make_search_response(pool_address: str = "0xabcdef1234567890") -> dict:
    """Build a mock CoinGecko Onchain pool search response."""
    return {
        "data": [
            {
                "id": f"eth_{pool_address}",
                "type": "pool",
                "attributes": {
                    "address": pool_address,
                    "name": "WETH / USDC",
                },
            },
        ],
    }


EXACT_POOL = "0x1111111111111111111111111111111111111111"
EXACT_TOKEN0 = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
EXACT_TOKEN1 = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
EXACT_START = 1_766_001_600
SOLANA_POOL = "HJPjoWUrhoZzkNfRpHuieeFk9WcZWjwy6PBjZ81ngndJ"
SOLANA_TOKEN0 = "So11111111111111111111111111111111111111112"
SOLANA_TOKEN1 = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"


def _make_exact_response(
    *,
    timestamps: tuple[object, ...] = (EXACT_START, EXACT_START + 3600),
    base: str = EXACT_TOKEN0,
    quote: str = EXACT_TOKEN1,
    volume: object = 0,
) -> dict:
    return {
        "data": {
            "id": "opaque-provider-observation-id",
            "type": "ohlcv_request_response",
            "attributes": {
                "ohlcv_list": [[timestamp, 1, 2, 0.5, 1.5, volume] for timestamp in reversed(timestamps)],
            },
        },
        "meta": {
            "base": {"address": base},
            "quote": {"address": quote},
        },
    }


# ---------------------------------------------------------------------------
# DataProvider protocol tests
# ---------------------------------------------------------------------------


class TestDataProviderProtocol:
    """Test DataProvider protocol compliance."""

    def test_name(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        assert provider.name == "coingecko_onchain"

    def test_data_class(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        assert provider.data_class == DataClassification.INFORMATIONAL

    def test_health_initial(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        h = provider.health()
        assert h["status"] == "healthy"
        assert h["total_requests"] == 0
        assert h["successful_requests"] == 0
        assert h["cache_hits"] == 0
        assert h["errors"] == 0
        assert h["success_rate"] == 100.0

    def test_health_after_errors(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        provider._metrics.total_requests = 10
        provider._metrics.errors = 6
        provider._metrics.successful_requests = 4
        h = provider.health()
        assert h["status"] == "degraded"


# ---------------------------------------------------------------------------
# OHLCVProvider protocol tests
# ---------------------------------------------------------------------------


class TestOHLCVProviderProtocol:
    """Test OHLCVProvider protocol compliance."""

    def test_supported_timeframes(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        tf = provider.supported_timeframes
        assert tf == CANONICAL_OHLCV_TIMEFRAMES
        assert all(isinstance(timeframe, OHLCVTimeframe) for timeframe in tf)

    @pytest.mark.asyncio
    async def test_invalid_timeframe_raises(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        with pytest.raises(ValueError, match="Invalid timeframe"):
            await provider.get_ohlcv("WETH", timeframe="7m")

    @pytest.mark.asyncio
    async def test_oversized_cache_identity_is_rejected(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        """Alternate in-process callers cannot retain arbitrarily large cache keys."""
        with pytest.raises(ValueError, match="token must be at most 128 characters"):
            await provider.get_ohlcv(
                "T" * 129,
                timeframe="1h",
                pool_address="0xabc",
                chain="ethereum",
            )


# ---------------------------------------------------------------------------
# get_ohlcv tests (with mocked HTTP)
# ---------------------------------------------------------------------------


class TestGetOHLCV:
    """Test get_ohlcv with mocked HTTP responses."""

    @pytest.mark.asyncio
    async def test_fetch_with_pool_address(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        """Fetch OHLCV with explicit pool address."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=_make_ohlcv_response())
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.closed = False

        provider._session = mock_session

        candles = await provider.get_ohlcv(
            "WETH",
            timeframe="1h",
            limit=100,
            pool_address="0xabc123",
            chain="ethereum",
        )

        assert len(candles) == 3
        # Should be sorted ascending by timestamp
        assert candles[0].timestamp < candles[1].timestamp < candles[2].timestamp
        assert isinstance(candles[0].open, Decimal)
        assert isinstance(candles[0].close, Decimal)
        assert isinstance(candles[0].volume, Decimal)

    @pytest.mark.asyncio
    async def test_include_empty_intervals_adds_query_param(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        """VIB-4875: the flag is sent to CoinGecko Onchain as include_empty_intervals=true."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=_make_ohlcv_response())
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.closed = False
        provider._session = mock_session

        await provider.get_ohlcv(
            "WETH",
            timeframe="1h",
            limit=100,
            pool_address="0xabc123",
            chain="ethereum",
            include_empty_intervals=True,
        )

        params = mock_session.get.call_args.kwargs["params"]
        assert params["include_empty_intervals"] == "true"

    @pytest.mark.asyncio
    async def test_include_empty_intervals_omitted_by_default(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        """Default (False) keeps the request byte-identical to legacy behaviour."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=_make_ohlcv_response())
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.closed = False
        provider._session = mock_session

        await provider.get_ohlcv("WETH", timeframe="1h", limit=100, pool_address="0xabc123", chain="ethereum")

        params = mock_session.get.call_args.kwargs["params"]
        assert "include_empty_intervals" not in params

    @pytest.mark.asyncio
    async def test_include_empty_intervals_uses_distinct_cache_entry(
        self, provider: CoinGeckoOnchainOHLCVProvider
    ) -> None:
        """Continuous and sparse candles must never share a cached response."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=_make_ohlcv_response())
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.closed = False
        provider._session = mock_session

        await provider.get_ohlcv(
            "WETH", timeframe="1h", pool_address="0xabc123", chain="ethereum", include_empty_intervals=False
        )
        await provider.get_ohlcv(
            "WETH", timeframe="1h", pool_address="0xabc123", chain="ethereum", include_empty_intervals=True
        )

        assert mock_session.get.call_count == 2

    @pytest.mark.asyncio
    async def test_fetch_with_search(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        """Fetch OHLCV by searching for pool first."""
        search_resp = AsyncMock()
        search_resp.status = 200
        search_resp.json = AsyncMock(return_value=_make_search_response())
        search_resp.__aenter__ = AsyncMock(return_value=search_resp)
        search_resp.__aexit__ = AsyncMock(return_value=False)

        ohlcv_resp = AsyncMock()
        ohlcv_resp.status = 200
        ohlcv_resp.json = AsyncMock(return_value=_make_ohlcv_response())
        ohlcv_resp.__aenter__ = AsyncMock(return_value=ohlcv_resp)
        ohlcv_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.closed = False
        # First call = search, second call = OHLCV
        mock_session.get = MagicMock(side_effect=[search_resp, ohlcv_resp])

        provider._session = mock_session

        candles = await provider.get_ohlcv(
            "WETH",
            timeframe="1h",
            limit=100,
            chain="ethereum",
        )

        assert len(candles) == 3
        assert mock_session.get.call_count == 2

    @pytest.mark.asyncio
    async def test_unsupported_chain_raises(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        """Unsupported chain raises DataSourceUnavailable."""
        with pytest.raises(DataSourceUnavailable, match="Unsupported chain"):
            await provider.get_ohlcv("WETH", chain="fantom")


class TestExactPoolOHLCV:
    @staticmethod
    def _session(payload: dict) -> AsyncMock:
        response = AsyncMock()
        response.status = 200
        response.json = AsyncMock(return_value=payload)
        response.__aenter__ = AsyncMock(return_value=response)
        response.__aexit__ = AsyncMock(return_value=False)
        session = AsyncMock()
        session.get = MagicMock(return_value=response)
        session.closed = False
        return session

    @pytest.mark.asyncio
    async def test_exact_pool_rejects_oversized_retained_identity(
        self, provider: CoinGeckoOnchainOHLCVProvider
    ) -> None:
        """Direct exact-provider callers cannot retain arbitrarily large cache keys."""
        with pytest.raises(ValueError, match="pool_address must be at most 128 characters"):
            await provider.get_exact_pool_ohlcv(
                chain="ethereum",
                pool_address="P" * 129,
                base_token_address=EXACT_TOKEN0,
                quote_token_address=EXACT_TOKEN1,
                timeframe=OHLCVTimeframe.ONE_HOUR,
                start_ts=EXACT_START,
                end_ts=EXACT_START + 7200,
                binding_hash="11" * 32,
                feature_identity="22" * 32,
            )

    @pytest.mark.asyncio
    async def test_exact_pool_request_uses_token_denominated_interval_and_accepts_measured_zero(
        self, provider: CoinGeckoOnchainOHLCVProvider
    ) -> None:
        session = self._session(_make_exact_response())
        provider._session = session

        result = await provider.get_exact_pool_ohlcv(
            chain="ethereum",
            pool_address=EXACT_POOL,
            base_token_address=EXACT_TOKEN0,
            quote_token_address=EXACT_TOKEN1,
            timeframe=OHLCVTimeframe.ONE_HOUR,
            start_ts=EXACT_START,
            end_ts=EXACT_START + 7200,
            binding_hash="11" * 32,
            feature_identity="22" * 32,
        )

        assert tuple(int(candle.timestamp.timestamp()) for candle in result.candles) == (
            EXACT_START,
            EXACT_START + 3600,
        )
        assert result.candles[0].volume == Decimal(0)
        call = session.get.call_args
        assert call.args[0].endswith(f"/pools/{EXACT_POOL}/ohlcv/hour")
        # ``before_timestamp`` is INCLUSIVE upstream and must name the LAST
        # expected bucket (EXACT_START + 3600), not the exclusive end of the
        # half-open interval (EXACT_START + 7200). This assertion previously
        # pinned the exclusive end, which made every live request come back one
        # bucket short and fail the coverage check below (VIB-6734).
        assert call.kwargs["params"] == {
            "aggregate": "1",
            "before_timestamp": EXACT_START + 3600,
            "limit": 2,
            "currency": "token",
            "token": EXACT_TOKEN0,
            "include_empty_intervals": "true",
        }

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("timeframe", "buckets"),
        [
            (OHLCVTimeframe.ONE_HOUR, 1),
            (OHLCVTimeframe.ONE_HOUR, 2),
            (OHLCVTimeframe.ONE_HOUR, 5),
            (OHLCVTimeframe.FIFTEEN_MINUTES, 4),
        ],
    )
    async def test_exact_pool_query_window_covers_every_expected_bucket_VIB_6734(
        self,
        provider: CoinGeckoOnchainOHLCVProvider,
        timeframe: OHLCVTimeframe,
        buckets: int,
    ) -> None:
        """The outgoing query must be able to return every expected bucket.

        Upstream ``before_timestamp`` is inclusive, so anchoring it on the
        exclusive ``end_ts`` shifts the window one bucket late: ``start_ts`` is
        never requested and the coverage check can never pass. That made the
        exact lane fail closed on every input (VIB-6734).

        This pins the relationship rather than a literal, so it fails for any
        timeframe or window length if the anchoring regresses.
        """
        span = timeframe.seconds * buckets
        expected = tuple(range(EXACT_START, EXACT_START + span, timeframe.seconds))
        session = self._session(_make_exact_response(timestamps=expected))
        provider._session = session

        await provider.get_exact_pool_ohlcv(
            chain="ethereum",
            pool_address=EXACT_POOL,
            base_token_address=EXACT_TOKEN0,
            quote_token_address=EXACT_TOKEN1,
            timeframe=timeframe,
            start_ts=EXACT_START,
            end_ts=EXACT_START + span,
            binding_hash="11" * 32,
            feature_identity="22" * 32,
        )

        params = session.get.call_args.kwargs["params"]
        assert params["limit"] == len(expected)
        # Inclusive anchor on the newest expected bucket.
        assert params["before_timestamp"] == expected[-1]
        # The inclusive window [anchor - (limit-1)*tf, anchor] must contain the
        # oldest expected bucket -- the property that actually has to hold.
        oldest_reachable = params["before_timestamp"] - (params["limit"] - 1) * timeframe.seconds
        assert oldest_reachable == expected[0], (
            f"query window starts at {oldest_reachable} but the oldest expected bucket is {expected[0]}"
        )

    @pytest.mark.asyncio
    async def test_exact_pool_response_must_echo_the_bound_asset_pair(
        self, provider: CoinGeckoOnchainOHLCVProvider
    ) -> None:
        provider._session = self._session(_make_exact_response(quote="0xcccccccccccccccccccccccccccccccccccccccc"))

        with pytest.raises(DataSourceUnavailable, match="token identity mismatch"):
            await provider.get_exact_pool_ohlcv(
                chain="ethereum",
                pool_address=EXACT_POOL,
                base_token_address=EXACT_TOKEN0,
                quote_token_address=EXACT_TOKEN1,
                timeframe=OHLCVTimeframe.ONE_HOUR,
                start_ts=EXACT_START,
                end_ts=EXACT_START + 7200,
                binding_hash="11" * 32,
                feature_identity="22" * 32,
            )

    @pytest.mark.asyncio
    async def test_exact_pool_response_must_echo_the_requested_direction(
        self, provider: CoinGeckoOnchainOHLCVProvider
    ) -> None:
        provider._session = self._session(_make_exact_response(base=EXACT_TOKEN1, quote=EXACT_TOKEN0))

        with pytest.raises(DataSourceUnavailable, match="token identity mismatch"):
            await provider.get_exact_pool_ohlcv(
                chain="ethereum",
                pool_address=EXACT_POOL,
                base_token_address=EXACT_TOKEN0,
                quote_token_address=EXACT_TOKEN1,
                timeframe=OHLCVTimeframe.ONE_HOUR,
                start_ts=EXACT_START,
                end_ts=EXACT_START + 7200,
                binding_hash="11" * 32,
                feature_identity="22" * 32,
            )

    @pytest.mark.asyncio
    async def test_exact_pool_preserves_case_sensitive_solana_identity(self) -> None:
        provider = CoinGeckoOnchainOHLCVProvider(api_key="test-key")
        session = self._session(_make_exact_response(base=SOLANA_TOKEN0, quote=SOLANA_TOKEN1))
        provider._session = session

        result = await provider.get_exact_pool_ohlcv(
            chain="solana",
            pool_address=SOLANA_POOL,
            base_token_address=SOLANA_TOKEN0,
            quote_token_address=SOLANA_TOKEN1,
            timeframe=OHLCVTimeframe.ONE_HOUR,
            start_ts=EXACT_START,
            end_ts=EXACT_START + 7200,
            binding_hash="11" * 32,
            feature_identity="22" * 32,
        )

        assert result.pool_address == SOLANA_POOL
        assert result.base_token_address == SOLANA_TOKEN0
        assert result.quote_token_address == SOLANA_TOKEN1
        call = session.get.call_args
        assert f"/pools/{SOLANA_POOL}/" in call.args[0]
        assert call.kwargs["params"]["token"] == SOLANA_TOKEN0

    @pytest.mark.asyncio
    async def test_exact_pool_response_must_cover_every_requested_bucket(
        self, provider: CoinGeckoOnchainOHLCVProvider
    ) -> None:
        provider._session = self._session(_make_exact_response(timestamps=(EXACT_START,)))

        with pytest.raises(DataSourceUnavailable, match="complete requested interval"):
            await provider.get_exact_pool_ohlcv(
                chain="ethereum",
                pool_address=EXACT_POOL,
                base_token_address=EXACT_TOKEN0,
                quote_token_address=EXACT_TOKEN1,
                timeframe=OHLCVTimeframe.ONE_HOUR,
                start_ts=EXACT_START,
                end_ts=EXACT_START + 7200,
                binding_hash="11" * 32,
                feature_identity="22" * 32,
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "malformed_row",
        (
            [EXACT_START, 1, 2, 1.25, 1.5, 0],
            [EXACT_START, 1, 1.25, 0.5, 1.5, 0],
        ),
    )
    async def test_exact_pool_response_rejects_impossible_ohlc_geometry(
        self,
        provider: CoinGeckoOnchainOHLCVProvider,
        malformed_row: list[object],
    ) -> None:
        payload = _make_exact_response()
        payload["data"]["attributes"]["ohlcv_list"][1] = malformed_row
        provider._session = self._session(payload)

        with pytest.raises(DataSourceUnavailable, match="invalid values"):
            await provider.get_exact_pool_ohlcv(
                chain="ethereum",
                pool_address=EXACT_POOL,
                base_token_address=EXACT_TOKEN0,
                quote_token_address=EXACT_TOKEN1,
                timeframe=OHLCVTimeframe.ONE_HOUR,
                start_ts=EXACT_START,
                end_ts=EXACT_START + 7200,
                binding_hash="11" * 32,
                feature_identity="22" * 32,
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("invalid_timestamp", [float(EXACT_START), True])
    async def test_exact_pool_response_rejects_non_integer_timestamps_without_coercion(
        self,
        provider: CoinGeckoOnchainOHLCVProvider,
        invalid_timestamp: object,
    ) -> None:
        provider._session = self._session(_make_exact_response(timestamps=(invalid_timestamp, EXACT_START + 3600)))

        with pytest.raises(DataSourceUnavailable, match="malformed row"):
            await provider.get_exact_pool_ohlcv(
                chain="ethereum",
                pool_address=EXACT_POOL,
                base_token_address=EXACT_TOKEN0,
                quote_token_address=EXACT_TOKEN1,
                timeframe=OHLCVTimeframe.ONE_HOUR,
                start_ts=EXACT_START,
                end_ts=EXACT_START + 7200,
                binding_hash="11" * 32,
                feature_identity="22" * 32,
            )

    @pytest.mark.asyncio
    async def test_exact_pool_cache_keys_include_direction_and_interval(
        self,
        provider: CoinGeckoOnchainOHLCVProvider,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        session = self._session(_make_exact_response())
        provider._session = session
        kwargs = {
            "chain": "ethereum",
            "pool_address": EXACT_POOL,
            "base_token_address": EXACT_TOKEN0,
            "quote_token_address": EXACT_TOKEN1,
            "timeframe": OHLCVTimeframe.ONE_HOUR,
            "start_ts": EXACT_START,
            "end_ts": EXACT_START + 7200,
            "binding_hash": "11" * 32,
            "feature_identity": "22" * 32,
        }

        await provider.get_exact_pool_ohlcv(**kwargs)
        await provider.get_exact_pool_ohlcv(**kwargs)
        assert session.get.call_count == 1

        reverse_session = self._session(_make_exact_response(base=EXACT_TOKEN1, quote=EXACT_TOKEN0))
        provider._session = reverse_session
        await provider.get_exact_pool_ohlcv(
            **{
                **kwargs,
                "base_token_address": EXACT_TOKEN1,
                "quote_token_address": EXACT_TOKEN0,
            }
        )
        assert reverse_session.get.call_count == 1

        identity_session = self._session(_make_exact_response())
        provider._session = identity_session
        await provider.get_exact_pool_ohlcv(**{**kwargs, "feature_identity": "33" * 32})
        assert identity_session.get.call_count == 1

        version_session = self._session(_make_exact_response())
        provider._session = version_session
        monkeypatch.setattr(
            "almanak.gateway.data.ohlcv.coingecko_onchain_provider.EXACT_POOL_OHLCV_CONTRACT_VERSION",
            "coingecko_onchain.pool_ohlcv.v2",
        )
        await provider.get_exact_pool_ohlcv(**kwargs)
        assert version_session.get.call_count == 1

        interval_session = self._session(_make_exact_response(timestamps=(EXACT_START + 7200, EXACT_START + 10800)))
        provider._session = interval_session
        await provider.get_exact_pool_ohlcv(**{**kwargs, "start_ts": EXACT_START + 7200, "end_ts": EXACT_START + 14400})
        assert interval_session.get.call_count == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", [429, 500])
    async def test_exact_http_errors_increment_health_metrics(self, status: int) -> None:
        provider = CoinGeckoOnchainOHLCVProvider(api_key="test-key")
        response = AsyncMock()
        response.status = status
        response.text = AsyncMock(return_value="upstream failure")
        response.__aenter__ = AsyncMock(return_value=response)
        response.__aexit__ = AsyncMock(return_value=False)
        session = AsyncMock()
        session.get = MagicMock(return_value=response)
        session.closed = False
        provider._session = session

        with pytest.raises(DataSourceUnavailable):
            await provider.get_exact_pool_ohlcv(
                chain="ethereum",
                pool_address=EXACT_POOL,
                base_token_address=EXACT_TOKEN0,
                quote_token_address=EXACT_TOKEN1,
                timeframe=OHLCVTimeframe.ONE_HOUR,
                start_ts=EXACT_START,
                end_ts=EXACT_START + 7200,
                binding_hash="11" * 32,
                feature_identity="22" * 32,
            )

        assert provider.health()["errors"] == 1

    @pytest.mark.asyncio
    async def test_exact_cache_is_lru_bounded_and_expired_entries_are_removed(self) -> None:
        provider = CoinGeckoOnchainOHLCVProvider(
            api_key="test-key",
            cache_ttl=60,
            exact_cache_max_entries=2,
        )
        kwargs = {
            "chain": "ethereum",
            "pool_address": EXACT_POOL,
            "base_token_address": EXACT_TOKEN0,
            "quote_token_address": EXACT_TOKEN1,
            "timeframe": OHLCVTimeframe.ONE_HOUR,
            "start_ts": EXACT_START,
            "end_ts": EXACT_START + 7200,
            "binding_hash": "11" * 32,
        }
        for identity in ("22" * 32, "33" * 32, "44" * 32):
            provider._session = self._session(_make_exact_response())
            await provider.get_exact_pool_ohlcv(**kwargs, feature_identity=identity)
        assert len(provider._exact_cache) == 2

        first_key = next(iter(provider._exact_cache))
        first_result, _ = provider._exact_cache[first_key]
        provider._exact_cache[first_key] = (first_result, 0.0)
        provider._session = self._session(_make_exact_response())
        await provider.get_exact_pool_ohlcv(**kwargs, feature_identity="55" * 32)

        assert first_key not in provider._exact_cache
        assert len(provider._exact_cache) == 2


class TestGetOHLCVFailures:
    @pytest.mark.asyncio
    async def test_http_error_raises(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        """Non-200 HTTP status raises DataSourceUnavailable."""
        mock_response = AsyncMock()
        mock_response.status = 500
        mock_response.text = AsyncMock(return_value="Internal Server Error")
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.closed = False
        provider._session = mock_session

        with pytest.raises(DataSourceUnavailable, match="HTTP 500"):
            await provider.get_ohlcv(
                "WETH",
                timeframe="1h",
                pool_address="0xabc",
                chain="ethereum",
            )

    @pytest.mark.asyncio
    async def test_http_429_raises_rate_limit(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        """429 status raises DataSourceUnavailable with rate limit message."""
        mock_response = AsyncMock()
        mock_response.status = 429
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.closed = False
        provider._session = mock_session

        with pytest.raises(DataSourceUnavailable, match="Rate limited"):
            await provider.get_ohlcv(
                "WETH",
                timeframe="1h",
                pool_address="0xabc",
                chain="ethereum",
            )

    @pytest.mark.asyncio
    async def test_empty_response_raises(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        """Empty OHLCV list raises DataSourceUnavailable."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=_make_ohlcv_response(candles=[]))
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.closed = False
        provider._session = mock_session

        with pytest.raises(DataSourceUnavailable, match="No OHLCV data"):
            await provider.get_ohlcv(
                "WETH",
                timeframe="1h",
                pool_address="0xabc",
                chain="ethereum",
            )

    @pytest.mark.asyncio
    async def test_missing_api_key_raises_before_http(self) -> None:
        """CoinGecko Onchain OHLCV requires a gateway-owned CoinGecko key."""
        provider = CoinGeckoOnchainOHLCVProvider(api_key="", request_timeout=5.0)
        mock_session = AsyncMock()
        mock_session.get = MagicMock()
        mock_session.closed = False
        provider._session = mock_session

        with pytest.raises(DataSourceUnavailable, match="requires a valid COINGECKO_API_KEY"):
            await provider.get_ohlcv(
                "WETH",
                timeframe="1h",
                pool_address="0xabc",
                chain="ethereum",
            )

        mock_session.get.assert_not_called()


# ---------------------------------------------------------------------------
# Response parsing tests
# ---------------------------------------------------------------------------


class TestParseResponse:
    """Test OHLCV response parsing logic."""

    def test_parse_valid_response(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        data = _make_ohlcv_response()
        candles = provider._parse_ohlcv_response(data)
        assert len(candles) == 3
        # Sorted ascending
        assert candles[0].timestamp < candles[1].timestamp
        assert candles[1].timestamp < candles[2].timestamp

    def test_parse_candle_values(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        data = _make_ohlcv_response(
            candles=[
                [1700000000, 1800.5, 1812.0, 1795.3, 1810.7, 45000.0],
            ]
        )
        candles = provider._parse_ohlcv_response(data)
        assert len(candles) == 1
        c = candles[0]
        assert c.open == Decimal("1800.5")
        assert c.high == Decimal("1812.0")
        assert c.low == Decimal("1795.3")
        assert c.close == Decimal("1810.7")
        assert c.volume == Decimal("45000.0")
        assert c.timestamp == datetime.fromtimestamp(1700000000, tz=UTC)

    def test_parse_empty_response(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        candles = provider._parse_ohlcv_response({})
        assert candles == []

    def test_parse_malformed_entries_skipped(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        data = _make_ohlcv_response(
            candles=[
                [1700000000, 1800.0, 1812.0, 1795.0, 1810.0, 45000.0],  # Valid
                [1700003600, "bad"],  # Too short
                [1700007200],  # Way too short
            ]
        )
        candles = provider._parse_ohlcv_response(data)
        assert len(candles) == 1

    def test_parse_missing_attributes(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        candles = provider._parse_ohlcv_response({"data": {}})
        assert candles == []

    def test_parse_none_data(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        candles = provider._parse_ohlcv_response({"data": None})
        assert candles == []


# ---------------------------------------------------------------------------
# Caching tests
# ---------------------------------------------------------------------------


class TestCaching:
    """Test in-memory cache behavior."""

    @pytest.mark.asyncio
    async def test_cache_hit(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        """Second call returns cached data without HTTP request."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=_make_ohlcv_response())
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.closed = False
        provider._session = mock_session

        # First call
        candles1 = await provider.get_ohlcv("WETH", timeframe="1h", pool_address="0xabc", chain="ethereum")
        # Second call (should be cached)
        candles2 = await provider.get_ohlcv("WETH", timeframe="1h", pool_address="0xabc", chain="ethereum")

        assert candles1 == candles2
        # Only one HTTP call was made
        assert mock_session.get.call_count == 1
        assert provider._metrics.cache_hits == 1

    def test_cache_expiry(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        """Cache entries expire after TTL."""
        provider._cache_ttl = 1  # 1 second TTL

        # Manually populate cache with expired data
        key = "WETH:ethereum:1h:100:auto"
        candles = [
            OHLCVCandle(
                timestamp=datetime.now(UTC),
                open=Decimal("1800"),
                high=Decimal("1810"),
                low=Decimal("1790"),
                close=Decimal("1805"),
                volume=Decimal("100"),
            )
        ]
        # Set cached_at to 10 seconds ago (well past TTL)
        provider._cache[key] = (candles, time.monotonic() - 10)

        result = provider._get_cached(key)
        assert result is None

    def test_cache_fresh(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        """Fresh cache entries are returned."""
        key = "WETH:ethereum:1h:100:auto"
        candles = [
            OHLCVCandle(
                timestamp=datetime.now(UTC),
                open=Decimal("1800"),
                high=Decimal("1810"),
                low=Decimal("1790"),
                close=Decimal("1805"),
                volume=Decimal("100"),
            )
        ]
        provider._cache[key] = (candles, time.monotonic())

        result = provider._get_cached(key)
        assert result is not None
        assert len(result) == 1

    def test_candle_cache_is_lru_bounded_and_prunes_all_expired_entries(self) -> None:
        """Service-lifetime candle caching has a hard bound and global TTL pruning."""
        provider = CoinGeckoOnchainOHLCVProvider(
            api_key="test-key",
            cache_ttl=60,
            cache_max_entries=2,
        )

        provider._update_cache("first", [])
        provider._update_cache("second", [])
        assert provider._get_cached("first") == []

        provider._update_cache("third", [])
        assert "first" in provider._cache
        assert "second" not in provider._cache

        provider._cache["expired"] = ([], time.monotonic() - 61)
        provider._update_cache("fourth", [])

        assert "expired" not in provider._cache
        assert len(provider._cache) == 2

    def test_pool_cache_is_lru_bounded_and_prunes_stale_retention(self) -> None:
        """Pool mappings retain bounded fallback history, never unbounded keys."""
        provider = CoinGeckoOnchainOHLCVProvider(
            api_key="test-key",
            cache_max_entries=2,
            pool_cache_ttl=60,
            pool_cache_stale_ttl=120,
        )

        provider._store_pool_address("first", "0x1")
        provider._store_pool_address("second", "0x2")
        assert provider._get_cached_pool_address("first", allow_stale=True) == "0x1"

        provider._store_pool_address("third", "0x3")
        assert "first" in provider._pool_cache
        assert "second" not in provider._pool_cache

        expired_at = time.monotonic() - 121
        provider._pool_cache["expired"] = ("0xdead", expired_at, expired_at)
        provider._store_pool_address("fourth", "0x4")

        assert "expired" not in provider._pool_cache
        assert len(provider._pool_cache) == 2

    def test_pool_cooldowns_are_lru_bounded_and_globally_pruned(self) -> None:
        """Expired and least-recent cooldown keys cannot accumulate for process lifetime."""
        provider = CoinGeckoOnchainOHLCVProvider(
            api_key="test-key",
            cache_max_entries=2,
            pool_search_cooldown=30,
        )
        provider._pool_search_cooldowns["expired"] = time.monotonic() - 1

        provider._start_pool_search_cooldown("first")
        provider._start_pool_search_cooldown("second")
        assert "expired" not in provider._pool_search_cooldowns
        assert provider._pool_search_cooldown_remaining("first") is not None

        provider._start_pool_search_cooldown("third")

        assert "first" in provider._pool_search_cooldowns
        assert "second" not in provider._pool_search_cooldowns
        assert len(provider._pool_search_cooldowns) == 2

    def test_clear_cache(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        key = "test:key"
        now = time.monotonic()
        provider._cache[key] = ([], time.monotonic())
        provider._pool_cache[key] = ("0xabc", now, now)
        provider._pool_search_cooldowns[key] = time.monotonic() + 30
        assert len(provider._cache) == 1
        assert len(provider._pool_cache) == 1
        assert len(provider._pool_search_cooldowns) == 1
        provider.clear_cache()
        assert len(provider._cache) == 0
        assert len(provider._pool_cache) == 0
        assert len(provider._pool_search_cooldowns) == 0

    def test_solana_cache_keys_preserve_base58_case(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        """Case-distinct Solana addresses must not collide in long-lived caches."""
        lower_token = "HJPjoWUrhoZzkNfRpHuieeFk9WcZWjwy6PBjZ81ngndJ"
        upper_token = lower_token.upper()

        lower_key = provider._cache_key(lower_token, "solana", "1h", 100, lower_token)
        upper_key = provider._cache_key(upper_token, "solana", "1h", 100, upper_token)
        lower_pool_key = provider._pool_cache_key(lower_token, "USD", chain="solana", network="solana")
        upper_pool_key = provider._pool_cache_key(upper_token, "USD", chain="solana", network="solana")

        assert lower_key != upper_key
        assert lower_pool_key != upper_pool_key


# ---------------------------------------------------------------------------
# Rate limiter tests
# ---------------------------------------------------------------------------


class TestTokenBucket:
    """Test _TokenBucket rate limiter."""

    def test_initial_tokens(self) -> None:
        bucket = _TokenBucket(rate=5, period=1.0)
        # Should have 5 tokens initially
        for _ in range(5):
            assert bucket.acquire() is True
        # 6th should fail
        assert bucket.acquire() is False

    def test_token_refill(self) -> None:
        bucket = _TokenBucket(rate=10, period=1.0)
        # Consume all tokens
        for _ in range(10):
            bucket.acquire()
        assert bucket.acquire() is False

        # Simulate time passing by manipulating internal state
        bucket._last_refill = time.monotonic() - 1.1  # 1.1 seconds ago
        bucket._tokens = 0.0
        # After refill, should have tokens again
        assert bucket.acquire() is True

    def test_no_exceed_max_tokens(self) -> None:
        bucket = _TokenBucket(rate=5, period=1.0)
        # Wait a long time (simulated)
        bucket._last_refill = time.monotonic() - 100.0
        bucket._tokens = 0.0
        # Should refill to max, not beyond
        assert bucket.acquire() is True
        # Should have at most rate-1 tokens left
        for _ in range(4):
            assert bucket.acquire() is True
        assert bucket.acquire() is False

    @pytest.mark.asyncio
    async def test_rate_limit_blocks_request(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        """When rate limiter is exhausted, get_ohlcv raises."""
        # Exhaust the rate limiter
        provider._rate_limiter = _TokenBucket(rate=1, period=60.0)
        provider._rate_limiter.acquire()  # Use the only token

        with pytest.raises(DataSourceUnavailable, match="Rate limited"):
            await provider.get_ohlcv(
                "WETH",
                timeframe="1h",
                pool_address="0xabc",
                chain="ethereum",
            )


# ---------------------------------------------------------------------------
# Chain and timeframe mapping tests
# ---------------------------------------------------------------------------


class TestMappings:
    """Test chain-to-network and timeframe-to-GT mappings."""

    def test_chain_to_network_coverage(self) -> None:
        """All expected chains are mapped."""
        expected = {
            "ethereum",
            "arbitrum",
            "base",
            "optimism",
            "polygon",
            "avalanche",
            "bsc",
            "sonic",
            "solana",
            "mantle",
            "robinhood",
        }
        assert expected == set(_CHAIN_TO_NETWORK.keys())

    def test_chain_network_values(self) -> None:
        assert _CHAIN_TO_NETWORK["ethereum"] == "eth"
        assert _CHAIN_TO_NETWORK["arbitrum"] == "arbitrum"
        assert _CHAIN_TO_NETWORK["base"] == "base"
        assert _CHAIN_TO_NETWORK["polygon"] == "polygon_pos"
        assert _CHAIN_TO_NETWORK["avalanche"] == "avax"
        assert _CHAIN_TO_NETWORK["bsc"] == "bsc"

    def test_timeframe_mapping_coverage(self) -> None:
        """All OHLCV timeframes are mapped."""
        assert set(CANONICAL_OHLCV_TIMEFRAMES) == set(COINGECKO_ONCHAIN_OHLCV_TIMEFRAMES.mapping)

    def test_timeframe_mapping_values(self) -> None:
        mapping = COINGECKO_ONCHAIN_OHLCV_TIMEFRAMES.mapping
        assert mapping[OHLCVTimeframe.ONE_MINUTE] == CoinGeckoOnchainOHLCVParams(aggregate="1", timeframe="minute")
        assert mapping[OHLCVTimeframe.ONE_HOUR] == CoinGeckoOnchainOHLCVParams(aggregate="1", timeframe="hour")
        assert mapping[OHLCVTimeframe.FOUR_HOURS] == CoinGeckoOnchainOHLCVParams(aggregate="4", timeframe="hour")
        assert mapping[OHLCVTimeframe.ONE_DAY] == CoinGeckoOnchainOHLCVParams(aggregate="1", timeframe="day")


# ---------------------------------------------------------------------------
# Pool search tests
# ---------------------------------------------------------------------------


class TestPoolSearch:
    """Test pool address resolution via search."""

    @pytest.mark.asyncio
    async def test_search_returns_pool_url(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        """Pool search returns correct OHLCV URL."""
        search_resp = AsyncMock()
        search_resp.status = 200
        search_resp.json = AsyncMock(return_value=_make_search_response("0xdeadbeef"))
        search_resp.__aenter__ = AsyncMock(return_value=search_resp)
        search_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=search_resp)
        mock_session.closed = False
        provider._session = mock_session

        url = await provider._resolve_pool_ohlcv_url(
            "WETH", "USDC", chain="ethereum", network="eth", timeframe_key="hour"
        )
        assert "0xdeadbeef" in url
        assert "/ohlcv/hour" in url

    @pytest.mark.asyncio
    async def test_search_result_is_reused_across_candle_shapes(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        """Pool discovery is independent of timeframe and candle limit."""
        search_resp = AsyncMock()
        search_resp.status = 200
        search_resp.json = AsyncMock(return_value=_make_search_response("0xdeadbeef"))
        search_resp.__aenter__ = AsyncMock(return_value=search_resp)
        search_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=search_resp)
        mock_session.closed = False
        provider._session = mock_session

        hour_url = await provider._resolve_pool_ohlcv_url(
            "WETH", "USD", chain="ethereum", network="eth", timeframe_key="hour"
        )
        day_url = await provider._resolve_pool_ohlcv_url(
            "WETH", "USD", chain="ethereum", network="eth", timeframe_key="day"
        )

        assert mock_session.get.call_count == 1
        assert hour_url.endswith("/0xdeadbeef/ohlcv/hour")
        assert day_url.endswith("/0xdeadbeef/ohlcv/day")
        assert provider._metrics.pool_cache_hits == 1

    @pytest.mark.asyncio
    async def test_concurrent_searches_are_single_flight(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        """Concurrent misses for one token issue only one search request."""
        search_resp = AsyncMock()
        search_resp.status = 200
        search_resp.json = AsyncMock(return_value=_make_search_response("0xdeadbeef"))
        search_resp.__aenter__ = AsyncMock(return_value=search_resp)
        search_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=search_resp)
        mock_session.closed = False
        provider._session = mock_session

        urls = await asyncio.gather(
            provider._resolve_pool_ohlcv_url("WETH", "USD", chain="ethereum", network="eth", timeframe_key="hour"),
            provider._resolve_pool_ohlcv_url("WETH", "USD", chain="ethereum", network="eth", timeframe_key="day"),
        )

        assert mock_session.get.call_count == 1
        assert urls[0].endswith("/ohlcv/hour")
        assert urls[1].endswith("/ohlcv/day")

    @pytest.mark.asyncio
    async def test_search_timeout_is_bounded_and_typed(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        """Pool discovery has a shorter deadline than the candle request."""
        mock_session = AsyncMock()
        mock_session.get = MagicMock(side_effect=TimeoutError)
        mock_session.closed = False
        provider._session = mock_session

        with pytest.raises(DataSourceTimeout) as exc_info:
            await provider._resolve_pool_ohlcv_url("WETH", "USD", chain="ethereum", network="eth", timeframe_key="hour")

        assert exc_info.value.timeout_seconds == provider._pool_search_timeout
        assert mock_session.get.call_args.kwargs["timeout"].total == provider._pool_search_timeout
        assert provider._metrics.pool_search_timeouts == 1

    @pytest.mark.asyncio
    async def test_search_timeout_suppresses_immediate_repeat(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        """Router retries do not pay the same upstream timeout repeatedly."""
        mock_session = AsyncMock()
        mock_session.get = MagicMock(side_effect=TimeoutError)
        mock_session.closed = False
        provider._session = mock_session

        with pytest.raises(DataSourceTimeout):
            await provider._resolve_pool_ohlcv_url("WETH", "USD", chain="ethereum", network="eth", timeframe_key="hour")
        with pytest.raises(DataSourceUnavailable, match="cooldown active") as exc_info:
            await provider._resolve_pool_ohlcv_url("WETH", "USD", chain="ethereum", network="eth", timeframe_key="hour")

        assert exc_info.value.retry_after is None
        assert mock_session.get.call_count == 1
        assert provider._metrics.pool_searches_suppressed == 1

    @pytest.mark.asyncio
    async def test_expired_search_cooldown_allows_new_attempt(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        """Discovery resumes automatically once the suppression window expires."""
        key = provider._pool_cache_key("WETH", "USD", chain="ethereum", network="eth")
        provider._pool_search_cooldowns[key] = time.monotonic() - 1

        search_resp = AsyncMock()
        search_resp.status = 200
        search_resp.json = AsyncMock(return_value=_make_search_response("0xdeadbeef"))
        search_resp.__aenter__ = AsyncMock(return_value=search_resp)
        search_resp.__aexit__ = AsyncMock(return_value=False)
        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=search_resp)
        mock_session.closed = False
        provider._session = mock_session

        url = await provider._resolve_pool_ohlcv_url(
            "WETH", "USD", chain="ethereum", network="eth", timeframe_key="hour"
        )

        assert url.endswith("/0xdeadbeef/ohlcv/hour")
        assert mock_session.get.call_count == 1
        assert key not in provider._pool_search_cooldowns

    @pytest.mark.asyncio
    async def test_aiohttp_timeout_is_classified_as_timeout(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        """aiohttp's timeout subclass must not be swallowed as a generic client error."""
        mock_session = AsyncMock()
        mock_session.get = MagicMock(side_effect=aiohttp.ServerTimeoutError())
        mock_session.closed = False
        provider._session = mock_session

        with pytest.raises(DataSourceTimeout):
            await provider.get_ohlcv("WETH", timeframe="1h", pool_address="0xabc", chain="ethereum")

    @pytest.mark.asyncio
    async def test_search_timeout_uses_expired_pool_mapping(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        """A transient refresh failure can safely reuse a known pool address."""
        key = provider._pool_cache_key("WETH", "USD", chain="ethereum", network="eth")
        provider._pool_cache_ttl = 1
        discovered_at = time.monotonic() - 10
        provider._pool_cache[key] = ("0xstale", discovered_at, discovered_at)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(side_effect=TimeoutError)
        mock_session.closed = False
        provider._session = mock_session

        url = await provider._resolve_pool_ohlcv_url(
            "WETH", "USD", chain="ethereum", network="eth", timeframe_key="hour"
        )
        cached_url = await provider._resolve_pool_ohlcv_url(
            "WETH", "USD", chain="ethereum", network="eth", timeframe_key="day"
        )

        assert url.endswith("/0xstale/ohlcv/hour")
        assert cached_url.endswith("/0xstale/ohlcv/day")
        assert mock_session.get.call_count == 1
        assert provider._metrics.pool_search_timeouts == 1

    @pytest.mark.asyncio
    async def test_transient_http_error_uses_stale_mapping_for_bounded_grace(
        self, provider: CoinGeckoOnchainOHLCVProvider
    ) -> None:
        """Retryable HTTP failures use stale identity without pinning it for the full TTL."""
        key = provider._pool_cache_key("WETH", "USD", chain="ethereum", network="eth")
        provider._pool_cache_ttl = 3600
        provider._pool_search_cooldown = 30
        discovered_at = time.monotonic() - 7200
        provider._pool_cache[key] = ("0xstale", discovered_at, discovered_at)

        search_resp = AsyncMock()
        search_resp.status = 503
        search_resp.__aenter__ = AsyncMock(return_value=search_resp)
        search_resp.__aexit__ = AsyncMock(return_value=False)
        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=search_resp)
        mock_session.closed = False
        provider._session = mock_session

        url = await provider._resolve_pool_ohlcv_url(
            "WETH", "USD", chain="ethereum", network="eth", timeframe_key="hour"
        )
        cached_url = await provider._resolve_pool_ohlcv_url(
            "WETH", "USD", chain="ethereum", network="eth", timeframe_key="day"
        )

        _, cached_at, retained_discovered_at = provider._pool_cache[key]
        remaining_ttl = provider._pool_cache_ttl - (time.monotonic() - cached_at)
        assert url.endswith("/0xstale/ohlcv/hour")
        assert cached_url.endswith("/0xstale/ohlcv/day")
        assert remaining_ttl == pytest.approx(provider._pool_search_cooldown, abs=0.1)
        assert retained_discovered_at == discovered_at
        assert mock_session.get.call_count == 1
        assert key in provider._pool_search_cooldowns

    @pytest.mark.asyncio
    async def test_pool_search_rate_limit_uses_candle_retry_delay(
        self, provider: CoinGeckoOnchainOHLCVProvider
    ) -> None:
        """CoinGecko pool-search 429 responses retain the provider-wide retry delay."""
        search_resp = AsyncMock()
        search_resp.status = 429
        search_resp.__aenter__ = AsyncMock(return_value=search_resp)
        search_resp.__aexit__ = AsyncMock(return_value=False)
        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=search_resp)
        mock_session.closed = False
        provider._session = mock_session

        with pytest.raises(DataSourceUnavailable, match="HTTP 429") as exc_info:
            await provider._resolve_pool_ohlcv_url("WETH", "USD", chain="ethereum", network="eth", timeframe_key="hour")

        assert exc_info.value.retry_after == 60.0

    @pytest.mark.asyncio
    async def test_permanent_http_error_does_not_use_stale_mapping(
        self, provider: CoinGeckoOnchainOHLCVProvider
    ) -> None:
        """Permanent discovery failures remain visible even when a stale mapping exists."""
        key = provider._pool_cache_key("WETH", "USD", chain="ethereum", network="eth")
        provider._pool_cache_ttl = 1
        discovered_at = time.monotonic() - 10
        provider._pool_cache[key] = ("0xstale", discovered_at, discovered_at)

        search_resp = AsyncMock()
        search_resp.status = 404
        search_resp.__aenter__ = AsyncMock(return_value=search_resp)
        search_resp.__aexit__ = AsyncMock(return_value=False)
        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=search_resp)
        mock_session.closed = False
        provider._session = mock_session

        with pytest.raises(DataSourceUnavailable, match="HTTP 404") as exc_info:
            await provider._resolve_pool_ohlcv_url("WETH", "USD", chain="ethereum", network="eth", timeframe_key="hour")

        assert exc_info.value.retry_after is None
        assert key not in provider._pool_search_cooldowns

    @pytest.mark.asyncio
    async def test_network_with_underscore_uses_exact_prefix_fallback(
        self, provider: CoinGeckoOnchainOHLCVProvider
    ) -> None:
        """Vendor network IDs containing underscores do not corrupt addresses."""
        search_resp = AsyncMock()
        search_resp.status = 200
        search_resp.json = AsyncMock(return_value={"data": [{"id": "polygon_pos_0xdeadbeef", "attributes": {}}]})
        search_resp.__aenter__ = AsyncMock(return_value=search_resp)
        search_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=search_resp)
        mock_session.closed = False
        provider._session = mock_session

        url = await provider._resolve_pool_ohlcv_url(
            "WETH", "USD", chain="polygon", network="polygon_pos", timeframe_key="hour"
        )

        assert url.endswith("/0xdeadbeef/ohlcv/hour")

    @pytest.mark.asyncio
    async def test_search_no_pools_raises(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        """Empty search results raise DataSourceUnavailable."""
        search_resp = AsyncMock()
        search_resp.status = 200
        search_resp.json = AsyncMock(return_value={"data": []})
        search_resp.__aenter__ = AsyncMock(return_value=search_resp)
        search_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=search_resp)
        mock_session.closed = False
        provider._session = mock_session

        with pytest.raises(DataSourceUnavailable, match="No pools found"):
            await provider._resolve_pool_ohlcv_url(
                "UNKNOWNTOKEN", "USDC", chain="ethereum", network="eth", timeframe_key="hour"
            )

    @pytest.mark.asyncio
    async def test_search_http_error_raises(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        """Search HTTP error raises DataSourceUnavailable."""
        search_resp = AsyncMock()
        search_resp.status = 500
        search_resp.__aenter__ = AsyncMock(return_value=search_resp)
        search_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=search_resp)
        mock_session.closed = False
        provider._session = mock_session

        with pytest.raises(DataSourceUnavailable, match="Pool search failed"):
            await provider._resolve_pool_ohlcv_url(
                "WETH", "USDC", chain="ethereum", network="eth", timeframe_key="hour"
            )


# ---------------------------------------------------------------------------
# Metrics tracking tests
# ---------------------------------------------------------------------------


class TestMetrics:
    """Test health metrics tracking."""

    @pytest.mark.asyncio
    async def test_success_increments_metrics(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=_make_ohlcv_response())
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.closed = False
        provider._session = mock_session

        await provider.get_ohlcv("WETH", timeframe="1h", pool_address="0xabc", chain="ethereum")

        assert provider._metrics.total_requests == 1
        assert provider._metrics.successful_requests == 1
        assert provider._metrics.errors == 0
        assert provider._metrics.total_latency_ms > 0

    @pytest.mark.asyncio
    async def test_error_increments_error_count(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        with pytest.raises(DataSourceUnavailable):
            await provider.get_ohlcv("WETH", timeframe="1h", chain="fantom")

        assert provider._metrics.total_requests == 1
        assert provider._metrics.errors == 1


# ---------------------------------------------------------------------------
# CoinGecko auth tests
# ---------------------------------------------------------------------------


class TestCoinGeckoAuth:
    """Test CoinGecko Onchain host/header selection."""

    def test_api_key_selects_pro_host_and_header(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        assert "pro-api.coingecko.com" in provider._api_base
        assert provider._headers["x-cg-pro-api-key"] == "test-key"

    def test_missing_api_key_uses_no_auth_header(self) -> None:
        provider = CoinGeckoOnchainOHLCVProvider(api_key="")
        assert "x-cg-pro-api-key" not in provider._headers


# ---------------------------------------------------------------------------
# DataProvider.fetch() sync wrapper tests
# ---------------------------------------------------------------------------


class TestFetchWrapper:
    """Test the synchronous fetch() DataProvider method."""

    def test_fetch_returns_data_envelope(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        """fetch() returns a DataEnvelope wrapping candle list."""
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=_make_ohlcv_response())
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.closed = False
        provider._session = mock_session

        envelope = provider.fetch(
            token="WETH",
            timeframe="1h",
            limit=100,
            pool_address="0xabc",
            chain="ethereum",
        )

        assert isinstance(envelope, DataEnvelope)
        assert isinstance(envelope.value, list)
        assert len(envelope.value) == 3
        assert envelope.meta.source == "coingecko_onchain"
        assert envelope.meta.finality == "off_chain"
        assert envelope.meta.confidence == 0.9


# ---------------------------------------------------------------------------
# Limit capping test
# ---------------------------------------------------------------------------


class TestLimitCapping:
    """Test that limit is capped at 1000."""

    @pytest.mark.asyncio
    async def test_limit_capped_at_1000(self, provider: CoinGeckoOnchainOHLCVProvider) -> None:
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=_make_ohlcv_response())
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_response)
        mock_session.closed = False
        provider._session = mock_session

        await provider.get_ohlcv(
            "WETH",
            timeframe="1h",
            limit=5000,
            pool_address="0xabc",
            chain="ethereum",
        )

        # Verify the limit param sent to API was capped at 1000
        call_kwargs = mock_session.get.call_args
        params = call_kwargs.kwargs.get("params") or call_kwargs[1].get("params")
        assert params["limit"] == 1000


# ---------------------------------------------------------------------------
# Context manager tests
# ---------------------------------------------------------------------------


class TestContextManager:
    """Test async context manager protocol."""

    @pytest.mark.asyncio
    async def test_async_context_manager(self) -> None:
        async with CoinGeckoOnchainOHLCVProvider() as provider:
            assert isinstance(provider, CoinGeckoOnchainOHLCVProvider)
        # Session should be closed after exit
        assert provider._session is None or provider._session.closed
