"""Historical pool state data reader.

Provides historical pool snapshots (TVL, volume, fee revenue, reserves) from
multiple data providers with graceful fallback:
    Primary: The Graph subgraphs (Uniswap V3, Aerodrome)
    Fallback 1: DeFi Llama pools API
    Fallback 2: GeckoTerminal pool OHLCV

Results are stored in VersionedDataCache with finality tagging for deterministic
backtest replay.  All returns are wrapped in DataEnvelope with INFORMATIONAL
classification (graceful fallback on provider failure).

Example:
    from almanak.framework.data.pools.history import PoolHistoryReader, PoolSnapshot

    reader = PoolHistoryReader()
    envelope = reader.get_pool_history(
        pool_address="0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",
        chain="arbitrum",
        start_date=datetime(2024, 1, 1),
        end_date=datetime(2024, 3, 31),
        resolution="1h",
    )
    for snap in envelope.value:
        print(snap.tvl, snap.volume_24h, snap.fee_revenue_24h)
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

import aiohttp

from almanak.framework.data.cache.versioned_cache import VersionedDataCache
from almanak.framework.data.exceptions import DataUnavailableError
from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.framework.data.models import (
    DataClassification,
    DataEnvelope,
    DataMeta,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data Models
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PoolSnapshot:
    """Historical pool state at a specific point in time.

    Attributes:
        tvl: Total value locked in USD.
        volume_24h: 24-hour trading volume in USD.
        fee_revenue_24h: 24-hour fee revenue in USD.
        token0_reserve: Reserve amount of token0 in human-readable units.
        token1_reserve: Reserve amount of token1 in human-readable units.
        timestamp: UTC datetime of the snapshot.
    """

    tvl: Decimal
    volume_24h: Decimal
    fee_revenue_24h: Decimal
    token0_reserve: Decimal
    token1_reserve: Decimal
    timestamp: datetime


# ---------------------------------------------------------------------------
# The Graph subgraph URLs
# ---------------------------------------------------------------------------

# Uniswap V3 subgraph endpoint (hosted service and decentralized network)
_SUBGRAPH_URLS: dict[str, dict[str, str]] = {
    "uniswap_v3": {
        "ethereum": "https://gateway.thegraph.com/api/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV",
        "arbitrum": "https://gateway.thegraph.com/api/subgraphs/id/FbCGRftH4a3yZe65cWJGHCEMRtgqDcuMXoQnkEFjez2u",
        "base": "https://gateway.thegraph.com/api/subgraphs/id/43Hwfi3dJSoGpyas9VwNoDAv55yjgGrPCNzXBKiJPRBy",
        "optimism": "https://gateway.thegraph.com/api/subgraphs/id/Cghf4LfVqPiFw6fp6Y5X5Ubc8UpmUhSfJL82zwiBFLaj",
        "polygon": "https://gateway.thegraph.com/api/subgraphs/id/3hCPRGMxr5ARo8gHDFMKjKct3x3dRFGbzvfBhW3pRiXA",
    },
    "aerodrome": {
        "base": "https://gateway.thegraph.com/api/subgraphs/id/GENunSHWLBXm59C1FPChkGTq95gkEWiXEsGQMDbnBMtw",
    },
}

# DeFi Llama yields API
_LLAMA_YIELDS_API = "https://yields.llama.fi"
_LLAMA_POOLS_API = "https://yields.llama.fi/pools"

# GeckoTerminal API
_GECKOTERMINAL_API = "https://api.geckoterminal.com/api/v2"

# Chain to GeckoTerminal network mapping
_CHAIN_TO_GECKO_NETWORK: dict[str, str] = {
    "ethereum": "eth",
    "arbitrum": "arbitrum",
    "base": "base",
    "optimism": "optimism",
    "polygon": "polygon_pos",
    "avalanche": "avax",
    "bsc": "bsc",
    "sonic": "sonic",
    "solana": "solana",
}

# Resolution to seconds mapping
_RESOLUTION_SECONDS: dict[str, int] = {
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}


# ---------------------------------------------------------------------------
# Token bucket rate limiter (shared pattern)
# ---------------------------------------------------------------------------


class _TokenBucket:
    """Thread-safe token bucket rate limiter."""

    def __init__(self, rate: int = 10, period: float = 1.0) -> None:
        self._rate = rate
        self._period = period
        self._tokens = float(rate)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> bool:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(float(self._rate), self._tokens + elapsed * (self._rate / self._period))
            self._last_refill = now
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False


# ---------------------------------------------------------------------------
# PoolHistoryReader
# ---------------------------------------------------------------------------


@dataclass
class _ProviderMetrics:
    """Mutable metrics for a provider."""

    requests: int = 0
    successes: int = 0
    failures: int = 0


class PoolHistoryReader:
    """Reads historical pool snapshots from multiple data providers.

    Provider fallback chain:
        1. The Graph subgraphs (Uniswap V3, Aerodrome)
        2. DeFi Llama pools/yields API
        3. GeckoTerminal pool OHLCV

    All results are wrapped in DataEnvelope with INFORMATIONAL classification
    and stored in VersionedDataCache for deterministic replay.

    Args:
        cache: Optional VersionedDataCache for disk persistence.
            Default creates a cache under ~/.almanak/data_cache/pool_history/.
        request_timeout: HTTP request timeout in seconds. Default 15.
        thegraph_api_key: Optional API key for The Graph decentralized network.
    """

    def __init__(
        self,
        cache: VersionedDataCache | None = None,
        request_timeout: float = 15.0,
        thegraph_api_key: str | None = None,
    ) -> None:
        self._cache = cache or VersionedDataCache(data_type="pool_history")
        self._request_timeout = request_timeout
        self._thegraph_api_key = thegraph_api_key
        self._rate_limiter = _TokenBucket(rate=5, period=1.0)
        self._metrics: dict[str, _ProviderMetrics] = {
            "thegraph": _ProviderMetrics(),
            "defillama": _ProviderMetrics(),
            "geckoterminal": _ProviderMetrics(),
        }
        self._session: aiohttp.ClientSession | None = None

    # -- Public API ----------------------------------------------------------

    def get_pool_history(
        self,
        pool_address: str,
        chain: str,
        start_date: datetime,
        end_date: datetime | None = None,
        resolution: str = "1h",
    ) -> DataEnvelope[list[PoolSnapshot]]:
        """Fetch historical pool snapshots with provider fallback.

        Args:
            pool_address: Pool contract address.
            chain: Chain name (e.g. "arbitrum", "ethereum", "base").
            start_date: Start of the history window (UTC).
            end_date: End of the history window. Default: now.
            resolution: Data resolution: "1h", "4h", or "1d". Default "1h".

        Returns:
            DataEnvelope[list[PoolSnapshot]] with INFORMATIONAL classification.

        Raises:
            DataUnavailableError: If all providers fail.
        """
        if end_date is None:
            end_date = datetime.now(UTC)

        if resolution not in _RESOLUTION_SECONDS:
            raise ValueError(f"Unsupported resolution '{resolution}'. Supported: {', '.join(_RESOLUTION_SECONDS)}")

        chain_lower = chain.lower()
        pool_lower = pool_address.lower()

        # Check cache first
        cache_key = f"{pool_lower}:{chain_lower}:{int(start_date.timestamp())}:{int(end_date.timestamp())}:{resolution}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            snapshots = _deserialize_snapshots(cached.data)
            finality = cached.finality_status
            meta = DataMeta(
                source=f"cache({finality})",
                observed_at=datetime.now(UTC),
                finality="off_chain",
                staleness_ms=0,
                latency_ms=0,
                confidence=0.9 if finality == "finalized" else 0.7,
                cache_hit=True,
            )
            logger.debug(
                "pool_history_cache_hit pool=%s chain=%s versions=%s",
                pool_lower,
                chain_lower,
                cached.dataset_version,
            )
            return DataEnvelope(
                value=snapshots,
                meta=meta,
                classification=DataClassification.INFORMATIONAL,
            )

        # Try providers in order
        start_time = time.monotonic()
        result = self._fetch_with_fallback(pool_lower, chain_lower, start_date, end_date, resolution)
        latency_ms = int((time.monotonic() - start_time) * 1000)

        source, snapshots = result

        # Sort by timestamp ascending
        snapshots.sort(key=lambda s: s.timestamp)

        # Determine finality: data older than 24h is finalized
        now = datetime.now(UTC)
        cutoff = now - timedelta(hours=24)
        all_finalized = all(s.timestamp < cutoff for s in snapshots)
        finality_status = "finalized" if all_finalized else "provisional"

        # Store in cache
        serialized = _serialize_snapshots(snapshots)
        self._cache.put(cache_key, serialized, finality_status=finality_status)

        meta = DataMeta(
            source=source,
            observed_at=now,
            finality="off_chain",
            staleness_ms=0,
            latency_ms=latency_ms,
            confidence=0.85,
            cache_hit=False,
        )

        logger.info(
            "pool_history_fetched source=%s pool=%s chain=%s snapshots=%d latency_ms=%d",
            source,
            pool_lower,
            chain_lower,
            len(snapshots),
            latency_ms,
        )

        return DataEnvelope(
            value=snapshots,
            meta=meta,
            classification=DataClassification.INFORMATIONAL,
        )

    def health(self) -> dict[str, object]:
        """Return health metrics for all providers."""
        return {
            name: {
                "requests": m.requests,
                "successes": m.successes,
                "failures": m.failures,
            }
            for name, m in self._metrics.items()
        }

    # -- Provider fallback chain ---------------------------------------------

    def _fetch_with_fallback(
        self,
        pool_address: str,
        chain: str,
        start_date: datetime,
        end_date: datetime,
        resolution: str,
    ) -> tuple[str, list[PoolSnapshot]]:
        """Try providers in order, return (source_name, snapshots)."""
        errors: list[str] = []

        # Provider 1: The Graph
        try:
            snapshots = self._fetch_from_thegraph(pool_address, chain, start_date, end_date, resolution)
            if snapshots:
                return "thegraph", snapshots
        except Exception as e:
            errors.append(f"thegraph: {e}")
            logger.debug("pool_history_provider_failed provider=thegraph error=%s", e)

        # Provider 2: DeFi Llama
        try:
            snapshots = self._fetch_from_defillama(pool_address, chain, start_date, end_date, resolution)
            if snapshots:
                return "defillama", snapshots
        except Exception as e:
            errors.append(f"defillama: {e}")
            logger.debug("pool_history_provider_failed provider=defillama error=%s", e)

        # Provider 3: GeckoTerminal
        try:
            snapshots = self._fetch_from_geckoterminal(pool_address, chain, start_date, end_date, resolution)
            if snapshots:
                return "geckoterminal", snapshots
        except Exception as e:
            errors.append(f"geckoterminal: {e}")
            logger.debug("pool_history_provider_failed provider=geckoterminal error=%s", e)

        raise DataUnavailableError(
            data_type="pool_history",
            instrument=pool_address,
            reason=f"All providers failed: {'; '.join(errors)}",
        )

    # -- The Graph -----------------------------------------------------------

    def _fetch_from_thegraph(
        self,
        pool_address: str,
        chain: str,
        start_date: datetime,
        end_date: datetime,
        resolution: str,
    ) -> list[PoolSnapshot]:
        """Fetch historical data from The Graph subgraphs."""
        self._metrics["thegraph"].requests += 1

        # Find matching subgraph
        subgraph_url = self._find_subgraph_url(pool_address, chain)
        if subgraph_url is None:
            self._metrics["thegraph"].failures += 1
            raise DataSourceUnavailable(
                source="thegraph",
                reason=f"No subgraph available for chain {chain}",
            )

        start_ts = int(start_date.timestamp())
        end_ts = int(end_date.timestamp())

        # Determine entity based on resolution
        if resolution == "1h":
            entity = "poolHourDatas"
            period_field = "periodStartUnix"
        else:
            entity = "poolDayDatas"
            period_field = "date"

        # Build GraphQL query
        query = self._build_subgraph_query(
            entity=entity,
            period_field=period_field,
            pool_address=pool_address,
            start_ts=start_ts,
            end_ts=end_ts,
        )

        try:
            raw_data = self._run_async(self._query_subgraph(subgraph_url, query))
        except Exception as e:
            self._metrics["thegraph"].failures += 1
            raise DataSourceUnavailable(source="thegraph", reason=str(e)) from e

        snapshots = self._parse_subgraph_response(raw_data, entity, period_field)

        if not snapshots:
            self._metrics["thegraph"].failures += 1
            raise DataSourceUnavailable(
                source="thegraph",
                reason=f"No data returned for pool {pool_address} on {chain}",
            )

        self._metrics["thegraph"].successes += 1
        return snapshots

    def _find_subgraph_url(self, pool_address: str, chain: str) -> str | None:
        """Find the appropriate subgraph URL for a pool."""
        # Try each protocol's subgraph for this chain
        for _protocol, chain_urls in _SUBGRAPH_URLS.items():
            url = chain_urls.get(chain)
            if url is not None:
                return url
        return None

    def _build_subgraph_query(
        self,
        entity: str,
        period_field: str,
        pool_address: str,
        start_ts: int,
        end_ts: int,
        first: int = 1000,
    ) -> str:
        """Build a GraphQL query for The Graph subgraph."""
        return f"""{{
  {entity}(
    first: {first}
    orderBy: {period_field}
    orderDirection: asc
    where: {{
      pool: "{pool_address}"
      {period_field}_gte: {start_ts}
      {period_field}_lte: {end_ts}
    }}
  ) {{
    {period_field}
    tvlUSD
    volumeUSD
    feesUSD
    liquidity
    token0Price
    token1Price
  }}
}}"""

    async def _query_subgraph(self, url: str, query: str) -> dict[str, Any]:
        """Execute a GraphQL query against a subgraph."""
        session = await self._get_session()
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if self._thegraph_api_key:
            headers["Authorization"] = f"Bearer {self._thegraph_api_key}"

        async with session.post(url, json={"query": query}, headers=headers) as response:
            if response.status != 200:
                text = await response.text()
                raise DataSourceUnavailable(
                    source="thegraph",
                    reason=f"HTTP {response.status}: {text[:200]}",
                )
            data = await response.json()
            if "errors" in data:
                raise DataSourceUnavailable(
                    source="thegraph",
                    reason=f"GraphQL errors: {data['errors'][:2]}",
                )
            return data.get("data", {})

    def _parse_subgraph_response(
        self,
        data: dict[str, Any],
        entity: str,
        period_field: str,
    ) -> list[PoolSnapshot]:
        """Parse The Graph response into PoolSnapshot list."""
        items = data.get(entity, [])
        snapshots: list[PoolSnapshot] = []

        for item in items:
            try:
                ts = int(item.get(period_field, 0))
                timestamp = datetime.fromtimestamp(ts, tz=UTC)

                tvl = _safe_decimal(item.get("tvlUSD", "0"))
                volume = _safe_decimal(item.get("volumeUSD", "0"))
                fees = _safe_decimal(item.get("feesUSD", "0"))
                token0_price = _safe_decimal(item.get("token0Price", "0"))
                token1_price = _safe_decimal(item.get("token1Price", "0"))

                # Estimate reserves from TVL and prices
                token0_reserve, token1_reserve = _estimate_reserves(tvl, token0_price, token1_price)

                snapshots.append(
                    PoolSnapshot(
                        tvl=tvl,
                        volume_24h=volume,
                        fee_revenue_24h=fees,
                        token0_reserve=token0_reserve,
                        token1_reserve=token1_reserve,
                        timestamp=timestamp,
                    )
                )
            except (ValueError, TypeError, InvalidOperation):
                logger.debug("Skipping malformed subgraph entry: %s", item)
                continue

        return snapshots

    # -- DeFi Llama ----------------------------------------------------------

    def _fetch_from_defillama(
        self,
        pool_address: str,
        chain: str,
        start_date: datetime,
        end_date: datetime,
        resolution: str,
    ) -> list[PoolSnapshot]:
        """Fetch pool history from DeFi Llama yields API."""
        self._metrics["defillama"].requests += 1

        try:
            raw = self._run_async(self._query_defillama_pool(pool_address, chain))
        except Exception as e:
            self._metrics["defillama"].failures += 1
            raise DataSourceUnavailable(source="defillama", reason=str(e)) from e

        snapshots = self._parse_defillama_response(raw, start_date, end_date, resolution)

        if not snapshots:
            self._metrics["defillama"].failures += 1
            raise DataSourceUnavailable(
                source="defillama",
                reason=f"No DeFi Llama data for pool {pool_address} on {chain}",
            )

        self._metrics["defillama"].successes += 1
        return snapshots

    async def _query_defillama_pool(self, pool_address: str, chain: str) -> list[dict[str, Any]]:
        """Query DeFi Llama for pool yield history.

        DeFi Llama pools API uses its own pool IDs, not contract addresses.
        We search for a matching pool by address in the pools listing.
        """
        session = await self._get_session()

        if not self._rate_limiter.acquire():
            raise DataSourceUnavailable(source="defillama", reason="Rate limited")

        # Fetch all pools and find matching one
        async with session.get(_LLAMA_POOLS_API) as response:
            if response.status != 200:
                text = await response.text()
                raise DataSourceUnavailable(
                    source="defillama",
                    reason=f"HTTP {response.status}: {text[:200]}",
                )
            data = await response.json()

        pools = data.get("data", [])
        pool_lower = pool_address.lower()

        # Search for pool by address (DeFi Llama often includes address in pool ID)
        matching: list[dict[str, Any]] = []
        for pool in pools:
            pool_id = str(pool.get("pool", "")).lower()
            if pool_lower in pool_id:
                matching.append(pool)

        if not matching:
            raise DataSourceUnavailable(
                source="defillama",
                reason=f"Pool {pool_address} not found in DeFi Llama data",
            )

        # Use the first matching pool; fetch its chart data
        pool_id = matching[0].get("pool", "")

        if not self._rate_limiter.acquire():
            raise DataSourceUnavailable(source="defillama", reason="Rate limited")

        chart_url = f"{_LLAMA_YIELDS_API}/chart/{pool_id}"
        async with session.get(chart_url) as response:
            if response.status != 200:
                text = await response.text()
                raise DataSourceUnavailable(
                    source="defillama",
                    reason=f"Chart HTTP {response.status}: {text[:200]}",
                )
            chart_data = await response.json()

        return chart_data.get("data", [])

    def _parse_defillama_response(
        self,
        data: list[dict[str, Any]],
        start_date: datetime,
        end_date: datetime,
        resolution: str,
    ) -> list[PoolSnapshot]:
        """Parse DeFi Llama pool chart data into PoolSnapshot list.

        DeFi Llama chart data has daily resolution with fields:
            timestamp, tvlUsd, apy, apyBase, il7d
        """
        snapshots: list[PoolSnapshot] = []

        for item in data:
            try:
                ts_str = item.get("timestamp", "")
                if isinstance(ts_str, str) and ts_str:
                    # DeFi Llama uses ISO format timestamps
                    timestamp = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                    if timestamp.tzinfo is None:
                        timestamp = timestamp.replace(tzinfo=UTC)
                elif isinstance(ts_str, int | float):
                    timestamp = datetime.fromtimestamp(ts_str, tz=UTC)
                else:
                    continue

                # Filter by date range
                if timestamp < start_date or timestamp > end_date:
                    continue

                tvl = _safe_decimal(item.get("tvlUsd", "0"))
                # DeFi Llama doesn't always have volume/fees in chart data
                # Use apyBase as a proxy for fee revenue if available
                apy_base = float(item.get("apyBase") or 0)
                daily_fee_rate = apy_base / 365.0 / 100.0 if apy_base > 0 else 0.0
                fee_revenue_24h = tvl * Decimal(str(daily_fee_rate))

                snapshots.append(
                    PoolSnapshot(
                        tvl=tvl,
                        volume_24h=Decimal("0"),  # Not available in chart data
                        fee_revenue_24h=fee_revenue_24h,
                        token0_reserve=Decimal("0"),
                        token1_reserve=Decimal("0"),
                        timestamp=timestamp,
                    )
                )
            except (ValueError, TypeError, InvalidOperation):
                logger.debug("Skipping malformed DeFi Llama entry: %s", item)
                continue

        # If resolution is less than daily, the data is still daily-granularity
        # from DeFi Llama -- we return as-is (best available)
        return snapshots

    # -- GeckoTerminal -------------------------------------------------------

    def _fetch_from_geckoterminal(
        self,
        pool_address: str,
        chain: str,
        start_date: datetime,
        end_date: datetime,
        resolution: str,
    ) -> list[PoolSnapshot]:
        """Fetch pool OHLCV from GeckoTerminal as a fallback source."""
        self._metrics["geckoterminal"].requests += 1

        network = _CHAIN_TO_GECKO_NETWORK.get(chain)
        if network is None:
            self._metrics["geckoterminal"].failures += 1
            raise DataSourceUnavailable(
                source="geckoterminal",
                reason=f"Unsupported chain: {chain}",
            )

        try:
            raw = self._run_async(self._query_geckoterminal(pool_address, network, resolution))
        except Exception as e:
            self._metrics["geckoterminal"].failures += 1
            raise DataSourceUnavailable(source="geckoterminal", reason=str(e)) from e

        snapshots = self._parse_geckoterminal_response(raw, start_date, end_date)

        if not snapshots:
            self._metrics["geckoterminal"].failures += 1
            raise DataSourceUnavailable(
                source="geckoterminal",
                reason=f"No GeckoTerminal data for pool {pool_address} on {chain}",
            )

        self._metrics["geckoterminal"].successes += 1
        return snapshots

    async def _query_geckoterminal(
        self,
        pool_address: str,
        network: str,
        resolution: str,
    ) -> list[list[Any]]:
        """Query GeckoTerminal for pool OHLCV data."""
        session = await self._get_session()

        if not self._rate_limiter.acquire():
            raise DataSourceUnavailable(source="geckoterminal", reason="Rate limited")

        # Map resolution to GeckoTerminal timeframe
        if resolution == "1h":
            timeframe = "hour"
            aggregate = 1
        elif resolution == "4h":
            timeframe = "hour"
            aggregate = 4
        else:  # 1d
            timeframe = "day"
            aggregate = 1

        url = f"{_GECKOTERMINAL_API}/networks/{network}/pools/{pool_address}/ohlcv/{timeframe}"
        params = {"aggregate": str(aggregate), "limit": "1000"}

        async with session.get(url, params=params) as response:
            if response.status != 200:
                text = await response.text()
                raise DataSourceUnavailable(
                    source="geckoterminal",
                    reason=f"HTTP {response.status}: {text[:200]}",
                )
            data = await response.json()

        # GeckoTerminal OHLCV response structure
        attributes = data.get("data", {}).get("attributes", {})
        return attributes.get("ohlcv_list", [])

    def _parse_geckoterminal_response(
        self,
        ohlcv_list: list[list[Any]],
        start_date: datetime,
        end_date: datetime,
    ) -> list[PoolSnapshot]:
        """Parse GeckoTerminal OHLCV into PoolSnapshot list.

        GeckoTerminal OHLCV format: [timestamp, open, high, low, close, volume]
        We use volume as volume_24h proxy.
        """
        snapshots: list[PoolSnapshot] = []

        for candle in ohlcv_list:
            try:
                if len(candle) < 6:
                    continue

                ts = int(candle[0])
                timestamp = datetime.fromtimestamp(ts, tz=UTC)

                if timestamp < start_date or timestamp > end_date:
                    continue

                volume = _safe_decimal(str(candle[5]))

                # GeckoTerminal doesn't provide TVL or reserves directly
                # We can only provide volume data from this source
                snapshots.append(
                    PoolSnapshot(
                        tvl=Decimal("0"),
                        volume_24h=volume,
                        fee_revenue_24h=Decimal("0"),
                        token0_reserve=Decimal("0"),
                        token1_reserve=Decimal("0"),
                        timestamp=timestamp,
                    )
                )
            except (ValueError, TypeError, InvalidOperation):
                logger.debug("Skipping malformed GeckoTerminal entry")
                continue

        # GeckoTerminal returns newest first; reverse to ascending
        snapshots.reverse()
        return snapshots

    # -- Internal helpers ----------------------------------------------------

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self._request_timeout)
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                headers={"Accept": "application/json"},
            )
        return self._session

    async def close(self) -> None:
        """Close the HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    def _run_async(self, coro: Any) -> Any:
        """Run an async coroutine synchronously."""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                import concurrent.futures

                with concurrent.futures.ThreadPoolExecutor() as pool:
                    return pool.submit(asyncio.run, coro).result()
            else:
                return loop.run_until_complete(coro)
        except RuntimeError:
            return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Serialization helpers for VersionedDataCache
# ---------------------------------------------------------------------------


def _serialize_snapshots(snapshots: list[PoolSnapshot]) -> list[dict[str, Any]]:
    """Serialize PoolSnapshot list to JSON-compatible dicts."""
    return [
        {
            "tvl": str(s.tvl),
            "volume_24h": str(s.volume_24h),
            "fee_revenue_24h": str(s.fee_revenue_24h),
            "token0_reserve": str(s.token0_reserve),
            "token1_reserve": str(s.token1_reserve),
            "timestamp": s.timestamp.isoformat(),
        }
        for s in snapshots
    ]


def _deserialize_snapshots(data: Any) -> list[PoolSnapshot]:
    """Deserialize JSON dicts back to PoolSnapshot list."""
    if not isinstance(data, list):
        return []
    snapshots: list[PoolSnapshot] = []
    for item in data:
        try:
            ts_str = item["timestamp"]
            timestamp = datetime.fromisoformat(ts_str)
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=UTC)
            snapshots.append(
                PoolSnapshot(
                    tvl=Decimal(item["tvl"]),
                    volume_24h=Decimal(item["volume_24h"]),
                    fee_revenue_24h=Decimal(item["fee_revenue_24h"]),
                    token0_reserve=Decimal(item["token0_reserve"]),
                    token1_reserve=Decimal(item["token1_reserve"]),
                    timestamp=timestamp,
                )
            )
        except (KeyError, ValueError, InvalidOperation):
            continue
    return snapshots


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------


def _safe_decimal(value: Any) -> Decimal:
    """Convert a value to Decimal, returning 0 on failure."""
    if value is None:
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")


def _estimate_reserves(
    tvl: Decimal,
    token0_price: Decimal,
    token1_price: Decimal,
) -> tuple[Decimal, Decimal]:
    """Estimate token reserves from TVL assuming 50/50 split.

    For concentrated liquidity pools this is an approximation.
    Actual reserves depend on tick distribution.
    """
    if tvl <= 0:
        return Decimal("0"), Decimal("0")

    half_tvl = tvl / 2

    if token0_price > 0:
        token0_reserve = half_tvl / token0_price
    else:
        token0_reserve = Decimal("0")

    if token1_price > 0:
        token1_reserve = half_tvl / token1_price
    else:
        token1_reserve = Decimal("0")

    return token0_reserve, token1_reserve


__all__ = [
    "PoolHistoryReader",
    "PoolSnapshot",
]
