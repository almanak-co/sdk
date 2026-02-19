"""Historical lending and funding rate data reader.

Provides historical rate snapshots for backtesting rate arbitrage and carry
strategies.  Multiple data providers with graceful fallback:

    Lending rates:
        Primary: The Graph subgraphs (Aave V3, Compound V3)
        Fallback: DeFi Llama yields API

    Funding rates:
        Primary: Hyperliquid API (real historical data)
        Fallback: GMX V2 API

Results are stored in VersionedDataCache with finality tagging for deterministic
backtest replay.  All returns are wrapped in DataEnvelope with INFORMATIONAL
classification.

Example:
    from almanak.framework.data.rates.history import (
        RateHistoryReader,
        LendingRateSnapshot,
        FundingRateSnapshot,
    )

    reader = RateHistoryReader()

    # Lending rate history
    envelope = reader.get_lending_rate_history(
        protocol="aave_v3",
        token="USDC",
        chain="arbitrum",
        days=90,
    )
    for snap in envelope.value:
        print(snap.supply_apy, snap.borrow_apy, snap.utilization, snap.timestamp)

    # Funding rate history
    envelope = reader.get_funding_rate_history(
        venue="hyperliquid",
        market_symbol="ETH-USD",
        hours=168,
    )
    for snap in envelope.value:
        print(snap.rate, snap.annualized_rate, snap.timestamp)
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
class LendingRateSnapshot:
    """Historical lending rate at a specific point in time.

    Attributes:
        supply_apy: Supply APY as a percentage (e.g. 5.25 for 5.25%).
        borrow_apy: Borrow APY as a percentage.
        utilization: Pool utilization as a percentage (0-100), or None if unavailable.
        timestamp: UTC datetime of the snapshot.
    """

    supply_apy: Decimal
    borrow_apy: Decimal
    utilization: Decimal | None
    timestamp: datetime


@dataclass(frozen=True)
class FundingRateSnapshot:
    """Historical funding rate at a specific point in time.

    Attributes:
        rate: Funding rate for the period (e.g. 0.0001 for 0.01%).
        annualized_rate: Rate annualized for comparison.
        timestamp: UTC datetime of the snapshot.
    """

    rate: Decimal
    annualized_rate: Decimal
    timestamp: datetime


# ---------------------------------------------------------------------------
# The Graph subgraph URLs for lending protocols
# ---------------------------------------------------------------------------

_LENDING_SUBGRAPH_URLS: dict[str, dict[str, str]] = {
    "aave_v3": {
        "ethereum": "https://gateway.thegraph.com/api/subgraphs/id/Cd2gEDVeqnjBn1hSeqFMitw8Q1iiyV9FYUZkLNRcL87g",
        "arbitrum": "https://gateway.thegraph.com/api/subgraphs/id/DLuE98kEb26JkDQ5XoFnnx8eErhKfarRsCAmiRoqPY5C",
        "optimism": "https://gateway.thegraph.com/api/subgraphs/id/DSfLz8oQBUeU5atALgUFQKMTSYV9mZAVYp4noLSXAfvb",
        "base": "https://gateway.thegraph.com/api/subgraphs/id/GQFbb95cE6d8mB1EuRBxMjiRkT6ZLPB3ETtCkP2Muw9k",
        "polygon": "https://gateway.thegraph.com/api/subgraphs/id/Co2URyXjnxaw8WqxKyVHdirq9Ahhm5vcTs4pMT4SRkTb",
        "avalanche": "https://gateway.thegraph.com/api/subgraphs/id/2h9woxy8RTjHu1HJsCEnmzpPHFArU33avmUh4f71JpVn",
    },
    "compound_v3": {
        "ethereum": "https://gateway.thegraph.com/api/subgraphs/id/7REYb41fALHzAGd2M1v8qGFNQzciLJmi6kTibbhZDQ5b",
        "arbitrum": "https://gateway.thegraph.com/api/subgraphs/id/GJi1MjfTidc5DG2vBUuXce5C9giFoL3LRjc5BnTWERTM",
        "base": "https://gateway.thegraph.com/api/subgraphs/id/7dM2jFaYoPjFKLRWiYjj5M9nqGxAkFtEgV6jb5o3HAWc",
        "polygon": "https://gateway.thegraph.com/api/subgraphs/id/FjC15Rzwwv3r3bJrN47sxSvr1C4HtpivFPYCPPmKSoaZ",
    },
}

# DeFi Llama yields API
_LLAMA_YIELDS_API = "https://yields.llama.fi"
_LLAMA_POOLS_API = "https://yields.llama.fi/pools"

# Hyperliquid API
_HYPERLIQUID_API = "https://api.hyperliquid.xyz/info"

# DeFi Llama chain name mapping (our chain name -> DeFi Llama chain name)
_CHAIN_TO_LLAMA: dict[str, str] = {
    "ethereum": "Ethereum",
    "arbitrum": "Arbitrum",
    "base": "Base",
    "optimism": "Optimism",
    "polygon": "Polygon",
    "avalanche": "Avalanche",
    "bsc": "Binance",
    "sonic": "Sonic",
}

# Protocol name to DeFi Llama project name mapping
_PROTOCOL_TO_LLAMA_PROJECT: dict[str, str] = {
    "aave_v3": "aave-v3",
    "morpho_blue": "morpho-blue",
    "compound_v3": "compound-v3",
}

# Hyperliquid market symbol to coin mapping
_HYPER_MARKET_TO_COIN: dict[str, str] = {
    "ETH-USD": "ETH",
    "BTC-USD": "BTC",
    "ARB-USD": "ARB",
    "LINK-USD": "LINK",
    "SOL-USD": "SOL",
    "DOGE-USD": "DOGE",
    "AVAX-USD": "AVAX",
    "OP-USD": "OP",
}

# Hours in a year for annualization
_HOURS_PER_YEAR = 8760


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
# RateHistoryReader
# ---------------------------------------------------------------------------


@dataclass
class _ProviderMetrics:
    """Mutable metrics for a provider."""

    requests: int = 0
    successes: int = 0
    failures: int = 0


class RateHistoryReader:
    """Reads historical lending and funding rate data from multiple providers.

    Lending provider fallback chain:
        1. The Graph subgraphs (Aave V3, Compound V3)
        2. DeFi Llama yields API (all protocols)

    Funding provider fallback chain:
        1. Hyperliquid API (real historical data)
        2. GMX V2 API (on-chain funding snapshots)

    All results are wrapped in DataEnvelope with INFORMATIONAL classification
    and stored in VersionedDataCache for deterministic replay.

    Args:
        cache: Optional VersionedDataCache for disk persistence.
            Default creates a cache under ~/.almanak/data_cache/rate_history/.
        request_timeout: HTTP request timeout in seconds. Default 15.
        thegraph_api_key: Optional API key for The Graph decentralized network.
    """

    def __init__(
        self,
        cache: VersionedDataCache | None = None,
        request_timeout: float = 15.0,
        thegraph_api_key: str | None = None,
    ) -> None:
        self._cache = cache or VersionedDataCache(data_type="rate_history")
        self._request_timeout = request_timeout
        self._thegraph_api_key = thegraph_api_key
        self._rate_limiter = _TokenBucket(rate=5, period=1.0)
        self._metrics: dict[str, _ProviderMetrics] = {
            "thegraph": _ProviderMetrics(),
            "defillama": _ProviderMetrics(),
            "hyperliquid": _ProviderMetrics(),
            "gmx_v2": _ProviderMetrics(),
        }
        self._session: aiohttp.ClientSession | None = None

    # -- Public API: Lending Rates ------------------------------------------

    def get_lending_rate_history(
        self,
        protocol: str,
        token: str,
        chain: str,
        days: int = 90,
    ) -> DataEnvelope[list[LendingRateSnapshot]]:
        """Fetch historical lending rate snapshots with provider fallback.

        Args:
            protocol: Lending protocol (e.g. "aave_v3", "morpho_blue", "compound_v3").
            token: Token symbol (e.g. "USDC", "WETH").
            chain: Chain name (e.g. "arbitrum", "ethereum").
            days: Number of days of history to fetch. Default 90.

        Returns:
            DataEnvelope[list[LendingRateSnapshot]] with INFORMATIONAL classification.
            Snapshots are sorted ascending by timestamp.

        Raises:
            DataUnavailableError: If all providers fail.
        """
        protocol_lower = protocol.lower()
        token_upper = token.upper()
        chain_lower = chain.lower()

        # Validate inputs
        if days < 1:
            raise ValueError("days must be >= 1")

        end_date = datetime.now(UTC)
        start_date = end_date - timedelta(days=days)

        # Check cache
        cache_key = f"lending:{protocol_lower}:{token_upper}:{chain_lower}:{int(start_date.timestamp())}:{int(end_date.timestamp())}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            snapshots = _deserialize_lending_snapshots(cached.data)
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
                "lending_rate_history_cache_hit protocol=%s token=%s chain=%s version=%s",
                protocol_lower,
                token_upper,
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
        result = self._fetch_lending_with_fallback(protocol_lower, token_upper, chain_lower, start_date, end_date)
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
        serialized = _serialize_lending_snapshots(snapshots)
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
            "lending_rate_history_fetched source=%s protocol=%s token=%s chain=%s snapshots=%d latency_ms=%d",
            source,
            protocol_lower,
            token_upper,
            chain_lower,
            len(snapshots),
            latency_ms,
        )

        return DataEnvelope(
            value=snapshots,
            meta=meta,
            classification=DataClassification.INFORMATIONAL,
        )

    # -- Public API: Funding Rates ------------------------------------------

    def get_funding_rate_history(
        self,
        venue: str,
        market_symbol: str,
        hours: int = 168,
    ) -> DataEnvelope[list[FundingRateSnapshot]]:
        """Fetch historical funding rate snapshots with provider fallback.

        Args:
            venue: Perps venue (e.g. "hyperliquid", "gmx_v2").
            market_symbol: Market symbol (e.g. "ETH-USD", "BTC-USD").
            hours: Number of hours of history to fetch. Default 168 (7 days).

        Returns:
            DataEnvelope[list[FundingRateSnapshot]] with INFORMATIONAL classification.
            Snapshots are sorted ascending by timestamp.

        Raises:
            DataUnavailableError: If all providers fail.
        """
        venue_lower = venue.lower()
        market_upper = market_symbol.upper()

        if hours < 1:
            raise ValueError("hours must be >= 1")

        end_date = datetime.now(UTC)
        start_date = end_date - timedelta(hours=hours)

        # Check cache
        cache_key = f"funding:{venue_lower}:{market_upper}:{int(start_date.timestamp())}:{int(end_date.timestamp())}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            snapshots = _deserialize_funding_snapshots(cached.data)
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
                "funding_rate_history_cache_hit venue=%s market=%s version=%s",
                venue_lower,
                market_upper,
                cached.dataset_version,
            )
            return DataEnvelope(
                value=snapshots,
                meta=meta,
                classification=DataClassification.INFORMATIONAL,
            )

        # Try providers in order
        start_time = time.monotonic()
        result = self._fetch_funding_with_fallback(venue_lower, market_upper, start_date, end_date, hours)
        latency_ms = int((time.monotonic() - start_time) * 1000)

        source, snapshots = result

        # Sort by timestamp ascending
        snapshots.sort(key=lambda s: s.timestamp)

        # Determine finality
        now = datetime.now(UTC)
        cutoff = now - timedelta(hours=24)
        all_finalized = all(s.timestamp < cutoff for s in snapshots)
        finality_status = "finalized" if all_finalized else "provisional"

        # Store in cache
        serialized = _serialize_funding_snapshots(snapshots)
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
            "funding_rate_history_fetched source=%s venue=%s market=%s snapshots=%d latency_ms=%d",
            source,
            venue_lower,
            market_upper,
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

    # -- Lending: Provider fallback chain -----------------------------------

    def _fetch_lending_with_fallback(
        self,
        protocol: str,
        token: str,
        chain: str,
        start_date: datetime,
        end_date: datetime,
    ) -> tuple[str, list[LendingRateSnapshot]]:
        """Try lending rate providers in order, return (source_name, snapshots)."""
        errors: list[str] = []

        # Provider 1: The Graph
        try:
            snapshots = self._fetch_lending_from_thegraph(protocol, token, chain, start_date, end_date)
            if snapshots:
                return "thegraph", snapshots
        except Exception as e:
            errors.append(f"thegraph: {e}")
            logger.debug("lending_history_provider_failed provider=thegraph error=%s", e)

        # Provider 2: DeFi Llama
        try:
            snapshots = self._fetch_lending_from_defillama(protocol, token, chain, start_date, end_date)
            if snapshots:
                return "defillama", snapshots
        except Exception as e:
            errors.append(f"defillama: {e}")
            logger.debug("lending_history_provider_failed provider=defillama error=%s", e)

        raise DataUnavailableError(
            data_type="lending_rate_history",
            instrument=f"{protocol}/{token}",
            reason=f"All providers failed: {'; '.join(errors)}",
        )

    # -- Lending: The Graph -------------------------------------------------

    def _fetch_lending_from_thegraph(
        self,
        protocol: str,
        token: str,
        chain: str,
        start_date: datetime,
        end_date: datetime,
    ) -> list[LendingRateSnapshot]:
        """Fetch historical lending rates from The Graph subgraphs."""
        self._metrics["thegraph"].requests += 1

        subgraph_url = self._find_lending_subgraph_url(protocol, chain)
        if subgraph_url is None:
            self._metrics["thegraph"].failures += 1
            raise DataSourceUnavailable(
                source="thegraph",
                reason=f"No subgraph available for {protocol} on {chain}",
            )

        start_ts = int(start_date.timestamp())
        end_ts = int(end_date.timestamp())

        query = self._build_lending_query(protocol, token, start_ts, end_ts)

        try:
            raw_data = self._run_async(self._query_subgraph(subgraph_url, query))
        except Exception as e:
            self._metrics["thegraph"].failures += 1
            raise DataSourceUnavailable(source="thegraph", reason=str(e)) from e

        snapshots = self._parse_lending_subgraph_response(raw_data, protocol)

        if not snapshots:
            self._metrics["thegraph"].failures += 1
            raise DataSourceUnavailable(
                source="thegraph",
                reason=f"No data returned for {protocol}/{token} on {chain}",
            )

        self._metrics["thegraph"].successes += 1
        return snapshots

    def _find_lending_subgraph_url(self, protocol: str, chain: str) -> str | None:
        """Find the subgraph URL for a lending protocol and chain."""
        protocol_urls = _LENDING_SUBGRAPH_URLS.get(protocol, {})
        return protocol_urls.get(chain)

    def _build_lending_query(
        self,
        protocol: str,
        token: str,
        start_ts: int,
        end_ts: int,
        first: int = 1000,
    ) -> str:
        """Build a GraphQL query for lending rate history.

        Aave V3 subgraph uses reserveParamsHistoryItems with liquidityRate,
        variableBorrowRate, and utilizationRate fields.
        Compound V3 subgraph uses marketHourlySnapshots.
        """
        if protocol == "aave_v3":
            return f"""{{
  reserveParamsHistoryItems(
    first: {first}
    orderBy: timestamp
    orderDirection: asc
    where: {{
      reserve_: {{ symbol: "{token}" }}
      timestamp_gte: {start_ts}
      timestamp_lte: {end_ts}
    }}
  ) {{
    timestamp
    liquidityRate
    variableBorrowRate
    utilizationRate
  }}
}}"""
        elif protocol == "compound_v3":
            return f"""{{
  marketHourlySnapshots(
    first: {first}
    orderBy: timestamp
    orderDirection: asc
    where: {{
      market_: {{ inputToken_: {{ symbol: "{token}" }} }}
      timestamp_gte: {start_ts}
      timestamp_lte: {end_ts}
    }}
  ) {{
    timestamp
    rates {{
      rate
      side
    }}
  }}
}}"""
        else:
            # Generic fallback query for other protocols
            return f"""{{
  reserveParamsHistoryItems(
    first: {first}
    orderBy: timestamp
    orderDirection: asc
    where: {{
      reserve_: {{ symbol: "{token}" }}
      timestamp_gte: {start_ts}
      timestamp_lte: {end_ts}
    }}
  ) {{
    timestamp
    liquidityRate
    variableBorrowRate
    utilizationRate
  }}
}}"""

    def _parse_lending_subgraph_response(
        self,
        data: dict[str, Any],
        protocol: str,
    ) -> list[LendingRateSnapshot]:
        """Parse The Graph response into LendingRateSnapshot list."""
        if protocol == "compound_v3":
            return self._parse_compound_response(data)
        return self._parse_aave_response(data)

    def _parse_aave_response(self, data: dict[str, Any]) -> list[LendingRateSnapshot]:
        """Parse Aave V3 subgraph response.

        Aave rates are in RAY units (1e27).
        liquidityRate = supply APY in RAY
        variableBorrowRate = borrow APY in RAY
        utilizationRate = utilization in RAY (0-1e27)
        """
        items = data.get("reserveParamsHistoryItems", [])
        snapshots: list[LendingRateSnapshot] = []

        for item in items:
            try:
                ts = int(item.get("timestamp", 0))
                timestamp = datetime.fromtimestamp(ts, tz=UTC)

                # Convert RAY rates to percentage
                liquidity_rate = _safe_decimal(item.get("liquidityRate", "0"))
                borrow_rate = _safe_decimal(item.get("variableBorrowRate", "0"))
                utilization_raw = _safe_decimal(item.get("utilizationRate", "0"))

                ray = Decimal("1e27")
                supply_apy = (liquidity_rate / ray) * Decimal("100")
                borrow_apy = (borrow_rate / ray) * Decimal("100")
                utilization = (utilization_raw / ray) * Decimal("100") if utilization_raw > 0 else None

                snapshots.append(
                    LendingRateSnapshot(
                        supply_apy=supply_apy,
                        borrow_apy=borrow_apy,
                        utilization=utilization,
                        timestamp=timestamp,
                    )
                )
            except (ValueError, TypeError, InvalidOperation):
                logger.debug("Skipping malformed Aave rate entry: %s", item)
                continue

        return snapshots

    def _parse_compound_response(self, data: dict[str, Any]) -> list[LendingRateSnapshot]:
        """Parse Compound V3 subgraph response.

        Compound rates are stored in rates[] array with 'side' field
        indicating LENDER or BORROWER.
        """
        items = data.get("marketHourlySnapshots", [])
        snapshots: list[LendingRateSnapshot] = []

        for item in items:
            try:
                ts = int(item.get("timestamp", 0))
                timestamp = datetime.fromtimestamp(ts, tz=UTC)

                rates = item.get("rates", [])
                supply_apy = Decimal("0")
                borrow_apy = Decimal("0")

                for rate_entry in rates:
                    side = str(rate_entry.get("side", "")).upper()
                    rate_val = _safe_decimal(rate_entry.get("rate", "0"))
                    if side == "LENDER":
                        supply_apy = rate_val
                    elif side == "BORROWER":
                        borrow_apy = rate_val

                snapshots.append(
                    LendingRateSnapshot(
                        supply_apy=supply_apy,
                        borrow_apy=borrow_apy,
                        utilization=None,
                        timestamp=timestamp,
                    )
                )
            except (ValueError, TypeError, InvalidOperation):
                logger.debug("Skipping malformed Compound rate entry: %s", item)
                continue

        return snapshots

    # -- Lending: DeFi Llama ------------------------------------------------

    def _fetch_lending_from_defillama(
        self,
        protocol: str,
        token: str,
        chain: str,
        start_date: datetime,
        end_date: datetime,
    ) -> list[LendingRateSnapshot]:
        """Fetch historical lending rates from DeFi Llama yields API."""
        self._metrics["defillama"].requests += 1

        try:
            raw = self._run_async(self._query_defillama_lending(protocol, token, chain))
        except Exception as e:
            self._metrics["defillama"].failures += 1
            raise DataSourceUnavailable(source="defillama", reason=str(e)) from e

        snapshots = self._parse_defillama_lending_response(raw, start_date, end_date)

        if not snapshots:
            self._metrics["defillama"].failures += 1
            raise DataSourceUnavailable(
                source="defillama",
                reason=f"No DeFi Llama yield data for {protocol}/{token} on {chain}",
            )

        self._metrics["defillama"].successes += 1
        return snapshots

    async def _query_defillama_lending(
        self,
        protocol: str,
        token: str,
        chain: str,
    ) -> list[dict[str, Any]]:
        """Query DeFi Llama for lending yield history.

        DeFi Llama pools API returns all pools; we filter by protocol project
        name, token symbol, and chain. Then fetch chart data for the matching pool.
        """
        session = await self._get_session()

        if not self._rate_limiter.acquire():
            raise DataSourceUnavailable(source="defillama", reason="Rate limited")

        # Fetch all pools and find matching lending pool
        async with session.get(_LLAMA_POOLS_API) as response:
            if response.status != 200:
                text = await response.text()
                raise DataSourceUnavailable(
                    source="defillama",
                    reason=f"HTTP {response.status}: {text[:200]}",
                )
            data = await response.json()

        pools = data.get("data", [])
        llama_project = _PROTOCOL_TO_LLAMA_PROJECT.get(protocol, protocol)
        llama_chain = _CHAIN_TO_LLAMA.get(chain, chain.capitalize())
        token_upper = token.upper()

        matching: list[dict[str, Any]] = []
        for pool in pools:
            project = str(pool.get("project", "")).lower()
            pool_chain = str(pool.get("chain", ""))
            pool_symbol = str(pool.get("symbol", "")).upper()

            if project == llama_project and pool_chain == llama_chain and token_upper in pool_symbol:
                matching.append(pool)

        if not matching:
            raise DataSourceUnavailable(
                source="defillama",
                reason=f"Pool not found for {protocol}/{token} on {chain}",
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

    def _parse_defillama_lending_response(
        self,
        data: list[dict[str, Any]],
        start_date: datetime,
        end_date: datetime,
    ) -> list[LendingRateSnapshot]:
        """Parse DeFi Llama chart data into LendingRateSnapshot list.

        DeFi Llama chart data has daily resolution with fields:
            timestamp, tvlUsd, apy (supply APY), apyBase, apyReward,
            apyBorrow, apyBaseBorrow, apyRewardBorrow
        """
        snapshots: list[LendingRateSnapshot] = []

        for item in data:
            try:
                ts_str = item.get("timestamp", "")
                if isinstance(ts_str, str) and ts_str:
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

                # DeFi Llama provides APY as percentage values directly
                supply_apy = _safe_decimal(item.get("apy") or item.get("apyBase") or "0")
                borrow_apy = _safe_decimal(item.get("apyBorrow") or item.get("apyBaseBorrow") or "0")

                snapshots.append(
                    LendingRateSnapshot(
                        supply_apy=supply_apy,
                        borrow_apy=borrow_apy,
                        utilization=None,  # Not available in DeFi Llama chart data
                        timestamp=timestamp,
                    )
                )
            except (ValueError, TypeError, InvalidOperation):
                logger.debug("Skipping malformed DeFi Llama lending entry: %s", item)
                continue

        return snapshots

    # -- Funding: Provider fallback chain -----------------------------------

    def _fetch_funding_with_fallback(
        self,
        venue: str,
        market_symbol: str,
        start_date: datetime,
        end_date: datetime,
        hours: int,
    ) -> tuple[str, list[FundingRateSnapshot]]:
        """Try funding rate providers in order, return (source_name, snapshots)."""
        errors: list[str] = []

        # Provider 1: Hyperliquid (real historical data)
        if venue in ("hyperliquid", "gmx_v2"):  # GMX V2 uses Hyperliquid only (DeFi Llama has no funding rates)
            try:
                snapshots = self._fetch_funding_from_hyperliquid(market_symbol, start_date, end_date)
                if snapshots:
                    return "hyperliquid", snapshots
            except Exception as e:
                errors.append(f"hyperliquid: {e}")
                logger.debug("funding_history_provider_failed provider=hyperliquid error=%s", e)

        # GMX V2 has no DeFi Llama funding rate support -- skip fallback
        if venue == "gmx_v2":
            raise DataUnavailableError(
                data_type="funding_rate_history",
                instrument=f"{venue}/{market_symbol}",
                reason=f"GMX V2 funding rates only available via Hyperliquid: {'; '.join(errors)}",
            )

        # Provider 2: DeFi Llama (funding rate aggregation)
        try:
            snapshots = self._fetch_funding_from_defillama(venue, market_symbol, start_date, end_date)
            if snapshots:
                return "defillama", snapshots
        except Exception as e:
            errors.append(f"defillama: {e}")
            logger.debug("funding_history_provider_failed provider=defillama error=%s", e)

        raise DataUnavailableError(
            data_type="funding_rate_history",
            instrument=f"{venue}/{market_symbol}",
            reason=f"All providers failed: {'; '.join(errors)}",
        )

    # -- Funding: Hyperliquid -----------------------------------------------

    def _fetch_funding_from_hyperliquid(
        self,
        market_symbol: str,
        start_date: datetime,
        end_date: datetime,
    ) -> list[FundingRateSnapshot]:
        """Fetch historical funding rates from Hyperliquid API."""
        self._metrics["hyperliquid"].requests += 1

        coin = _HYPER_MARKET_TO_COIN.get(market_symbol)
        if coin is None:
            self._metrics["hyperliquid"].failures += 1
            raise DataSourceUnavailable(
                source="hyperliquid",
                reason=f"Unsupported market: {market_symbol}",
            )

        try:
            raw = self._run_async(self._query_hyperliquid_funding(coin, start_date, end_date))
        except Exception as e:
            self._metrics["hyperliquid"].failures += 1
            raise DataSourceUnavailable(source="hyperliquid", reason=str(e)) from e

        snapshots = self._parse_hyperliquid_funding_response(raw, start_date, end_date)

        if not snapshots:
            self._metrics["hyperliquid"].failures += 1
            raise DataSourceUnavailable(
                source="hyperliquid",
                reason=f"No data returned for {market_symbol}",
            )

        self._metrics["hyperliquid"].successes += 1
        return snapshots

    async def _query_hyperliquid_funding(
        self,
        coin: str,
        start_date: datetime,
        end_date: datetime,
    ) -> list[dict[str, Any]]:
        """Query Hyperliquid for historical funding rate data.

        Hyperliquid API: POST /info with type=fundingHistory
        Returns funding rate snapshots with 8h intervals.
        """
        session = await self._get_session()

        if not self._rate_limiter.acquire():
            raise DataSourceUnavailable(source="hyperliquid", reason="Rate limited")

        start_ms = int(start_date.timestamp() * 1000)
        end_ms = int(end_date.timestamp() * 1000)

        payload = {
            "type": "fundingHistory",
            "coin": coin,
            "startTime": start_ms,
            "endTime": end_ms,
        }

        async with session.post(
            _HYPERLIQUID_API,
            json=payload,
            headers={"Content-Type": "application/json"},
        ) as response:
            if response.status != 200:
                text = await response.text()
                raise DataSourceUnavailable(
                    source="hyperliquid",
                    reason=f"HTTP {response.status}: {text[:200]}",
                )
            data = await response.json()

        if not isinstance(data, list):
            return []
        return data

    def _parse_hyperliquid_funding_response(
        self,
        data: list[dict[str, Any]],
        start_date: datetime,
        end_date: datetime,
    ) -> list[FundingRateSnapshot]:
        """Parse Hyperliquid funding history response.

        Response format: list of {coin, fundingRate, premium, time}
        where fundingRate is the hourly rate (e.g. "0.00001234")
        and time is ISO format timestamp.
        """
        snapshots: list[FundingRateSnapshot] = []

        for item in data:
            try:
                time_str = item.get("time", "")
                if isinstance(time_str, str) and time_str:
                    timestamp = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
                    if timestamp.tzinfo is None:
                        timestamp = timestamp.replace(tzinfo=UTC)
                elif isinstance(time_str, int | float):
                    # Millisecond timestamp
                    timestamp = datetime.fromtimestamp(time_str / 1000, tz=UTC)
                else:
                    continue

                if timestamp < start_date or timestamp > end_date:
                    continue

                rate = _safe_decimal(item.get("fundingRate", "0"))
                # Annualize: hourly rate * 8760
                annualized = rate * Decimal(str(_HOURS_PER_YEAR))

                snapshots.append(
                    FundingRateSnapshot(
                        rate=rate,
                        annualized_rate=annualized,
                        timestamp=timestamp,
                    )
                )
            except (ValueError, TypeError, InvalidOperation):
                logger.debug("Skipping malformed Hyperliquid funding entry: %s", item)
                continue

        return snapshots

    # -- Funding: DeFi Llama ------------------------------------------------

    def _fetch_funding_from_defillama(
        self,
        venue: str,
        market_symbol: str,
        start_date: datetime,
        end_date: datetime,
    ) -> list[FundingRateSnapshot]:
        """Fetch funding rates from DeFi Llama perps API as fallback.

        DeFi Llama provides perps data at https://yields.llama.fi/perps
        but coverage is limited. This is a best-effort fallback.
        """
        self._metrics["defillama"].requests += 1

        try:
            raw = self._run_async(self._query_defillama_funding(venue, market_symbol))
        except Exception as e:
            self._metrics["defillama"].failures += 1
            raise DataSourceUnavailable(source="defillama", reason=str(e)) from e

        snapshots = self._parse_defillama_funding_response(raw, start_date, end_date)

        if not snapshots:
            self._metrics["defillama"].failures += 1
            raise DataSourceUnavailable(
                source="defillama",
                reason=f"No DeFi Llama funding data for {venue}/{market_symbol}",
            )

        self._metrics["defillama"].successes += 1
        return snapshots

    async def _query_defillama_funding(
        self,
        venue: str,
        market_symbol: str,
    ) -> list[dict[str, Any]]:
        """Query DeFi Llama perps endpoint for funding history."""
        session = await self._get_session()

        if not self._rate_limiter.acquire():
            raise DataSourceUnavailable(source="defillama", reason="Rate limited")

        # DeFi Llama perps endpoint
        url = f"{_LLAMA_YIELDS_API}/perps"
        async with session.get(url) as response:
            if response.status != 200:
                text = await response.text()
                raise DataSourceUnavailable(
                    source="defillama",
                    reason=f"HTTP {response.status}: {text[:200]}",
                )
            data = await response.json()

        # Filter by venue and market
        pools = data.get("data", [])
        coin = _HYPER_MARKET_TO_COIN.get(market_symbol, market_symbol.split("-")[0])
        venue_lower = venue.lower()

        matching: list[dict[str, Any]] = []
        for pool in pools:
            project = str(pool.get("project", "")).lower()
            symbol = str(pool.get("symbol", "")).upper()
            if venue_lower in project and coin.upper() in symbol:
                matching.append(pool)

        return matching

    def _parse_defillama_funding_response(
        self,
        data: list[dict[str, Any]],
        start_date: datetime,
        end_date: datetime,
    ) -> list[FundingRateSnapshot]:
        """Parse DeFi Llama perps data into FundingRateSnapshot list.

        DeFi Llama perps data is a snapshot (not historical), so we produce
        a single-point snapshot from the current data if available.
        """
        snapshots: list[FundingRateSnapshot] = []
        now = datetime.now(UTC)

        for item in data:
            try:
                # DeFi Llama perps provides current funding rate
                rate_val = item.get("fundingRate")
                if rate_val is None:
                    continue

                rate = _safe_decimal(rate_val)
                annualized = rate * Decimal(str(_HOURS_PER_YEAR))

                snapshots.append(
                    FundingRateSnapshot(
                        rate=rate,
                        annualized_rate=annualized,
                        timestamp=now,
                    )
                )
            except (ValueError, TypeError, InvalidOperation):
                logger.debug("Skipping malformed DeFi Llama funding entry: %s", item)
                continue

        return snapshots

    # -- Internal helpers ----------------------------------------------------

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


def _serialize_lending_snapshots(snapshots: list[LendingRateSnapshot]) -> list[dict[str, Any]]:
    """Serialize LendingRateSnapshot list to JSON-compatible dicts."""
    return [
        {
            "supply_apy": str(s.supply_apy),
            "borrow_apy": str(s.borrow_apy),
            "utilization": str(s.utilization) if s.utilization is not None else None,
            "timestamp": s.timestamp.isoformat(),
        }
        for s in snapshots
    ]


def _deserialize_lending_snapshots(data: Any) -> list[LendingRateSnapshot]:
    """Deserialize JSON dicts back to LendingRateSnapshot list."""
    if not isinstance(data, list):
        return []
    snapshots: list[LendingRateSnapshot] = []
    for item in data:
        try:
            ts_str = item["timestamp"]
            timestamp = datetime.fromisoformat(ts_str)
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=UTC)
            utilization_raw = item.get("utilization")
            utilization = Decimal(utilization_raw) if utilization_raw is not None else None
            snapshots.append(
                LendingRateSnapshot(
                    supply_apy=Decimal(item["supply_apy"]),
                    borrow_apy=Decimal(item["borrow_apy"]),
                    utilization=utilization,
                    timestamp=timestamp,
                )
            )
        except (KeyError, ValueError, InvalidOperation):
            continue
    return snapshots


def _serialize_funding_snapshots(snapshots: list[FundingRateSnapshot]) -> list[dict[str, Any]]:
    """Serialize FundingRateSnapshot list to JSON-compatible dicts."""
    return [
        {
            "rate": str(s.rate),
            "annualized_rate": str(s.annualized_rate),
            "timestamp": s.timestamp.isoformat(),
        }
        for s in snapshots
    ]


def _deserialize_funding_snapshots(data: Any) -> list[FundingRateSnapshot]:
    """Deserialize JSON dicts back to FundingRateSnapshot list."""
    if not isinstance(data, list):
        return []
    snapshots: list[FundingRateSnapshot] = []
    for item in data:
        try:
            ts_str = item["timestamp"]
            timestamp = datetime.fromisoformat(ts_str)
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=UTC)
            snapshots.append(
                FundingRateSnapshot(
                    rate=Decimal(item["rate"]),
                    annualized_rate=Decimal(item["annualized_rate"]),
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


__all__ = [
    "FundingRateSnapshot",
    "LendingRateSnapshot",
    "RateHistoryReader",
]
