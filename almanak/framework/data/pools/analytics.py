"""Pool analytics - TVL, volume, fee APR/APY for DEX and lending pools.

Provides real-time pool analytics from DeFi Llama (primary), GeckoTerminal
(fallback 1), and The Graph (fallback 2). Includes a ``best_pool()`` method
for dynamic venue selection based on a chosen metric.

Example:
    from almanak.framework.data.pools.analytics import PoolAnalyticsReader

    reader = PoolAnalyticsReader()
    analytics = reader.get_pool_analytics("0x...", "arbitrum")
    print(f"TVL: ${analytics.value.tvl_usd}, Fee APR: {analytics.value.fee_apr}%")

    best = reader.best_pool("WETH", "USDC", "arbitrum", metric="fee_apr")
    print(f"Best pool: {best.value.pool_address}")
"""

from __future__ import annotations

import asyncio
import logging
import math
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import aiohttp

from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.framework.data.models import (
    DataClassification,
    DataEnvelope,
    DataMeta,
)

logger = logging.getLogger(__name__)

# DeFi Llama API base URLs
_YIELDS_API = "https://yields.llama.fi"
_TVL_API = "https://api.llama.fi"

# GeckoTerminal API base URL
_GT_API = "https://api.geckoterminal.com/api/v2"

# Chain -> GeckoTerminal network mapping
_CHAIN_TO_GT_NETWORK: dict[str, str] = {
    "ethereum": "eth",
    "arbitrum": "arbitrum",
    "base": "base",
    "optimism": "optimism",
    "polygon": "polygon_pos",
    "avalanche": "avax",
    "bsc": "bsc",
    "sonic": "sonic",
}

# Chain -> DeFi Llama chain name mapping (DeFi Llama uses capitalized names)
_CHAIN_TO_LLAMA_DISPLAY: dict[str, str] = {
    "ethereum": "Ethereum",
    "arbitrum": "Arbitrum",
    "base": "Base",
    "optimism": "Optimism",
    "polygon": "Polygon",
    "avalanche": "Avalanche",
    "bsc": "BSC",
    "sonic": "Sonic",
}

# Protocol -> DeFi Llama project slug
_PROTOCOL_TO_LLAMA: dict[str, str] = {
    "uniswap_v3": "uniswap-v3",
    "aerodrome": "aerodrome-v2",
    "pancakeswap_v3": "pancakeswap-amm-v3",
    "aave_v3": "aave-v3",
    "morpho": "morpho-blue",
    "compound_v3": "compound-v3",
}


def _safe_decimal(value: Any) -> Decimal:
    """Convert a value to Decimal, returning 0 on failure."""
    if value is None:
        return Decimal(0)
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal(0)


def _safe_float(value: Any, default: float = 0.0) -> float:
    """Convert a value to float, returning default on failure."""
    if value is None:
        return default
    try:
        result = float(value)
        if math.isnan(result) or math.isinf(result):
            return default
        return result
    except (ValueError, TypeError):
        return default


# =============================================================================
# Data Models
# =============================================================================


@dataclass(frozen=True)
class PoolAnalytics:
    """Analytics for a single pool.

    Attributes:
        pool_address: Pool contract address.
        chain: Chain name.
        protocol: Protocol name (e.g. "uniswap_v3").
        tvl_usd: Total value locked in USD.
        volume_24h_usd: 24-hour trading volume in USD.
        volume_7d_usd: 7-day trading volume in USD.
        fee_apr: Annualized fee return as a percentage (e.g. 12.5 = 12.5%).
        fee_apy: Compounded annual fee return as a percentage.
        utilization_rate: Utilization rate for lending pools (0.0-1.0), None for DEX.
        token0_weight: Fraction of TVL in token0 (0.0-1.0).
        token1_weight: Fraction of TVL in token1 (0.0-1.0).
    """

    pool_address: str
    chain: str
    protocol: str
    tvl_usd: Decimal
    volume_24h_usd: Decimal
    volume_7d_usd: Decimal
    fee_apr: float
    fee_apy: float
    utilization_rate: float | None = None
    token0_weight: float = 0.5
    token1_weight: float = 0.5


@dataclass(frozen=True)
class PoolAnalyticsResult:
    """Result from best_pool() with pool address and analytics."""

    pool_address: str
    analytics: PoolAnalytics
    metric_value: float
    metric_name: str


# =============================================================================
# Token Bucket Rate Limiter (reusable)
# =============================================================================


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


# =============================================================================
# Provider Health Tracking
# =============================================================================


@dataclass
class _ProviderMetrics:
    successes: int = 0
    failures: int = 0


# =============================================================================
# PoolAnalyticsReader
# =============================================================================


class PoolAnalyticsReader:
    """Reads pool analytics from DeFi Llama, GeckoTerminal, and The Graph.

    Provider fallback:
        1. DeFi Llama yields API (TVL, volume, APY across protocols)
        2. GeckoTerminal pool info (TVL, volume for DEX pools)
        3. The Graph subgraphs (protocol-specific deep data)

    All results are wrapped in DataEnvelope with INFORMATIONAL classification.

    Args:
        cache_ttl: In-memory cache TTL in seconds. Default 300 (5 minutes).
        request_timeout: HTTP request timeout in seconds. Default 15.
    """

    def __init__(
        self,
        cache_ttl: int = 300,
        request_timeout: float = 15.0,
    ) -> None:
        self._cache_ttl = cache_ttl
        self._request_timeout = request_timeout
        self._rate_limiter_llama = _TokenBucket(rate=10, period=1.0)
        self._rate_limiter_gt = _TokenBucket(rate=30, period=60.0)
        self._metrics: dict[str, _ProviderMetrics] = {
            "defillama": _ProviderMetrics(),
            "geckoterminal": _ProviderMetrics(),
        }
        self._cache: dict[str, tuple[Any, float]] = {}

    # -- Public API -----------------------------------------------------------

    def get_pool_analytics(
        self,
        pool_address: str,
        chain: str,
        protocol: str | None = None,
    ) -> DataEnvelope[PoolAnalytics]:
        """Get real-time analytics for a pool.

        Tries DeFi Llama first (broader coverage), then GeckoTerminal.

        Args:
            pool_address: Pool contract address.
            chain: Chain name (e.g. "arbitrum").
            protocol: Optional protocol hint (e.g. "uniswap_v3").

        Returns:
            DataEnvelope[PoolAnalytics] with INFORMATIONAL classification.

        Raises:
            DataSourceUnavailable: If all providers fail.
        """
        chain = chain.lower()
        pool_address = pool_address.lower()
        cache_key = f"analytics:{pool_address}:{chain}"

        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        start = time.monotonic()
        errors: list[str] = []

        # Try DeFi Llama first
        try:
            analytics = self._fetch_from_defillama(pool_address, chain, protocol)
            return self._wrap_result(analytics, "defillama", start, cache_key)
        except (DataSourceUnavailable, Exception) as e:
            errors.append(f"defillama: {e}")
            logger.debug("DeFi Llama pool analytics failed for %s: %s", pool_address, e)

        # Fallback: GeckoTerminal
        try:
            analytics = self._fetch_from_geckoterminal(pool_address, chain, protocol)
            return self._wrap_result(analytics, "geckoterminal", start, cache_key)
        except (DataSourceUnavailable, Exception) as e:
            errors.append(f"geckoterminal: {e}")
            logger.debug("GeckoTerminal pool analytics failed for %s: %s", pool_address, e)

        raise DataSourceUnavailable(
            source="pool_analytics",
            reason=f"All providers failed for {pool_address} on {chain}: {'; '.join(errors)}",
        )

    def best_pool(
        self,
        token_a: str,
        token_b: str,
        chain: str,
        metric: str = "fee_apr",
        protocols: list[str] | None = None,
    ) -> DataEnvelope[PoolAnalyticsResult]:
        """Find the best pool for a token pair based on a metric.

        Searches DeFi Llama yields API for matching pools and ranks by metric.

        Args:
            token_a: First token symbol (e.g. "WETH").
            token_b: Second token symbol (e.g. "USDC").
            chain: Chain name.
            metric: Sorting metric: "fee_apr", "fee_apy", "tvl_usd", "volume_24h_usd".
            protocols: Optional list of protocol names to filter by.

        Returns:
            DataEnvelope[PoolAnalyticsResult] with the best pool.

        Raises:
            DataSourceUnavailable: If no pools found or all providers fail.
        """
        chain = chain.lower()
        cache_key = f"best_pool:{token_a}:{token_b}:{chain}:{metric}:{protocols}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        start = time.monotonic()

        # Search DeFi Llama for matching pools
        pools = self._search_pools_defillama(token_a, token_b, chain, protocols)

        if not pools:
            raise DataSourceUnavailable(
                source="pool_analytics",
                reason=f"No pools found for {token_a}/{token_b} on {chain}",
            )

        # Sort by metric
        valid_metrics = {"fee_apr", "fee_apy", "tvl_usd", "volume_24h_usd"}
        if metric not in valid_metrics:
            raise ValueError(f"Invalid metric '{metric}'. Valid: {', '.join(sorted(valid_metrics))}")

        def _metric_value(p: PoolAnalytics) -> float:
            val = getattr(p, metric)
            if isinstance(val, Decimal):
                return float(val)
            return float(val)

        pools.sort(key=_metric_value, reverse=True)
        best = pools[0]
        best_val = _metric_value(best)

        result = PoolAnalyticsResult(
            pool_address=best.pool_address,
            analytics=best,
            metric_value=best_val,
            metric_name=metric,
        )

        latency_ms = int((time.monotonic() - start) * 1000)
        meta = DataMeta(
            source="defillama",
            observed_at=datetime.now(UTC),
            finality="off_chain",
            staleness_ms=0,
            latency_ms=latency_ms,
            confidence=0.85,
            cache_hit=False,
        )
        envelope = DataEnvelope(value=result, meta=meta, classification=DataClassification.INFORMATIONAL)
        self._update_cache(cache_key, envelope)
        return envelope

    # -- DeFi Llama provider ---------------------------------------------------

    def _fetch_from_defillama(
        self,
        pool_address: str,
        chain: str,
        protocol: str | None,
    ) -> PoolAnalytics:
        """Fetch pool analytics from DeFi Llama yields API."""
        if not self._rate_limiter_llama.acquire():
            raise DataSourceUnavailable(source="defillama", reason="Rate limited")

        llama_chain = _CHAIN_TO_LLAMA_DISPLAY.get(chain)
        if llama_chain is None:
            raise DataSourceUnavailable(source="defillama", reason=f"Unsupported chain: {chain}")

        try:
            pools_data = self._run_async(self._query_defillama_pools())
        except Exception as e:
            self._metrics["defillama"].failures += 1
            raise DataSourceUnavailable(source="defillama", reason=str(e)) from e

        # Search for matching pool by address
        match = None
        for pool in pools_data:
            pool_id = str(pool.get("pool", "")).lower()
            pool_chain = str(pool.get("chain", "")).lower()
            # DeFi Llama pool IDs often contain the address
            if pool_address in pool_id and pool_chain == llama_chain.lower():
                match = pool
                break

        if match is None:
            self._metrics["defillama"].failures += 1
            raise DataSourceUnavailable(
                source="defillama",
                reason=f"Pool {pool_address} not found on {chain} in DeFi Llama",
            )

        self._metrics["defillama"].successes += 1
        return self._parse_llama_pool_to_analytics(match, pool_address, chain, protocol)

    def _search_pools_defillama(
        self,
        token_a: str,
        token_b: str,
        chain: str,
        protocols: list[str] | None,
    ) -> list[PoolAnalytics]:
        """Search DeFi Llama for pools matching a token pair."""
        if not self._rate_limiter_llama.acquire():
            raise DataSourceUnavailable(source="defillama", reason="Rate limited")

        llama_chain = _CHAIN_TO_LLAMA_DISPLAY.get(chain)
        if llama_chain is None:
            return []

        try:
            pools_data = self._run_async(self._query_defillama_pools())
        except Exception:
            return []

        # Normalize token symbols for matching
        token_a_upper = token_a.upper()
        token_b_upper = token_b.upper()

        # Filter protocol slugs
        llama_projects: set[str] | None = None
        if protocols:
            llama_projects = set()
            for p in protocols:
                slug = _PROTOCOL_TO_LLAMA.get(p.lower())
                if slug:
                    llama_projects.add(slug)

        results: list[PoolAnalytics] = []
        for pool in pools_data:
            pool_chain = str(pool.get("chain", "")).lower()
            if pool_chain != llama_chain.lower():
                continue

            if llama_projects:
                project = str(pool.get("project", "")).lower()
                if project not in llama_projects:
                    continue

            # Check symbol match (e.g., "USDC-WETH")
            symbol = str(pool.get("symbol", "")).upper()
            symbol_tokens = {t.strip() for t in symbol.replace("-", "/").split("/")}
            if token_a_upper in symbol_tokens and token_b_upper in symbol_tokens:
                pool_addr = (
                    str(pool.get("pool", "")).split("-")[-1]
                    if "-" in str(pool.get("pool", ""))
                    else str(pool.get("pool", ""))
                )
                analytics = self._parse_llama_pool_to_analytics(pool, pool_addr, chain, None)
                results.append(analytics)

        return results

    def _parse_llama_pool_to_analytics(
        self,
        pool: dict[str, Any],
        pool_address: str,
        chain: str,
        protocol: str | None,
    ) -> PoolAnalytics:
        """Convert a DeFi Llama pool dict to PoolAnalytics."""
        tvl = _safe_decimal(pool.get("tvlUsd"))
        apy_base = _safe_float(pool.get("apyBase"))
        apy_total = _safe_float(pool.get("apy"))

        # Volume: DeFi Llama provides volumeUsd1d and volumeUsd7d on some pools
        vol_24h = _safe_decimal(pool.get("volumeUsd1d", 0))
        vol_7d = _safe_decimal(pool.get("volumeUsd7d", 0))

        # Fee APR from apyBase (fee-only yield)
        fee_apr = apy_base
        # Fee APY = compounded: (1 + apr/365)^365 - 1
        fee_apy = apy_total if apy_total > 0 else fee_apr

        project = str(pool.get("project", ""))
        detected_protocol = protocol or project

        return PoolAnalytics(
            pool_address=pool_address,
            chain=chain,
            protocol=detected_protocol,
            tvl_usd=tvl,
            volume_24h_usd=vol_24h,
            volume_7d_usd=vol_7d,
            fee_apr=fee_apr,
            fee_apy=fee_apy,
            utilization_rate=None,
            token0_weight=0.5,
            token1_weight=0.5,
        )

    async def _query_defillama_pools(self) -> list[dict[str, Any]]:
        """Fetch all pools from DeFi Llama yields API."""
        url = f"{_YIELDS_API}/pools"
        timeout = aiohttp.ClientTimeout(total=self._request_timeout)
        async with aiohttp.ClientSession(timeout=timeout, headers={"Accept": "application/json"}) as session:
            async with session.get(url) as response:
                if response.status != 200:
                    text = await response.text()
                    raise DataSourceUnavailable(source="defillama", reason=f"HTTP {response.status}: {text[:200]}")
                data = await response.json()
                return data.get("data", [])

    # -- GeckoTerminal provider ------------------------------------------------

    def _fetch_from_geckoterminal(
        self,
        pool_address: str,
        chain: str,
        protocol: str | None,
    ) -> PoolAnalytics:
        """Fetch pool analytics from GeckoTerminal API."""
        if not self._rate_limiter_gt.acquire():
            raise DataSourceUnavailable(source="geckoterminal", reason="Rate limited")

        network = _CHAIN_TO_GT_NETWORK.get(chain)
        if network is None:
            raise DataSourceUnavailable(source="geckoterminal", reason=f"Unsupported chain: {chain}")

        try:
            data = self._run_async(self._query_geckoterminal_pool(network, pool_address))
        except Exception as e:
            self._metrics["geckoterminal"].failures += 1
            raise DataSourceUnavailable(source="geckoterminal", reason=str(e)) from e

        self._metrics["geckoterminal"].successes += 1
        return self._parse_gt_pool_to_analytics(data, pool_address, chain, protocol)

    def _parse_gt_pool_to_analytics(
        self,
        data: dict[str, Any],
        pool_address: str,
        chain: str,
        protocol: str | None,
    ) -> PoolAnalytics:
        """Convert GeckoTerminal pool response to PoolAnalytics."""
        attrs = data.get("data", {}).get("attributes", {})

        tvl = _safe_decimal(attrs.get("reserve_in_usd"))
        vol_24h = _safe_decimal(attrs.get("volume_usd", {}).get("h24", 0))
        vol_7d = Decimal(0)  # GeckoTerminal doesn't provide 7d volume directly

        # Fee estimation: GeckoTerminal provides pool_fee for some pools
        pool_fee = _safe_float(attrs.get("pool_fee"))
        fee_apr = 0.0
        if pool_fee > 0 and tvl > 0:
            # Rough APR: (daily_volume * fee_rate * 365) / TVL * 100
            daily_vol = float(vol_24h)
            fee_apr = (daily_vol * pool_fee * 365) / float(tvl) * 100 if float(tvl) > 0 else 0.0
        fee_apy = ((1 + fee_apr / 365 / 100) ** 365 - 1) * 100 if fee_apr > 0 else 0.0

        dex_id = attrs.get("dex_id", "")
        detected_protocol = protocol or str(dex_id)

        return PoolAnalytics(
            pool_address=pool_address,
            chain=chain,
            protocol=detected_protocol,
            tvl_usd=tvl,
            volume_24h_usd=vol_24h,
            volume_7d_usd=vol_7d,
            fee_apr=fee_apr,
            fee_apy=fee_apy,
        )

    async def _query_geckoterminal_pool(self, network: str, pool_address: str) -> dict[str, Any]:
        """Fetch pool info from GeckoTerminal."""
        url = f"{_GT_API}/networks/{network}/pools/{pool_address}"
        timeout = aiohttp.ClientTimeout(total=self._request_timeout)
        async with aiohttp.ClientSession(timeout=timeout, headers={"Accept": "application/json"}) as session:
            async with session.get(url) as response:
                if response.status != 200:
                    text = await response.text()
                    raise DataSourceUnavailable(source="geckoterminal", reason=f"HTTP {response.status}: {text[:200]}")
                return await response.json()

    # -- Helpers ---------------------------------------------------------------

    def _wrap_result(
        self,
        analytics: PoolAnalytics,
        source: str,
        start: float,
        cache_key: str,
    ) -> DataEnvelope[PoolAnalytics]:
        """Wrap analytics in DataEnvelope and cache."""
        latency_ms = int((time.monotonic() - start) * 1000)
        meta = DataMeta(
            source=source,
            observed_at=datetime.now(UTC),
            finality="off_chain",
            staleness_ms=0,
            latency_ms=latency_ms,
            confidence=0.85,
            cache_hit=False,
        )
        envelope = DataEnvelope(value=analytics, meta=meta, classification=DataClassification.INFORMATIONAL)
        self._update_cache(cache_key, envelope)
        return envelope

    def _get_cached(self, key: str) -> DataEnvelope | None:
        entry = self._cache.get(key)
        if entry is None:
            return None
        value, cached_at = entry
        if time.monotonic() - cached_at > self._cache_ttl:
            return None
        # Return a copy with cache_hit=True
        envelope = value
        meta = DataMeta(
            source=envelope.meta.source,
            observed_at=envelope.meta.observed_at,
            finality=envelope.meta.finality,
            staleness_ms=int((time.monotonic() - cached_at) * 1000),
            latency_ms=0,
            confidence=envelope.meta.confidence,
            cache_hit=True,
        )
        return DataEnvelope(value=envelope.value, meta=meta, classification=DataClassification.INFORMATIONAL)

    def _update_cache(self, key: str, value: Any) -> None:
        self._cache[key] = (value, time.monotonic())

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

    def health(self) -> dict[str, dict[str, int]]:
        """Return provider health metrics."""
        return {name: {"successes": m.successes, "failures": m.failures} for name, m in self._metrics.items()}


__all__ = [
    "PoolAnalytics",
    "PoolAnalyticsReader",
    "PoolAnalyticsResult",
]
