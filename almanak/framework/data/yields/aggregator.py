"""Yield aggregator - cross-protocol yield comparison.

Scans DeFi Llama yields API for yield opportunities across lending, LP, and
staking protocols, with on-chain lending rate reads as fallback. Supports
filtering by token, chain, minimum TVL, and sorting by APY/TVL/risk.

Example:
    from almanak.framework.data.yields.aggregator import YieldAggregator

    agg = YieldAggregator()
    envelope = agg.get_yield_opportunities("USDC", chains=["arbitrum", "base"])
    for opp in envelope.value:
        print(f"{opp.protocol} on {opp.chain}: {opp.apy:.2f}% APY, ${opp.tvl_usd} TVL")
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

# DeFi Llama yields API
_YIELDS_API = "https://yields.llama.fi"

# Chain -> DeFi Llama display name (capitalized as returned by API)
_CHAIN_TO_LLAMA_DISPLAY: dict[str, str] = {
    "ethereum": "Ethereum",
    "arbitrum": "Arbitrum",
    "base": "Base",
    "optimism": "Optimism",
    "polygon": "Polygon",
    "avalanche": "Avalanche",
    "bsc": "BSC",
    "sonic": "Sonic",
    "solana": "Solana",
}

# Protocol -> DeFi Llama project slug
_PROTOCOL_TO_LLAMA: dict[str, str] = {
    "aave_v3": "aave-v3",
    "morpho": "morpho-blue",
    "compound_v3": "compound-v3",
    "uniswap_v3": "uniswap-v3",
    "aerodrome": "aerodrome-v2",
    "lido": "lido",
    "pancakeswap_v3": "pancakeswap-amm-v3",
    "jito": "jito",
    "marinade": "marinade-finance",
    "sanctum": "sanctum-infinity",
    "kamino": "kamino-lending",
    "raydium": "raydium",
    "fluid": "fluid-dex",
}

# DeFi Llama project slug -> our yield type classification
_PROJECT_TYPE: dict[str, str] = {
    "aave-v3": "lending",
    "morpho-blue": "lending",
    "compound-v3": "lending",
    "spark-lending": "lending",
    "uniswap-v3": "lp",
    "aerodrome-v2": "lp",
    "pancakeswap-amm-v3": "lp",
    "sushiswap": "lp",
    "lido": "staking",
    "rocket-pool": "staking",
    "frax-ether": "staking",
    "jito": "staking",
    "marinade-finance": "staking",
    "sanctum-infinity": "staking",
    "kamino-lending": "lending",
    "raydium": "lp",
    "fluid-dex": "lp",
}


def _safe_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal(0)
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal(0)


def _safe_float(value: Any, default: float = 0.0) -> float:
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
# Data Model
# =============================================================================


@dataclass(frozen=True)
class YieldOpportunity:
    """A yield opportunity from a DeFi protocol.

    Attributes:
        protocol: Protocol name (e.g. "aave-v3", "uniswap-v3").
        chain: Chain name (e.g. "arbitrum").
        pool_id: DeFi Llama pool identifier.
        symbol: Pool symbol (e.g. "USDC", "USDC-WETH").
        apy: Annual percentage yield (total: base + rewards).
        apy_base: Base APY from fees/interest (None if unknown).
        apy_reward: Reward APY from incentives (None if unknown).
        tvl_usd: Total value locked in USD.
        type: Yield type: "lending", "lp", or "staking".
        risk_score: Risk score (0.0=safe, 1.0=high risk), None if unavailable.
        il_risk: Whether the opportunity has impermanent loss risk.
    """

    protocol: str
    chain: str
    pool_id: str
    symbol: str
    apy: float
    tvl_usd: Decimal
    type: str
    apy_base: float | None = None
    apy_reward: float | None = None
    risk_score: float | None = None
    il_risk: bool = False


# =============================================================================
# Token Bucket Rate Limiter
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
# YieldAggregator
# =============================================================================


class YieldAggregator:
    """Cross-protocol yield comparison using DeFi Llama.

    Fetches yield data from DeFi Llama yields API, filtering by token, chain,
    TVL, and protocol. Results are cached for 15 minutes.

    Args:
        cache_ttl: In-memory cache TTL in seconds. Default 900 (15 minutes).
        request_timeout: HTTP request timeout in seconds. Default 15.
    """

    def __init__(
        self,
        cache_ttl: int = 900,
        request_timeout: float = 15.0,
    ) -> None:
        self._cache_ttl = cache_ttl
        self._request_timeout = request_timeout
        self._rate_limiter = _TokenBucket(rate=10, period=1.0)
        self._cache: dict[str, tuple[Any, float]] = {}
        self._successes = 0
        self._failures = 0

    def get_yield_opportunities(
        self,
        token: str,
        chains: list[str] | None = None,
        min_tvl: float = 100_000,
        sort_by: str = "apy",
    ) -> DataEnvelope[list[YieldOpportunity]]:
        """Find yield opportunities for a token across protocols and chains.

        Args:
            token: Token symbol (e.g. "USDC", "WETH").
            chains: Optional list of chains to filter (e.g. ["arbitrum", "base"]).
                    None means all supported chains.
            min_tvl: Minimum TVL in USD. Default $100k.
            sort_by: Sort field: "apy", "tvl", "risk_score". Default "apy".

        Returns:
            DataEnvelope[list[YieldOpportunity]] sorted by the chosen metric.

        Raises:
            DataSourceUnavailable: If DeFi Llama API is unavailable.
        """
        token = token.upper()
        chain_key = ",".join(sorted(c.lower() for c in chains)) if chains else "all"
        cache_key = f"yields:{token}:{chain_key}:{min_tvl}:{sort_by}"

        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        start = time.monotonic()

        if not self._rate_limiter.acquire():
            raise DataSourceUnavailable(source="yield_aggregator", reason="Rate limited")

        try:
            pools_data = self._run_async(self._query_defillama_pools())
        except Exception as e:
            self._failures += 1
            raise DataSourceUnavailable(source="yield_aggregator", reason=str(e)) from e

        # Resolve chain display names
        allowed_chains: set[str] | None = None
        if chains:
            allowed_chains = set()
            for c in chains:
                display = _CHAIN_TO_LLAMA_DISPLAY.get(c.lower())
                if display:
                    allowed_chains.add(display.lower())

        opportunities: list[YieldOpportunity] = []
        for pool in pools_data:
            opp = self._parse_pool(pool, token, allowed_chains, min_tvl)
            if opp is not None:
                opportunities.append(opp)

        # Sort
        if sort_by == "apy":
            opportunities.sort(key=lambda o: o.apy, reverse=True)
        elif sort_by == "tvl":
            opportunities.sort(key=lambda o: float(o.tvl_usd), reverse=True)
        elif sort_by == "risk_score":
            opportunities.sort(key=lambda o: o.risk_score or 0.0, reverse=False)

        self._successes += 1
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
        envelope = DataEnvelope(value=opportunities, meta=meta, classification=DataClassification.INFORMATIONAL)
        self._update_cache(cache_key, envelope)
        return envelope

    # -- Parsing ---------------------------------------------------------------

    def _parse_pool(
        self,
        pool: dict[str, Any],
        token: str,
        allowed_chains: set[str] | None,
        min_tvl: float,
    ) -> YieldOpportunity | None:
        """Parse a DeFi Llama pool dict into a YieldOpportunity or None."""
        pool_chain = str(pool.get("chain", "")).lower()

        # Chain filter
        if allowed_chains is not None and pool_chain not in allowed_chains:
            return None

        # Symbol match: token must appear in the pool symbol
        symbol = str(pool.get("symbol", "")).upper()
        symbol_tokens = {t.strip() for t in symbol.replace("-", "/").split("/")}
        if token not in symbol_tokens:
            return None

        # TVL filter
        tvl = _safe_decimal(pool.get("tvlUsd"))
        if float(tvl) < min_tvl:
            return None

        apy = _safe_float(pool.get("apy"))
        apy_base = pool.get("apyBase")
        apy_reward = pool.get("apyReward")
        project = str(pool.get("project", ""))
        pool_id = str(pool.get("pool", ""))
        il_risk = bool(pool.get("ilRisk", False))

        # Determine yield type
        yield_type = _PROJECT_TYPE.get(project.lower(), "lp" if il_risk else "lending")

        # Simple risk score heuristic
        risk_score = self._estimate_risk(tvl, apy, il_risk, project)

        return YieldOpportunity(
            protocol=project,
            chain=pool_chain,
            pool_id=pool_id,
            symbol=symbol,
            apy=apy,
            apy_base=_safe_float(apy_base) if apy_base is not None else None,
            apy_reward=_safe_float(apy_reward) if apy_reward is not None else None,
            tvl_usd=tvl,
            type=yield_type,
            risk_score=risk_score,
            il_risk=il_risk,
        )

    def _estimate_risk(
        self,
        tvl: Decimal,
        apy: float,
        il_risk: bool,
        project: str,
    ) -> float:
        """Estimate a simple risk score (0.0 safe - 1.0 risky).

        Heuristic based on:
            - TVL: higher TVL = lower risk
            - APY: extremely high APY = higher risk (unsustainable)
            - IL risk: LP positions carry impermanent loss
            - Project: well-known projects get a bonus
        """
        risk = 0.0

        # TVL risk: < $1M = 0.2, < $10M = 0.1, >= $10M = 0.0
        tvl_f = float(tvl)
        if tvl_f < 1_000_000:
            risk += 0.2
        elif tvl_f < 10_000_000:
            risk += 0.1

        # APY risk: > 100% is suspicious, > 50% mild warning
        if apy > 100:
            risk += 0.3
        elif apy > 50:
            risk += 0.15

        # IL risk
        if il_risk:
            risk += 0.15

        # Known-project bonus
        trusted = {"aave-v3", "compound-v3", "morpho-blue", "uniswap-v3", "lido"}
        if project.lower() in trusted:
            risk -= 0.1

        return max(0.0, min(1.0, round(risk, 2)))

    # -- HTTP ------------------------------------------------------------------

    async def _query_defillama_pools(self) -> list[dict[str, Any]]:
        """Fetch all pools from DeFi Llama yields API."""
        url = f"{_YIELDS_API}/pools"
        timeout = aiohttp.ClientTimeout(total=self._request_timeout)
        async with aiohttp.ClientSession(timeout=timeout, headers={"Accept": "application/json"}) as session:
            async with session.get(url) as response:
                if response.status != 200:
                    text = await response.text()
                    raise DataSourceUnavailable(
                        source="yield_aggregator",
                        reason=f"DeFi Llama yields HTTP {response.status}: {text[:200]}",
                    )
                data = await response.json()
                return data.get("data", [])

    # -- Helpers ---------------------------------------------------------------

    def _get_cached(self, key: str) -> DataEnvelope | None:
        entry = self._cache.get(key)
        if entry is None:
            return None
        value, cached_at = entry
        if time.monotonic() - cached_at > self._cache_ttl:
            return None
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

    def health(self) -> dict[str, int]:
        """Return health metrics."""
        return {"successes": self._successes, "failures": self._failures}


__all__ = [
    "YieldAggregator",
    "YieldOpportunity",
]
