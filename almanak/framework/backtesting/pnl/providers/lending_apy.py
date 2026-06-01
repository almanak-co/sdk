"""Lending APY data provider for historical interest rates.

This module provides a client for fetching historical supply and borrow APY
data for lending markets. Used by the backtesting interest-accrual
calculators (``almanak/framework/backtesting/pnl/calculators/interest.py``)
to compute realistic interest charges across a backtest window.

**VIB-4859 / W7**: This module is now a thin gRPC client of the gateway's
``RateHistoryService.GetLendingRateHistory``. All HTTP / subgraph egress
has moved into the gateway sidecar via
:class:`GatewayLendingRateHistoryCapability` implementations on the
corresponding connectors. The strategy container holds no protocol-specific
subgraph URL tables, no rate-limiter state, and no aiohttp session.

The :class:`LendingAPYProvider` public API +
:class:`LendingAPYData` dataclass are preserved verbatim for back-compat;
internals are a thin gRPC dispatch. The default-rate fallback layer is
kept strategy-side as a deterministic offline-backtest convenience (used
when the gateway is unreachable). The per-DEX sub-package providers
(``pnl/providers/lending/{aave_v3_apy,compound_v3_apy,morpho_apy,spark_apy}.py``)
continue to expose the historical-APY surface for callers that needed
per-DEX configuration; they are now thin wrappers around the same gRPC
service (see ``pnl/providers/lending/__init__.py``).

Example:
    from almanak.framework.backtesting.pnl.providers.lending_apy import (
        LendingAPYProvider,
        LendingAPYData,
    )
    from datetime import datetime, timezone

    provider = LendingAPYProvider()

    # Get historical APY for Aave V3 USDC
    apy = await provider.get_historical_apy(
        protocol="aave_v3",
        market="USDC",
        timestamp=datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc),
    )
    print(f"Supply APY: {apy.supply_apy_pct}%, Borrow APY: {apy.borrow_apy_pct}%")
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.data.interfaces import DataSourceUnavailable

logger = logging.getLogger(__name__)


def _get_connected_gateway_client() -> tuple[Any, Any]:
    """Return ``(client, gateway_pb2)`` with the client connected, or raise.

    Bundles the import + connect dance so each gateway-backed fetcher
    can just call this once.
    """
    try:
        from almanak.framework.gateway_client import get_gateway_client
        from almanak.gateway.proto import gateway_pb2
    except ImportError as exc:
        raise DataSourceUnavailable(
            source="gateway",
            reason=f"Gateway client unavailable: {exc}",
        ) from exc

    client = get_gateway_client()
    if not client.is_connected:
        try:
            client.connect()
        except Exception as exc:
            raise DataSourceUnavailable(
                source="gateway",
                reason=f"Gateway connect failed: {exc}",
            ) from exc
    return client, gateway_pb2


def _fetch_lending_rate_side(
    client: Any,
    gateway_pb2: Any,
    *,
    protocol: str,
    chain: str,
    market: str,
    side: str,
) -> Any:
    """Issue a single ``GetLendingRateCurrent`` RPC and return the response.

    Wraps both transport failures and gateway-side ``success=False``
    envelopes in ``DataSourceUnavailable``.
    """
    req = gateway_pb2.GetLendingRateCurrentRequest(
        protocol=protocol,
        chain=chain,
        asset_symbol=market.upper(),
        side=side,
    )
    try:
        resp = client.rate_history.GetLendingRateCurrent(req)
    except Exception as exc:
        raise DataSourceUnavailable(
            source="gateway",
            reason=f"GetLendingRateCurrent ({side}) RPC failed: {exc}",
        ) from exc
    if not resp.success:
        raise DataSourceUnavailable(
            source=resp.source or "gateway",
            reason=resp.error or f"{side} rate unavailable",
        )
    return resp


# =============================================================================
# Constants (preserved API surface)
# =============================================================================

# Supported protocols
SUPPORTED_PROTOCOLS = ["aave_v3", "compound_v3"]

# Default cache TTL: 1 hour for historical data
DEFAULT_CACHE_TTL_SECONDS = 3600

# Rate limit settings (legacy — preserved for back-compat with callers
# that pass them to LendingAPYProvider; rate limiting now lives on the
# gateway side and is ignored here).
DEFAULT_REQUESTS_PER_MINUTE = 30
DEFAULT_REQUEST_TIMEOUT_SECONDS = 30


# Default APYs per protocol (as decimal, 0.03 = 3%) — used when the
# gateway is unreachable so offline backtests don't crash.
DEFAULT_SUPPLY_APYS: dict[str, Decimal] = {
    "aave_v3": Decimal("0.03"),  # 3% supply
    "compound_v3": Decimal("0.025"),  # 2.5% supply
}

DEFAULT_BORROW_APYS: dict[str, Decimal] = {
    "aave_v3": Decimal("0.05"),  # 5% borrow
    "compound_v3": Decimal("0.045"),  # 4.5% borrow
}


# Legacy address tables — preserved for back-compat with strategy code
# that imported the dicts directly (e.g. for ad-hoc subgraph queries).
# The gateway-side capability bodies own their own asset → address
# resolution via ``GatewayAddressCapability`` so these tables are no
# longer load-bearing for the W7 dispatch.
AAVE_V3_MARKETS: dict[str, dict[str, str]] = {
    "ethereum": {
        "USDC": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        "USDT": "0xdac17f958d2ee523a2206206994597c13d831ec7",
        "DAI": "0x6b175474e89094c44da98b954eedeac495271d0f",
        "WETH": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
        "WBTC": "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
        "LINK": "0x514910771af9ca656af840dff83e8264ecf986ca",
    },
    "arbitrum": {
        "USDC": "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
        "USDC.e": "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8",
    },
    "polygon": {},
    "base": {},
    "optimism": {},
    "avalanche": {},
}

COMPOUND_V3_MARKETS: dict[str, dict[str, str]] = {
    "ethereum": {
        "USDC": "0xc3d688b66703497daa19211eedff47f25384cdc3",
        "WETH": "0xa17581a9e3356d9a858b789d68b4d866e593ae94",
    },
    "arbitrum": {
        "USDC": "0xa5edbdd9646f8dff606d7448e414884c7d905dca",
        "USDC.e": "0x9c4ec768c28520b50860ea7a15bd7213a9ff58bf",
    },
    "polygon": {"USDC": "0xf25212e676d1f7f89cd72ffee66158f541246445"},
    "base": {
        "USDC": "0xb125e6687d4313864e53df431d5425969c15eb2f",
        "WETH": "0x46e6b214b524310239732d51387075e0e70970bf",
    },
}

# Empty legacy subgraph-URL dicts — preserved so callers that imported
# them by name don't break at import. The gateway-side capability owns
# the real URLs via ``GatewaySubgraphCapability``.
AAVE_V3_SUBGRAPHS: dict[str, str] = {}
COMPOUND_V3_SUBGRAPHS: dict[str, str] = {}


# =============================================================================
# Exceptions
# =============================================================================


class LendingAPYError(Exception):
    """Base exception for lending APY provider errors."""


class LendingAPYNotFoundError(LendingAPYError):
    """Raised when APY data is not found for a market.

    .. deprecated:: VIB-4859 (W7)
        Prefer raising / catching
        :class:`almanak.framework.data.interfaces.DataSourceUnavailable`
        for new code. This exception remains for back-compat.
    """

    def __init__(self, protocol: str, market: str, timestamp: datetime) -> None:
        self.protocol = protocol
        self.market = market
        self.timestamp = timestamp
        super().__init__(f"Lending APY not found for {protocol} {market} at {timestamp.isoformat()}")


class LendingAPYRateLimitError(LendingAPYError):
    """Raised when API rate limit is exceeded.

    .. deprecated:: VIB-4859 (W7)
        Rate limiting moved to the gateway side. This exception is kept
        for back-compat but is no longer raised by the framework client.
    """

    def __init__(self, retry_after_seconds: float | None = None) -> None:
        self.retry_after_seconds = retry_after_seconds
        msg = "Lending APY API rate limit exceeded"
        if retry_after_seconds:
            msg += f", retry after {retry_after_seconds}s"
        super().__init__(msg)


class UnsupportedProtocolError(LendingAPYError):
    """Raised when protocol is not supported."""

    def __init__(self, protocol: str) -> None:
        self.protocol = protocol
        super().__init__(f"Unsupported protocol: {protocol}. Supported: {SUPPORTED_PROTOCOLS}")


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class LendingAPYData:
    """APY data for a lending market at a specific time.

    Attributes:
        protocol: The lending protocol (aave_v3, compound_v3)
        market: The market/asset identifier (e.g., "USDC", "WETH")
        timestamp: The timestamp this rate applies to
        supply_apy: The supply APY as a decimal (0.03 = 3%)
        borrow_apy: The borrow APY as a decimal (0.05 = 5%)
        supply_apy_pct: Supply APY as percentage (3.0 = 3%)
        borrow_apy_pct: Borrow APY as percentage (5.0 = 5%)
        utilization_rate: Market utilization (0-1, if available)
        total_supply_usd: Total supplied in USD (if available)
        total_borrow_usd: Total borrowed in USD (if available)
        source: Data source (subgraph, api, fallback)
    """

    protocol: str
    market: str
    timestamp: datetime
    supply_apy: Decimal
    borrow_apy: Decimal
    supply_apy_pct: Decimal = Decimal("0")
    borrow_apy_pct: Decimal = Decimal("0")
    utilization_rate: Decimal | None = None
    total_supply_usd: Decimal | None = None
    total_borrow_usd: Decimal | None = None
    source: str = "subgraph"

    def __post_init__(self) -> None:
        """Calculate percentage APYs if not provided."""
        if self.supply_apy_pct == Decimal("0") and self.supply_apy != Decimal("0"):
            self.supply_apy_pct = self.supply_apy * Decimal("100")
        if self.borrow_apy_pct == Decimal("0") and self.borrow_apy != Decimal("0"):
            self.borrow_apy_pct = self.borrow_apy * Decimal("100")

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "protocol": self.protocol,
            "market": self.market,
            "timestamp": self.timestamp.isoformat(),
            "supply_apy": str(self.supply_apy),
            "borrow_apy": str(self.borrow_apy),
            "supply_apy_pct": str(self.supply_apy_pct),
            "borrow_apy_pct": str(self.borrow_apy_pct),
            "utilization_rate": str(self.utilization_rate) if self.utilization_rate else None,
            "total_supply_usd": str(self.total_supply_usd) if self.total_supply_usd else None,
            "total_borrow_usd": str(self.total_borrow_usd) if self.total_borrow_usd else None,
            "source": self.source,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> LendingAPYData:
        """Deserialize from dictionary."""
        return cls(
            protocol=data["protocol"],
            market=data["market"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            supply_apy=Decimal(data["supply_apy"]),
            borrow_apy=Decimal(data["borrow_apy"]),
            supply_apy_pct=Decimal(data.get("supply_apy_pct", "0")),
            borrow_apy_pct=Decimal(data.get("borrow_apy_pct", "0")),
            utilization_rate=Decimal(data["utilization_rate"]) if data.get("utilization_rate") else None,
            total_supply_usd=Decimal(data["total_supply_usd"]) if data.get("total_supply_usd") else None,
            total_borrow_usd=Decimal(data["total_borrow_usd"]) if data.get("total_borrow_usd") else None,
            source=data.get("source", "subgraph"),
        )


@dataclass
class CachedLendingAPY:
    """Cached lending APY data with expiration."""

    data: LendingAPYData
    fetched_at: float
    ttl_seconds: float

    @property
    def is_expired(self) -> bool:
        """Check if the cached data has expired."""
        return time.time() - self.fetched_at > self.ttl_seconds


@dataclass
class RateLimitState:
    """Tracks rate limit state for exponential backoff.

    .. deprecated:: VIB-4859 (W7)
        Rate limiting moved to the gateway side. This class is preserved
        for back-compat with callers that constructed one directly; the
        new framework client never uses it.
    """

    last_limit_time: float | None = None
    backoff_seconds: float = 1.0
    consecutive_limits: int = 0
    requests_this_minute: int = 0
    minute_start: float = field(default_factory=time.time)

    def record_rate_limit(self) -> None:
        self.last_limit_time = time.time()
        self.consecutive_limits += 1
        self.backoff_seconds = min(32.0, 2 ** (self.consecutive_limits - 1))

    def record_success(self) -> None:
        self.consecutive_limits = 0
        self.backoff_seconds = 1.0

    def get_wait_time(self) -> float:
        if self.last_limit_time is None:
            return 0.0
        elapsed = time.time() - self.last_limit_time
        return max(0.0, self.backoff_seconds - elapsed)

    def record_request(self) -> None:
        current_time = time.time()
        if current_time - self.minute_start >= 60:
            self.minute_start = current_time
            self.requests_this_minute = 0
        self.requests_this_minute += 1


# =============================================================================
# Lending APY Provider (thin gRPC client — VIB-4859 / W7)
# =============================================================================


class LendingAPYProvider:
    """Provider for fetching historical lending APY through the gateway.

    All upstream egress (TheGraph subgraphs, DefiLlama aggregator) lives
    gateway-side via :class:`GatewayLendingRateHistoryCapability`
    implementations on the corresponding connectors. The strategy
    container only speaks gRPC.

    The public API (``get_historical_apy``, ``get_current_apy``,
    ``get_default_supply_apy``, ``get_default_borrow_apy``) and the
    :class:`LendingAPYData` dataclass shape are preserved verbatim for
    back-compat. When the gateway is unreachable, the provider falls
    back to per-protocol default rates so offline backtests don't crash.

    Args:
        chain: Blockchain for subgraph queries (ethereum, arbitrum, etc.)
        api_key: Ignored (kept for back-compat). Egress lives gateway-side
            now; the gateway holds the TheGraph API key.
        cache_ttl_seconds: Cache TTL in seconds (default 3600 = 1 hour)
        request_timeout: Ignored (kept for back-compat). RPC deadlines are
            controlled by the gateway client config.
        requests_per_minute: Ignored (kept for back-compat). Rate limiting
            lives gateway-side.
    """

    def __init__(
        self,
        chain: str = "ethereum",
        api_key: str | None = None,
        cache_ttl_seconds: float = DEFAULT_CACHE_TTL_SECONDS,
        request_timeout: float = DEFAULT_REQUEST_TIMEOUT_SECONDS,
        requests_per_minute: int = DEFAULT_REQUESTS_PER_MINUTE,
    ) -> None:
        chain_lower = chain.lower()
        # Preserve pre-W7 chain validation surface (tests in
        # tests/unit/backtesting/pnl/test_lending_apy_provider.py rely on
        # the ValueError). Supported chains are the union of the
        # back-compat market tables; the gateway is the authority at
        # request time.
        supported_chains = set(AAVE_V3_MARKETS.keys()) | set(COMPOUND_V3_MARKETS.keys())
        if chain_lower not in supported_chains:
            raise ValueError(f"Unsupported chain: {chain}. Supported: {sorted(supported_chains)}")

        self._chain = chain_lower
        self._api_key = api_key
        self._cache_ttl_seconds = cache_ttl_seconds
        self._request_timeout = request_timeout
        self._requests_per_minute = requests_per_minute

        # Cache: (protocol, market, timestamp_hour) -> CachedLendingAPY
        self._cache: dict[tuple[str, str, datetime], CachedLendingAPY] = {}

        # Rate-limit state preserved for back-compat (no longer used by
        # the framework client; the gateway side owns rate limits now).
        self._rate_limit_states: dict[str, RateLimitState] = {
            "aave_v3": RateLimitState(),
            "compound_v3": RateLimitState(),
        }

    @property
    def chain(self) -> str:
        """Get the chain this provider queries."""
        return self._chain

    @property
    def provider_name(self) -> str:
        """Get the provider name."""
        return f"lending_apy_{self._chain}"

    async def close(self) -> None:
        """No-op shutdown hook (preserved for context-manager callers)."""

    async def __aenter__(self) -> LendingAPYProvider:
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        await self.close()

    def _normalize_timestamp(self, timestamp: datetime) -> datetime:
        """Normalize timestamp to hourly boundary for caching."""
        return timestamp.replace(minute=0, second=0, microsecond=0)

    def _get_cache_key(self, protocol: str, market: str, timestamp: datetime) -> tuple[str, str, datetime]:
        return (protocol.lower(), market.upper(), self._normalize_timestamp(timestamp))

    def _get_from_cache(self, protocol: str, market: str, timestamp: datetime) -> LendingAPYData | None:
        key = self._get_cache_key(protocol, market, timestamp)
        cached = self._cache.get(key)
        if cached is None:
            return None
        if cached.is_expired:
            del self._cache[key]
            return None
        logger.debug(f"Cache hit for {protocol} {market} at {timestamp.isoformat()}")
        return cached.data

    def _add_to_cache(self, data: LendingAPYData) -> None:
        key = self._get_cache_key(data.protocol, data.market, data.timestamp)
        self._cache[key] = CachedLendingAPY(
            data=data,
            fetched_at=time.time(),
            ttl_seconds=self._cache_ttl_seconds,
        )

    async def get_historical_apy(
        self,
        protocol: str,
        market: str,
        timestamp: datetime,
    ) -> LendingAPYData:
        """Get historical APY for a market at a specific timestamp.

        Args:
            protocol: The lending protocol (aave_v3, compound_v3)
            market: The market identifier (e.g., "USDC", "WETH")
            timestamp: The timestamp to query APY for

        Returns:
            LendingAPYData with supply and borrow APY information

        Raises:
            UnsupportedProtocolError: If protocol is not supported
        """
        protocol_lower = protocol.lower()

        if protocol_lower not in SUPPORTED_PROTOCOLS:
            raise UnsupportedProtocolError(protocol)

        cached = self._get_from_cache(protocol_lower, market, timestamp)
        if cached is not None:
            return cached

        try:
            data = await self._fetch_apy_via_gateway(protocol_lower, market, timestamp)
        except DataSourceUnavailable as exc:
            logger.warning(
                "Gateway lending-APY lookup unavailable for %s/%s on %s: %s; using default.",
                protocol_lower,
                market,
                self._chain,
                exc,
            )
            data = self._get_default_apy(protocol_lower, market, timestamp)

        self._add_to_cache(data)

        logger.info(
            "Fetched APY for %s %s: supply=%.4f%%, borrow=%.4f%% (provider: %s)",
            protocol_lower,
            market,
            float(data.supply_apy_pct),
            float(data.borrow_apy_pct),
            self.provider_name,
        )
        return data

    async def get_current_apy(
        self,
        protocol: str,
        market: str,
    ) -> LendingAPYData:
        """Get current APY for a market.

        Convenience method that queries the current timestamp.
        """
        return await self.get_historical_apy(
            protocol=protocol,
            market=market,
            timestamp=datetime.now(UTC),
        )

    async def _fetch_apy_via_gateway(
        self,
        protocol: str,
        market: str,
        timestamp: datetime,
    ) -> LendingAPYData:
        """Fetch APY through ``RateHistoryService.GetLendingRateCurrent``.

        Single-point lookup: pulls the *current* live rate from the
        gateway and stamps the request timestamp on the returned data.
        Historical-window queries (lending-rate series across a backtest
        window) lands once the gateway-side capability gains historical
        coverage (tracked in VIB-4870).
        """
        client, gateway_pb2 = _get_connected_gateway_client()

        # Issue supply + borrow lookups in sequence (two cheap RPC calls).
        supply_resp = _fetch_lending_rate_side(
            client,
            gateway_pb2,
            protocol=protocol,
            chain=self._chain,
            market=market,
            side="supply",
        )
        borrow_resp = _fetch_lending_rate_side(
            client,
            gateway_pb2,
            protocol=protocol,
            chain=self._chain,
            market=market,
            side="borrow",
        )

        supply_pct = Decimal(supply_resp.point.supply_apy_pct or "0")
        borrow_pct = Decimal(borrow_resp.point.borrow_apy_pct or "0")
        utilization = (
            Decimal(supply_resp.point.utilization_pct) / Decimal("100") if supply_resp.point.utilization_pct else None
        )

        return LendingAPYData(
            protocol=protocol,
            market=market.upper(),
            timestamp=self._normalize_timestamp(timestamp),
            supply_apy=supply_pct / Decimal("100"),
            borrow_apy=borrow_pct / Decimal("100"),
            supply_apy_pct=supply_pct,
            borrow_apy_pct=borrow_pct,
            utilization_rate=utilization,
            source="gateway",
        )

    def _get_default_apy(
        self,
        protocol: str,
        market: str,
        timestamp: datetime,
    ) -> LendingAPYData:
        """Return default APY when the gateway is unreachable."""
        supply_apy = DEFAULT_SUPPLY_APYS.get(protocol, Decimal("0.03"))
        borrow_apy = DEFAULT_BORROW_APYS.get(protocol, Decimal("0.05"))
        return LendingAPYData(
            protocol=protocol,
            market=market.upper(),
            timestamp=self._normalize_timestamp(timestamp),
            supply_apy=supply_apy,
            borrow_apy=borrow_apy,
            source="fallback",
        )

    def get_default_supply_apy(self, protocol: str) -> Decimal:
        """Get the default supply APY for a protocol."""
        return DEFAULT_SUPPLY_APYS.get(protocol.lower(), Decimal("0.03"))

    def get_default_borrow_apy(self, protocol: str) -> Decimal:
        """Get the default borrow APY for a protocol."""
        return DEFAULT_BORROW_APYS.get(protocol.lower(), Decimal("0.05"))

    def clear_cache(self) -> None:
        """Clear all cached APY data."""
        self._cache.clear()

    def get_cache_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        total = len(self._cache)
        expired = sum(1 for c in self._cache.values() if c.is_expired)
        return {
            "total_entries": total,
            "expired_entries": expired,
            "valid_entries": total - expired,
        }

    def to_dict(self) -> dict[str, Any]:
        """Serialize provider config to dictionary."""
        return {
            "provider_name": self.provider_name,
            "chain": self._chain,
            "cache_ttl_seconds": self._cache_ttl_seconds,
            "request_timeout": self._request_timeout,
            "requests_per_minute": self._requests_per_minute,
            "supported_protocols": SUPPORTED_PROTOCOLS,
        }


__all__ = [
    "AAVE_V3_MARKETS",
    "AAVE_V3_SUBGRAPHS",
    "COMPOUND_V3_MARKETS",
    "COMPOUND_V3_SUBGRAPHS",
    "CachedLendingAPY",
    "DEFAULT_BORROW_APYS",
    "DEFAULT_SUPPLY_APYS",
    "LendingAPYData",
    "LendingAPYError",
    "LendingAPYNotFoundError",
    "LendingAPYProvider",
    "LendingAPYRateLimitError",
    "RateLimitState",
    "SUPPORTED_PROTOCOLS",
    "UnsupportedProtocolError",
]
