"""Historical lending and funding rate data reader.

Provides historical rate snapshots for backtesting rate arbitrage and carry
strategies.

**VIB-4859 / W7**: This module is now a thin gRPC client of the gateway's
``RateHistoryService``. All HTTP / Web3 egress for lending APY + funding
rate history happens on the gateway side via
:class:`GatewayLendingRateHistoryCapability` and
:class:`GatewayFundingHistoryCapability` implementations on the
corresponding connectors. The strategy container holds no protocol-specific
dispatch and no outbound HTTP clients.

The :class:`RateHistoryReader` public API +
:class:`LendingRateSnapshot` / :class:`FundingRateSnapshot` dataclasses
are preserved verbatim. Cache integration with
:class:`VersionedDataCache` stays strategy-side (it's the backtest-replay
determinism layer per the plan §7.4 risk mitigation).

The DeFi Llama fallback chain that lived in the pre-W7 code is tracked
in VIB-4870 — when an additional subgraph / aggregator source is wired
into the gateway, it joins this dispatcher automatically through the
``GatewayLendingRateHistoryCapability`` registry.

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

import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from almanak.framework.data.cache.versioned_cache import VersionedDataCache
from almanak.framework.data.exceptions import DataUnavailableError
from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.framework.data.models import (
    DataClassification,
    DataEnvelope,
    DataMeta,
)

logger = logging.getLogger(__name__)


def _rate_history_get_connected_gateway_client() -> tuple[Any, Any]:
    """Return ``(client, gateway_pb2)`` with the client connected, or raise."""
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


def _call_get_funding_rate_history(
    client: Any,
    gateway_pb2: Any,
    *,
    venue: str,
    market: str,
    start_ts: int,
    end_ts: int,
) -> Any:
    """Issue one ``GetFundingRateHistory`` RPC and return the response.

    Wraps transport + ``success=False`` failures as ``DataSourceUnavailable``.
    """
    request = gateway_pb2.GetFundingRateHistoryRequest(
        venue=venue,
        market=market,
        chain="",
        start_ts=start_ts,
        end_ts=end_ts,
    )
    try:
        response = client.rate_history.GetFundingRateHistory(request)
    except Exception as exc:
        raise DataSourceUnavailable(
            source="gateway",
            reason=f"GetFundingRateHistory RPC failed: {exc}",
        ) from exc
    if not response.success:
        raise DataSourceUnavailable(
            source=response.source or "gateway",
            reason=response.error or "GetFundingRateHistory returned success=false",
        )
    return response


def _funding_points_to_snapshots(points: Any) -> list[FundingRateSnapshot]:
    """Convert proto funding points into ``FundingRateSnapshot`` rows."""
    snapshots: list[FundingRateSnapshot] = []
    for point in points:
        rate = _safe_decimal(point.rate_hourly or "0")
        annualized = _safe_decimal(point.rate_annualized or str(rate * Decimal(_HOURS_PER_YEAR)))
        snapshots.append(
            FundingRateSnapshot(
                rate=rate,
                annualized_rate=annualized,
                timestamp=datetime.fromtimestamp(point.timestamp, tz=UTC),
            )
        )
    return snapshots


def _build_cached_envelope(snapshots: list[Any], finality_status: str) -> Any:
    """Build the cache-hit ``DataEnvelope`` for a list of snapshots."""
    meta = DataMeta(
        source=f"cache({finality_status})",
        observed_at=datetime.now(UTC),
        finality="off_chain",
        staleness_ms=0,
        latency_ms=0,
        confidence=0.9 if finality_status == "finalized" else 0.7,
        cache_hit=True,
    )
    return DataEnvelope(
        value=snapshots,
        meta=meta,
        classification=DataClassification.INFORMATIONAL,
    )


def _build_fetched_envelope(snapshots: list[Any], source: str, latency_ms: int, now: datetime) -> Any:
    """Build the post-fetch ``DataEnvelope`` for a list of snapshots."""
    meta = DataMeta(
        source=source,
        observed_at=now,
        finality="off_chain",
        staleness_ms=0,
        latency_ms=latency_ms,
        confidence=0.85,
        cache_hit=False,
    )
    return DataEnvelope(
        value=snapshots,
        meta=meta,
        classification=DataClassification.INFORMATIONAL,
    )


def _snapshots_finality_status(snapshots: list[Any], now: datetime) -> str:
    """Return ``"finalized"`` if every snapshot is older than 24 h, else ``"provisional"``."""
    cutoff = now - timedelta(hours=24)
    all_finalized = all(s.timestamp < cutoff for s in snapshots)
    return "finalized" if all_finalized else "provisional"


def _build_lending_snapshot(supply_point: Any, borrow_by_ts: dict[int, str]) -> Any:
    """Build a single ``LendingRateSnapshot`` from a supply point + borrow timestamp map."""
    supply_apy = _safe_decimal(supply_point.supply_apy_pct or "0")
    borrow_apy = _safe_decimal(borrow_by_ts.get(supply_point.timestamp) or "0")
    utilization = _safe_decimal(supply_point.utilization_pct) if supply_point.utilization_pct else None
    return LendingRateSnapshot(
        supply_apy=supply_apy,
        borrow_apy=borrow_apy,
        utilization=utilization,
        timestamp=datetime.fromtimestamp(supply_point.timestamp, tz=UTC),
    )


def _merge_lending_snapshot_pairs(supply_resp: Any, borrow_resp: Any) -> list[Any]:
    """Merge supply + borrow ``GetLendingRateHistory`` responses into snapshots.

    Each snapshot carries both APY sides at the same timestamp, matching
    the pre-W7 ``LendingRateSnapshot`` shape. Borrow points without a
    timestamp-matching supply point are dropped (no half-pair records).
    """
    borrow_by_ts: dict[int, str] = {p.timestamp: p.borrow_apy_pct for p in borrow_resp.points}
    return [_build_lending_snapshot(point, borrow_by_ts) for point in supply_resp.points]


def _call_get_lending_rate_history_side(
    client: Any,
    gateway_pb2: Any,
    *,
    protocol: str,
    chain: str,
    token: str,
    side: str,
    start_ts: int,
    end_ts: int,
) -> Any:
    """Issue one ``GetLendingRateHistory`` RPC and return the response.

    Raises ``DataSourceUnavailable`` on transport failure or gateway-side
    ``success=False`` envelope.
    """
    request = gateway_pb2.GetLendingRateHistoryRequest(
        protocol=protocol,
        chain=chain,
        asset_symbol=token,
        side=side,
        start_ts=start_ts,
        end_ts=end_ts,
    )
    try:
        resp = client.rate_history.GetLendingRateHistory(request)
    except Exception as exc:
        raise DataSourceUnavailable(
            source="gateway",
            reason=f"GetLendingRateHistory ({side}) RPC failed: {exc}",
        ) from exc
    if not resp.success:
        raise DataSourceUnavailable(
            source=resp.source or "gateway",
            reason=resp.error or f"GetLendingRateHistory({side}) returned success=false",
        )
    return resp


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


# Hours per year for annualization (preserved for back-compat with
# pre-W7 callers that imported the constant).
_HOURS_PER_YEAR = 8760


def _safe_decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    """Best-effort Decimal coercion (preserved from pre-W7 helper)."""
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Serialization helpers (preserved for VersionedDataCache integration)
# ---------------------------------------------------------------------------


def _serialize_lending_snapshots(
    snapshots: list[LendingRateSnapshot],
) -> list[dict[str, Any]]:
    """Serialize lending snapshots for VersionedDataCache disk persistence."""
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
    """Deserialize lending snapshots from VersionedDataCache."""
    items = data if isinstance(data, list) else []
    snapshots: list[LendingRateSnapshot] = []
    for item in items:
        try:
            ts_str = item.get("timestamp")
            timestamp = datetime.fromisoformat(ts_str)
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=UTC)
            util_str = item.get("utilization")
            snapshots.append(
                LendingRateSnapshot(
                    supply_apy=_safe_decimal(item.get("supply_apy", "0")),
                    borrow_apy=_safe_decimal(item.get("borrow_apy", "0")),
                    utilization=_safe_decimal(util_str) if util_str is not None else None,
                    timestamp=timestamp,
                )
            )
        except (KeyError, ValueError, TypeError):
            continue
    return snapshots


def _serialize_funding_snapshots(
    snapshots: list[FundingRateSnapshot],
) -> list[dict[str, Any]]:
    """Serialize funding snapshots for VersionedDataCache disk persistence."""
    return [
        {
            "rate": str(s.rate),
            "annualized_rate": str(s.annualized_rate),
            "timestamp": s.timestamp.isoformat(),
        }
        for s in snapshots
    ]


def _deserialize_funding_snapshots(data: Any) -> list[FundingRateSnapshot]:
    """Deserialize funding snapshots from VersionedDataCache."""
    items = data if isinstance(data, list) else []
    snapshots: list[FundingRateSnapshot] = []
    for item in items:
        try:
            ts_str = item.get("timestamp")
            timestamp = datetime.fromisoformat(ts_str)
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=UTC)
            snapshots.append(
                FundingRateSnapshot(
                    rate=_safe_decimal(item.get("rate", "0")),
                    annualized_rate=_safe_decimal(item.get("annualized_rate", "0")),
                    timestamp=timestamp,
                )
            )
        except (KeyError, ValueError, TypeError):
            continue
    return snapshots


# ---------------------------------------------------------------------------
# Provider health metrics (preserved for back-compat with operators)
# ---------------------------------------------------------------------------


@dataclass
class _ProviderMetrics:
    """Mutable metrics for a provider (mirrors pre-W7 surface)."""

    requests: int = 0
    successes: int = 0
    failures: int = 0


# ---------------------------------------------------------------------------
# RateHistoryReader (thin gRPC client of RateHistoryService — VIB-4859 / W7)
# ---------------------------------------------------------------------------


class RateHistoryReader:
    """Reads historical lending and funding rate data through the gateway.

    All upstream egress (TheGraph subgraphs, DefiLlama aggregator,
    Hyperliquid Info API) lives gateway-side via
    :class:`GatewayLendingRateHistoryCapability` /
    :class:`GatewayFundingHistoryCapability` implementations on the
    corresponding connectors. The strategy container only speaks the
    ``RateHistoryService`` gRPC contract.

    Results are wrapped in :class:`DataEnvelope` with INFORMATIONAL
    classification and stored in :class:`VersionedDataCache` for
    deterministic backtest replay.

    Args:
        cache: Optional VersionedDataCache for disk persistence. Default
            creates a cache under ``~/.almanak/data_cache/rate_history/``.
        request_timeout: Ignored (kept for back-compat). RPC deadlines are
            controlled by the gateway client config.
        thegraph_api_key: Ignored (kept for back-compat). Egress lives
            gateway-side; the gateway is the one that talks to TheGraph.
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
        # Pre-W7 metrics surface preserved for operators that key on the
        # provider name. The new dispatcher only emits "gateway" and
        # "cache" categories, but the legacy keys exist as zeros so
        # downstream operators that iterate them don't crash.
        self._metrics: dict[str, _ProviderMetrics] = {
            "gateway": _ProviderMetrics(),
            "cache": _ProviderMetrics(),
            "thegraph": _ProviderMetrics(),
            "defillama": _ProviderMetrics(),
            "hyperliquid": _ProviderMetrics(),
            "gmx_v2": _ProviderMetrics(),
        }

    # -- Public API: Lending Rates ------------------------------------------

    def get_lending_rate_history(
        self,
        protocol: str,
        token: str,
        chain: str,
        days: int = 90,
    ) -> DataEnvelope[list[LendingRateSnapshot]]:
        """Fetch historical lending rate snapshots via the gateway.

        Args:
            protocol: Lending protocol (e.g. "aave_v3", "morpho_blue", "compound_v3").
            token: Token symbol (e.g. "USDC", "WETH").
            chain: Chain name (e.g. "arbitrum", "ethereum").
            days: Number of days of history to fetch. Default 90.

        Returns:
            DataEnvelope[list[LendingRateSnapshot]] with INFORMATIONAL classification.
            Snapshots are sorted ascending by timestamp.

        Raises:
            DataUnavailableError: If the gateway returns success=false and no cached data is available.
        """
        protocol_lower = protocol.lower()
        token_upper = token.upper()
        chain_lower = chain.lower()

        if days < 1:
            raise ValueError("days must be >= 1")

        end_date = datetime.now(UTC)
        start_date = end_date - timedelta(days=days)
        start_ts = int(start_date.timestamp())
        end_ts = int(end_date.timestamp())

        # Cache check (preserved for backtest determinism).
        cache_key = f"lending:{protocol_lower}:{token_upper}:{chain_lower}:{start_ts}:{end_ts}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            self._metrics["cache"].successes += 1
            snapshots = _deserialize_lending_snapshots(cached.data)
            logger.debug(
                "lending_rate_history_cache_hit protocol=%s token=%s chain=%s version=%s",
                protocol_lower,
                token_upper,
                chain_lower,
                cached.dataset_version,
            )
            return _build_cached_envelope(snapshots, cached.finality_status)

        # Gateway fetch.
        start_time = time.monotonic()
        try:
            snapshots = self._fetch_lending_via_gateway(protocol_lower, token_upper, chain_lower, start_ts, end_ts)
        except DataSourceUnavailable as exc:
            self._metrics["gateway"].failures += 1
            raise DataUnavailableError(
                data_type="lending_rate_history",
                instrument=f"{protocol_lower}/{token_upper}",
                reason=str(exc),
            ) from exc

        latency_ms = int((time.monotonic() - start_time) * 1000)
        now = datetime.now(UTC)
        snapshots.sort(key=lambda s: s.timestamp)
        finality_status = _snapshots_finality_status(snapshots, now)
        serialized = _serialize_lending_snapshots(snapshots)
        self._cache.put(cache_key, serialized, finality_status=finality_status)
        logger.info(
            "lending_rate_history_fetched source=gateway protocol=%s token=%s chain=%s snapshots=%d latency_ms=%d",
            protocol_lower,
            token_upper,
            chain_lower,
            len(snapshots),
            latency_ms,
        )
        return _build_fetched_envelope(snapshots, "gateway", latency_ms, now)

    # -- Public API: Funding Rates ------------------------------------------

    def get_funding_rate_history(
        self,
        venue: str,
        market_symbol: str,
        hours: int = 168,
    ) -> DataEnvelope[list[FundingRateSnapshot]]:
        """Fetch historical funding rate snapshots via the gateway.

        Args:
            venue: Perps venue (e.g. "hyperliquid", "gmx_v2").
            market_symbol: Market symbol (e.g. "ETH-USD", "BTC-USD").
            hours: Number of hours of history to fetch. Default 168 (7 days).

        Returns:
            DataEnvelope[list[FundingRateSnapshot]] with INFORMATIONAL classification.
            Snapshots are sorted ascending by timestamp.

        Raises:
            DataUnavailableError: If the gateway returns success=false and no cached data is available.
        """
        venue_lower = venue.lower()
        market_upper = market_symbol.upper()

        if hours < 1:
            raise ValueError("hours must be >= 1")

        end_date = datetime.now(UTC)
        start_date = end_date - timedelta(hours=hours)
        start_ts = int(start_date.timestamp())
        end_ts = int(end_date.timestamp())

        cache_key = f"funding:{venue_lower}:{market_upper}:{start_ts}:{end_ts}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            self._metrics["cache"].successes += 1
            snapshots = _deserialize_funding_snapshots(cached.data)
            logger.debug(
                "funding_rate_history_cache_hit venue=%s market=%s version=%s",
                venue_lower,
                market_upper,
                cached.dataset_version,
            )
            return _build_cached_envelope(snapshots, cached.finality_status)

        start_time = time.monotonic()
        try:
            snapshots = self._fetch_funding_via_gateway(venue_lower, market_upper, start_ts, end_ts)
        except DataSourceUnavailable as exc:
            self._metrics["gateway"].failures += 1
            raise DataUnavailableError(
                data_type="funding_rate_history",
                instrument=f"{venue_lower}/{market_upper}",
                reason=str(exc),
            ) from exc

        latency_ms = int((time.monotonic() - start_time) * 1000)
        now = datetime.now(UTC)
        snapshots.sort(key=lambda s: s.timestamp)
        finality_status = _snapshots_finality_status(snapshots, now)
        serialized = _serialize_funding_snapshots(snapshots)
        self._cache.put(cache_key, serialized, finality_status=finality_status)
        logger.info(
            "funding_rate_history_fetched source=gateway venue=%s market=%s snapshots=%d latency_ms=%d",
            venue_lower,
            market_upper,
            len(snapshots),
            latency_ms,
        )
        return _build_fetched_envelope(snapshots, "gateway", latency_ms, now)

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

    # -- Gateway adapter (thin gRPC client) ---------------------------------

    def _fetch_lending_via_gateway(
        self,
        protocol: str,
        token: str,
        chain: str,
        start_ts: int,
        end_ts: int,
    ) -> list[LendingRateSnapshot]:
        """Translate the ``GetLendingRateHistory`` RPC into ``LendingRateSnapshot``s.

        Issues supply + borrow lookups in sequence (per the pre-W7 shape)
        and merges the points by timestamp.
        """
        self._metrics["gateway"].requests += 1

        client, gateway_pb2 = _rate_history_get_connected_gateway_client()
        supply_resp = _call_get_lending_rate_history_side(
            client,
            gateway_pb2,
            protocol=protocol,
            chain=chain,
            token=token,
            side="supply",
            start_ts=start_ts,
            end_ts=end_ts,
        )
        borrow_resp = _call_get_lending_rate_history_side(
            client,
            gateway_pb2,
            protocol=protocol,
            chain=chain,
            token=token,
            side="borrow",
            start_ts=start_ts,
            end_ts=end_ts,
        )

        snapshots = _merge_lending_snapshot_pairs(supply_resp, borrow_resp)
        self._metrics["gateway"].successes += 1
        return snapshots

    def _fetch_funding_via_gateway(
        self,
        venue: str,
        market: str,
        start_ts: int,
        end_ts: int,
    ) -> list[FundingRateSnapshot]:
        """Translate the ``GetFundingRateHistory`` RPC into ``FundingRateSnapshot``s."""
        self._metrics["gateway"].requests += 1

        client, gateway_pb2 = _rate_history_get_connected_gateway_client()
        response = _call_get_funding_rate_history(
            client,
            gateway_pb2,
            venue=venue,
            market=market,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        snapshots = _funding_points_to_snapshots(response.points)

        self._metrics["gateway"].successes += 1
        return snapshots


__all__ = [
    "FundingRateSnapshot",
    "LendingRateSnapshot",
    "RateHistoryReader",
    "_deserialize_funding_snapshots",
    "_deserialize_lending_snapshots",
    "_safe_decimal",
    "_serialize_funding_snapshots",
    "_serialize_lending_snapshots",
]
