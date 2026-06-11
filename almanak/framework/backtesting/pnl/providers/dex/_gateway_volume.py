"""Shared gateway-backed DEX-volume fetch for the per-DEX consumers.

**VIB-4870 / W7**: the per-DEX volume providers under this package are
now thin gRPC clients of the gateway's ``RateHistoryService.GetDexVolumeHistory``
RPC. The TheGraph subgraph HTTP egress they used to do (in the old
shared subgraph client) has moved into the gateway sidecar — every DEX's
``GatewayDexVolumeCapability.fetch_volume_history`` delegates to the
shared gateway-side egress helper
``almanak/gateway/services/_dex_volume_subgraph.py``. The strategy
container holds no subgraph URLs, no API key, and opens no socket.

This module centralises the consumer-side boilerplate so each per-DEX
provider's ``get_volume`` stays a few lines: build the request from the
``(dex, chain, pool_address, date-range)`` tuple, issue the RPC, and map
the wire envelope back to the framework's
:class:`~almanak.framework.backtesting.pnl.types.VolumeResult` list with
the provider's own ``DATA_SOURCE`` + ``HIGH`` confidence stamped on each
point.

**Byte-equivalence (W7 §6).** Volume values feed backtest PnL, so the
mapping below is byte-equivalent to the pre-W7 per-provider
``_parse_volume_data``: the gateway returns the daily volume decoded via
``Decimal(str(...))`` (same as the old parse) and a unix-seconds
``timestamp`` already normalised to midnight UTC (the gateway converts
Curve's Messari day-numbers back to seconds), so
``datetime.fromtimestamp(ts, UTC)`` reproduces the old per-provider
timestamp construction exactly. The provider stamps its own
``DATA_SOURCE`` (e.g. ``"uniswap_v3_subgraph"``) rather than the
response's top-level ``source`` (``"the_graph"``) to preserve the
pre-W7 provenance string the consumers / fixtures asserted.

**No silent zeros (VIB-4859 decision 4).** An empty / errored / rate-
limited subgraph surfaces as ``success=False`` from the gateway and is
raised here as :class:`DataSourceUnavailable` — the pre-W7 silent
``Decimal("0")`` LOW-confidence fallback row is intentionally gone.

Strategy code MUST NOT import the gateway-side helper; it reaches the
egress only through the gRPC client wired here.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

from almanak.core.enums import Chain
from almanak.framework.data.interfaces import DataSourceUnavailable

from ...types import DataConfidence, DataSourceInfo, VolumeResult

# Daily granularity is the only resolution the gateway capability serves
# (the underlying subgraphs are ``*DayDatas`` / day snapshots). The
# dispatcher rejects any other ``interval_secs`` with ``success=False``.
_SECONDS_PER_DAY = 86400


def _get_connected_gateway_client() -> tuple[Any, Any]:
    """Return ``(client, gateway_pb2)`` with the client connected, or raise.

    Mirrors the connect dance in the sibling W7 consumers
    (``twap.py`` / ``lending_apy.py``) so every gateway-backed fetcher
    shares one import + connect path.
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


def _date_to_window(start_date: date, end_date: date) -> tuple[int, int]:
    """Translate an inclusive day range into the gateway request window.

    The gateway filters the subgraph on ``time_field`` in ``[start_ts,
    end_ts]`` (inclusive on both ends, in the field's own unit) and
    requires ``start_ts < end_ts``. To reproduce the pre-W7 per-provider
    filter — ``date_gte: midnight(start)`` / ``date_lte: midnight(end)``
    (and Curve's day-number equivalent) — without tripping the strict
    ``<`` validator on a single-day query, we send:

    * ``start_ts`` = midnight(start_date) UTC, and
    * ``end_ts``   = midnight(end_date) + 86399 (23:59:59 of end_date).

    A daily row's timestamp is exactly midnight UTC, so
    ``end_ts = midnight(end_date) + 86399`` includes ``end_date`` and
    excludes ``end_date + 1`` (``< midnight(end_date+1)``), matching the
    old ``<= midnight(end_date)`` filter. For Curve the gateway floors
    ``end_ts // 86400`` back to ``day(end_date)``. ``start_ts < end_ts``
    holds for every range, including ``start_date == end_date``.
    """
    start_ts = int(datetime.combine(start_date, datetime.min.time(), tzinfo=UTC).timestamp())
    end_ts = int(datetime.combine(end_date, datetime.min.time(), tzinfo=UTC).timestamp()) + _SECONDS_PER_DAY - 1
    return start_ts, end_ts


async def fetch_volume_via_gateway(
    *,
    dex: str,
    chain: Chain,
    pool_address: str,
    start_date: date,
    end_date: date,
    data_source: str,
) -> list[VolumeResult]:
    """Fetch daily DEX volume over the gateway and map to ``VolumeResult``.

    Args:
        dex: The gateway ``dex_name`` routing key (e.g. ``"uniswap_v3"``,
            ``"balancer_v2"``) the connector's
            :class:`GatewayDexVolumeCapability` registered under.
        chain: The chain enum; lowercased to the gateway chain key.
        pool_address: Pool / pair / LB-pair address (lowercased server-side).
        start_date: Inclusive start of the day range.
        end_date: Inclusive end of the day range.
        data_source: The provider's ``DATA_SOURCE`` provenance string,
            stamped on each returned :class:`VolumeResult` (preserves the
            pre-W7 source label for byte-equivalence).

    Returns:
        One :class:`VolumeResult` per daily point the gateway returned,
        each with ``confidence=HIGH`` and ``timestamp`` at midnight UTC.

    Raises:
        DataSourceUnavailable: gateway unreachable, RPC failed, or the
            subgraph returned no / errored data (``success=False``). No
            silent ``Decimal("0")`` LOW-confidence fallback row — that
            pre-W7 behaviour is intentionally removed.
    """
    client, gateway_pb2 = _get_connected_gateway_client()

    start_ts, end_ts = _date_to_window(start_date, end_date)
    request = gateway_pb2.GetDexVolumeHistoryRequest(
        dex=dex,
        chain=chain.value.lower(),
        pool_address=pool_address,
        start_ts=start_ts,
        end_ts=end_ts,
        interval_secs=_SECONDS_PER_DAY,
    )
    try:
        response = client.rate_history.GetDexVolumeHistory(request)
    except Exception as exc:
        raise DataSourceUnavailable(
            source="gateway",
            reason=f"GetDexVolumeHistory RPC failed: {exc}",
        ) from exc

    if not response.success:
        raise DataSourceUnavailable(
            source=response.source or "gateway",
            reason=response.error or "GetDexVolumeHistory returned success=false",
        )

    return [
        VolumeResult(
            value=Decimal(point.volume_usd),
            source_info=DataSourceInfo(
                source=data_source,
                confidence=DataConfidence.HIGH,
                timestamp=datetime.fromtimestamp(point.timestamp, tz=UTC),
            ),
        )
        for point in response.points
    ]


__all__ = ["GatewayDexVolumeProvider", "fetch_volume_via_gateway"]


class GatewayDexVolumeProvider:
    """Generic gateway-backed volume provider for any declared DEX.

    Parametrized by the connector-declared dispatch key: chain support, the
    gateway routing key, and the provenance string all come from the DEX's
    ``DexVolumeDecl`` via
    :class:`~almanak.connectors._strategy_base.dex_volume_registry.DexVolumeRegistry`.
    The aggregator (``multi_dex_volume``) constructs these directly, so a new
    DEX's volume lane needs no framework wrapper class (VIB-4851 Phase D).
    """

    def __init__(
        self,
        protocol: str,
        client: Any | None = None,
        fallback_volume: Decimal = Decimal("0"),
        requests_per_minute: int = 100,
    ) -> None:
        """Initialize for one declared DEX.

        Args:
            protocol: Declared dispatch key or alias (resolved on first use).
            client: Ignored (kept for ctor parity with the legacy wrappers).
            fallback_volume: Preserved for introspection; the pre-W7
                silent-zero fallback row stays removed.
            requests_per_minute: Ignored (gateway owns rate limiting).
        """
        self._protocol = protocol
        self._fallback_volume = fallback_volume

    def _entry(self):  # noqa: ANN202 - registry entry resolved lazily
        """Resolve the connector-declared facts for this DEX."""
        from almanak.connectors._strategy_base.dex_volume_registry import (
            DexVolumeRegistry,
        )

        entry = DexVolumeRegistry.entry_for(self._protocol)
        if entry is None:
            raise ValueError(f"No DEX volume declaration for protocol: {self._protocol}")
        return entry

    @property
    def supported_chains(self) -> list[Chain]:
        """Chains the owning connector declares volume data for."""
        return [Chain(c.upper()) for c in self._entry().chains]

    async def close(self) -> None:
        """No-op shutdown hook (no owned client to close)."""
        return None

    async def __aenter__(self) -> GatewayDexVolumeProvider:
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()

    async def get_volume(
        self,
        pool_address: str,
        chain: Chain,
        start_date: date,
        end_date: date,
    ) -> list[VolumeResult]:
        """Fetch historical daily volume for a pool via the gateway.

        Raises:
            ValueError: If the chain is outside the declared support set.
            DataSourceUnavailable: gateway unreachable / RPC failed / the
                subgraph returned no or errored data (no silent zero-fill).
        """
        entry = self._entry()
        if chain.value.lower() not in entry.chains:
            supported = [c.upper() for c in entry.chains]
            raise ValueError(f"Unsupported chain: {chain}. Supported chains: {supported}")

        return await fetch_volume_via_gateway(
            dex=entry.dex,
            chain=chain,
            pool_address=pool_address,
            start_date=start_date,
            end_date=end_date,
            data_source=entry.volume_data_source,
        )
