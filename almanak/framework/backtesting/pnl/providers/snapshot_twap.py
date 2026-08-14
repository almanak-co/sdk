"""Exact-pool historical TWAP lane for strategy-facing backtest snapshots.

The live ``MarketSnapshot.twap`` surface reads a named pool's native
``observe()`` oracle. Backtests must preserve that identity and observation
semantics: a ratio of two independently sourced USD prices is not a pool TWAP.

Strategies declare their prewarm requirements with
:class:`HistoricalTWAPTarget` through ``get_backtest_twap_targets()`` or a
``backtest_twap_targets`` attribute. A deliberately narrow compatibility
decoder recognizes the generated strategy shape that predates that typed
contract (``swap_pool`` / ``protocol`` plus either
``pool_twap_window_seconds`` or ``twap_window_seconds`` and matching pool /
protocol instance attributes). It never searches arbitrary config or discovers
pools from pair labels.
"""

from __future__ import annotations

from bisect import bisect_right
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

from almanak.core.finality import DataFinality
from almanak.framework.backtesting.pnl.data_manifest import (
    CONSUMER_STRATEGY_DECISION,
    LANE_TWAP,
    OUTCOME_REFUSED,
    OUTCOME_SERVED,
)
from almanak.framework.backtesting.pnl.data_provider import is_address_like
from almanak.framework.backtesting.pnl.providers.perp._gateway_history import run_sync_gateway_call
from almanak.framework.backtesting.pnl.providers.twap import HistoricalTWAPPoint, fetch_historical_twap_points
from almanak.framework.data.models import DataClassification, DataEnvelope, DataMeta
from almanak.framework.data.pools.aggregation import AggregatedPrice, PoolContribution
from almanak.framework.market.errors import PoolPriceUnavailableError

if TYPE_CHECKING:
    from almanak.framework.backtesting.pnl.data_manifest import RunDataManifest


_ARCHIVE_LADDER = ("archive_observe",)


def _utc(timestamp: datetime) -> datetime:
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=UTC)
    return timestamp.astimezone(UTC)


def _unix_seconds(timestamp: datetime) -> int:
    return int(_utc(timestamp).timestamp())


@dataclass(frozen=True, slots=True)
class HistoricalTWAPTarget:
    """One exact pool/window dependency declared by a backtest strategy."""

    chain: str
    protocol: str
    pool_address: str
    window_seconds: int

    def __post_init__(self) -> None:
        chain = self.chain.strip().lower()
        protocol = self.protocol.strip().lower().replace("-", "_")
        pool_address = self.pool_address.strip().lower()
        if not chain:
            raise ValueError("HistoricalTWAPTarget.chain is required")
        if not protocol:
            raise ValueError("HistoricalTWAPTarget.protocol is required")
        if not is_address_like(pool_address):
            raise ValueError(f"HistoricalTWAPTarget.pool_address is not an EVM address: {self.pool_address!r}")
        if self.window_seconds <= 0:
            raise ValueError(f"HistoricalTWAPTarget.window_seconds must be > 0, got {self.window_seconds}")
        object.__setattr__(self, "chain", chain)
        object.__setattr__(self, "protocol", protocol)
        object.__setattr__(self, "pool_address", pool_address)

    @property
    def key(self) -> tuple[str, str, str, int]:
        return self.chain, self.protocol, self.pool_address, self.window_seconds

    @property
    def manifest_key(self) -> str:
        return f"{self.chain}:{self.protocol}:{self.pool_address}:window={self.window_seconds}"


def _typed_declarations(strategy: Any) -> object | None:
    getter = getattr(strategy, "get_backtest_twap_targets", None)
    if callable(getter):
        return getter()
    return getattr(strategy, "backtest_twap_targets", None)


def _require_historical_twap(protocol: str) -> None:
    """Require the connector's native historical-TWAP facet."""
    from almanak.connectors._strategy_base.pool_data import PoolDataFacet, PoolDataSource
    from almanak.connectors._strategy_pool_data_registry import POOL_DATA_REGISTRY

    if POOL_DATA_REGISTRY.supports_from(protocol, PoolDataFacet.TWAP, PoolDataSource.GATEWAY_TWAP):
        return
    reason = POOL_DATA_REGISTRY.unsupported_reason(protocol, PoolDataFacet.TWAP)
    raise ValueError(f"protocol {protocol!r} does not support historical pool TWAP: {reason}")


def _legacy_exact_pool_target(
    strategy: Any,
    strategy_config: Mapping[str, Any],
    *,
    default_chain: str,
) -> HistoricalTWAPTarget | None:
    """Decode only the pre-contract generated exact-pool strategy shape.

    Compatibility is intentionally conjunctive: the exact pool, protocol, and
    one recognized window key must be present; initialized pool and protocol
    attributes must match them; and strategy metadata must declare the same
    protocol.  The current generator emits ``pool_twap_window_seconds`` as a
    config-only dependency while passing its value literally to ``market.twap``;
    if a same-named strategy attribute is present, it remains an assertion.
    This supports both generated artifacts without treating an arbitrary
    ``swap_pool`` key as permission to fetch archive data.
    """
    if "swap_pool" not in strategy_config or "protocol" not in strategy_config:
        return None
    raw_pool = strategy_config.get("swap_pool")
    raw_protocol = strategy_config.get("protocol")
    window_key = next(
        (key for key in ("pool_twap_window_seconds", "twap_window_seconds") if key in strategy_config),
        None,
    )
    if window_key is None:
        return None
    raw_window = strategy_config.get(window_key)
    if not isinstance(raw_pool, str) or not isinstance(raw_protocol, str) or raw_window is None:
        return None
    try:
        window = int(raw_window)
    except (TypeError, ValueError):
        return None
    pool = raw_pool.strip().lower()
    protocol = raw_protocol.strip().lower().replace("-", "_")
    strategy_pool_matches = any(
        str(getattr(strategy, attribute, "")).strip().lower() == pool for attribute in ("swap_pool", "pool")
    )
    missing = object()
    strategy_window = getattr(strategy, window_key, missing)
    strategy_window_matches = (
        strategy_window == window if window_key == "twap_window_seconds" or strategy_window is not missing else True
    )
    if (
        not strategy_pool_matches
        or str(getattr(strategy, "protocol", "")).strip().lower().replace("-", "_") != protocol
        or not strategy_window_matches
    ):
        return None
    metadata = getattr(strategy, "STRATEGY_METADATA", None)
    if metadata is None:
        get_metadata = getattr(strategy, "get_metadata", None)
        metadata = get_metadata() if callable(get_metadata) else None
    supported = {
        str(value).strip().lower().replace("-", "_") for value in (getattr(metadata, "supported_protocols", None) or ())
    }
    if protocol not in supported:
        return None
    _require_historical_twap(protocol)
    return HistoricalTWAPTarget(
        chain=default_chain,
        protocol=protocol,
        pool_address=pool,
        window_seconds=window,
    )


def declared_historical_twap_targets(
    strategy: Any,
    strategy_config: Mapping[str, Any],
    *,
    default_chain: str,
) -> tuple[HistoricalTWAPTarget, ...]:
    """Return typed, exact-pool dependencies safe to prewarm before tick 1."""
    raw = _typed_declarations(strategy)
    if raw is not None:
        if isinstance(raw, HistoricalTWAPTarget):
            values: Iterable[object] = (raw,)
        elif isinstance(raw, Iterable) and not isinstance(raw, str | bytes | Mapping):
            values = raw
        else:
            raise ValueError(
                "backtest TWAP declarations must be HistoricalTWAPTarget or an iterable of HistoricalTWAPTarget"
            )
        targets = tuple(values)
        if not all(isinstance(target, HistoricalTWAPTarget) for target in targets):
            raise ValueError("every backtest TWAP declaration must be a HistoricalTWAPTarget")
        # Explicit typed declarations remain the highest-precedence extension
        # seam.  They may be served by a caller-provided provider that is not a
        # connector-manifest capability, so only generated/config-shape
        # discovery is gated by POOL_DATA_REGISTRY.
        return tuple(dict.fromkeys(cast(tuple[HistoricalTWAPTarget, ...], targets)))

    legacy = _legacy_exact_pool_target(strategy, strategy_config, default_chain=default_chain)
    return (legacy,) if legacy is not None else ()


@dataclass(frozen=True, slots=True)
class _TWAPSeries:
    target: HistoricalTWAPTarget
    sample_targets: tuple[int, ...]
    points: tuple[HistoricalTWAPPoint, ...]


class SnapshotTWAPSource:
    """Run-scoped immutable historical exact-pool TWAP plane."""

    def __init__(
        self,
        *,
        start_time: datetime,
        end_time: datetime,
        sample_interval_seconds: int,
        manifest: RunDataManifest | None = None,
        fetcher: Callable[..., list[HistoricalTWAPPoint]] | None = None,
    ) -> None:
        self._start_ts = _unix_seconds(start_time)
        self._end_ts = _unix_seconds(end_time)
        if self._end_ts < self._start_ts:
            raise ValueError("end_time must be greater than or equal to start_time")
        if sample_interval_seconds <= 0:
            raise ValueError("sample_interval_seconds must be > 0")
        self._sample_interval_seconds = sample_interval_seconds
        self._manifest = manifest
        self._fetcher = fetcher or fetch_historical_twap_points
        self._series: dict[tuple[str, str, str, int], _TWAPSeries] = {}

    async def materialize_history(self, target: HistoricalTWAPTarget) -> int:
        """Load and validate complete one-observation-per-grid-point history."""
        if target.key in self._series:
            return len(self._series[target.key].points)
        points = await run_sync_gateway_call(
            self._fetcher,
            protocol=target.protocol,
            chain=target.chain,
            pool_address=target.pool_address,
            start_ts=self._start_ts,
            end_ts=self._end_ts,
            interval_secs=self._sample_interval_seconds,
            window_secs=target.window_seconds,
        )
        sample_targets = tuple(range(self._start_ts, self._end_ts + 1, self._sample_interval_seconds))
        if len(points) != len(sample_targets):
            raise ValueError(
                f"Historical TWAP coverage incomplete for {target.manifest_key}: "
                f"requested={len(sample_targets)}, received={len(points)}"
            )
        for sample_target, point in zip(sample_targets, points, strict=True):
            if point.price <= 0 or not point.source.strip():
                raise ValueError(
                    f"Historical TWAP observation is missing positive price/source provenance "
                    f"for {target.manifest_key} at sample {sample_target}"
                )
            if point.block_number is None or point.block_number <= 0:
                raise ValueError(
                    f"Historical TWAP observation omitted its archive block anchor "
                    f"for {target.manifest_key} at sample {sample_target}"
                )
            if point.timestamp > sample_target:
                raise ValueError(
                    f"Historical TWAP observation {point.timestamp} is after sample target {sample_target} "
                    f"for {target.manifest_key}"
                )
            age = sample_target - point.timestamp
            if age > self._sample_interval_seconds:
                raise ValueError(
                    f"Historical TWAP observation is stale by {age}s at sample {sample_target} "
                    f"for {target.manifest_key}"
                )
        self._series[target.key] = _TWAPSeries(target, sample_targets, tuple(points))
        return len(points)

    def view_at(self, timestamp: datetime) -> SnapshotTWAPView:
        return SnapshotTWAPView(self, _unix_seconds(timestamp))

    def _resolve(
        self,
        *,
        chain: str,
        protocol: str,
        pool_address: str,
        window_seconds: int,
        tick_ts: int,
    ) -> tuple[HistoricalTWAPTarget, HistoricalTWAPPoint]:
        key = (
            chain.strip().lower(),
            protocol.strip().lower().replace("-", "_"),
            pool_address.strip().lower(),
            window_seconds,
        )
        series = self._series.get(key)
        if series is None:
            reason = (
                "exact pool/window was not declared and prewarmed; declare a HistoricalTWAPTarget "
                "via get_backtest_twap_targets()"
            )
            try:
                requested = HistoricalTWAPTarget(*key)
            except ValueError:
                manifest_key = f"{key[0]}:{key[1]}:{key[2]}:window={key[3]}"
                self._record_key(manifest_key, tick_ts, source="none", outcome=OUTCOME_REFUSED, detail=reason)
                raise PoolPriceUnavailableError(pool_address, reason) from None
            self._record(requested, tick_ts, source="none", outcome=OUTCOME_REFUSED, detail=reason)
            raise PoolPriceUnavailableError(pool_address, reason)
        index = bisect_right(series.sample_targets, tick_ts) - 1
        if index < 0:
            reason = "no historical TWAP sample exists at or before this backtest tick"
            self._record(series.target, tick_ts, source="none", outcome=OUTCOME_REFUSED, detail=reason)
            raise PoolPriceUnavailableError(pool_address, reason)
        point = series.points[index]
        age = tick_ts - point.timestamp
        if point.timestamp > tick_ts or age > self._sample_interval_seconds:
            reason = (
                f"historical TWAP observation at {point.timestamp} is not fresh for tick {tick_ts} "
                f"(sample_interval_seconds={self._sample_interval_seconds})"
            )
            self._record(series.target, tick_ts, source=point.source, outcome=OUTCOME_REFUSED, detail=reason)
            raise PoolPriceUnavailableError(pool_address, reason)
        return series.target, point

    def _record(
        self,
        target: HistoricalTWAPTarget,
        tick_ts: int,
        *,
        source: str,
        outcome: str,
        detail: str,
    ) -> None:
        self._record_key(
            target.manifest_key,
            tick_ts,
            source=source,
            outcome=outcome,
            detail=detail,
        )

    def _record_key(
        self,
        manifest_key: str,
        tick_ts: int,
        *,
        source: str,
        outcome: str,
        detail: str,
    ) -> None:
        if self._manifest is None:
            return
        self._manifest.record(
            lane=LANE_TWAP,
            key=manifest_key,
            consumer=CONSUMER_STRATEGY_DECISION,
            source=source,
            outcome=outcome,
            at=datetime.fromtimestamp(tick_ts, UTC),
            detail=detail,
            ladder=_ARCHIVE_LADDER,
        )


class SnapshotTWAPView:
    """Synchronous price-aggregator view bound to one simulated timestamp."""

    requires_decimals = False

    def __init__(self, source: SnapshotTWAPSource, tick_ts: int) -> None:
        self._source = source
        self._tick_ts = tick_ts

    def twap(
        self,
        pool_address: str,
        chain: str,
        protocol: str,
        window_seconds: int = 300,
        token0_decimals: int | None = None,
        token1_decimals: int | None = None,
    ) -> DataEnvelope[AggregatedPrice]:
        del token0_decimals, token1_decimals
        target, point = self._source._resolve(
            chain=chain,
            protocol=protocol,
            pool_address=pool_address,
            window_seconds=window_seconds,
            tick_ts=self._tick_ts,
        )
        contribution = PoolContribution(
            pool_address=target.pool_address,
            protocol=target.protocol,
            price=point.price,
            weight=1.0,
        )
        assert point.block_number is not None  # materialize_history requires a positive anchor
        aggregated = AggregatedPrice(
            price=point.price,
            sources=[contribution],
            block_range=(point.block_number, point.block_number),
            method="twap",
            window_seconds=target.window_seconds,
            pool_count=1,
        )
        observed_at = datetime.fromtimestamp(point.timestamp, UTC)
        detail = (
            f"window_seconds={target.window_seconds}; "
            f"sample_interval_seconds={self._source._sample_interval_seconds}; "
            f"observed_at={observed_at.isoformat()}"
        )
        self._source._record(
            target,
            self._tick_ts,
            source=f"historical:{point.source}",
            outcome=OUTCOME_SERVED,
            detail=detail,
        )
        return DataEnvelope(
            value=aggregated,
            meta=DataMeta(
                source=f"historical:{point.source}",
                observed_at=observed_at,
                block_number=point.block_number,
                finality=DataFinality.LATEST,
                staleness_ms=(self._tick_ts - point.timestamp) * 1000,
                confidence=1.0,
                cache_hit=True,
            ),
            classification=DataClassification.EXECUTION_GRADE,
        )
