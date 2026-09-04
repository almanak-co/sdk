"""Exact-address historical Uniswap V3 pool state for backtest snapshots."""

from __future__ import annotations

import logging
from bisect import bisect_right
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast

from almanak.core.constants import canonical_chain_name
from almanak.core.finality import DataFinality
from almanak.framework.backtesting.pnl.data_manifest import (
    CONSUMER_STRATEGY_DECISION,
    LANE_POOL_STATE,
    LANE_POOL_TVL,
    OUTCOME_SERVED,
)
from almanak.framework.backtesting.pnl.data_provider import MarketState, is_address_like
from almanak.framework.backtesting.pnl.providers.perp._gateway_history import run_sync_gateway_call
from almanak.framework.data.interfaces import DataSourceTimeout, DataSourceUnavailable, data_source_error_from_grpc
from almanak.framework.data.models import DataClassification, DataEnvelope, DataMeta
from almanak.framework.data.pools.descriptor import PoolDescriptor
from almanak.framework.data.pools.reader import PoolPrice
from almanak.framework.market.errors import PoolPriceUnavailableError, PoolReservesUnavailableError

# Each state point expands to slot0/liquidity/two balance archive calls. Keep a
# page comfortably inside the fixed gateway deadline; long windows are served
# through multiple independently bounded RPCs.
_MAX_POINTS_PER_REQUEST = 128
# The client deadline is fixed per RPC while archive latency is not, so a page
# that expires it is retried as two half pages: each carries half the archive
# work against the same deadline. A page is only split while both halves stay
# at or above the floor, which bounds the retries to four splits per page; a
# page that cannot be split further and still expires is a real outage, not
# variance.
_MIN_POINTS_PER_REQUEST = 8
_ARCHIVE_LADDER = ("on_chain_archive",)

logger = logging.getLogger(__name__)


def _unix_seconds(timestamp: datetime) -> int:
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)
    return int(timestamp.astimezone(UTC).timestamp())


@dataclass(frozen=True, slots=True)
class HistoricalPoolStateTarget:
    """One exact-address pool-state dependency declared by a strategy."""

    chain: str
    protocol: str
    pool_address: str
    token_addresses: tuple[str, str] | None = None
    fee_tier: int | None = None

    def __post_init__(self) -> None:
        chain = canonical_chain_name(self.chain.strip()).strip().lower()
        protocol = self.protocol.strip().lower().replace("-", "_")
        pool = self.pool_address.strip().lower()
        if not chain or not protocol:
            raise ValueError("HistoricalPoolStateTarget chain and protocol are required")
        if not is_address_like(pool):
            raise ValueError(f"HistoricalPoolStateTarget.pool_address is not an EVM address: {pool!r}")
        tokens = self.token_addresses
        if tokens is not None:
            normalized = tuple(address.strip().lower() for address in tokens)
            if len(set(normalized)) != 2 or not all(is_address_like(address) for address in normalized):
                raise ValueError("HistoricalPoolStateTarget.token_addresses must contain two distinct EVM addresses")
            object.__setattr__(self, "token_addresses", cast(tuple[str, str], normalized))
        if self.fee_tier is not None and self.fee_tier <= 0:
            raise ValueError("HistoricalPoolStateTarget.fee_tier must be positive when provided")
        object.__setattr__(self, "chain", chain)
        object.__setattr__(self, "protocol", protocol)
        object.__setattr__(self, "pool_address", pool)

    @property
    def key(self) -> tuple[str, str, str]:
        return self.chain, self.protocol, self.pool_address

    @property
    def manifest_key(self) -> str:
        return ":".join(self.key)


def _address_from_token_config(value: object) -> str | None:
    if not isinstance(value, Mapping):
        return None
    address = value.get("address")
    return address.strip().lower() if isinstance(address, str) and is_address_like(address.strip()) else None


def _fee_tier_assertion(protocol: str, value: Any) -> int | None:
    """Decode ``fee_tier`` only when the connector says it means a fee tier.

    Some configs reuse this field for a different factory discriminator
    (Aerodrome Slipstream stores tick spacing there); the archive-authenticated
    ``fee()`` stays authoritative and a malformed value never suppresses the
    hint.
    """
    from almanak.connectors._strategy_base.pool_reader import PoolDiscriminatorKind
    from almanak.connectors._strategy_pool_data_registry import POOL_DATA_REGISTRY

    pool_data = POOL_DATA_REGISTRY.require(protocol)
    reader = pool_data.price_reader
    if reader is None or reader.discriminator_kind is not PoolDiscriminatorKind.FEE_TIER:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# Exactly the keys the former config-key bridge read: a hint may only prewarm what main
# already prewarmed, so no result changes for a config that ran before. Any other key
# (``pool_address``, bespoke names) is the strategy's business and resolves at first use.
_POOL_HINT_KEYS = ("swap_pool", "pool")


def hinted_historical_pool_state_target(
    strategy: Any,
    config: Mapping[str, Any],
    *,
    default_chain: str,
) -> HistoricalPoolStateTarget | None:
    """A prewarm HINT read only from generated ``pool`` / ``swap_pool`` keys.

    Hints only decide what readiness prewarms before tick 1 so a strategy that
    names its pool in config keeps the exact-pool plane from tick 0. They never
    gate anything: a missing, malformed or unservable hint costs nothing,
    because the pool is authenticated at first use from the intent or read
    that names it. This function therefore never raises.
    """
    try:
        pool = next(
            (
                str(value).strip().lower()
                for value in (config.get(key) for key in _POOL_HINT_KEYS)
                if isinstance(value, str) and is_address_like(value.strip())
            ),
            None,
        )
        if pool is None:
            return None
        candidates: list[str] = []
        for value in (config.get("protocol"), getattr(strategy, "protocol", None)):
            if isinstance(value, str) and value.strip():
                candidates.append(value.strip().lower().replace("-", "_"))
        metadata = getattr(strategy, "STRATEGY_METADATA", None)
        if metadata is None:
            get_metadata = getattr(strategy, "get_metadata", None)
            metadata = get_metadata() if callable(get_metadata) else None
        for value in getattr(metadata, "supported_protocols", None) or ():
            candidates.append(str(value).strip().lower().replace("-", "_"))
        supported = []
        for protocol in dict.fromkeys(candidates):
            try:
                _require_historical_pool_state(protocol)
            except ValueError:
                continue
            supported.append(protocol)
        if len(supported) != 1:
            return None  # ambiguous or unsupported: first use resolves it from the intent
        protocol = supported[0]
        base = _address_from_token_config(config.get("base_token"))
        quote = _address_from_token_config(config.get("quote_token"))
        return HistoricalPoolStateTarget(
            chain=default_chain,
            protocol=protocol,
            pool_address=pool,
            token_addresses=(base, quote) if base is not None and quote is not None else None,
            fee_tier=_fee_tier_assertion(protocol, config.get("fee_tier")),
        )
    except Exception as exc:  # noqa: BLE001 — a hint must never refuse a run
        logger.debug("Ignoring unusable exact-pool prewarm hint: %s", exc)
        return None


def require_historical_pool_state(protocol: str) -> None:
    """Require the connector's historical-state facet with its own reason.

    Public so the engine's first-use exact-pool discovery applies the same
    support gate as declaration-driven preflight.
    """
    _require_historical_pool_state(protocol)


def _require_historical_pool_state(protocol: str) -> None:
    """Require the connector's historical-state facet with its own reason."""
    from almanak.connectors._strategy_base.pool_data import PoolDataFacet, PoolDataSource
    from almanak.connectors._strategy_pool_data_registry import POOL_DATA_REGISTRY

    if POOL_DATA_REGISTRY.supports_from(
        protocol,
        PoolDataFacet.HISTORICAL_STATE,
        PoolDataSource.GATEWAY_POOL_STATE,
    ):
        return
    reason = POOL_DATA_REGISTRY.unsupported_reason(protocol, PoolDataFacet.HISTORICAL_STATE)
    raise ValueError(f"protocol {protocol!r} does not support historical pool state: {reason}")


def declared_historical_pool_state_targets(
    strategy: Any,
    strategy_config: Mapping[str, Any],
    *,
    default_chain: str,
) -> tuple[HistoricalPoolStateTarget, ...]:
    getter = getattr(strategy, "get_backtest_pool_state_targets", None)
    raw = getter() if callable(getter) else getattr(strategy, "backtest_pool_state_targets", None)
    if raw is not None:
        if isinstance(raw, HistoricalPoolStateTarget):
            values: Iterable[object] = (raw,)
        elif isinstance(raw, Iterable) and not isinstance(raw, str | bytes | Mapping):
            values = raw
        else:
            raise ValueError("pool-state declarations must be HistoricalPoolStateTarget values")
        targets = tuple(values)
        if not all(isinstance(target, HistoricalPoolStateTarget) for target in targets):
            raise ValueError("every pool-state declaration must be a HistoricalPoolStateTarget")
        # Explicit typed declarations may be served by a caller-provided
        # provider that is not a connector-manifest capability, so they are
        # not gated by POOL_DATA_REGISTRY here (first-use discovery is).
        return tuple(dict.fromkeys(cast(tuple[HistoricalPoolStateTarget, ...], targets)))
    # No config-key discovery: identity comes from the typed hook above or, at
    # first use, from the intent/read that names the pool.
    return ()


@dataclass(frozen=True, slots=True)
class HistoricalPoolStatePoint:
    timestamp: int
    block_number: int
    sqrt_price_x96: int
    tick: int
    liquidity: int
    token0: str
    token1: str
    token0_decimals: int
    token1_decimals: int
    fee_tier: int
    reserve0_raw: int
    reserve1_raw: int
    source: str


@dataclass(frozen=True, slots=True)
class HistoricalPoolTVL:
    """Exact-block pool balances valued on the historical USD price plane."""

    tvl_usd: Decimal
    token0_value_usd: Decimal
    token1_value_usd: Decimal
    token0_weight: float
    token1_weight: float


def _get_pool_state_series_response(client: Any, request: Any) -> Any:
    try:
        return client.rate_history.GetDexPoolStateSeries(request, timeout=client.config.timeout)
    except Exception as exc:
        typed = data_source_error_from_grpc(exc, default_source="gateway")
        if typed is not None:
            raise typed from exc
        from almanak.framework.backtesting.pnl.providers.twap import _gateway_client_deadline_error

        deadline = _gateway_client_deadline_error(exc, timeout_seconds=client.config.timeout)
        if deadline is not None:
            raise deadline from exc
        raise DataSourceUnavailable(source="gateway", reason=f"GetDexPoolStateSeries RPC failed: {exc}") from exc


def _pool_state_response_source(response: Any, normalized: tuple[str, str, str], expected_count: int) -> str:
    received = (
        response.dex.strip().lower(),
        response.chain.strip().lower(),
        response.pool_address.strip().lower(),
    )
    if not response.success:
        raise DataSourceUnavailable(
            source=response.source or "gateway",
            reason=response.error or "GetDexPoolStateSeries returned success=false",
        )
    if received != normalized or len(response.points) != expected_count:
        raise DataSourceUnavailable(
            source=response.source or "gateway",
            reason=(
                f"GetDexPoolStateSeries identity/coverage mismatch: requested={normalized}/{expected_count}, "
                f"received={received}/{len(response.points)}"
            ),
        )
    source = response.source.strip()
    if not source:
        raise DataSourceUnavailable(source="gateway", reason="GetDexPoolStateSeries omitted provenance source")
    return source


def _pool_state_point(raw: Any, *, sample: int, source: str) -> HistoricalPoolStatePoint:
    point = HistoricalPoolStatePoint(
        timestamp=int(raw.timestamp),
        block_number=int(raw.block_number),
        sqrt_price_x96=int(raw.sqrt_price_x96),
        tick=int(raw.tick),
        liquidity=int(raw.liquidity),
        token0=raw.token0.strip().lower(),
        token1=raw.token1.strip().lower(),
        token0_decimals=int(raw.token0_decimals),
        token1_decimals=int(raw.token1_decimals),
        fee_tier=int(raw.fee_tier),
        reserve0_raw=int(raw.reserve0_raw),
        reserve1_raw=int(raw.reserve1_raw),
        source=source,
    )
    if (
        point.timestamp > sample
        or point.block_number <= 0
        or point.sqrt_price_x96 <= 0
        or point.liquidity < 0
        or not is_address_like(point.token0)
        or not is_address_like(point.token1)
        or point.token0 == point.token1
        or not 0 <= point.token0_decimals <= 255
        or not 0 <= point.token1_decimals <= 255
        or point.fee_tier <= 0
        or point.reserve0_raw < 0
        or point.reserve1_raw < 0
    ):
        raise DataSourceUnavailable(
            source=source,
            reason=f"GetDexPoolStateSeries returned malformed/no-lookahead-invalid point at {sample}",
        )
    return point


def fetch_historical_pool_state_points(
    *,
    protocol: str,
    chain: str,
    pool_address: str,
    start_ts: int,
    end_ts: int,
    interval_secs: int,
) -> list[HistoricalPoolStatePoint]:
    """Fetch a complete exact-address pool-state grid through the gateway."""
    from almanak.framework.backtesting.pnl.providers.twap import _twap_get_connected_gateway_client

    client, gateway_pb2 = _twap_get_connected_gateway_client()
    targets = list(range(start_ts, end_ts + 1, interval_secs))
    normalized = (protocol.strip().lower(), chain.strip().lower(), pool_address.strip().lower())
    points: list[HistoricalPoolStatePoint] = []
    for offset in range(0, len(targets), _MAX_POINTS_PER_REQUEST):
        chunk = targets[offset : offset + _MAX_POINTS_PER_REQUEST]
        points.extend(_fetch_pool_state_page(client, gateway_pb2, normalized, chunk, interval_secs))
    return points


def _fetch_pool_state_page(
    client: Any,
    gateway_pb2: Any,
    normalized: tuple[str, str, str],
    chunk: list[int],
    interval_secs: int,
) -> list[HistoricalPoolStatePoint]:
    request = gateway_pb2.GetDexPoolStateSeriesRequest(
        dex=normalized[0],
        chain=normalized[1],
        pool_address=normalized[2],
        start_ts=chunk[0],
        end_ts=chunk[-1],
        interval_secs=interval_secs,
    )
    try:
        response = _get_pool_state_series_response(client, request)
    except DataSourceTimeout as exc:
        if len(chunk) < 2 * _MIN_POINTS_PER_REQUEST:
            raise
        half = len(chunk) // 2
        logger.warning(
            "Pool-state page of %d points for %s/%s/%s hit the %.0fs gateway deadline; retrying as two pages of %d and %d",
            len(chunk),
            *normalized,
            exc.timeout_seconds,
            half,
            len(chunk) - half,
        )
        return _fetch_pool_state_page(client, gateway_pb2, normalized, chunk[:half], interval_secs) + (
            _fetch_pool_state_page(client, gateway_pb2, normalized, chunk[half:], interval_secs)
        )
    source = _pool_state_response_source(response, normalized, len(chunk))
    return [
        _pool_state_point(raw, sample=sample, source=source) for sample, raw in zip(chunk, response.points, strict=True)
    ]


@dataclass(frozen=True, slots=True)
class _Series:
    target: HistoricalPoolStateTarget
    samples: tuple[int, ...]
    points: tuple[HistoricalPoolStatePoint, ...]


class SnapshotPoolStateSource:
    """Run-scoped immutable historical exact-pool state plane."""

    def __init__(
        self,
        *,
        start_time: datetime,
        end_time: datetime,
        sample_interval_seconds: int,
        manifest: Any | None = None,
        fetcher: Callable[..., list[HistoricalPoolStatePoint]] | None = None,
        first_use_feasibility: Callable[[], Any] | None = None,
    ) -> None:
        self._start_ts = _unix_seconds(start_time)
        self._end_ts = _unix_seconds(end_time)
        self._interval = sample_interval_seconds
        self._manifest = manifest
        self._fetcher = fetcher or fetch_historical_pool_state_points
        # The declared and hinted prewarm paths bound the window before their
        # first gateway page; a decide()-time fetch pays the same serial paging
        # cost on the iteration task, so it is bounded by the same estimate
        # instead of stalling the run past the job budget.
        self._first_use_feasibility = first_use_feasibility
        self._series: dict[tuple[str, str, str], _Series] = {}
        self._first_use_failures: dict[tuple[str, str, str], str] = {}

    @staticmethod
    def _first_use_key(chain: str, protocol: str, pool_address: str) -> tuple[str, str, str]:
        return (
            canonical_chain_name(chain.strip()).strip().lower(),
            protocol.strip().lower().replace("-", "_"),
            pool_address.strip().lower(),
        )

    def first_use_failure(self, chain: str, protocol: str, pool_address: str) -> str | None:
        """The remembered reason this pool could not be served, if it already failed once."""
        return self._first_use_failures.get(self._first_use_key(chain, protocol, pool_address))

    def remember_first_use_failure(self, chain: str, protocol: str, pool_address: str, reason: str) -> None:
        """Record a failed authentication so later ticks refuse instantly instead of re-fetching.

        Without this, a strategy that keeps emitting an intent for a pool the
        archive cannot serve pays the whole run-window fetch on every attempt.
        """
        self._first_use_failures.setdefault(self._first_use_key(chain, protocol, pool_address), reason)

    def enforce_first_use_feasibility(self) -> None:
        """Apply the run's shared feasibility policy before a lazy fetch."""
        if self._first_use_feasibility is not None:
            self._first_use_feasibility()

    def resolve_or_materialize(
        self,
        chain: str,
        protocol: str,
        pool_address: str,
        tick_ts: int,
    ) -> tuple[HistoricalPoolStateTarget, HistoricalPoolStatePoint]:
        """``_resolve`` that fetches an undeclared exact pool the first time a protocol-scoped read names it.

        A miss is remembered for the run so an unservable pool refuses
        instantly on later ticks. Reads without a protocol (``pool_price``)
        keep their existing fallback: there is no factory to authenticate
        against without one.
        """
        key = self._first_use_key(chain, protocol, pool_address)
        if key not in self._series:
            failed = self._first_use_failures.get(key)
            if failed is None:
                try:
                    _require_historical_pool_state(key[1])
                    self.enforce_first_use_feasibility()
                    self.materialize_history_blocking(HistoricalPoolStateTarget(*key))
                except Exception as exc:  # noqa: BLE001 — fail-closed refusal, remembered for the run
                    failed = f"first-use exact-pool state fetch failed for {key[0]}:{key[1]}:{key[2]}: {exc}"
                    self._first_use_failures[key] = failed
                    logger.warning(failed)
            if failed is not None:
                raise PoolPriceUnavailableError(pool_address, failed, exact_pool_unavailable=True)
        return self._resolve(*key, tick_ts)

    async def materialize_history(self, target: HistoricalPoolStateTarget) -> int:
        existing = self._series.get(target.key)
        if existing is not None:
            self._validate_target_identity(target, existing.points)
            return len(existing.points)
        points = await run_sync_gateway_call(self._fetcher, **self._fetch_kwargs(target))
        return self._install_points(target, points)

    def materialize_history_blocking(self, target: HistoricalPoolStateTarget) -> int:
        """Load one exact pool synchronously, for a decide()-time read that names an undeclared pool.

        Same fetch and validation as :meth:`materialize_history`, run inline
        because decide() cannot await inside the engine's iteration task.
        """
        existing = self._series.get(target.key)
        if existing is not None:
            self._validate_target_identity(target, existing.points)
            return len(existing.points)
        points = self._fetcher(**self._fetch_kwargs(target))
        return self._install_points(target, points)

    def _fetch_kwargs(self, target: HistoricalPoolStateTarget) -> dict[str, Any]:
        return {
            "protocol": target.protocol,
            "chain": target.chain,
            "pool_address": target.pool_address,
            "start_ts": self._start_ts,
            "end_ts": self._end_ts,
            "interval_secs": self._interval,
        }

    def _install_points(self, target: HistoricalPoolStateTarget, points: list[HistoricalPoolStatePoint]) -> int:
        samples = tuple(range(self._start_ts, self._end_ts + 1, self._interval))
        if len(points) != len(samples):
            raise ValueError(f"incomplete historical pool-state coverage for {target.manifest_key}")
        observed_fees = {point.fee_tier for point in points}
        if len(observed_fees) != 1:
            raise ValueError(f"pool fee identity drift for {target.manifest_key}: observed={sorted(observed_fees)}")
        for sample, point in zip(samples, points, strict=True):
            if point.timestamp > sample or sample - point.timestamp > self._interval:
                raise ValueError(f"stale/future pool state for {target.manifest_key} at {sample}")
        self._validate_target_identity(target, points)

        observed_token_metadata = {
            (point.token0, point.token1, point.token0_decimals, point.token1_decimals) for point in points
        }
        if len(observed_token_metadata) != 1:
            raise ValueError(
                f"pool token metadata drift for {target.manifest_key}: observed={sorted(observed_token_metadata)}"
            )
        self._series[target.key] = _Series(target, samples, tuple(points))
        return len(points)

    @staticmethod
    def _validate_target_identity(
        target: HistoricalPoolStateTarget,
        points: Iterable[HistoricalPoolStatePoint],
    ) -> None:
        for point in points:
            if target.token_addresses is not None and {point.token0, point.token1} != set(target.token_addresses):
                raise ValueError(
                    f"pool token identity mismatch for {target.manifest_key}: expected={target.token_addresses}, got={(point.token0, point.token1)}"
                )
            if target.fee_tier is not None and point.fee_tier != target.fee_tier:
                raise ValueError(
                    f"pool fee mismatch for {target.manifest_key}: expected={target.fee_tier}, got={point.fee_tier}"
                )

    def pool_descriptor(self, chain: str, protocol: str, pool_address: str) -> PoolDescriptor | None:
        """Return the archive-authenticated immutable identity for one pool."""
        key = self._first_use_key(chain, protocol, pool_address)
        series = self._series.get(key)
        if series is None:
            return None
        point = series.points[0]
        from almanak.connectors._strategy_pool_reader_registry import POOL_READER_REGISTRY

        spec = POOL_READER_REGISTRY.lookup(series.target.protocol)
        # Unmeasured when several reviewed generations could own the pool; the
        # archive point does not carry the pool's own factory.
        factories = spec.factories_for(series.target.chain) if spec is not None else ()
        factory = factories[0] if len(factories) == 1 else None
        return PoolDescriptor(
            chain=series.target.chain,
            protocol=series.target.protocol,
            address=series.target.pool_address,
            token0=point.token0,
            token1=point.token1,
            token0_decimals=point.token0_decimals,
            token1_decimals=point.token1_decimals,
            fee_tier_units=point.fee_tier,
            provenance=f"historical:{point.source}",
            factory=factory,
        )

    @property
    def is_empty(self) -> bool:
        """True until at least one exact pool has been materialized.

        The run keeps one source alive even when nothing was declared so a
        pool discovered at first use has somewhere to land; the loop only
        promotes it to the decide()-time exact-pool view once it holds a
        series, so an empty source is behaviourally identical to no source.
        """
        return not self._series

    def descriptors(self) -> tuple[PoolDescriptor, ...]:
        """Return every materialized descriptor in deterministic key order."""
        return tuple(
            descriptor for key in sorted(self._series) if (descriptor := self.pool_descriptor(*key)) is not None
        )

    def verification_block(self, chain: str, protocol: str, pool_address: str) -> int:
        """Return the first archive block that authenticated a pool identity."""
        key = (chain.strip().lower(), protocol.strip().lower().replace("-", "_"), pool_address.strip().lower())
        series = self._series.get(key)
        if series is None or not series.points:
            raise ValueError(f"exact pool {':'.join(key)} was not materialized")
        return series.points[0].block_number

    def view_at(self, timestamp: datetime, fallback: Any | None = None) -> SnapshotPoolStateView:
        return SnapshotPoolStateView(self, _unix_seconds(timestamp), fallback=fallback)

    def _resolve(
        self, chain: str, protocol: str, pool_address: str, tick_ts: int
    ) -> tuple[HistoricalPoolStateTarget, HistoricalPoolStatePoint]:
        key = self._first_use_key(chain, protocol, pool_address)
        series = self._series.get(key)
        if series is None:
            raise PoolPriceUnavailableError(
                pool_address, "exact pool was not declared and prewarmed", exact_pool_unavailable=True
            )
        index = bisect_right(series.samples, tick_ts) - 1
        if index < 0:
            raise PoolPriceUnavailableError(pool_address, "no historical pool state exists at this tick")
        return series.target, series.points[index]

    def record(
        self,
        target: HistoricalPoolStateTarget,
        point: HistoricalPoolStatePoint,
        tick_ts: int,
        outcome: str,
        detail: str,
    ) -> None:
        if self._manifest is not None:
            self._manifest.record(
                lane=LANE_POOL_STATE,
                key=target.manifest_key,
                consumer=CONSUMER_STRATEGY_DECISION,
                source=f"historical:{point.source}",
                outcome=outcome,
                at=datetime.fromtimestamp(tick_ts, UTC),
                detail=detail,
                ladder=_ARCHIVE_LADDER,
            )

    def record_tvl(
        self,
        target: HistoricalPoolStateTarget,
        point: HistoricalPoolStatePoint,
        tick_ts: int,
        source: str,
        detail: str,
    ) -> None:
        if self._manifest is not None:
            self._manifest.record(
                lane=LANE_POOL_TVL,
                key=target.manifest_key,
                consumer=CONSUMER_STRATEGY_DECISION,
                source=source,
                outcome=OUTCOME_SERVED,
                at=datetime.fromtimestamp(tick_ts, UTC),
                detail=detail,
                ladder=_ARCHIVE_LADDER,
            )


class SnapshotPoolStateView:
    """Per-tick pool reader/registry backed by archive state."""

    def __init__(self, source: SnapshotPoolStateSource, tick_ts: int, *, fallback: Any | None = None) -> None:
        self._source = source
        self._tick_ts = tick_ts
        self._fallback = fallback
        self._snapshot: Any | None = None

    def protocols_for_chain(self, chain: str) -> list[str]:
        chain_key = canonical_chain_name(chain.strip()).strip().lower()
        protocols = {key[1] for key in self._source._series if key[0] == chain_key}
        if self._fallback is not None:
            protocols.update(self._fallback.protocols_for_chain(chain))
        return sorted(protocols)

    def get_reader(self, chain: str, protocol: str) -> SnapshotPoolStateView:
        del chain, protocol
        return self

    def bind_snapshot(self, snapshot: Any) -> None:
        self._snapshot = snapshot
        if self._fallback is not None:
            self._fallback.bind_snapshot(snapshot)

    def resolve_pool_address(self, token_a: str, token_b: str, chain: str, fee_tier: int = 3000) -> str | None:
        requested = {token_a.strip().lower(), token_b.strip().lower()}
        chain_key = canonical_chain_name(chain.strip()).strip().lower()
        match = next(
            (
                series.target.pool_address
                for series in self._source._series.values()
                if series.target.chain == chain_key
                and requested == {series.points[0].token0, series.points[0].token1}
                and fee_tier == series.points[0].fee_tier
            ),
            None,
        )
        if match is not None:
            return match
        if self._fallback is not None:
            return self._fallback.resolve_pool_address(token_a, token_b, chain, fee_tier)
        return None

    def _point(self, pool_address: str, chain: str) -> tuple[HistoricalPoolStateTarget, HistoricalPoolStatePoint]:
        for protocol in self.protocols_for_chain(chain):
            try:
                return self._source._resolve(chain, protocol, pool_address, self._tick_ts)
            except PoolPriceUnavailableError:
                continue
        raise PoolPriceUnavailableError(
            pool_address, "exact pool was not declared and prewarmed", exact_pool_unavailable=True
        )

    @staticmethod
    def _pool_price(
        target: HistoricalPoolStateTarget,
        point: HistoricalPoolStatePoint,
        tick_ts: int,
    ) -> DataEnvelope[PoolPrice]:
        raw_price = (Decimal(point.sqrt_price_x96) / Decimal(2**96)) ** 2
        price = raw_price * (Decimal(10) ** (point.token0_decimals - point.token1_decimals))
        observed_at = datetime.fromtimestamp(point.timestamp, UTC)
        return DataEnvelope(
            value=PoolPrice(
                price=price,
                tick=point.tick,
                liquidity=point.liquidity,
                fee_tier=point.fee_tier,
                block_number=point.block_number,
                timestamp=observed_at,
                pool_address=target.pool_address,
                token0_decimals=point.token0_decimals,
                token1_decimals=point.token1_decimals,
            ),
            meta=DataMeta(
                source=f"historical:{point.source}",
                observed_at=observed_at,
                block_number=point.block_number,
                finality=DataFinality.LATEST,
                staleness_ms=(tick_ts - point.timestamp) * 1000,
                confidence=1.0,
                cache_hit=True,
                freshness_reference_at=datetime.fromtimestamp(tick_ts, UTC),
            ),
            classification=DataClassification.EXECUTION_GRADE,
        )

    def read_pool_price(self, pool_address: str, chain: str) -> DataEnvelope[PoolPrice]:
        try:
            target, point = self._point(pool_address, chain)
        except PoolPriceUnavailableError:
            if self._fallback is not None:
                return self._fallback.read_pool_price(pool_address, chain)
            raise
        self._source.record(target, point, self._tick_ts, OUTCOME_SERVED, "slot0/liquidity archive state")
        return self._pool_price(target, point, self._tick_ts)

    @staticmethod
    def _historical_price(
        market_state: MarketState,
        chain: str,
        token_address: str,
    ) -> tuple[Decimal | None, datetime, str]:
        token_key = (chain, token_address)
        try:
            price = market_state.get_price(token_key)
        except KeyError:
            tick = market_state.timestamp
            tick = tick.replace(tzinfo=UTC) if tick.tzinfo is None else tick.astimezone(UTC)
            return None, tick, ""
        observation = market_state.get_price_observation(token_key)
        if observation is None:
            tick = market_state.timestamp
            tick = tick.replace(tzinfo=UTC) if tick.tzinfo is None else tick.astimezone(UTC)
            # Scalar-only prices from custom/legacy providers do not prove an
            # historical observation time or source.  They may still value the
            # portfolio, but must not satisfy strict historical pool-TVL
            # readiness or be promoted to fully measured analytics.
            return None, tick, ""
        if not price.is_finite() or price <= 0:
            raise ValueError(f"invalid historical USD price for {chain}:{token_address}: {price!r}")
        observed_at = observation.timestamp
        observed_at = observed_at.replace(tzinfo=UTC) if observed_at.tzinfo is None else observed_at.astimezone(UTC)
        if observation.is_stale:
            return None, observed_at, observation.source or "market_state"
        return price, observed_at, observation.source or "market_state"

    def read_pool_tvl_usd(
        self,
        pool_address: str,
        chain: str,
        protocol: str,
        market_state: MarketState,
    ) -> DataEnvelope[HistoricalPoolTVL]:
        """Value exact-block token balances without requiring an off-chain TVL feed.

        Both independently measured historical USD prices are preferred.  If
        only one pool token has USD coverage, the other leg is valued through
        this exact pool's same-block spot ratio.  No current reserve or price
        is ever backfilled into a historical tick.
        """
        target, point = self._source.resolve_or_materialize(chain, protocol, pool_address, self._tick_ts)
        chain_key = target.chain
        reserve0 = Decimal(point.reserve0_raw) / Decimal(10**point.token0_decimals)
        reserve1 = Decimal(point.reserve1_raw) / Decimal(10**point.token1_decimals)
        price0, observed0, source0 = self._historical_price(market_state, chain_key, point.token0)
        price1, observed1, source1 = self._historical_price(market_state, chain_key, point.token1)
        pool_price = (Decimal(point.sqrt_price_x96) / Decimal(2**96)) ** 2
        pool_price *= Decimal(10) ** (point.token0_decimals - point.token1_decimals)

        derived_leg = ""
        if price0 is None and price1 is None:
            raise ValueError(
                f"historical pool TVL cannot be valued for {target.manifest_key} at "
                f"{datetime.fromtimestamp(self._tick_ts, UTC).isoformat()}: no historical USD price "
                f"for either pool token ({point.token0}, {point.token1})"
            )
        if price0 is None:
            assert price1 is not None
            price0 = price1 * pool_price
            observed0, source0 = observed1, source1
            derived_leg = "token0_via_exact_pool_spot"
        elif price1 is None:
            price1 = price0 / pool_price
            observed1, source1 = observed0, source0
            derived_leg = "token1_via_exact_pool_spot"

        value0 = reserve0 * price0
        value1 = reserve1 * price1
        tvl = value0 + value1
        if not tvl.is_finite() or tvl < 0:
            raise ValueError(f"invalid derived historical TVL for {target.manifest_key}: {tvl!r}")
        if tvl == 0:
            weight0 = weight1 = 0.5
        else:
            weight0 = float(value0 / tvl)
            weight1 = float(value1 / tvl)

        block_observed_at = datetime.fromtimestamp(point.timestamp, UTC)
        observed_at = min(block_observed_at, observed0, observed1)
        price_sources = "+".join(sorted({source for source in (source0, source1) if source})) or "market_state"
        source = f"historical:{point.source}+historical_price:{price_sources}"
        detail = (
            f"balanceOf exact block={point.block_number}; token0={point.token0}; token1={point.token1}; "
            f"valuation={derived_leg or 'two_historical_usd_prices'}"
        )
        self._source.record(target, point, self._tick_ts, OUTCOME_SERVED, "balanceOf archive state for TVL")
        self._source.record_tvl(target, point, self._tick_ts, source, detail)
        return DataEnvelope(
            value=HistoricalPoolTVL(
                tvl_usd=tvl,
                token0_value_usd=value0,
                token1_value_usd=value1,
                token0_weight=weight0,
                token1_weight=weight1,
            ),
            meta=DataMeta(
                source=source,
                observed_at=observed_at,
                block_number=point.block_number,
                finality=DataFinality.LATEST,
                staleness_ms=max(0, (self._tick_ts - int(observed_at.timestamp())) * 1000),
                confidence=1.0 if not derived_leg else 0.95,
                cache_hit=True,
                freshness_reference_at=datetime.fromtimestamp(self._tick_ts, UTC),
            ),
            classification=DataClassification.INFORMATIONAL,
        )

    async def get_pool_reserves(self, pool_address: str, chain: str) -> Any:
        from almanak.framework.data.defi.pools import PoolReserves
        from almanak.framework.data.tokens.models import ChainToken, Token

        try:
            target, point = self._point(pool_address, chain)
        except PoolPriceUnavailableError as exc:
            raise PoolReservesUnavailableError(pool_address, exc.reason) from exc
        token0 = ChainToken(
            Token("UNKNOWN", "Unknown Token", point.token0_decimals, {chain: point.token0}),
            chain,
            point.token0,
            point.token0_decimals,
        )
        token1 = ChainToken(
            Token("UNKNOWN", "Unknown Token", point.token1_decimals, {chain: point.token1}),
            chain,
            point.token1,
            point.token1_decimals,
        )
        self._source.record(target, point, self._tick_ts, OUTCOME_SERVED, "slot0/liquidity/balanceOf archive state")
        return PoolReserves(
            pool_address=target.pool_address,
            dex=target.protocol,
            token0=token0,
            token1=token1,
            reserve0=Decimal(point.reserve0_raw) / Decimal(10**point.token0_decimals),
            reserve1=Decimal(point.reserve1_raw) / Decimal(10**point.token1_decimals),
            fee_tier=point.fee_tier,
            tvl_usd=Decimal("0"),
            last_updated=datetime.fromtimestamp(point.timestamp, UTC),
            sqrt_price_x96=point.sqrt_price_x96,
            tick=point.tick,
            liquidity=point.liquidity,
        )
