"""Exact-address historical Uniswap V3 pool state for backtest snapshots."""

from __future__ import annotations

from bisect import bisect_right
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast

from almanak.core.finality import DataFinality
from almanak.framework.backtesting.pnl.data_manifest import (
    CONSUMER_STRATEGY_DECISION,
    LANE_POOL_STATE,
    OUTCOME_SERVED,
)
from almanak.framework.backtesting.pnl.data_provider import is_address_like
from almanak.framework.backtesting.pnl.providers.perp._gateway_history import run_sync_gateway_call
from almanak.framework.data.interfaces import DataSourceUnavailable, data_source_error_from_grpc
from almanak.framework.data.models import DataClassification, DataEnvelope, DataMeta
from almanak.framework.data.pools.reader import PoolPrice
from almanak.framework.market.errors import PoolPriceUnavailableError, PoolReservesUnavailableError

_MAX_POINTS_PER_REQUEST = 512
_ARCHIVE_LADDER = ("on_chain_archive",)


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
        chain = self.chain.strip().lower()
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


def _legacy_target(
    strategy: Any,
    config: Mapping[str, Any],
    *,
    default_chain: str,
) -> HistoricalPoolStateTarget | None:
    required = ("pool", "protocol", "fee_tier", "base_token", "quote_token")
    if any(key not in config for key in required):
        return None
    pool = config.get("pool")
    protocol = config.get("protocol")
    base = _address_from_token_config(config.get("base_token"))
    quote = _address_from_token_config(config.get("quote_token"))
    if not isinstance(pool, str) or not isinstance(protocol, str) or base is None or quote is None:
        return None
    try:
        fee_tier = int(config["fee_tier"])
    except (TypeError, ValueError):
        return None
    if str(getattr(strategy, "pool", "")).strip().lower() != pool.strip().lower():
        return None
    if str(getattr(strategy, "protocol", "")).strip().lower().replace("-", "_") != protocol.strip().lower().replace(
        "-", "_"
    ):
        return None
    metadata = getattr(strategy, "STRATEGY_METADATA", None)
    supported = {
        str(value).strip().lower().replace("-", "_") for value in (getattr(metadata, "supported_protocols", None) or ())
    }
    if protocol.strip().lower().replace("-", "_") not in supported:
        return None
    return HistoricalPoolStateTarget(
        chain=default_chain,
        protocol=protocol,
        pool_address=pool,
        token_addresses=(base, quote),
        fee_tier=fee_tier,
    )


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
        return tuple(dict.fromkeys(cast(tuple[HistoricalPoolStateTarget, ...], targets)))
    legacy = _legacy_target(strategy, strategy_config, default_chain=default_chain)
    return (legacy,) if legacy is not None else ()


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


def _get_pool_state_series_response(client: Any, request: Any) -> Any:
    try:
        return client.rate_history.GetDexPoolStateSeries(request, timeout=client.config.timeout)
    except Exception as exc:
        typed = data_source_error_from_grpc(exc, default_source="gateway")
        if typed is not None:
            raise typed from exc
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
        or point.fee_tier <= 0
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
        request = gateway_pb2.GetDexPoolStateSeriesRequest(
            dex=normalized[0],
            chain=normalized[1],
            pool_address=normalized[2],
            start_ts=chunk[0],
            end_ts=chunk[-1],
            interval_secs=interval_secs,
        )
        response = _get_pool_state_series_response(client, request)
        source = _pool_state_response_source(response, normalized, len(chunk))
        for sample, raw in zip(chunk, response.points, strict=True):
            points.append(_pool_state_point(raw, sample=sample, source=source))
    return points


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
    ) -> None:
        self._start_ts = _unix_seconds(start_time)
        self._end_ts = _unix_seconds(end_time)
        self._interval = sample_interval_seconds
        self._manifest = manifest
        self._fetcher = fetcher or fetch_historical_pool_state_points
        self._series: dict[tuple[str, str, str], _Series] = {}

    async def materialize_history(self, target: HistoricalPoolStateTarget) -> int:
        points = await run_sync_gateway_call(
            self._fetcher,
            protocol=target.protocol,
            chain=target.chain,
            pool_address=target.pool_address,
            start_ts=self._start_ts,
            end_ts=self._end_ts,
            interval_secs=self._interval,
        )
        samples = tuple(range(self._start_ts, self._end_ts + 1, self._interval))
        if len(points) != len(samples):
            raise ValueError(f"incomplete historical pool-state coverage for {target.manifest_key}")
        observed_fees = {point.fee_tier for point in points}
        if len(observed_fees) != 1:
            raise ValueError(f"pool fee identity drift for {target.manifest_key}: observed={sorted(observed_fees)}")
        for sample, point in zip(samples, points, strict=True):
            if point.timestamp > sample or sample - point.timestamp > self._interval:
                raise ValueError(f"stale/future pool state for {target.manifest_key} at {sample}")
            if target.token_addresses is not None and {point.token0, point.token1} != set(target.token_addresses):
                raise ValueError(
                    f"pool token identity mismatch for {target.manifest_key}: expected={target.token_addresses}, got={(point.token0, point.token1)}"
                )
            if target.fee_tier is not None and point.fee_tier != target.fee_tier:
                raise ValueError(
                    f"pool fee mismatch for {target.manifest_key}: expected={target.fee_tier}, got={point.fee_tier}"
                )
        self._series[target.key] = _Series(target, samples, tuple(points))
        return len(points)

    def view_at(self, timestamp: datetime, fallback: Any | None = None) -> SnapshotPoolStateView:
        return SnapshotPoolStateView(self, _unix_seconds(timestamp), fallback=fallback)

    def _resolve(
        self, chain: str, protocol: str, pool_address: str, tick_ts: int
    ) -> tuple[HistoricalPoolStateTarget, HistoricalPoolStatePoint]:
        key = (chain.strip().lower(), protocol.strip().lower().replace("-", "_"), pool_address.strip().lower())
        series = self._series.get(key)
        if series is None:
            raise PoolPriceUnavailableError(pool_address, "exact pool was not declared and prewarmed")
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


class SnapshotPoolStateView:
    """Per-tick pool reader/registry backed by archive state."""

    def __init__(self, source: SnapshotPoolStateSource, tick_ts: int, *, fallback: Any | None = None) -> None:
        self._source = source
        self._tick_ts = tick_ts
        self._fallback = fallback
        self._snapshot: Any | None = None

    def protocols_for_chain(self, chain: str) -> list[str]:
        protocols = {key[1] for key in self._source._series if key[0] == chain.strip().lower()}
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
        chain_key = chain.strip().lower()
        match = next(
            (
                series.target.pool_address
                for series in self._source._series.values()
                if series.target.chain == chain_key
                and series.target.token_addresses is not None
                and requested == set(series.target.token_addresses)
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
        raise PoolPriceUnavailableError(pool_address, "exact pool was not declared and prewarmed")

    @staticmethod
    def _pool_price(target: HistoricalPoolStateTarget, point: HistoricalPoolStatePoint) -> DataEnvelope[PoolPrice]:
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
                confidence=1.0,
                cache_hit=True,
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
        return self._pool_price(target, point)

    async def get_pool_reserves(self, pool_address: str, chain: str) -> Any:
        from almanak.framework.data.defi.pools import DexType, PoolReserves
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
            dex=cast(DexType, target.protocol),
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
