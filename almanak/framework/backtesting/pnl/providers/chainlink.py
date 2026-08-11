"""Thin gateway client for Chainlink live and historical prices.

Feed metadata, ABI handling, archive RPC access and round traversal are owned by
``almanak.integrations.chainlink`` in the gateway.  This module intentionally
contains no HTTP/Web3 client and never reads an RPC URL.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from almanak.core.chains import ChainRegistry
from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.integrations.chainlink.catalog import (
    CATALOG,
    CHAINLINK_DEVIATION_THRESHOLDS,
    CHAINLINK_HEARTBEATS,
    CHAINLINK_PRICE_FEEDS,
    DEFAULT_CHAINLINK_CHAIN,
    LEGACY_ARCHIVE_RPC_CHAINS,
    TOKEN_TO_PAIR,
)
from almanak.integrations.chainlink.codec import (
    DECIMALS_SELECTOR,
    GET_ROUND_DATA_SELECTOR,
    LATEST_ROUND_DATA_SELECTOR,
)

from ..data_provider import (
    OHLCV,
    HistoricalDataCapability,
    HistoricalDataConfig,
    HistoricalPriceObservation,
    MarketState,
    TokenRef,
    token_ref_display,
)
from ..types import DataConfidence, DataSourceInfo

logger = logging.getLogger(__name__)

# Compatibility constants. They describe client/cache policy only; transport
# configuration is gateway-owned.
ARCHIVE_RPC_URL_ENV_PATTERN = "ARCHIVE_RPC_URL_{chain}"
ARCHIVE_RPC_CHAINS = list(LEGACY_ARCHIVE_RPC_CHAINS)
DEFAULT_CACHE_DIR = ".almanak_cache/chainlink"
DATA_STALENESS_THRESHOLD_SECONDS = 3600
MAX_ROUNDS_TO_FETCH = 50_000
MAX_BINARY_SEARCH_ITERATIONS = 50
MAX_MULTICALL_BATCH_SIZE = 100
_HISTORY_REQUEST_MAX_POINTS = 10_000
_HISTORY_REQUEST_MAX_WINDOW = timedelta(days=366)
_PERSISTENT_CACHE_SCHEMA_VERSION = 2


class ChainlinkStaleDataError(Exception):
    def __init__(self, token: str, age_seconds: float, heartbeat_seconds: int, updated_at: datetime) -> None:
        self.token = token
        self.age_seconds = age_seconds
        self.heartbeat_seconds = heartbeat_seconds
        self.updated_at = updated_at
        super().__init__(
            f"Chainlink data for {token} is stale: updated {age_seconds:.0f}s ago (heartbeat: {heartbeat_seconds}s)"
        )


@dataclass(frozen=True)
class ChainlinkRoundData:
    round_id: int
    answer: int
    started_at: int
    updated_at: int
    answered_in_round: int


@dataclass(frozen=True)
class ChainlinkPriceFeed:
    address: str
    pair: str
    decimals: int = 8
    heartbeat_seconds: int = 3600
    deviation_threshold: Decimal = Decimal("1.0")


@dataclass(frozen=True)
class BinarySearchResult:
    round_id: int
    round_data: ChainlinkRoundData
    exact_match: bool = False


@dataclass(frozen=True)
class ChainlinkPriceResult:
    price: Decimal
    timestamp: datetime
    round_id: int
    confidence: DataConfidence
    source_info: DataSourceInfo
    is_stale: bool = False


@dataclass
class CachedPrice:
    price: Decimal
    timestamp: datetime
    fetched_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    ttl_seconds: int = 60

    @property
    def is_expired(self) -> bool:
        return (datetime.now(UTC) - self.fetched_at).total_seconds() > self.ttl_seconds

    @property
    def age_seconds(self) -> float:
        return (datetime.now(UTC) - self.fetched_at).total_seconds()


@dataclass
class PersistentCacheConfig:
    enabled: bool = False
    cache_dir: str = DEFAULT_CACHE_DIR
    max_age_days: int = 30

    def get_cache_path(self, chain: str) -> Path | None:
        if not self.enabled:
            return None
        path = Path(self.cache_dir)
        path.mkdir(parents=True, exist_ok=True)
        return path / f"{chain}_prices_v2.json"


class PriceCache:
    """Local materialized-view cache; no provider transport state."""

    def __init__(self, ttl_seconds: int = 60) -> None:
        self.ttl_seconds = ttl_seconds
        self.data: dict[str, list[tuple[datetime, Decimal]]] = {}
        self.coverage: dict[str, list[tuple[datetime, datetime]]] = {}
        self._live: dict[str, CachedPrice] = {}

    def get_price_at(self, token: str, timestamp: datetime) -> Decimal | None:
        return _price_at(self.data.get(token.upper(), ()), timestamp)

    def get_live_price(self, token: str) -> CachedPrice | None:
        value = self._live.get(token.upper())
        if value is not None and value.is_expired:
            self._live.pop(token.upper(), None)
            return None
        return value

    def set_live_price(self, token: str, price: Decimal, timestamp: datetime | None = None) -> None:
        self._live[token.upper()] = CachedPrice(
            price=price,
            timestamp=_utc(timestamp or datetime.now(UTC)),
            ttl_seconds=self.ttl_seconds,
        )

    def clear_live_cache(self, token: str | None = None) -> None:
        if token is None:
            self._live.clear()
        else:
            self._live.pop(token.upper(), None)

    def add_coverage(self, token: str, start: datetime, end: datetime) -> None:
        """Record a fully fetched interval without inferring coverage from sparse points."""
        start_utc, end_utc = _utc(start), _utc(end)
        if end_utc < start_utc:
            raise ValueError("Historical cache coverage requires start <= end")
        ranges = sorted([*self.coverage.get(token.upper(), ()), (start_utc, end_utc)])
        merged: list[tuple[datetime, datetime]] = []
        for range_start, range_end in ranges:
            if not merged or range_start > merged[-1][1]:
                merged.append((range_start, range_end))
                continue
            previous_start, previous_end = merged[-1]
            merged[-1] = (previous_start, max(previous_end, range_end))
        self.coverage[token.upper()] = merged

    def covers(self, token: str, start: datetime, end: datetime) -> bool:
        start_utc, end_utc = _utc(start), _utc(end)
        return any(
            range_start <= start_utc and range_end >= end_utc
            for range_start, range_end in self.coverage.get(token.upper(), ())
        )

    def get_cache_stats(self) -> dict[str, Any]:
        return {
            "live_tokens": sorted(self._live),
            "live_count": len(self._live),
            "live_expired_count": sum(value.is_expired for value in self._live.values()),
            "historical_tokens": sorted(self.data),
            "historical_count": len(self.data),
            "total_historical_points": sum(len(points) for points in self.data.values()),
            "historical_coverage_ranges": sum(len(ranges) for ranges in self.coverage.values()),
            "ttl_seconds": self.ttl_seconds,
        }


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _observation_at(
    points: Sequence[tuple[datetime, Decimal]],
    timestamp: datetime,
) -> tuple[datetime, Decimal] | None:
    target = _utc(timestamp)
    eligible = [(ts, price) for ts, price in points if _utc(ts) <= target]
    return max(eligible, key=lambda item: _utc(item[0])) if eligible else None


def _price_at(points: Sequence[tuple[datetime, Decimal]], timestamp: datetime) -> Decimal | None:
    observation = _observation_at(points, timestamp)
    return observation[1] if observation is not None else None


def _connected_gateway() -> tuple[Any, Any]:
    from almanak.framework.gateway_client import get_gateway_client
    from almanak.gateway.proto import gateway_pb2

    client = get_gateway_client()
    if not client.is_connected:
        client.connect()
    return client, gateway_pb2


class ChainlinkDataProvider:
    """Gateway-backed Chainlink historical-data provider."""

    DEFAULT_PRIORITY = 10
    _SUPPORTED_TOKENS = list(TOKEN_TO_PAIR)
    _SUPPORTED_CHAINS = list(CATALOG.chains)

    def __init__(
        self,
        chain: str = DEFAULT_CHAINLINK_CHAIN,
        rpc_url: str = "",
        cache_ttl_seconds: int = 60,
        priority: int | None = None,
        persistent_cache_config: PersistentCacheConfig | None = None,
    ) -> None:
        descriptor = ChainRegistry.try_resolve(chain)
        self._chain = descriptor.name if descriptor is not None else chain.lower()
        if not CATALOG.supports_chain(self._chain):
            raise ValueError(f"Unsupported chain: {chain}. Available chains: {', '.join(CATALOG.chains)}")
        if rpc_url:
            logger.warning(
                "ChainlinkDataProvider.rpc_url is deprecated and ignored; configure the gateway with "
                "ARCHIVE_RPC_URL_%s instead",
                self._chain.upper(),
            )
        if cache_ttl_seconds < 0:
            raise ValueError("cache_ttl_seconds must be non-negative")
        self._rpc_url = ""  # compatibility introspection; never populated or used
        self._cache_ttl_seconds = cache_ttl_seconds
        self._priority = self.DEFAULT_PRIORITY if priority is None else priority
        self._persistent_cache_config = persistent_cache_config or PersistentCacheConfig()
        # Historical materializations are independent of the live-price TTL.
        # Keep the cache object even when TTL=0 so preloaded backtests work.
        self._cache = PriceCache(max(cache_ttl_seconds, 0))
        self._archive_access_verified = False
        self._has_archive_access = False
        self._price_feeds = CHAINLINK_PRICE_FEEDS[self._chain]
        self._load_persistent_cache()

    @property
    def priority(self) -> int:
        return self._priority

    @property
    def cache_ttl_seconds(self) -> int:
        return self._cache_ttl_seconds

    def get_feed_address(self, token: str) -> str | None:
        spec = CATALOG.feed_for_token(self._chain, token)
        return spec.address if spec is not None else None

    def get_feed_config(self, token: str) -> ChainlinkPriceFeed | None:
        spec = CATALOG.feed_for_token(self._chain, token)
        if spec is None:
            return None
        return ChainlinkPriceFeed(
            address=spec.address,
            pair=spec.pair,
            decimals=spec.decimals,
            heartbeat_seconds=spec.heartbeat_seconds,
            deviation_threshold=spec.deviation_threshold_pct,
        )

    @staticmethod
    def _convert_price(raw_answer: int, decimals: int) -> Decimal:
        return Decimal(raw_answer) / Decimal(10**decimals)

    def is_data_stale(
        self,
        updated_at: datetime,
        token: str,
        current_time: datetime | None = None,
    ) -> bool:
        feed = self.get_feed_config(token)
        heartbeat = feed.heartbeat_seconds if feed else CHAINLINK_HEARTBEATS["default"]
        age = (_utc(current_time or datetime.now(UTC)) - _utc(updated_at)).total_seconds()
        return age > heartbeat * 1.1

    def _check_staleness(
        self,
        token: str,
        updated_at: datetime,
        current_time: datetime | None = None,
        *,
        raise_on_stale: bool = True,
    ) -> bool:
        stale = self.is_data_stale(updated_at, token, current_time)
        if stale and raise_on_stale:
            feed = self.get_feed_config(token)
            heartbeat = feed.heartbeat_seconds if feed else CHAINLINK_HEARTBEATS["default"]
            now = _utc(current_time or datetime.now(UTC))
            raise ChainlinkStaleDataError(token, (now - _utc(updated_at)).total_seconds(), heartbeat, _utc(updated_at))
        return stale

    def _cached_latest_price(
        self,
        token: str,
        *,
        use_cache: bool,
        raise_on_stale: bool,
    ) -> tuple[bool, Decimal | None]:
        if not use_cache or self._cache_ttl_seconds <= 0:
            return False, None
        cached = self._cache.get_live_price(token)
        if cached is None:
            return False, None
        if self._check_staleness(token, cached.timestamp, raise_on_stale=raise_on_stale):
            return True, None
        return True, cached.price

    def _latest_price_from_response(self, token: str, response: Any, *, raise_on_stale: bool) -> Decimal | None:
        if not response.success:
            raise DataSourceUnavailable(source="chainlink", reason=response.error or "current price unavailable")
        if not response.point.price:
            raise DataSourceUnavailable(source="chainlink", reason="current oracle observation is empty")
        try:
            price = Decimal(response.point.price)
        except (ArithmeticError, ValueError) as exc:
            raise DataSourceUnavailable(source="chainlink", reason="current oracle price is malformed") from exc
        if not price.is_finite() or price <= 0 or response.point.timestamp <= 0:
            raise DataSourceUnavailable(source="chainlink", reason="current oracle observation is invalid")
        updated_at = datetime.fromtimestamp(response.point.timestamp, tz=UTC)
        if self._check_staleness(token, updated_at, raise_on_stale=raise_on_stale):
            return None
        if self._cache_ttl_seconds > 0:
            self._cache.set_live_price(token, price, updated_at)
        return price

    def _cached_history_covering(
        self,
        token: str,
        start: datetime,
        end: datetime,
    ) -> list[tuple[datetime, Decimal]] | None:
        points = self._cache.data.get(token.upper(), [])
        if not points or not self._cache.covers(token, start, end):
            return None
        return points

    async def get_latest_price(self, token: str, use_cache: bool = True, raise_on_stale: bool = True) -> Decimal | None:
        token_upper = token.upper()
        if self.get_feed_config(token_upper) is None:
            return None
        cache_hit, cached_price = self._cached_latest_price(
            token_upper,
            use_cache=use_cache,
            raise_on_stale=raise_on_stale,
        )
        if cache_hit:
            return cached_price
        client, gateway_pb2 = _connected_gateway()
        request = gateway_pb2.GetOraclePriceRequest(
            provider="chainlink",
            token=token_upper,
            chain=self._chain,
        )
        try:
            response = await asyncio.to_thread(client.rate_history.GetOraclePrice, request)
        except Exception as exc:
            raise DataSourceUnavailable(source="gateway", reason=f"GetOraclePrice RPC failed: {exc}") from exc
        return self._latest_price_from_response(token_upper, response, raise_on_stale=raise_on_stale)

    def get_latest_price_sync(self, token: str, use_cache: bool = True, raise_on_stale: bool = True) -> Decimal | None:
        token_upper = token.upper()
        if self.get_feed_config(token_upper) is None:
            return None
        cache_hit, cached_price = self._cached_latest_price(
            token_upper,
            use_cache=use_cache,
            raise_on_stale=raise_on_stale,
        )
        if cache_hit:
            return cached_price
        client, gateway_pb2 = _connected_gateway()
        response = client.rate_history.GetOraclePrice(
            gateway_pb2.GetOraclePriceRequest(provider="chainlink", token=token_upper, chain=self._chain)
        )
        return self._latest_price_from_response(token_upper, response, raise_on_stale=raise_on_stale)

    async def _fetch_history(self, token: str, start: datetime, end: datetime) -> list[tuple[datetime, Decimal]]:
        client, gateway_pb2 = _connected_gateway()
        start_utc, end_utc = _utc(start), _utc(end)
        points: list[tuple[datetime, Decimal]] = []
        pending: list[tuple[datetime, datetime]] = []
        cursor = start_utc
        while cursor < end_utc:
            chunk_end = min(cursor + _HISTORY_REQUEST_MAX_WINDOW, end_utc)
            pending.append((cursor, chunk_end))
            cursor = chunk_end

        request_count = 0
        while pending:
            cursor, chunk_end = pending.pop()
            request_count += 1
            if request_count > 256:
                raise DataSourceUnavailable(source="chainlink", reason="history pagination exceeded its request bound")
            request = gateway_pb2.GetOraclePriceHistoryRequest(
                provider="chainlink",
                chain=self._chain,
                token=token.upper(),
                start_ts=int(cursor.timestamp()),
                end_ts=int(chunk_end.timestamp()),
                max_points=_HISTORY_REQUEST_MAX_POINTS,
            )
            try:
                response = await asyncio.to_thread(client.rate_history.GetOraclePriceHistory, request)
            except Exception as exc:
                raise DataSourceUnavailable(
                    source="gateway",
                    reason=f"GetOraclePriceHistory RPC failed: {exc}",
                ) from exc
            if not response.success:
                raise DataSourceUnavailable(source="chainlink", reason=response.error or "history unavailable")
            chunk_points = [self._decode_history_point(point) for point in response.points]
            if getattr(response, "truncated", False) or len(chunk_points) >= _HISTORY_REQUEST_MAX_POINTS:
                split_ts = getattr(response, "recommended_split_ts", 0) or int(
                    (cursor.timestamp() + chunk_end.timestamp()) // 2
                )
                split = datetime.fromtimestamp(split_ts, tz=UTC)
                if not cursor < split < chunk_end:
                    raise DataSourceUnavailable(
                        source="chainlink",
                        reason="history pagination returned a non-progressing split point",
                    )
                # LIFO stack: append right first so the left/older half is read first.
                pending.extend(((split, chunk_end), (cursor, split)))
                continue
            points.extend(chunk_points)
        deduplicated = {int(timestamp.timestamp()): (timestamp, price) for timestamp, price in points}
        return [deduplicated[key] for key in sorted(deduplicated)]

    @staticmethod
    def _decode_history_point(point: Any) -> tuple[datetime, Decimal]:
        try:
            price = Decimal(point.price)
        except (ArithmeticError, ValueError) as exc:
            raise DataSourceUnavailable(source="chainlink", reason="history contains a malformed price") from exc
        if point.timestamp <= 0 or not price.is_finite() or price <= 0:
            raise DataSourceUnavailable(source="chainlink", reason="history contains an invalid oracle observation")
        return datetime.fromtimestamp(point.timestamp, tz=UTC), price

    async def get_price(self, token: str, timestamp: datetime | None = None, raise_on_stale: bool = True) -> Decimal:
        token_upper = token.upper()
        if token_upper not in TOKEN_TO_PAIR:
            raise ValueError(f"Unknown token: {token}")
        if self.get_feed_config(token_upper) is None:
            raise ValueError(f"No Chainlink feed available for {token} on {self._chain}")
        now = datetime.now(UTC)
        requested = _utc(timestamp or now)
        if abs((now - requested).total_seconds()) <= 60:
            latest = await self.get_latest_price(token_upper, raise_on_stale=raise_on_stale)
            if latest is not None:
                return latest
        cached = self._cache.get_price_at(token_upper, requested)
        if cached is not None:
            return cached
        history_start = requested - timedelta(hours=2)
        history_end = requested + timedelta(seconds=1)
        history = await self._fetch_history(token_upper, history_start, history_end)
        if history:
            self.set_historical_prices(token_upper, history, covered_range=(history_start, history_end))
            fetched = _price_at(history, requested)
            if fetched is not None:
                return fetched
        raise ValueError(f"Historical price data for {token} at {requested} not available through the gateway")

    async def get_ohlcv(
        self,
        token: str,
        start: datetime,
        end: datetime,
        interval_seconds: int = 3600,
    ) -> list[OHLCV]:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        if self.get_feed_config(token) is None:
            raise ValueError(f"No Chainlink feed available for {token} on {self._chain}")
        token_upper = token.upper()
        start_utc, end_utc = _utc(start), _utc(end)
        cached_points = self._cached_history_covering(token_upper, start_utc, end_utc)
        if cached_points is None:
            fetched_points = await self._fetch_history(token, start, end)
            self.set_historical_prices(token, fetched_points, covered_range=(start_utc, end_utc))
            cached_points = self._cached_history_covering(token_upper, start_utc, end_utc) or fetched_points
        output: list[OHLCV] = []
        current = start_utc
        while current <= end_utc:
            price = _price_at(cached_points, current)
            if price is not None:
                output.append(OHLCV(timestamp=current, open=price, high=price, low=price, close=price, volume=None))
            current += timedelta(seconds=interval_seconds)
        return output

    def set_historical_prices(
        self,
        token: str,
        prices: list[tuple[datetime, Decimal]],
        *,
        covered_range: tuple[datetime, datetime] | None = None,
    ) -> None:
        token_upper = token.upper()
        merged = {_utc(timestamp): price for timestamp, price in self._cache.data.get(token_upper, ())}
        merged.update({_utc(timestamp): price for timestamp, price in prices})
        self._cache.data[token_upper] = sorted(merged.items(), key=lambda item: item[0])
        if covered_range is None and prices:
            timestamps = [_utc(timestamp) for timestamp, _price in prices]
            covered_range = min(timestamps), max(timestamps)
        if covered_range is not None:
            self._cache.add_coverage(token_upper, *covered_range)
        self._save_persistent_cache()

    def clear_cache(self, token: str | None = None) -> None:
        if token is None:
            self._cache.data.clear()
            self._cache.coverage.clear()
            self._cache.clear_live_cache()
        else:
            self._cache.data.pop(token.upper(), None)
            self._cache.coverage.pop(token.upper(), None)
            self._cache.clear_live_cache(token)

    def get_cache_stats(self) -> dict[str, Any]:
        return {**self._cache.get_cache_stats(), "caching_enabled": self._cache_ttl_seconds > 0}

    def set_cache_ttl(self, ttl_seconds: int) -> None:
        if ttl_seconds < 0:
            raise ValueError("ttl_seconds must be non-negative")
        self._cache_ttl_seconds = ttl_seconds
        if ttl_seconds <= 0:
            self._cache.clear_live_cache()
        else:
            self._cache.ttl_seconds = ttl_seconds

    async def _verify_archive_access(self) -> bool:
        if self._archive_access_verified:
            return self._has_archive_access
        token = next(iter(self.supported_tokens), None)
        if token is None:
            self._archive_access_verified = True
            return False
        try:
            now = datetime.now(UTC)
            await self._fetch_history(token, now - timedelta(hours=2), now)
            self._has_archive_access = True
        except (DataSourceUnavailable, ValueError):
            self._has_archive_access = False
        self._archive_access_verified = True
        return self._has_archive_access

    async def verify_archive_access(self) -> bool:
        return await self._verify_archive_access()

    async def iterate(self, config: HistoricalDataConfig) -> AsyncIterator[tuple[datetime, MarketState]]:
        unsupported = [token_ref_display(token) for token in config.tokens if not isinstance(token, str)]
        if unsupported:
            raise DataSourceUnavailable(
                source="chainlink",
                reason="Chainlink iteration supports symbol tokens only: " + ", ".join(unsupported),
            )
        has_gateway_history = await self._verify_archive_access()
        if not has_gateway_history:
            logger.warning(
                "Gateway Chainlink history is unavailable; iteration is restricted to preloaded cache coverage"
            )
        series: dict[str, list[tuple[datetime, Decimal]]] = {}
        source_by_token: dict[str, str] = {}
        for token in config.tokens:
            assert isinstance(token, str)
            token_upper = token.upper()
            points = self._cached_history_covering(token_upper, config.start_time, config.end_time)
            if points is None:
                if not has_gateway_history:
                    raise DataSourceUnavailable(
                        source="chainlink",
                        reason=(
                            "Gateway Chainlink history is unavailable and the local cache does not cover "
                            f"{token_upper} for the requested interval"
                        ),
                    )
                fetched_points = await self._fetch_history(token, config.start_time, config.end_time)
                self.set_historical_prices(
                    token,
                    fetched_points,
                    covered_range=(config.start_time, config.end_time),
                )
                points = (
                    self._cached_history_covering(token_upper, config.start_time, config.end_time) or fetched_points
                )
                source_by_token[token_upper] = "historical"
            else:
                source_by_token[token_upper] = "cache"
            series[token_upper] = points
        current, end = _utc(config.start_time), _utc(config.end_time)
        interval = timedelta(seconds=config.interval_seconds)
        while current <= end:
            prices: dict[TokenRef, Decimal] = {}
            ohlcv: dict[TokenRef, OHLCV] = {}
            price_observations: dict[TokenRef, HistoricalPriceObservation] = {}
            historical_price_hits = 0
            cache_price_hits = 0
            for token in config.tokens:
                assert isinstance(token, str)
                observation = _observation_at(series[token.upper()], current)
                if observation is None:
                    continue
                observed_at, price = observation
                prices[token.upper()] = price
                price_observations[token.upper()] = HistoricalPriceObservation(
                    price=price,
                    timestamp=_utc(observed_at),
                    source=f"chainlink_{source_by_token[token.upper()]}",
                    confidence=DataConfidence.HIGH,
                )
                if source_by_token[token.upper()] == "historical":
                    historical_price_hits += 1
                else:
                    cache_price_hits += 1
                if config.include_ohlcv:
                    ohlcv[token.upper()] = OHLCV(
                        timestamp=current, open=price, high=price, low=price, close=price, volume=None
                    )
            if not prices:
                raise DataSourceUnavailable(source="chainlink", reason=f"No prices at {current.isoformat()}")
            active_sources = {source_by_token[str(token).upper()] for token in prices}
            if active_sources == {"historical"}:
                data_source = "chainlink_historical"
            elif active_sources == {"cache"}:
                data_source = "chainlink_cache"
            else:
                data_source = "chainlink_mixed"
            yield (
                current,
                MarketState(
                    timestamp=current,
                    prices=prices,
                    ohlcv=ohlcv,
                    chain=config.chains[0] if config.chains else self._chain,
                    block_number=None,
                    gas_price_gwei=None,
                    price_observations=price_observations,
                    metadata={
                        "data_source": data_source,
                        "historical_price_hits": historical_price_hits,
                        "cache_price_hits": cache_price_hits,
                    },
                ),
            )
            current += interval

    def _load_persistent_cache(self) -> None:
        path = self._persistent_cache_config.get_cache_path(self._chain)
        if path is None or not path.exists():
            return
        try:
            payload = json.loads(path.read_text())
            if payload.get("schema_version") != _PERSISTENT_CACHE_SCHEMA_VERSION:
                raise ValueError("unsupported Chainlink cache schema")
            if payload.get("chain") != self._chain:
                raise ValueError("Chainlink cache chain mismatch")
            cached_at = datetime.fromisoformat(payload.get("cached_at", "1970-01-01T00:00:00+00:00"))
            if (datetime.now(UTC) - _utc(cached_at)).days > self._persistent_cache_config.max_age_days:
                return
            data = {
                token: [(datetime.fromtimestamp(point[0], tz=UTC), Decimal(point[1])) for point in points]
                for token, points in payload.get("prices", {}).items()
            }
            coverage = {
                token: [
                    (datetime.fromtimestamp(interval[0], tz=UTC), datetime.fromtimestamp(interval[1], tz=UTC))
                    for interval in intervals
                ]
                for token, intervals in payload.get("coverage", {}).items()
            }
            if any(not price.is_finite() or price <= 0 for points in data.values() for _timestamp, price in points):
                raise ValueError("Chainlink cache contains invalid prices")
            # Hydrate atomically only after every section has parsed.
            self._cache.data = data
            self._cache.coverage = coverage
        except Exception as exc:
            logger.warning("Failed to load Chainlink client cache: %s", exc)

    def _save_persistent_cache(self) -> None:
        path = self._persistent_cache_config.get_cache_path(self._chain)
        if path is None:
            return
        payload = {
            "schema_version": _PERSISTENT_CACHE_SCHEMA_VERSION,
            "cached_at": datetime.now(UTC).isoformat(),
            "chain": self._chain,
            "prices": {
                token: [[int(timestamp.timestamp()), str(price)] for timestamp, price in points]
                for token, points in self._cache.data.items()
            },
            "coverage": {
                token: [[int(start.timestamp()), int(end.timestamp())] for start, end in intervals]
                for token, intervals in self._cache.coverage.items()
            },
        }
        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
                temporary_path = Path(handle.name)
                json.dump(payload, handle, indent=2)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary_path, path)
        except OSError as exc:
            logger.warning("Failed to save Chainlink client cache: %s", exc)
        finally:
            if temporary_path is not None and temporary_path.exists():
                temporary_path.unlink(missing_ok=True)

    @property
    def provider_name(self) -> str:
        return f"chainlink_{self._chain}"

    @property
    def supported_tokens(self) -> list[str]:
        return [token for token in TOKEN_TO_PAIR if CATALOG.feed_for_token(self._chain, token) is not None]

    @property
    def supported_chains(self) -> list[str]:
        return list(CATALOG.chains)

    @property
    def min_timestamp(self) -> datetime | None:
        return datetime(2020, 1, 1, tzinfo=UTC)

    @property
    def max_timestamp(self) -> datetime | None:
        return datetime.now(UTC) - timedelta(seconds=15)

    @property
    def historical_capability(self) -> HistoricalDataCapability:
        return HistoricalDataCapability.FULL if self._has_archive_access else HistoricalDataCapability.PRE_CACHE

    async def close(self) -> None:
        return None

    async def __aenter__(self) -> ChainlinkDataProvider:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()


__all__ = [
    "ARCHIVE_RPC_CHAINS",
    "ARCHIVE_RPC_URL_ENV_PATTERN",
    "BinarySearchResult",
    "CHAINLINK_DEVIATION_THRESHOLDS",
    "CHAINLINK_HEARTBEATS",
    "CHAINLINK_PRICE_FEEDS",
    "CachedPrice",
    "ChainlinkDataProvider",
    "ChainlinkPriceFeed",
    "ChainlinkPriceResult",
    "ChainlinkRoundData",
    "ChainlinkStaleDataError",
    "DATA_STALENESS_THRESHOLD_SECONDS",
    "DECIMALS_SELECTOR",
    "GET_ROUND_DATA_SELECTOR",
    "LATEST_ROUND_DATA_SELECTOR",
    "MAX_BINARY_SEARCH_ITERATIONS",
    "MAX_MULTICALL_BATCH_SIZE",
    "MAX_ROUNDS_TO_FETCH",
    "PersistentCacheConfig",
    "PriceCache",
    "TOKEN_TO_PAIR",
]
