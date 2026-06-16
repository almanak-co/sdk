"""Gateway-backed gas price provider for backtesting.

The public API of this module is intentionally compatible with the legacy
``EtherscanGasPriceProvider`` surface, but all live gas data now flows through
``RateHistoryService.GetGasPriceAt``. The framework process keeps cache and
fallback behavior; the gateway owns explorer and chain-node egress.
"""

from __future__ import annotations

import logging
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from almanak.core.chains import ChainRegistry
from almanak.core.enums import ChainFamily
from almanak.framework.data.interfaces import DataSourceUnavailable

from ..types import DataConfidence

if TYPE_CHECKING:
    from collections.abc import Iterator

    from almanak.framework.backtesting.config import BacktestDataConfig

logger = logging.getLogger(__name__)


@dataclass
class GasPrice:
    """Gas price data at a specific point in time."""

    timestamp: datetime
    chain: str
    base_fee_gwei: Decimal | None = None
    priority_fee_gwei: Decimal | None = None
    gas_price_gwei: Decimal | None = None
    source: str = "unknown"
    confidence: DataConfidence = DataConfidence.MEDIUM

    def __post_init__(self) -> None:
        if self.base_fee_gwei is None and self.priority_fee_gwei is None and self.gas_price_gwei is None:
            raise ValueError("At least one of base_fee_gwei, priority_fee_gwei, or gas_price_gwei must be set")

    @property
    def effective_gas_price_gwei(self) -> Decimal:
        """Return the effective gas price used for cost estimation."""
        if self.gas_price_gwei is not None:
            return self.gas_price_gwei
        return (self.base_fee_gwei or Decimal("0")) + (self.priority_fee_gwei or Decimal("0"))

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-compatible dictionary."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "chain": self.chain,
            "base_fee_gwei": str(self.base_fee_gwei) if self.base_fee_gwei is not None else None,
            "priority_fee_gwei": str(self.priority_fee_gwei) if self.priority_fee_gwei is not None else None,
            "gas_price_gwei": str(self.gas_price_gwei) if self.gas_price_gwei is not None else None,
            "source": self.source,
            "confidence": self.confidence.value,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> GasPrice:
        """Deserialize from :meth:`to_dict` output."""
        confidence_str = data.get("confidence", "medium")
        confidence = DataConfidence(confidence_str) if confidence_str else DataConfidence.MEDIUM
        return cls(
            timestamp=datetime.fromisoformat(data["timestamp"]),
            chain=data["chain"],
            base_fee_gwei=Decimal(data["base_fee_gwei"]) if data.get("base_fee_gwei") else None,
            priority_fee_gwei=Decimal(data["priority_fee_gwei"]) if data.get("priority_fee_gwei") else None,
            gas_price_gwei=Decimal(data["gas_price_gwei"]) if data.get("gas_price_gwei") else None,
            source=data.get("source", "unknown"),
            confidence=confidence,
        )


@dataclass
class GasPriceCache:
    """In-memory gas price cache with TTL."""

    data: dict[tuple[str, str], GasPrice] = field(default_factory=dict)
    ttl_seconds: int = 60
    _fetch_times: dict[tuple[str, str], datetime] = field(default_factory=dict)

    def _make_key(self, chain: str, timestamp: datetime) -> tuple[str, str]:
        rounded = _normalize_timestamp(timestamp).replace(second=0, microsecond=0)
        return (chain.lower(), rounded.isoformat())

    def get(self, chain: str, timestamp: datetime) -> GasPrice | None:
        key = self._make_key(chain, timestamp)
        gas_price = self.data.get(key)
        if gas_price is None:
            return None
        fetch_time = self._fetch_times.get(key)
        if fetch_time is not None and (datetime.now(UTC) - fetch_time).total_seconds() > self.ttl_seconds:
            self.data.pop(key, None)
            self._fetch_times.pop(key, None)
            return None
        return gas_price

    def set(self, gas_price: GasPrice) -> None:
        key = self._make_key(gas_price.chain, gas_price.timestamp)
        self.data[key] = gas_price
        self._fetch_times[key] = datetime.now(UTC)

    def set_batch(self, gas_prices: list[GasPrice]) -> int:
        now = datetime.now(UTC)
        for gas_price in gas_prices:
            key = self._make_key(gas_price.chain, gas_price.timestamp)
            self.data[key] = gas_price
            self._fetch_times[key] = now
        return len(gas_prices)

    def clear(self, chain: str | None = None) -> int:
        if chain is None:
            count = len(self.data)
            self.data.clear()
            self._fetch_times.clear()
            return count

        chain_lower = chain.lower()
        keys = [key for key in self.data if key[0] == chain_lower]
        for key in keys:
            self.data.pop(key, None)
            self._fetch_times.pop(key, None)
        return len(keys)

    def get_nearest(self, chain: str, timestamp: datetime, max_delta_seconds: int = 300) -> GasPrice | None:
        chain_lower = chain.lower()
        target = _normalize_timestamp(timestamp)
        best: GasPrice | None = None
        best_delta = float("inf")
        for (cached_chain, _), gas_price in self.data.items():
            if cached_chain != chain_lower:
                continue
            delta = abs((gas_price.timestamp - target).total_seconds())
            if delta <= max_delta_seconds and delta < best_delta:
                best_delta = delta
                best = gas_price
        return best


class GasPriceDataCache:
    """SQLite-backed gas price cache.

    ``get_interpolated`` is retained as a compatibility alias, but the gateway
    migration removed synthetic client-side interpolation.
    """

    def __init__(self, db_path: str | None = None, ttl_seconds: int = 86400) -> None:
        self._ttl_seconds = ttl_seconds
        self._db_path = db_path or str(Path.home() / ".almanak" / "cache" / "gas_price_cache.db")
        self._hits = 0
        self._misses = 0
        self._interpolations = 0
        if self._db_path != ":memory:":
            Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn: sqlite3.Connection | None = sqlite3.connect(self._db_path) if self._db_path == ":memory:" else None
        self._init_gas_table()

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        if self._db_path == ":memory:":
            if self._conn is None:
                raise RuntimeError("GasPriceDataCache is closed")
            yield self._conn
            return

        conn = sqlite3.connect(self._db_path)
        try:
            yield conn
        finally:
            conn.close()

    def _init_gas_table(self) -> None:
        with self._connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS gas_prices (
                    chain TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    base_fee_gwei TEXT,
                    priority_fee_gwei TEXT,
                    gas_price_gwei TEXT,
                    source TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (chain, timestamp)
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_gas_chain_timestamp
                ON gas_prices (chain, timestamp)
                """
            )
            conn.commit()

    def _is_expired(self, created_at_str: str) -> bool:
        if self._ttl_seconds <= 0:
            return False
        return (datetime.now(UTC) - datetime.fromisoformat(created_at_str)).total_seconds() > self._ttl_seconds

    def get(self, chain: str, timestamp: datetime) -> GasPrice | None:
        chain_lower = chain.lower()
        rounded = _normalize_timestamp(timestamp).replace(second=0, microsecond=0)
        with self._connection() as conn:
            row = conn.execute(
                """
                SELECT base_fee_gwei, priority_fee_gwei, gas_price_gwei, source, created_at
                FROM gas_prices
                WHERE chain = ? AND timestamp = ?
                """,
                (chain_lower, rounded.isoformat()),
            ).fetchone()
        if row is None:
            self._misses += 1
            return None
        base_fee, priority_fee, gas_price, source, created_at = row
        if self._is_expired(created_at):
            self._misses += 1
            return None
        self._hits += 1
        return _row_to_gas_price(rounded, chain_lower, base_fee, priority_fee, gas_price, source)

    def set(self, gas_price: GasPrice) -> None:
        self.set_batch([gas_price])

    def set_batch(self, gas_prices: list[GasPrice]) -> int:
        if not gas_prices:
            return 0
        now = datetime.now(UTC).isoformat()
        rows = [
            (
                gp.chain.lower(),
                _normalize_timestamp(gp.timestamp).replace(second=0, microsecond=0).isoformat(),
                str(gp.base_fee_gwei) if gp.base_fee_gwei is not None else None,
                str(gp.priority_fee_gwei) if gp.priority_fee_gwei is not None else None,
                str(gp.gas_price_gwei) if gp.gas_price_gwei is not None else None,
                gp.source,
                now,
            )
            for gp in gas_prices
        ]
        with self._connection() as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO gas_prices
                (chain, timestamp, base_fee_gwei, priority_fee_gwei, gas_price_gwei, source, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()
        return len(gas_prices)

    def get_interpolated(self, chain: str, timestamp: datetime, max_delta_seconds: int = 3600) -> GasPrice | None:
        """Return an exact cache hit only.

        ``max_delta_seconds`` remains in the signature for callers compiled
        against the historical interpolation surface.
        """
        _ = max_delta_seconds
        return self.get(chain, timestamp)

    def get_range(self, chain: str, start: datetime, end: datetime) -> list[GasPrice]:
        chain_lower = chain.lower()
        start = _normalize_timestamp(start)
        end = _normalize_timestamp(end)
        ttl_cutoff = (
            (datetime.now(UTC) - timedelta(seconds=self._ttl_seconds)).isoformat() if self._ttl_seconds > 0 else None
        )
        with self._connection() as conn:
            if ttl_cutoff is None:
                rows = conn.execute(
                    """
                    SELECT timestamp, base_fee_gwei, priority_fee_gwei, gas_price_gwei, source
                    FROM gas_prices
                    WHERE chain = ? AND timestamp >= ? AND timestamp <= ?
                    ORDER BY timestamp ASC
                    """,
                    (chain_lower, start.isoformat(), end.isoformat()),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT timestamp, base_fee_gwei, priority_fee_gwei, gas_price_gwei, source
                    FROM gas_prices
                    WHERE chain = ? AND timestamp >= ? AND timestamp <= ? AND created_at >= ?
                    ORDER BY timestamp ASC
                    """,
                    (chain_lower, start.isoformat(), end.isoformat(), ttl_cutoff),
                ).fetchall()
        return [
            _row_to_gas_price(
                _normalize_timestamp(datetime.fromisoformat(ts)), chain_lower, base, priority, gas, source
            )
            for ts, base, priority, gas, source in rows
        ]

    def clear(self, chain: str | None = None) -> int:
        with self._connection() as conn:
            if chain is None:
                cursor = conn.execute("DELETE FROM gas_prices")
            else:
                cursor = conn.execute("DELETE FROM gas_prices WHERE chain = ?", (chain.lower(),))
            conn.commit()
            return int(cursor.rowcount)

    def count(self, chain: str | None = None) -> int:
        with self._connection() as conn:
            if chain is None:
                row = conn.execute("SELECT COUNT(*) FROM gas_prices").fetchone()
            else:
                row = conn.execute("SELECT COUNT(*) FROM gas_prices WHERE chain = ?", (chain.lower(),)).fetchone()
        return int(row[0]) if row else 0

    @property
    def stats(self) -> dict[str, Any]:
        total = self._hits + self._misses
        return {
            "hits": self._hits,
            "misses": self._misses,
            "interpolations": self._interpolations,
            "hit_rate": self._hits / total if total > 0 else 0.0,
            "total_entries": self.count(),
        }

    def reset_stats(self) -> None:
        self._hits = 0
        self._misses = 0
        self._interpolations = 0

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None


@runtime_checkable
class GasPriceProvider(Protocol):
    """Protocol for gas price providers used by the PnL engine."""

    async def get_gas_price(self, timestamp: datetime | None = None, chain: str = "ethereum") -> GasPrice: ...

    async def get_gas_prices_range(
        self,
        start: datetime,
        end: datetime,
        chain: str = "ethereum",
        interval_seconds: int = 3600,
    ) -> list[GasPrice]: ...

    @property
    def provider_name(self) -> str: ...

    @property
    def supported_chains(self) -> list[str]: ...


def _build_etherscan_api_urls() -> dict[str, str]:
    return {
        d.name: d.explorer.api_url
        for d in ChainRegistry.all()
        if d.family is ChainFamily.EVM and d.explorer.api_url is not None
    }


ETHERSCAN_API_URLS: dict[str, str] = _build_etherscan_api_urls()


def _build_etherscan_api_key_env_vars() -> dict[str, str]:
    return {
        d.name: d.explorer.api_key_env
        for d in ChainRegistry.all()
        if d.family is ChainFamily.EVM and d.explorer.api_key_env is not None
    }


# Deprecated compatibility descriptor: gateway code owns API-key resolution.
ETHERSCAN_API_KEY_ENV_VARS: dict[str, str] = _build_etherscan_api_key_env_vars()


def _build_default_gas_prices() -> dict[str, dict[str, Decimal]]:
    return {
        d.name: {
            "base_fee": Decimal(str(d.gas.fallback_base_fee_gwei)),
            "priority_fee": Decimal(str(d.gas.fallback_priority_fee_gwei)),
        }
        for d in ChainRegistry.all()
        if d.gas.fallback_base_fee_gwei is not None and d.gas.fallback_priority_fee_gwei is not None
    }


DEFAULT_GAS_PRICES: dict[str, dict[str, Decimal]] = _build_default_gas_prices()

_DEFAULT_BLOCK_TIME_SECONDS: float = 12.0


def _block_time_for(chain: str) -> float:
    """Return descriptor block time, falling back to the legacy default."""
    descriptor = ChainRegistry.try_resolve(chain)
    if descriptor is None or descriptor.rpc.block_time_seconds is None:
        return _DEFAULT_BLOCK_TIME_SECONDS
    return descriptor.rpc.block_time_seconds


# Deprecated compatibility descriptors: archive RPC URLs are resolved by the
# gateway, not by the framework gas provider.
ARCHIVE_RPC_URL_ENV_PATTERN = "ARCHIVE_RPC_URL_{chain}"

ARCHIVE_RPC_CHAINS: list[str] = [
    d.name for d in ChainRegistry.all() if d.rpc.block_time_seconds is not None and d.explorer.api_url is not None
]


class EtherscanGasPriceProvider:
    """Gas price provider backed by the gateway RateHistory service."""

    _SUPPORTED_CHAINS = list(ETHERSCAN_API_URLS.keys())

    def __init__(
        self,
        api_keys: dict[str, str] | None = None,
        request_timeout: float = 30.0,
        min_request_interval: float = 0.25,
        cache_ttl_seconds: int = 60,
        persistent_cache: GasPriceDataCache | None = None,
        use_interpolation: bool = True,
        data_config: BacktestDataConfig | None = None,
        archive_rpc_urls: dict[str, str] | None = None,
        *,
        api_key: str | None = None,
    ) -> None:
        """Initialize the gateway-backed provider.

        ``api_keys``, ``api_key``, ``request_timeout``, ``min_request_interval``,
        and ``archive_rpc_urls`` are accepted for constructor compatibility.
        Secret and endpoint resolution happens inside the gateway.
        """
        self._cache = GasPriceCache(ttl_seconds=cache_ttl_seconds)
        self._persistent_cache = persistent_cache
        self._use_interpolation = use_interpolation
        self._data_config = data_config
        self._gateway_retry_after: datetime | None = None

        logger.info(
            "Initialized gateway-backed EtherscanGasPriceProvider",
            extra={
                "cache_ttl_seconds": cache_ttl_seconds,
                "persistent_cache": persistent_cache is not None,
                "persistent_cache_mode": "exact_only",
                "data_config": data_config is not None,
            },
        )

    async def close(self) -> None:
        """Compatibility hook. The shared gateway client owns its channel."""

    async def get_gas_price(self, timestamp: datetime | None = None, chain: str = "ethereum") -> GasPrice:
        chain_lower = chain.lower()
        if chain_lower not in ETHERSCAN_API_URLS:
            raise ValueError(f"Unsupported chain: {chain}. Supported: {', '.join(self._SUPPORTED_CHAINS)}")

        target = datetime.now(UTC) if timestamp is None else _normalize_timestamp(timestamp)

        cached = self._cache.get(chain_lower, target)
        if cached is not None:
            return cached

        if self._persistent_cache is not None:
            cached = (
                self._persistent_cache.get_interpolated(chain_lower, target)
                if self._use_interpolation
                else self._persistent_cache.get(chain_lower, target)
            )
            if cached is not None:
                self._cache.set(cached)
                return cached

        if self._gateway_retry_after is not None and datetime.now(UTC) < self._gateway_retry_after:
            gas_price = self._get_fallback_gas_price(target, chain_lower)
        else:
            try:
                gas_price = self._get_gateway_gas_price(
                    chain=chain_lower, timestamp=target, is_current=timestamp is None
                )
                self._gateway_retry_after = None
            except DataSourceUnavailable as exc:
                retry_after = exc.retry_after if exc.retry_after is not None else float(self._cache.ttl_seconds)
                self._gateway_retry_after = (
                    datetime.now(UTC) + timedelta(seconds=retry_after) if retry_after > 0 else None
                )
                logger.warning("Gateway gas price unavailable for %s at %s: %s", chain_lower, target, exc)
                gas_price = self._get_fallback_gas_price(target, chain_lower)

        self._cache.set(gas_price)
        if self._persistent_cache is not None:
            self._persistent_cache.set(gas_price)
        return gas_price

    def _get_gateway_gas_price(self, *, chain: str, timestamp: datetime, is_current: bool) -> GasPrice:
        client, gateway_pb2 = _get_connected_gateway_client()
        request = gateway_pb2.GetGasPriceAtRequest(
            chain=chain,
            timestamp=0 if is_current else int(timestamp.timestamp()),
        )
        try:
            response = client.rate_history.GetGasPriceAt(request)
        except Exception as exc:
            raise DataSourceUnavailable(source="gateway", reason=f"GetGasPriceAt RPC failed: {exc}") from exc
        if not response.success:
            raise DataSourceUnavailable(
                source=response.source or "gateway",
                reason=response.error or "GetGasPriceAt returned success=false",
            )
        return _gas_price_from_gateway_response(response)

    async def get_gas_prices_range(
        self,
        start: datetime,
        end: datetime,
        chain: str = "ethereum",
        interval_seconds: int = 3600,
    ) -> list[GasPrice]:
        chain_lower = chain.lower()
        if chain_lower not in ETHERSCAN_API_URLS:
            raise ValueError(f"Unsupported chain: {chain}")
        if interval_seconds <= 0:
            raise ValueError(f"interval_seconds must be > 0, got {interval_seconds}")

        start = _normalize_timestamp(start)
        end = _normalize_timestamp(end)
        if end < start:
            raise ValueError("end must be >= start")

        gas_prices: list[GasPrice] = []
        current = start
        interval = timedelta(seconds=interval_seconds)
        while current <= end:
            gas_prices.append(await self.get_gas_price(current, chain_lower))
            current += interval
        return gas_prices

    def set_historical_gas_prices(self, gas_prices: list[GasPrice]) -> int:
        count = self._cache.set_batch(gas_prices)
        if self._persistent_cache is not None:
            self._persistent_cache.set_batch(gas_prices)
        return count

    def clear_cache(self, chain: str | None = None) -> int:
        count = self._cache.clear(chain)
        if self._persistent_cache is not None:
            self._persistent_cache.clear(chain)
        return count

    def _get_fallback_gas_price(self, timestamp: datetime, chain: str) -> GasPrice:
        if self._data_config is not None:
            gas_price = self._data_config.gas_fallback_gwei
        else:
            defaults = DEFAULT_GAS_PRICES.get(chain, DEFAULT_GAS_PRICES["ethereum"])
            gas_price = defaults["base_fee"] + defaults["priority_fee"]

        return GasPrice(
            timestamp=timestamp,
            chain=chain,
            gas_price_gwei=gas_price,
            source="config_fallback",
            confidence=DataConfidence.LOW,
        )

    @property
    def provider_name(self) -> str:
        return "etherscan"

    @property
    def supported_chains(self) -> list[str]:
        return self._SUPPORTED_CHAINS.copy()

    async def __aenter__(self) -> EtherscanGasPriceProvider:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()


def _get_connected_gateway_client() -> tuple[Any, Any]:
    try:
        from almanak.framework.gateway_client import get_gateway_client
        from almanak.gateway.proto import gateway_pb2
    except ImportError as exc:
        raise DataSourceUnavailable(source="gateway", reason=f"Gateway client unavailable: {exc}") from exc

    client = get_gateway_client()
    if not client.is_connected:
        try:
            client.connect()
        except Exception as exc:
            raise DataSourceUnavailable(source="gateway", reason=f"Gateway connect failed: {exc}") from exc
    return client, gateway_pb2


def _gas_price_from_gateway_response(response: Any) -> GasPrice:
    point = response.point
    timestamp = datetime.fromtimestamp(point.timestamp, tz=UTC)
    return GasPrice(
        timestamp=timestamp,
        chain=response.chain,
        base_fee_gwei=_decimal_from_proto(point.base_fee_gwei),
        priority_fee_gwei=_decimal_from_proto(point.priority_fee_gwei),
        gas_price_gwei=_decimal_from_proto(point.gas_price_gwei),
        source=response.source or "gateway",
        confidence=DataConfidence.HIGH,
    )


def _decimal_from_proto(value: str) -> Decimal | None:
    return Decimal(value) if value else None


def _normalize_timestamp(timestamp: datetime) -> datetime:
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=UTC)
    return timestamp.astimezone(UTC)


def _row_to_gas_price(
    timestamp: datetime,
    chain: str,
    base_fee: str | None,
    priority_fee: str | None,
    gas_price: str | None,
    source: str,
) -> GasPrice:
    return GasPrice(
        timestamp=timestamp,
        chain=chain,
        base_fee_gwei=Decimal(base_fee) if base_fee else None,
        priority_fee_gwei=Decimal(priority_fee) if priority_fee else None,
        gas_price_gwei=Decimal(gas_price) if gas_price else None,
        source=source,
    )


__all__ = [
    "GasPrice",
    "GasPriceCache",
    "GasPriceDataCache",
    "GasPriceProvider",
    "EtherscanGasPriceProvider",
    "ETHERSCAN_API_URLS",
    "ETHERSCAN_API_KEY_ENV_VARS",
    "DEFAULT_GAS_PRICES",
    "ARCHIVE_RPC_URL_ENV_PATTERN",
    "ARCHIVE_RPC_CHAINS",
]
