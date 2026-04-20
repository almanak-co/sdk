"""StateManager with tiered persistence.

Implements two-tier state storage:
- HOT: In-memory cache (<1ms access)
- WARM: PostgreSQL or SQLite (<10ms access)

Uses CAS (Compare-And-Swap) semantics via a version field for safe
concurrent updates.  Each agent has exactly one row in the WARM tier
(single-row-per-agent model).

Important: Each strategy uses exactly one gateway and vice versa.
No two strategies share a gateway.
"""

import hashlib
import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum, IntEnum, auto
from typing import TYPE_CHECKING, Any, Optional, Protocol, runtime_checkable

if TYPE_CHECKING:
    from almanak.framework.execution.clob_handler import ClobFill, ClobOrderState, ClobOrderStatus
    from almanak.framework.portfolio import PortfolioMetrics, PortfolioSnapshot

logger = logging.getLogger(__name__)


# =============================================================================
# EXCEPTIONS
# =============================================================================


class StateConflictError(Exception):
    """Raised when CAS update fails due to version mismatch.

    This error indicates that another process has modified the state
    since it was last read. The caller should reload the state and retry.
    """

    def __init__(
        self,
        strategy_id: str,
        expected_version: int,
        actual_version: int,
        message: str | None = None,
    ) -> None:
        self.strategy_id = strategy_id
        self.expected_version = expected_version
        self.actual_version = actual_version
        super().__init__(
            message
            or f"State conflict for strategy {strategy_id}: expected version {expected_version}, found {actual_version}"
        )


class StateNotFoundError(Exception):
    """Raised when state is not found in any tier."""

    def __init__(self, strategy_id: str, message: str | None = None) -> None:
        self.strategy_id = strategy_id
        super().__init__(message or f"State not found for strategy {strategy_id}")


# =============================================================================
# ENUMS
# =============================================================================


class StateTier(IntEnum):
    """Storage tier for state data.

    Ordered by access speed (fastest first).
    """

    HOT = 1  # In-memory cache (<1ms)
    WARM = 2  # PostgreSQL or SQLite (<10ms)


class WarmBackendType(Enum):
    """Type of backend to use for WARM tier storage.

    Attributes:
        POSTGRESQL: Production PostgreSQL database.
        SQLITE: Local SQLite database for development/lightweight deployments.
    """

    POSTGRESQL = auto()
    SQLITE = auto()


# =============================================================================
# PROTOCOLS
# =============================================================================


@runtime_checkable
class WarmStore(Protocol):
    """Protocol for WARM tier storage backends.

    Both PostgresStore and SQLiteStore implement this interface for
    consistent behavior across backends.

    Methods:
        initialize: Initialize the backend (create connections, schema, etc.)
        close: Close connections and release resources.
        get: Get state for a strategy.
        save: Save state with optional CAS semantics.
        delete: Delete/deactivate state for a strategy.
    """

    async def initialize(self) -> None:
        """Initialize the backend."""
        ...

    async def close(self) -> None:
        """Close the backend and release resources."""
        ...

    async def get(self, strategy_id: str) -> Optional["StateData"]:
        """Get active state for a strategy.

        Args:
            strategy_id: Strategy identifier.

        Returns:
            StateData if found, None otherwise.
        """
        ...

    async def save(
        self,
        state: "StateData",
        expected_version: int | None = None,
    ) -> bool:
        """Save state with optional CAS semantics.

        Args:
            state: State data to save.
            expected_version: Expected current version for CAS update.

        Returns:
            True if save succeeded.

        Raises:
            StateConflictError: If expected_version doesn't match.
        """
        ...

    async def delete(self, strategy_id: str) -> bool:
        """Delete/deactivate state for a strategy.

        Args:
            strategy_id: Strategy identifier.

        Returns:
            True if state was deleted.
        """
        ...


# =============================================================================
# DATA CLASSES
# =============================================================================


@dataclass
class StateData:
    """Strategy state data container.

    Attributes:
        strategy_id: Unique identifier for the strategy
        version: CAS version number (incremented on each update)
        state: The actual state data as a dictionary
        schema_version: Schema version for migrations
        checksum: SHA-256 hash of state data for integrity verification
        created_at: When this state version was created
        loaded_from: Which tier the state was loaded from
    """

    strategy_id: str
    version: int
    state: dict[str, Any]
    schema_version: int = 1
    checksum: str = ""
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    loaded_from: StateTier | None = None

    def __post_init__(self) -> None:
        """Calculate checksum if not provided."""
        if not self.checksum:
            self.checksum = self._calculate_checksum()

    def _calculate_checksum(self) -> str:
        """Calculate SHA-256 checksum of state data."""
        state_str = json.dumps(self.state, sort_keys=True, default=str)
        return hashlib.sha256(state_str.encode()).hexdigest()

    def verify_checksum(self) -> bool:
        """Verify the integrity of state data."""
        return self.checksum == self._calculate_checksum()

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "strategy_id": self.strategy_id,
            "version": self.version,
            "state": self.state,
            "schema_version": self.schema_version,
            "checksum": self.checksum,
            "created_at": self.created_at.isoformat(),
            "loaded_from": self.loaded_from.name if self.loaded_from else None,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "StateData":
        """Create StateData from dictionary."""
        loaded_from = None
        if data.get("loaded_from"):
            loaded_from = StateTier[data["loaded_from"]]

        created_at = data.get("created_at")
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        elif created_at is None:
            created_at = datetime.now(UTC)

        return cls(
            strategy_id=data["strategy_id"],
            version=data["version"],
            state=data["state"],
            schema_version=data.get("schema_version", 1),
            checksum=data.get("checksum", ""),
            created_at=created_at,
            loaded_from=loaded_from,
        )


@dataclass
class TierMetrics:
    """Metrics for a single tier operation.

    Attributes:
        tier: Which tier was accessed
        operation: Type of operation (load, save, delete)
        latency_ms: Operation latency in milliseconds
        success: Whether the operation succeeded
        error: Error message if operation failed
        timestamp: When the operation occurred
    """

    tier: StateTier
    operation: str
    latency_ms: float
    success: bool
    error: str | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for logging/metrics."""
        return {
            "tier": self.tier.name,
            "operation": self.operation,
            "latency_ms": self.latency_ms,
            "success": self.success,
            "error": self.error,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class PostgresConfig:
    """PostgreSQL connection configuration.

    Attributes:
        host: Database host
        port: Database port
        database: Database name
        user: Database user
        password: Database password
        pool_min_size: Minimum connection pool size
        pool_max_size: Maximum connection pool size
    """

    host: str = "localhost"
    port: int = 5432
    database: str = "almanak"
    user: str = "almanak"
    password: str = ""
    pool_min_size: int = 2
    pool_max_size: int = 10

    @property
    def dsn(self) -> str:
        """Generate PostgreSQL DSN string."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class SQLiteConfigLight:
    """Lightweight SQLite configuration for StateManagerConfig.

    This mirrors SQLiteConfig from backends.sqlite but avoids circular imports.
    The full SQLiteConfig class should be used when creating SQLiteStore directly.

    Attributes:
        db_path: Path to SQLite database file. Use ":memory:" for in-memory DB.
        wal_mode: Enable WAL mode for better concurrent read performance.
    """

    db_path: str = "./almanak_state.db"
    wal_mode: bool = True


@dataclass
class StateManagerConfig:
    """Configuration for StateManager.

    Attributes:
        enable_hot: Enable in-memory cache tier
        enable_warm: Enable WARM tier (PostgreSQL or SQLite)
        warm_backend: Which backend to use for WARM tier (POSTGRESQL or SQLITE)
        hot_cache_ttl_seconds: TTL for hot cache entries (0 = no expiry)
        hot_cache_max_size: Maximum entries in hot cache
        postgres_config: PostgreSQL configuration (used when warm_backend=POSTGRESQL)
        sqlite_config: SQLite configuration (used when warm_backend=SQLITE)
        metrics_callback: Optional callback for metrics reporting
        load_state_on_startup: Load all active states from WARM to HOT on startup

    Example:
        # PostgreSQL backend (default, production)
        config = StateManagerConfig(
            warm_backend=WarmBackendType.POSTGRESQL,
            postgres_config=PostgresConfig(host="localhost"),
        )

        # SQLite backend (local development)
        config = StateManagerConfig(
            warm_backend=WarmBackendType.SQLITE,
            sqlite_config=SQLiteConfigLight(db_path="./state.db"),
        )
    """

    enable_hot: bool = True
    enable_warm: bool = True
    warm_backend: WarmBackendType = WarmBackendType.POSTGRESQL
    hot_cache_ttl_seconds: int = 0  # 0 = no expiry
    hot_cache_max_size: int = 1000
    database_url: str | None = None  # Direct URL (overrides postgres_config)
    postgres_config: PostgresConfig = field(default_factory=PostgresConfig)
    sqlite_config: SQLiteConfigLight = field(default_factory=SQLiteConfigLight)
    metrics_callback: Callable[[TierMetrics], None] | None = None
    load_state_on_startup: bool = True


# =============================================================================
# HOT TIER - IN-MEMORY CACHE
# =============================================================================


class HotCache:
    """In-memory cache with optional TTL and size limits.

    Provides <1ms access for frequently accessed states.
    """

    def __init__(
        self,
        ttl_seconds: int = 0,
        max_size: int = 1000,
    ) -> None:
        self._cache: dict[str, tuple[StateData, float]] = {}  # strategy_id -> (data, timestamp)
        self._ttl_seconds = ttl_seconds
        self._max_size = max_size

    def get(self, strategy_id: str) -> StateData | None:
        """Get state from cache.

        Returns None if not found or expired.
        """
        entry = self._cache.get(strategy_id)
        if entry is None:
            return None

        data, timestamp = entry

        # Check TTL if enabled
        if self._ttl_seconds > 0:
            if time.time() - timestamp > self._ttl_seconds:
                del self._cache[strategy_id]
                return None

        return data

    def set(self, state: StateData) -> None:
        """Store state in cache.

        Evicts oldest entry if cache is full.
        """
        # Evict oldest if at capacity
        if len(self._cache) >= self._max_size and state.strategy_id not in self._cache:
            self._evict_oldest()

        self._cache[state.strategy_id] = (state, time.time())

    def delete(self, strategy_id: str) -> bool:
        """Delete state from cache.

        Returns True if entry was deleted, False if not found.
        """
        if strategy_id in self._cache:
            del self._cache[strategy_id]
            return True
        return False

    def clear(self) -> None:
        """Clear all entries from cache."""
        self._cache.clear()

    def _evict_oldest(self) -> None:
        """Evict the oldest cache entry."""
        if not self._cache:
            return

        oldest_key = min(self._cache.keys(), key=lambda k: self._cache[k][1])
        del self._cache[oldest_key]

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        return {
            "size": len(self._cache),
            "max_size": self._max_size,
            "ttl_seconds": self._ttl_seconds,
        }


# =============================================================================
# WARM TIER - POSTGRESQL
# =============================================================================


class PostgresStore:
    """PostgreSQL state storage.

    Provides <10ms access with full ACID guarantees and CAS support.
    Uses plain SQL (no stored functions) so it works against any
    PostgreSQL-compatible database without pre-applied migrations.

    Can be initialised with either a ``PostgresConfig`` or a raw
    ``database_url`` string.  When a URL is given the ``?schema=``
    query parameter (if present) is stripped and applied as
    ``search_path`` on every connection.
    """

    def __init__(self, config: PostgresConfig | None = None, *, database_url: str | None = None) -> None:
        self._schema: str | None = None
        if database_url:
            from almanak.gateway.database import _strip_schema_param

            self._dsn, self._schema = _strip_schema_param(database_url)
        elif config:
            self._dsn = config.dsn
        else:
            raise ValueError("PostgresStore requires either config or database_url")
        self._pool_min = config.pool_min_size if config else 2
        self._pool_max = config.pool_max_size if config else 10
        self._pool: Any = None  # asyncpg.Pool
        self._initialized = False

    async def initialize(self) -> None:
        """Initialize connection pool."""
        if self._initialized:
            return

        try:
            import asyncpg

            async def _init_connection(conn):
                if self._schema:
                    await conn.fetchval(
                        "SELECT pg_catalog.set_config('search_path', $1, false)",
                        self._schema,
                    )

            self._pool = await asyncpg.create_pool(
                dsn=self._dsn,
                min_size=self._pool_min,
                max_size=self._pool_max,
                init=_init_connection,
                statement_cache_size=0,
            )
            self._initialized = True
            logger.info("PostgreSQL connection pool initialized")
        except ImportError:
            logger.warning("asyncpg not installed, PostgreSQL tier disabled")
            raise
        except Exception as e:
            logger.error(f"Failed to initialize PostgreSQL pool: {e}")
            raise

    async def close(self) -> None:
        """Close connection pool."""
        if self._pool:
            await self._pool.close()
            self._initialized = False

    async def get(self, strategy_id: str) -> StateData | None:
        """Get state from PostgreSQL (single row per agent)."""
        if not self._initialized:
            await self.initialize()

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT version, state_data, schema_version,
                       checksum, created_at
                FROM v2_strategy_state
                WHERE agent_id = $1
                """,
                strategy_id,
            )

            if row is None:
                return None

            return StateData(
                strategy_id=strategy_id,
                version=row["version"],
                state=json.loads(row["state_data"]) if isinstance(row["state_data"], str) else row["state_data"],
                schema_version=row["schema_version"],
                checksum=row["checksum"] or "",
                created_at=row["created_at"],
                loaded_from=StateTier.WARM,
            )

    async def save(self, state: StateData, expected_version: int | None = None) -> bool:
        """Save state to PostgreSQL with optional CAS semantics.

        Single-row-per-agent model: uses UPSERT when *expected_version* is
        ``None``, or a version-guarded UPDATE for CAS.

        Returns:
            True if save succeeded.

        Raises:
            StateConflictError: If expected_version doesn't match current version.
        """
        if not self._initialized:
            await self.initialize()

        state_json = json.dumps(state.state, default=str)

        async with self._pool.acquire() as conn:
            if expected_version is None:
                # UPSERT: insert new or overwrite existing (version increments)
                await conn.execute(
                    """
                    INSERT INTO v2_strategy_state
                        (agent_id, version, state_data, schema_version, checksum,
                         created_at, updated_at)
                    VALUES ($1, $2, $3::jsonb, $4, $5, now(), now())
                    ON CONFLICT (agent_id) DO UPDATE SET
                        version = v2_strategy_state.version + 1,
                        state_data = EXCLUDED.state_data,
                        schema_version = EXCLUDED.schema_version,
                        checksum = EXCLUDED.checksum,
                        updated_at = now()
                    """,
                    state.strategy_id,
                    state.version,
                    state_json,
                    state.schema_version,
                    state.checksum,
                )
                return True
            else:
                # CAS update -- inline version check
                result = await conn.execute(
                    """
                    UPDATE v2_strategy_state
                    SET version = version + 1,
                        state_data = $3::jsonb,
                        schema_version = $4,
                        checksum = $5,
                        updated_at = now()
                    WHERE agent_id = $1
                      AND version = $2
                    """,
                    state.strategy_id,
                    expected_version,
                    state_json,
                    state.schema_version,
                    state.checksum,
                )

                if result == "UPDATE 0":
                    # Version mismatch -- get actual version for the error
                    actual = await conn.fetchval(
                        "SELECT version FROM v2_strategy_state WHERE agent_id = $1",
                        state.strategy_id,
                    )
                    raise StateConflictError(
                        strategy_id=state.strategy_id,
                        expected_version=expected_version,
                        actual_version=actual or 0,
                    )

                return True

    async def delete(self, strategy_id: str) -> bool:
        """Delete state row for a strategy.

        Returns True if state was deleted.
        """
        if not self._initialized:
            await self.initialize()

        async with self._pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM v2_strategy_state WHERE agent_id = $1",
                strategy_id,
            )
            return result != "DELETE 0"

    async def get_all_strategy_ids(self) -> list[str]:
        """Return all strategy IDs (for HOT cache warm-up)."""
        if not self._initialized:
            await self.initialize()

        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT agent_id FROM v2_strategy_state")
            return [row["agent_id"] for row in rows]


# =============================================================================
# STATE MANAGER
# =============================================================================


class StateManager:
    """Tiered state manager with HOT and WARM storage tiers.

    Provides:
    - <1ms access from HOT (in-memory) cache
    - <10ms access from WARM (PostgreSQL or SQLite) storage
    - CAS semantics for safe concurrent updates
    - Automatic tier fallback on load
    - Metrics tracking for each tier
    - Write-through from HOT to WARM tier

    The WARM tier backend can be either PostgreSQL (production) or SQLite
    (development/lightweight). Backend selection is via configuration:

    Usage:
        # PostgreSQL backend (default, production)
        config = StateManagerConfig(
            warm_backend=WarmBackendType.POSTGRESQL,
            postgres_config=PostgresConfig(host="localhost"),
        )
        manager = StateManager(config)
        await manager.initialize()

        # SQLite backend (development)
        config = StateManagerConfig(
            warm_backend=WarmBackendType.SQLITE,
            sqlite_config=SQLiteConfigLight(db_path="./state.db"),
        )
        manager = StateManager(config)
        await manager.initialize()

        # Save state (writes to HOT then WARM)
        state = StateData(strategy_id="strat-1", version=1, state={"key": "value"})
        await manager.save_state(state)

        # Load state (reads from fastest available tier)
        loaded = await manager.load_state("strat-1")

        # CAS update
        loaded.state["key"] = "new_value"
        await manager.save_state(loaded, expected_version=loaded.version)

        # Dependency injection: provide custom backend
        custom_sqlite = SQLiteStore(SQLiteConfig(db_path="./custom.db"))
        manager = StateManager(config, warm_backend=custom_sqlite)
    """

    def __init__(
        self,
        config: StateManagerConfig | None = None,
        warm_backend: WarmStore | None = None,
    ) -> None:
        """Initialize StateManager.

        Args:
            config: Configuration for the state manager. Uses defaults if not provided.
            warm_backend: Optional pre-configured WARM tier backend.
                If provided, this backend is used instead of creating one from config.
                Useful for dependency injection and testing.
        """
        self._config = config or StateManagerConfig()
        self._hot: HotCache | None = None
        self._warm: WarmStore | None = warm_backend
        self._warm_injected = warm_backend is not None
        self._metrics: list[TierMetrics] = []
        self._initialized = False

    async def initialize(self) -> None:
        """Initialize all enabled storage tiers.

        If load_state_on_startup is enabled in config, loads all active states
        from WARM tier to HOT tier for fast access.
        """
        if self._initialized:
            return

        # Initialize HOT tier (always works, no external dependencies)
        if self._config.enable_hot:
            self._hot = HotCache(
                ttl_seconds=self._config.hot_cache_ttl_seconds,
                max_size=self._config.hot_cache_max_size,
            )
            logger.info("HOT tier (in-memory cache) initialized")

        # Initialize WARM tier (PostgreSQL or SQLite)
        if self._config.enable_warm and not self._warm_injected:
            try:
                self._warm = await self._create_warm_backend()
            except ImportError as e:
                logger.warning(f"WARM tier disabled: {e}")
                self._warm = None
            except Exception as e:
                logger.warning(f"WARM tier disabled: {e}")
                self._warm = None
        elif self._warm_injected and self._warm is not None:
            # Initialize injected backend if not already initialized
            try:
                await self._warm.initialize()
                backend_type = type(self._warm).__name__
                logger.info(f"WARM tier ({backend_type}) initialized (injected)")
            except Exception as e:
                logger.warning(f"WARM tier (injected) initialization failed: {e}")
                self._warm = None

        self._initialized = True

        # Load states from WARM to HOT on startup
        if self._config.load_state_on_startup and self._warm and self._hot:
            await self._load_warm_to_hot()

    async def _create_warm_backend(self) -> WarmStore | None:
        """Create and initialize the WARM tier backend based on configuration.

        Returns:
            Initialized WarmStore instance, or None if initialization fails.
        """
        if self._config.warm_backend == WarmBackendType.SQLITE:
            return await self._create_sqlite_backend()
        else:
            return await self._create_postgres_backend()

    async def _create_sqlite_backend(self) -> WarmStore | None:
        """Create and initialize SQLite backend.

        Returns:
            Initialized SQLiteStore instance.
        """
        # Import here to avoid circular imports
        from .backends.sqlite import SQLiteConfig, SQLiteStore

        sqlite_config = SQLiteConfig(
            db_path=self._config.sqlite_config.db_path,
            wal_mode=self._config.sqlite_config.wal_mode,
        )
        store = SQLiteStore(sqlite_config)
        await store.initialize()
        logger.info(f"WARM tier (SQLite) initialized: {sqlite_config.db_path}")
        return store

    async def _create_postgres_backend(self) -> WarmStore | None:
        """Create and initialize PostgreSQL backend.

        Uses ``database_url`` when available (gateway-provided), otherwise
        falls back to ``PostgresConfig``.

        Returns:
            Initialized PostgresStore instance.
        """
        if self._config.database_url:
            store = PostgresStore(database_url=self._config.database_url)
        else:
            store = PostgresStore(self._config.postgres_config)
        await store.initialize()
        logger.info("WARM tier (PostgreSQL) initialized")
        return store

    async def _load_warm_to_hot(self) -> None:
        """Load all active states from WARM tier to HOT tier.

        This is called on startup when load_state_on_startup is enabled.
        Ensures fast access to frequently used states.
        """
        if not self._warm or not self._hot:
            return

        try:
            # Check if backend supports listing all strategy IDs
            if hasattr(self._warm, "get_all_strategy_ids"):
                strategy_ids = await self._warm.get_all_strategy_ids()
                loaded_count = 0
                for strategy_id in strategy_ids:
                    try:
                        state = await self._warm.get(strategy_id)
                        if state:
                            self._hot.set(state)
                            loaded_count += 1
                    except Exception as e:
                        logger.warning(f"Failed to load state {strategy_id} to HOT: {e}")

                if loaded_count > 0:
                    logger.info(f"Loaded {loaded_count} states from WARM to HOT tier on startup")
            else:
                logger.debug("WARM backend does not support get_all_strategy_ids, skipping startup load")
        except Exception as e:
            logger.warning(f"Failed to load states from WARM to HOT on startup: {e}")

    async def close(self) -> None:
        """Close all storage connections."""
        if self._warm:
            await self._warm.close()

        if self._hot:
            self._hot.clear()

        self._initialized = False

    def _record_metrics(
        self,
        tier: StateTier,
        operation: str,
        latency_ms: float,
        success: bool,
        error: str | None = None,
    ) -> None:
        """Record metrics for a tier operation."""
        metrics = TierMetrics(
            tier=tier,
            operation=operation,
            latency_ms=latency_ms,
            success=success,
            error=error,
        )
        self._metrics.append(metrics)

        # Call metrics callback if configured
        if self._config.metrics_callback:
            try:
                self._config.metrics_callback(metrics)
            except Exception as e:
                logger.warning(f"Metrics callback failed: {e}")

        # Log slow operations
        thresholds = {StateTier.HOT: 1.0, StateTier.WARM: 10.0}
        if latency_ms > thresholds.get(tier, 100.0):
            logger.warning(
                f"Slow {tier.name} tier {operation}: {latency_ms:.2f}ms (threshold: {thresholds.get(tier)}ms)"
            )

    async def load_state(self, strategy_id: str) -> StateData:
        """Load state from the fastest available tier.

        Tries tiers in order: HOT -> WARM.
        Populates HOT cache on WARM hit.

        Args:
            strategy_id: Strategy identifier

        Returns:
            StateData from the fastest available tier

        Raises:
            StateNotFoundError: If state not found in any tier
        """
        if not self._initialized:
            await self.initialize()

        # Try HOT tier first
        if self._hot:
            start = time.perf_counter()
            state = self._hot.get(strategy_id)
            latency = (time.perf_counter() - start) * 1000

            if state:
                self._record_metrics(StateTier.HOT, "load", latency, True)
                state.loaded_from = StateTier.HOT
                return state
            else:
                self._record_metrics(StateTier.HOT, "load", latency, True, "cache_miss")

        # Try WARM tier
        if self._warm:
            start = time.perf_counter()
            try:
                state = await self._warm.get(strategy_id)
                latency = (time.perf_counter() - start) * 1000

                if state:
                    self._record_metrics(StateTier.WARM, "load", latency, True)
                    state.loaded_from = StateTier.WARM

                    # Populate HOT cache
                    if self._hot:
                        self._hot.set(state)

                    return state
                else:
                    self._record_metrics(StateTier.WARM, "load", latency, True, "not_found")
            except Exception as e:
                latency = (time.perf_counter() - start) * 1000
                self._record_metrics(StateTier.WARM, "load", latency, False, str(e))
                logger.error(f"WARM tier load failed: {e}")

        raise StateNotFoundError(strategy_id)

    async def save_state(
        self,
        state: StateData,
        expected_version: int | None = None,
    ) -> StateData:
        """Save state to all tiers.

        Writes to WARM tier (source of truth) then updates HOT cache.

        Args:
            state: State data to save
            expected_version: Expected version for CAS update.
                            If None and state has version > 1, uses state.version - 1.
                            If None and state has version = 1, creates new state.

        Returns:
            Updated StateData with new version

        Raises:
            StateConflictError: If CAS update fails due to version mismatch
        """
        if not self._initialized:
            await self.initialize()

        # Determine expected version for CAS
        if expected_version is None and state.version > 1:
            expected_version = state.version - 1

        # Recalculate checksum
        state.checksum = state._calculate_checksum()
        state.created_at = datetime.now(UTC)

        # Save to WARM tier first (source of truth)
        if self._warm:
            start = time.perf_counter()
            try:
                await self._warm.save(state, expected_version)
                latency = (time.perf_counter() - start) * 1000
                self._record_metrics(StateTier.WARM, "save", latency, True)

                # Get updated version (PostgreSQL auto-increments)
                updated = await self._warm.get(state.strategy_id)
                if updated:
                    state = updated
            except StateConflictError:
                latency = (time.perf_counter() - start) * 1000
                self._record_metrics(StateTier.WARM, "save", latency, False, "version_conflict")
                raise
            except Exception as e:
                latency = (time.perf_counter() - start) * 1000
                self._record_metrics(StateTier.WARM, "save", latency, False, str(e))
                logger.error(f"WARM tier save failed: {e}")
                raise

        # Update HOT tier
        if self._hot:
            start = time.perf_counter()
            self._hot.set(state)
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.HOT, "save", latency, True)

        return state

    async def delete_state(self, strategy_id: str) -> bool:
        """Delete state from all tiers.

        Args:
            strategy_id: Strategy identifier

        Returns:
            True if state was deleted from at least one tier
        """
        if not self._initialized:
            await self.initialize()

        deleted = False

        # Delete from HOT tier
        if self._hot:
            start = time.perf_counter()
            hot_deleted = self._hot.delete(strategy_id)
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.HOT, "delete", latency, True)
            deleted = deleted or hot_deleted

        # Delete from WARM tier
        if self._warm:
            start = time.perf_counter()
            try:
                warm_deleted = await self._warm.delete(strategy_id)
                latency = (time.perf_counter() - start) * 1000
                self._record_metrics(StateTier.WARM, "delete", latency, True)
                deleted = deleted or warm_deleted
            except Exception as e:
                latency = (time.perf_counter() - start) * 1000
                self._record_metrics(StateTier.WARM, "delete", latency, False, str(e))
                logger.error(f"WARM tier delete failed: {e}")

        return deleted

    def invalidate_hot_cache(self, strategy_id: str | None = None) -> None:
        """Invalidate HOT tier cache.

        Args:
            strategy_id: Specific strategy to invalidate, or None to clear all
        """
        if not self._hot:
            return

        if strategy_id:
            self._hot.delete(strategy_id)
        else:
            self._hot.clear()

    def get_metrics(self, limit: int = 100) -> list[TierMetrics]:
        """Get recent tier metrics.

        Args:
            limit: Maximum number of metrics to return

        Returns:
            List of TierMetrics, newest first
        """
        return self._metrics[-limit:][::-1]

    def get_metrics_summary(self) -> dict[str, Any]:
        """Get summary of tier metrics.

        Returns:
            Dictionary with per-tier average latencies and success rates
        """
        summary: dict[str, Any] = {}

        for tier in StateTier:
            tier_metrics = [m for m in self._metrics if m.tier == tier]
            if not tier_metrics:
                continue

            successful = [m for m in tier_metrics if m.success]
            summary[tier.name] = {
                "total_operations": len(tier_metrics),
                "successful_operations": len(successful),
                "success_rate": len(successful) / len(tier_metrics) if tier_metrics else 0,
                "avg_latency_ms": (sum(m.latency_ms for m in successful) / len(successful) if successful else 0),
                "max_latency_ms": max((m.latency_ms for m in successful), default=0),
                "min_latency_ms": min((m.latency_ms for m in successful), default=0),
            }

        return summary

    def clear_metrics(self) -> None:
        """Clear all stored metrics."""
        self._metrics.clear()

    @property
    def is_initialized(self) -> bool:
        """Check if StateManager is initialized."""
        return self._initialized

    @property
    def enabled_tiers(self) -> list[StateTier]:
        """Get list of enabled and initialized tiers."""
        tiers = []
        if self._hot:
            tiers.append(StateTier.HOT)
        if self._warm:
            tiers.append(StateTier.WARM)
        return tiers

    @property
    def warm_backend_type(self) -> WarmBackendType | None:
        """Get the type of WARM backend being used.

        Returns:
            WarmBackendType.SQLITE, WarmBackendType.POSTGRESQL, or None if no WARM tier.
        """
        if not self._warm:
            return None

        # Check the actual type of the backend
        warm_type_name = type(self._warm).__name__
        if "SQLite" in warm_type_name:
            return WarmBackendType.SQLITE
        elif "Postgres" in warm_type_name:
            return WarmBackendType.POSTGRESQL
        else:
            # Unknown backend type, return configured type
            return self._config.warm_backend

    @property
    def warm_backend(self) -> WarmStore | None:
        """Get the WARM tier backend instance.

        Useful for accessing backend-specific functionality like
        get_version_history() on SQLiteStore.

        Returns:
            The WARM backend instance, or None if not initialized.
        """
        return self._warm

    # -------------------------------------------------------------------------
    # CLOB Order State Management
    # -------------------------------------------------------------------------

    async def save_clob_order(self, order: "ClobOrderState") -> bool:
        """Save or update a CLOB order state.

        Persists order state to the WARM tier for crash recovery
        and order tracking across strategy restarts.

        Args:
            order: ClobOrderState to persist.

        Returns:
            True if save succeeded, False if no WARM backend or error.
        """
        if not self._initialized:
            await self.initialize()

        if not self._warm:
            logger.warning("Cannot save CLOB order: no WARM backend configured")
            return False

        # Check if backend supports CLOB orders (SQLiteStore does)
        if not hasattr(self._warm, "save_clob_order"):
            logger.warning("WARM backend does not support CLOB order storage")
            return False

        start = time.perf_counter()
        try:
            result = await self._warm.save_clob_order(order)  # type: ignore[attr-defined]
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "save_clob_order", latency, True)
            return result
        except Exception as e:
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "save_clob_order", latency, False, str(e))
            logger.error(f"Failed to save CLOB order: {e}")
            return False

    async def get_clob_order(self, order_id: str) -> "ClobOrderState | None":
        """Get a CLOB order by order_id.

        Args:
            order_id: Order identifier.

        Returns:
            ClobOrderState if found, None otherwise.
        """
        if not self._initialized:
            await self.initialize()

        if not self._warm:
            logger.warning("Cannot get CLOB order: no WARM backend configured")
            return None

        if not hasattr(self._warm, "get_clob_order"):
            logger.warning("WARM backend does not support CLOB order storage")
            return None

        start = time.perf_counter()
        try:
            result = await self._warm.get_clob_order(order_id)  # type: ignore[attr-defined]
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "get_clob_order", latency, True)
            return result
        except Exception as e:
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "get_clob_order", latency, False, str(e))
            logger.error(f"Failed to get CLOB order: {e}")
            return None

    async def get_open_clob_orders(self, market_id: str | None = None) -> list["ClobOrderState"]:
        """Get all open CLOB orders, optionally filtered by market.

        Open orders are those with status: pending, submitted, live, partially_filled.

        Args:
            market_id: Optional market ID to filter by.

        Returns:
            List of open ClobOrderState, newest first.
        """
        if not self._initialized:
            await self.initialize()

        if not self._warm:
            logger.warning("Cannot get open CLOB orders: no WARM backend configured")
            return []

        if not hasattr(self._warm, "get_open_clob_orders"):
            logger.warning("WARM backend does not support CLOB order storage")
            return []

        start = time.perf_counter()
        try:
            result = await self._warm.get_open_clob_orders(market_id)  # type: ignore[attr-defined]
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "get_open_clob_orders", latency, True)
            return result
        except Exception as e:
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "get_open_clob_orders", latency, False, str(e))
            logger.error(f"Failed to get open CLOB orders: {e}")
            return []

    async def update_clob_order_status(
        self,
        order_id: str,
        status: "ClobOrderStatus",
        fills: list["ClobFill"] | None = None,
        filled_size: str | None = None,
        average_fill_price: str | None = None,
        error: str | None = None,
    ) -> bool:
        """Update the status and fill information of a CLOB order.

        Args:
            order_id: Order identifier.
            status: New order status.
            fills: Updated list of fills (replaces existing).
            filled_size: Updated filled size.
            average_fill_price: Updated average fill price.
            error: Error message if order failed.

        Returns:
            True if order was found and updated.
        """
        if not self._initialized:
            await self.initialize()

        if not self._warm:
            logger.warning("Cannot update CLOB order: no WARM backend configured")
            return False

        if not hasattr(self._warm, "update_clob_order_status"):
            logger.warning("WARM backend does not support CLOB order storage")
            return False

        start = time.perf_counter()
        try:
            result = await self._warm.update_clob_order_status(  # type: ignore[attr-defined]
                order_id=order_id,
                status=status,
                fills=fills,
                filled_size=filled_size,
                average_fill_price=average_fill_price,
                error=error,
            )
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "update_clob_order_status", latency, True)
            return result
        except Exception as e:
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "update_clob_order_status", latency, False, str(e))
            logger.error(f"Failed to update CLOB order status: {e}")
            return False

    # -------------------------------------------------------------------------
    # Portfolio Snapshot Management
    # -------------------------------------------------------------------------

    async def save_portfolio_snapshot(self, snapshot: "PortfolioSnapshot") -> int:
        """Save a portfolio snapshot.

        Persists portfolio value and position data for dashboard display
        and PnL tracking.

        Args:
            snapshot: PortfolioSnapshot to persist.

        Returns:
            Snapshot ID if save succeeded, 0 if no WARM backend or error.
        """
        if not self._initialized:
            await self.initialize()

        if not self._warm:
            logger.warning("Cannot save portfolio snapshot: no WARM backend configured")
            return 0

        if not hasattr(self._warm, "save_portfolio_snapshot"):
            logger.warning("WARM backend does not support portfolio snapshot storage")
            return 0

        start = time.perf_counter()
        try:
            result = await self._warm.save_portfolio_snapshot(snapshot)  # type: ignore[attr-defined]
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "save_portfolio_snapshot", latency, True)
            return result
        except Exception as e:
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "save_portfolio_snapshot", latency, False, str(e))
            logger.error(f"Failed to save portfolio snapshot: {e}")
            return 0

    async def get_latest_snapshot(self, strategy_id: str) -> "PortfolioSnapshot | None":
        """Get most recent portfolio snapshot for a strategy.

        Args:
            strategy_id: Strategy identifier.

        Returns:
            Latest PortfolioSnapshot if found, None otherwise.
        """
        if not self._initialized:
            await self.initialize()

        if not self._warm:
            logger.warning("Cannot get portfolio snapshot: no WARM backend configured")
            return None

        if not hasattr(self._warm, "get_latest_snapshot"):
            logger.warning("WARM backend does not support portfolio snapshot storage")
            return None

        start = time.perf_counter()
        try:
            result = await self._warm.get_latest_snapshot(strategy_id)  # type: ignore[attr-defined]
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "get_latest_snapshot", latency, True)
            return result
        except Exception as e:
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "get_latest_snapshot", latency, False, str(e))
            logger.error(f"Failed to get latest snapshot: {e}")
            return None

    async def get_snapshots_since(
        self,
        strategy_id: str,
        since: datetime,
        limit: int = 168,
    ) -> list["PortfolioSnapshot"]:
        """Get portfolio snapshots since a timestamp (for charts).

        Args:
            strategy_id: Strategy identifier.
            since: Start timestamp for query.
            limit: Maximum number of snapshots to return.

        Returns:
            List of PortfolioSnapshot, oldest first.
        """
        if not self._initialized:
            await self.initialize()

        if not self._warm:
            logger.warning("Cannot get portfolio snapshots: no WARM backend configured")
            return []

        if not hasattr(self._warm, "get_snapshots_since"):
            logger.warning("WARM backend does not support portfolio snapshot storage")
            return []

        start = time.perf_counter()
        try:
            result = await self._warm.get_snapshots_since(strategy_id, since, limit)  # type: ignore[attr-defined]
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "get_snapshots_since", latency, True)
            return result
        except Exception as e:
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "get_snapshots_since", latency, False, str(e))
            logger.error(f"Failed to get snapshots since {since}: {e}")
            return []

    async def get_snapshot_at(
        self,
        strategy_id: str,
        timestamp: datetime,
    ) -> "PortfolioSnapshot | None":
        """Get snapshot closest to a timestamp (for PnL calculation).

        Args:
            strategy_id: Strategy identifier.
            timestamp: Target timestamp.

        Returns:
            PortfolioSnapshot closest to timestamp, or None if not found.
        """
        if not self._initialized:
            await self.initialize()

        if not self._warm:
            logger.warning("Cannot get portfolio snapshot: no WARM backend configured")
            return None

        if not hasattr(self._warm, "get_snapshot_at"):
            logger.warning("WARM backend does not support portfolio snapshot storage")
            return None

        start = time.perf_counter()
        try:
            result = await self._warm.get_snapshot_at(strategy_id, timestamp)  # type: ignore[attr-defined]
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "get_snapshot_at", latency, True)
            return result
        except Exception as e:
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "get_snapshot_at", latency, False, str(e))
            logger.error(f"Failed to get snapshot at {timestamp}: {e}")
            return None

    async def save_portfolio_metrics(self, metrics: "PortfolioMetrics") -> bool:
        """Save or update portfolio metrics.

        Portfolio metrics store baseline values (initial_value_usd) that survive
        strategy restarts, enabling accurate PnL calculation.

        Args:
            metrics: PortfolioMetrics to persist.

        Returns:
            True if save succeeded, False if no WARM backend or error.
        """
        if not self._initialized:
            await self.initialize()

        if not self._warm:
            logger.warning("Cannot save portfolio metrics: no WARM backend configured")
            return False

        if not hasattr(self._warm, "save_portfolio_metrics"):
            logger.warning("WARM backend does not support portfolio metrics storage")
            return False

        start = time.perf_counter()
        try:
            result = await self._warm.save_portfolio_metrics(metrics)  # type: ignore[attr-defined]
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "save_portfolio_metrics", latency, True)
            return result
        except Exception as e:
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "save_portfolio_metrics", latency, False, str(e))
            logger.error(f"Failed to save portfolio metrics: {e}")
            return False

    async def get_portfolio_metrics(self, strategy_id: str) -> "PortfolioMetrics | None":
        """Get portfolio metrics for a strategy.

        Args:
            strategy_id: Strategy identifier.

        Returns:
            PortfolioMetrics if found, None otherwise.
        """
        if not self._initialized:
            await self.initialize()

        if not self._warm:
            logger.warning("Cannot get portfolio metrics: no WARM backend configured")
            return None

        if not hasattr(self._warm, "get_portfolio_metrics"):
            logger.warning("WARM backend does not support portfolio metrics storage")
            return None

        start = time.perf_counter()
        try:
            result = await self._warm.get_portfolio_metrics(strategy_id)  # type: ignore[attr-defined]
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "get_portfolio_metrics", latency, True)
            return result
        except Exception as e:
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "get_portfolio_metrics", latency, False, str(e))
            logger.error(f"Failed to get portfolio metrics: {e}")
            return None

    async def cleanup_old_snapshots(self, retention_days: int = 7) -> int:
        """Clean up old portfolio snapshots.

        Args:
            retention_days: Number of days of snapshots to retain.

        Returns:
            Number of snapshots deleted.
        """
        if not self._initialized:
            await self.initialize()

        if not self._warm:
            logger.warning("Cannot cleanup snapshots: no WARM backend configured")
            return 0

        if not hasattr(self._warm, "cleanup_old_snapshots"):
            logger.warning("WARM backend does not support portfolio snapshot cleanup")
            return 0

        start = time.perf_counter()
        try:
            result = await self._warm.cleanup_old_snapshots(retention_days)  # type: ignore[attr-defined]
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "cleanup_old_snapshots", latency, True)
            return result
        except Exception as e:
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "cleanup_old_snapshots", latency, False, str(e))
            logger.error(f"Failed to cleanup old snapshots: {e}")
            return 0
