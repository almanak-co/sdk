"""StateManager with tiered persistence.

Implements two-tier state storage:
- HOT: In-memory cache (<1ms access)
- WARM: PostgreSQL or SQLite (<10ms access)

Uses CAS (Compare-And-Swap) semantics via a version field for safe
concurrent updates.  Each agent has exactly one row in the WARM tier
(single-row-per-agent model).

Important: Each strategy uses exactly one gateway and vice versa.
No two strategies share a gateway.

Durability invariant (VIB-3156):
    A successful ``save_state()`` call guarantees durability or raises.

    Operationally, every file-backed WARM backend writes the new version,
    state_data, and checksum in a single atomic transaction with full
    fsync durability (``synchronous = FULL`` for SQLite).  The new row
    is only made visible to readers after the transaction commits to
    stable storage; therefore state rows never exist on disk with a
    version bump but a state_data/checksum mismatch.  Checksum
    consistency is validated BEFORE the row is written so that an
    invalid serialization never lands on disk at the real path.

    For gateway-backed backends (``GatewayStateManager``) atomicity is
    the gateway server's responsibility -- the client's ``SaveState``
    RPC is all-or-nothing from the caller's perspective.
"""

import copy
import hashlib
import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum, IntEnum, auto
from typing import TYPE_CHECKING, Any, Optional, Protocol, runtime_checkable

if TYPE_CHECKING:
    from almanak.framework.accounting.commit import HandleMapping, RegistryRow
    from almanak.framework.execution.clob_handler import ClobFill, ClobOrderState, ClobOrderStatus
    from almanak.framework.observability.ledger import LedgerEntry
    from almanak.framework.observability.position_events import PositionEvent
    from almanak.framework.portfolio import PortfolioMetrics, PortfolioSnapshot

from .exceptions import (  # noqa: E402 (re-exported for callers)
    AccountingPersistenceError,
    AccountingWriteKind,
)


def _default_local_db_path_str() -> str:
    """Resolve the local SQLite path via the canonical helper (VIB-3761).

    Wrapped so the import (and the hosted-mode check inside the helper) is
    deferred to dataclass construction rather than module load — this
    matters because ``StateManagerConfig`` is constructed in hosted mode
    even though its ``db_path`` is unused there. We swallow
    :class:`LocalPathError` so a hosted-mode construction does not fail
    just because someone touched the SQLite default; the path simply
    isn't used in that mode.
    """
    from almanak.framework.local_paths import LocalPathError, local_db_path

    try:
        return str(local_db_path())
    except LocalPathError:
        # Hosted mode — caller will use the Postgres backend. Return a
        # sentinel that fails loudly if accidentally used.
        return ":hosted-mode-no-sqlite-path:"


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

    db_path: str = field(default_factory=lambda: _default_local_db_path_str())
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

        Returns None if not found or expired. Returns a deep copy so callers
        cannot mutate the cached StateData; a failed CAS save must leave the
        cache on its prior value.
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

        return copy.deepcopy(data)

    def set(self, state: StateData) -> None:
        """Store state in cache.

        Evicts oldest entry if cache is full. Stores a deep copy so subsequent
        caller mutation of the passed-in object cannot retroactively alter
        cached state.
        """
        # Evict oldest if at capacity
        if len(self._cache) >= self._max_size and state.strategy_id not in self._cache:
            self._evict_oldest()

        self._cache[state.strategy_id] = (copy.deepcopy(state), time.time())

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
                FROM strategy_state
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

        Durability (VIB-3156):
            The write runs in a single transaction so the version, state_data,
            and checksum columns are updated atomically. PostgreSQL's default
            ``synchronous_commit = on`` guarantees the transaction is flushed
            to WAL before the call returns, so on success the caller has the
            durability guarantee: a crash after this function returns will
            either see the full new row or the prior row -- never a torn
            state with version bumped but stale checksum.

        Returns:
            True if save succeeded.

        Raises:
            StateConflictError: If expected_version doesn't match current version.
        """
        if not self._initialized:
            await self.initialize()

        state_json = json.dumps(state.state, default=str)

        async with self._pool.acquire() as conn, conn.transaction():
            if expected_version is None:
                # UPSERT: insert new or overwrite existing (version increments)
                await conn.execute(
                    """
                    INSERT INTO strategy_state
                        (agent_id, version, state_data, schema_version, checksum,
                         created_at, updated_at)
                    VALUES ($1, $2, $3::jsonb, $4, $5, now(), now())
                    ON CONFLICT (agent_id) DO UPDATE SET
                        version = strategy_state.version + 1,
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
                    UPDATE strategy_state
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
                    # Version mismatch -- read the actual version inside the
                    # same transaction so the error message reflects a
                    # consistent snapshot. The surrounding transaction will
                    # be rolled back by the raised exception.
                    actual = await conn.fetchval(
                        "SELECT version FROM strategy_state WHERE agent_id = $1",
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
                "DELETE FROM strategy_state WHERE agent_id = $1",
                strategy_id,
            )
            return result != "DELETE 0"

    async def get_all_strategy_ids(self) -> list[str]:
        """Return all strategy IDs (for HOT cache warm-up)."""
        if not self._initialized:
            await self.initialize()

        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT agent_id FROM strategy_state")
            return [row["agent_id"] for row in rows]

    # =========================================================================
    # Reader methods used by DashboardService (VIB-3933)
    #
    # Identity convention (per AGENTS.md):
    #   - Hosted Postgres tables key on ``agent_id`` (set from the
    #     platform-injected ``AGENT_ID`` env var via ``resolve_agent_id``).
    #   - Local SQLite tables key on ``strategy_id``.
    # Callers always pass the already-resolved value; the column name is the
    # only difference. ``accounting_events`` and ``position_events`` carry both
    # ``agent_id`` and ``deployment_id`` columns; we filter on ``deployment_id``
    # to mirror the SQLite signature, since under the 1 Gateway : 1 Strategy
    # rule the two values are identical for any row written in hosted mode.
    # =========================================================================

    async def get_latest_snapshot(self, strategy_id: str) -> "PortfolioSnapshot | None":
        """Most recent ``portfolio_snapshots`` row for a strategy."""
        if not self._initialized:
            await self.initialize()

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT agent_id, timestamp, iteration_number, total_value_usd,
                       available_cash_usd, deployed_capital_usd, wallet_total_value_usd,
                       value_confidence, positions_json::text AS positions_text,
                       token_prices_json::text AS token_prices_text,
                       wallet_balances_json::text AS wallet_balances_text,
                       chain,
                       deployment_id, cycle_id, execution_mode
                FROM portfolio_snapshots
                WHERE agent_id = $1
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                strategy_id,
            )
        if row is None:
            return None
        return _pg_row_to_portfolio_snapshot(row)

    async def get_snapshots_since(
        self,
        strategy_id: str,
        since: datetime,
        limit: int = 168,
    ) -> list["PortfolioSnapshot"]:
        """Snapshots for a strategy since ``since`` (timestamp ASC)."""
        if not self._initialized:
            await self.initialize()

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT agent_id, timestamp, iteration_number, total_value_usd,
                       available_cash_usd, deployed_capital_usd, wallet_total_value_usd,
                       value_confidence, positions_json::text AS positions_text,
                       token_prices_json::text AS token_prices_text,
                       wallet_balances_json::text AS wallet_balances_text,
                       chain,
                       deployment_id, cycle_id, execution_mode
                FROM portfolio_snapshots
                WHERE agent_id = $1 AND timestamp >= $2
                ORDER BY timestamp ASC
                LIMIT $3
                """,
                strategy_id,
                since,
                limit,
            )
        return [_pg_row_to_portfolio_snapshot(row) for row in rows]

    async def get_snapshot_at(
        self,
        strategy_id: str,
        timestamp: datetime,
    ) -> "PortfolioSnapshot | None":
        """Snapshot closest to ``timestamp`` (at or before)."""
        if not self._initialized:
            await self.initialize()

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT agent_id, timestamp, iteration_number, total_value_usd,
                       available_cash_usd, deployed_capital_usd, wallet_total_value_usd,
                       value_confidence, positions_json::text AS positions_text,
                       token_prices_json::text AS token_prices_text,
                       wallet_balances_json::text AS wallet_balances_text,
                       chain,
                       deployment_id, cycle_id, execution_mode
                FROM portfolio_snapshots
                WHERE agent_id = $1 AND timestamp <= $2
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                strategy_id,
                timestamp,
            )
        if row is None:
            return None
        return _pg_row_to_portfolio_snapshot(row)

    async def get_portfolio_metrics(self, strategy_id: str) -> "PortfolioMetrics | None":
        """Lifetime portfolio metrics row (one per strategy)."""
        if not self._initialized:
            await self.initialize()

        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT agent_id, initial_value_usd, initial_timestamp,
                       deposits_usd, withdrawals_usd, gas_spent_usd,
                       total_value_usd, positions_json::text AS positions_text,
                       cycle_id, deployment_id, execution_mode, is_complete,
                       updated_at
                FROM portfolio_metrics
                WHERE agent_id = $1
                """,
                strategy_id,
            )
        if row is None:
            return None
        return _pg_row_to_portfolio_metrics(row)

    async def get_ledger_entries(
        self,
        strategy_id: str,
        since: datetime | None = None,
        intent_type: str | None = None,
        limit: int = 100,
        before: datetime | None = None,
    ) -> list["LedgerEntry"]:
        """Transaction ledger entries (newest first), with optional filters.

        Mirrors :meth:`SQLiteStore.get_ledger_entries`.
        """
        if not self._initialized:
            await self.initialize()

        # Build the WHERE clause dynamically to keep the optional filter
        # parameters bound positionally for asyncpg.
        conditions = ["agent_id = $1"]
        params: list[Any] = [strategy_id]
        idx = 2
        if since is not None:
            conditions.append(f"timestamp > ${idx}")
            params.append(since)
            idx += 1
        if before is not None:
            conditions.append(f"timestamp < ${idx}")
            params.append(before)
            idx += 1
        if intent_type is not None:
            conditions.append(f"intent_type = ${idx}")
            params.append(intent_type)
            idx += 1
        where = " AND ".join(conditions)
        params.append(limit)

        sql = f"""
            SELECT id, cycle_id, agent_id, deployment_id, execution_mode,
                   timestamp, intent_type,
                   token_in, amount_in, token_out, amount_out,
                   effective_price, slippage_bps, gas_used, gas_usd,
                   tx_hash, chain, protocol, success, error,
                   extracted_data_json::text AS extracted_data_text,
                   price_inputs_json::text   AS price_inputs_text,
                   pre_state_json::text       AS pre_state_text,
                   post_state_json::text      AS post_state_text
            FROM transaction_ledger
            WHERE {where}
            ORDER BY timestamp DESC
            LIMIT ${idx}
        """

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        return [_pg_row_to_ledger_entry(row) for row in rows]

    async def sum_ledger_gas_usd(
        self,
        deployment_id: str,
        strategy_id: str | None = None,
    ) -> Decimal:
        """Σ transaction_ledger.gas_usd for a deployment (VIB-4225 ACC-02).

        Postgres counterpart of :meth:`SQLiteStore.sum_ledger_gas_usd`.
        ``NULLIF(gas_usd, '')::numeric`` handles the parser-didn't-emit
        empty-string case; ``COALESCE(SUM(...), 0)`` handles the no-rows
        case. Postgres reads are ``agent_id``-keyed (mirrors the rest of
        :class:`PostgresStateStore`).
        """
        if not self._initialized:
            await self.initialize()

        sql = """
            SELECT COALESCE(SUM(NULLIF(gas_usd, '')::numeric), 0) AS total
            FROM transaction_ledger
            WHERE deployment_id = $1
               OR (deployment_id = '' AND agent_id = $2)
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(sql, deployment_id, strategy_id or deployment_id)
        total = (row or {"total": 0})["total"]
        return Decimal(str(total or 0))

    async def get_accounting_events(
        self,
        deployment_id: str,
        event_type: str | None = None,
        position_key: str | None = None,
        limit: int = 500,
    ) -> list[dict]:
        """Typed accounting events as raw dicts (caller deserializes payload_json).

        Mirrors :meth:`SQLiteStore.get_accounting_events`.

        Filter is by ``deployment_id`` (not ``agent_id``) so the same
        signature works for SQLite and Postgres callers — see the identity
        comment at the top of this section.
        """
        if not self._initialized:
            await self.initialize()

        conditions = ["deployment_id = $1"]
        params: list[Any] = [deployment_id]
        idx = 2
        if event_type is not None:
            conditions.append(f"event_type = ${idx}")
            params.append(event_type)
            idx += 1
        if position_key is not None:
            conditions.append(f"position_key = ${idx}")
            params.append(position_key)
            idx += 1
        where = " AND ".join(conditions)
        params.append(limit)

        sql = f"""
            SELECT id, deployment_id, agent_id, cycle_id, execution_mode,
                   timestamp, chain, protocol, wallet_address,
                   event_type, position_key, ledger_entry_id, tx_hash,
                   confidence, payload_json::text AS payload_text,
                   schema_version
            FROM accounting_events
            WHERE {where}
            ORDER BY timestamp ASC
            LIMIT ${idx}
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        return [_pg_row_to_accounting_event_dict(row) for row in rows]

    async def get_position_events_dict(
        self,
        deployment_id: str,
        position_id: str | None = None,
        position_type: str | None = None,
        event_type: str | None = None,
    ) -> list[dict]:
        """Position lifecycle events as raw dicts (timestamp ASC).

        Mirrors :meth:`SQLiteStore.get_position_events_sync`. The ``_dict``
        suffix distinguishes this from :meth:`get_position_events` (not
        implemented here) which returns ``PositionEvent`` dataclasses.

        Filter is by ``deployment_id`` (see identity comment above).
        """
        if not self._initialized:
            await self.initialize()

        conditions = ["deployment_id = $1"]
        params: list[Any] = [deployment_id]
        idx = 2
        if position_id is not None:
            conditions.append(f"position_id = ${idx}")
            params.append(position_id)
            idx += 1
        if position_type is not None:
            conditions.append(f"position_type = ${idx}")
            params.append(position_type)
            idx += 1
        if event_type is not None:
            conditions.append(f"event_type = ${idx}")
            params.append(event_type)
            idx += 1
        where = " AND ".join(conditions)

        sql = f"""
            SELECT id, agent_id, deployment_id, cycle_id, execution_mode,
                   position_id, position_type, event_type, timestamp,
                   protocol, chain, token0, token1, amount0, amount1,
                   value_usd, tick_lower, tick_upper, liquidity, in_range,
                   fees_token0, fees_token1, leverage, entry_price,
                   mark_price, unrealized_pnl, is_long, tx_hash, gas_usd,
                   ledger_entry_id, protocol_fees_usd,
                   attribution_json::text AS attribution_text,
                   attribution_version
            FROM position_events
            WHERE {where}
            ORDER BY timestamp ASC
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        return [_pg_row_to_position_event_dict(row) for row in rows]


# =============================================================================
# Postgres row → framework type conversions (VIB-3933)
# =============================================================================
#
# These are module-level helpers (not PostgresStore methods) so the unit
# tests can exercise the row-shape parity with SQLite without touching a
# real database.
# =============================================================================


# Effectively-unbounded query limit for asyncpg LIMIT clauses where the
# semantic intent is "no cap" (cost-basis FIFO replay needs full history).
# 1e9 covers any realistic strategy lifetime by ~6 orders of magnitude
# while keeping the SQL signature compatible with int-typed limit columns.
_UNLIMITED_QUERY_LIMIT = 1_000_000_000


def _coerce_dt(value: Any) -> datetime | None:
    """Return a tz-aware datetime from a Postgres ``TIMESTAMPTZ`` value.

    asyncpg already returns a ``datetime``; the wrapper is here to handle
    the rare case where a row was hand-fabricated for tests using a string.
    Returns ``None`` only when ``value`` is ``None`` or an unrecognised type
    — for required (NOT NULL) columns use :func:`_require_dt` instead so
    a malformed row fails loudly.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    return None


def _require_dt(value: Any, column: str) -> datetime:
    """Coerce a NOT NULL ``TIMESTAMPTZ`` column; raise on missing/None.

    All timestamps written by the SDK to metrics_db are NOT NULL (verified
    against the Prisma schema — `portfolio_snapshots.timestamp`,
    `portfolio_metrics.initial_timestamp`/`updated_at`,
    `transaction_ledger.timestamp`, `accounting_events.timestamp`,
    `position_events.timestamp`). Silently substituting ``datetime.now(UTC)``
    for a missing value would mask real data integrity issues — VIB-3933
    review explicitly flagged that anti-pattern. If a row arrives here
    without its timestamp, surface it as an error so the caller can log
    and skip rather than fabricate a current timestamp.
    """
    coerced = _coerce_dt(value)
    if coerced is None:
        raise ValueError(
            f"Required timestamp column {column!r} is None or unparseable; "
            "the metrics_db schema declares this column NOT NULL — investigate "
            "row fabrication or a schema regression."
        )
    return coerced


def _pg_row_to_portfolio_snapshot(row: Any) -> "PortfolioSnapshot":
    """Convert a ``portfolio_snapshots`` row to a ``PortfolioSnapshot``."""
    from almanak.framework.portfolio.models import PortfolioSnapshot

    timestamp = _require_dt(row["timestamp"], "portfolio_snapshots.timestamp")
    positions_payload: Any = row.get("positions_text") or "[]"
    if isinstance(positions_payload, str):
        try:
            positions_payload = json.loads(positions_payload)
        except json.JSONDecodeError:
            positions_payload = []
    positions, snapshot_metadata = PortfolioSnapshot.unpack_positions_payload(positions_payload)

    token_prices: dict[str, dict] = {}
    tp_text = row.get("token_prices_text")
    if tp_text:
        try:
            parsed = json.loads(tp_text)
            if isinstance(parsed, dict):
                token_prices = parsed
        except json.JSONDecodeError:
            pass

    wallet_balances_raw: list[dict] = []
    wb_text = row.get("wallet_balances_text")
    if wb_text:
        try:
            parsed_wb = json.loads(wb_text)
            if isinstance(parsed_wb, list):
                wallet_balances_raw = parsed_wb
        except json.JSONDecodeError:
            pass

    deployed_capital_usd = str(row.get("deployed_capital_usd") or "0")
    wallet_total_value_usd = str(row.get("wallet_total_value_usd") or "0")

    return PortfolioSnapshot.from_dict(
        {
            "timestamp": timestamp.isoformat(),
            "strategy_id": row["agent_id"],
            "total_value_usd": str(row["total_value_usd"]),
            "available_cash_usd": str(row["available_cash_usd"]),
            "deployed_capital_usd": deployed_capital_usd,
            "wallet_total_value_usd": wallet_total_value_usd,
            "value_confidence": row["value_confidence"],
            "positions": positions,
            "wallet_balances": wallet_balances_raw,
            "token_prices": token_prices,
            "chain": row["chain"] or "",
            "iteration_number": row["iteration_number"] or 0,
            "snapshot_metadata": snapshot_metadata,
            # VIB-4097 (3.6) — Phase 4 identity, defensive read for legacy
            # rows. Older Postgres rows that pre-date VIB-4095 / 3.4 return
            # ``""`` rather than raising.
            "deployment_id": row.get("deployment_id") or "",
            "cycle_id": row.get("cycle_id") or "",
            "execution_mode": row.get("execution_mode") or "",
        }
    )


def _pg_row_to_portfolio_metrics(row: Any) -> "PortfolioMetrics":
    """Convert a ``portfolio_metrics`` row to a ``PortfolioMetrics``.

    ``timestamp`` is anchored to ``initial_timestamp`` (matching the writer
    in ``_save_metrics_helpers.build_portfolio_metrics`` which sets
    ``timestamp=inputs.timestamp`` from ``request.initial_timestamp``).
    Mapping it to ``updated_at`` would diverge read from write and make
    every Postgres read look like the strategy was newly started, skewing
    age / lifetime baseline logic that consumes ``metrics.timestamp``.
    """
    from decimal import Decimal

    from almanak.framework.portfolio.models import PortfolioMetrics

    initial_timestamp = _require_dt(row["initial_timestamp"], "portfolio_metrics.initial_timestamp")
    is_complete = bool(row["is_complete"]) if row.get("is_complete") is not None else True
    return PortfolioMetrics(
        strategy_id=row["agent_id"],
        timestamp=initial_timestamp,
        total_value_usd=Decimal(row.get("total_value_usd") or "0"),
        initial_value_usd=Decimal(row["initial_value_usd"]),
        deposits_usd=Decimal(row.get("deposits_usd") or "0"),
        withdrawals_usd=Decimal(row.get("withdrawals_usd") or "0"),
        gas_spent_usd=Decimal(row.get("gas_spent_usd") or "0"),
        positions_json=row.get("positions_text") or "[]",
        cycle_id=row.get("cycle_id"),
        deployment_id=row.get("deployment_id") or "",
        execution_mode=row.get("execution_mode") or "",
        is_complete=is_complete,
    )


def _pg_row_to_ledger_entry(row: Any) -> "LedgerEntry":
    """Convert a ``transaction_ledger`` row to a ``LedgerEntry``."""
    from almanak.framework.observability.ledger import LedgerEntry

    timestamp = _require_dt(row["timestamp"], "transaction_ledger.timestamp")
    return LedgerEntry(
        id=row["id"] or "",
        cycle_id=row.get("cycle_id") or "",
        strategy_id=row.get("agent_id") or "",
        deployment_id=row.get("deployment_id") or "",
        execution_mode=row.get("execution_mode") or "",
        timestamp=timestamp,
        intent_type=row.get("intent_type") or "",
        token_in=row.get("token_in") or "",
        amount_in=row.get("amount_in") or "",
        token_out=row.get("token_out") or "",
        amount_out=row.get("amount_out") or "",
        effective_price=row.get("effective_price") or "",
        slippage_bps=row.get("slippage_bps"),
        gas_used=row.get("gas_used") or 0,
        gas_usd=row.get("gas_usd") or "",
        tx_hash=row.get("tx_hash") or "",
        chain=row.get("chain") or "",
        protocol=row.get("protocol") or "",
        success=bool(row.get("success", True)),
        error=row.get("error") or "",
        extracted_data_json=row.get("extracted_data_text") or "",
        price_inputs_json=row.get("price_inputs_text") or "",
        pre_state_json=row.get("pre_state_text") or "",
        post_state_json=row.get("post_state_text") or "",
    )


def _pg_row_to_accounting_event_dict(row: Any) -> dict[str, Any]:
    """Convert an ``accounting_events`` row to the SQLite-shaped dict.

    Keys mirror :meth:`SQLiteStore.get_accounting_events_sync` so consumers
    reading either backend see identical shapes. ``timestamp`` is ISO-8601
    (matching SQLite); ``payload_json`` is a string (asyncpg returns the
    JSONB column as ``str`` because we cast ``::text``).
    """
    timestamp = _require_dt(row["timestamp"], "accounting_events.timestamp")
    return {
        "id": row["id"] or "",
        "deployment_id": row.get("deployment_id") or "",
        "agent_id": row.get("agent_id") or "",
        "strategy_id": row.get("agent_id") or "",
        "cycle_id": row.get("cycle_id") or "",
        "execution_mode": row.get("execution_mode") or "",
        "timestamp": timestamp.isoformat(),
        "chain": row.get("chain") or "",
        "protocol": row.get("protocol") or "",
        "wallet_address": row.get("wallet_address") or "",
        "event_type": row.get("event_type") or "",
        "position_key": row.get("position_key") or "",
        "ledger_entry_id": row.get("ledger_entry_id") or "",
        "tx_hash": row.get("tx_hash") or "",
        "confidence": row.get("confidence") or "",
        "payload_json": row.get("payload_text") or "{}",
        "schema_version": int(row.get("schema_version") or 1),
    }


def _pg_row_to_position_event_dict(row: Any) -> dict[str, Any]:
    """Convert a ``position_events`` row to the SQLite-shaped dict.

    ``protocol_fees_usd`` reads from the real Postgres column (added by
    VIB-3966 — metrics-database PR #27). The previous sentinel ``""`` was
    a workaround for a SDK→metrics-database timing-race miss from
    VIB-3205 (full timeline in
    ``docs/internal/VIB-3933-hosted-postgres-read-path.md`` Finding #3).
    The trailing ``or ""`` defends against legacy rows that pre-date the
    backfill default — ``""`` semantically means "parser did not emit"
    per AGENTS.md "Empty ≠ zero", which is the right reading both for a
    NULL row (shouldn't exist post-migration but cheap to defend) and a
    DEFAULT-applied empty string.
    """
    timestamp = _require_dt(row["timestamp"], "position_events.timestamp")
    return {
        "id": row["id"] or "",
        "agent_id": row.get("agent_id") or "",
        "deployment_id": row.get("deployment_id") or "",
        "cycle_id": row.get("cycle_id") or "",
        "execution_mode": row.get("execution_mode") or "",
        "position_id": row.get("position_id") or "",
        "position_type": row.get("position_type") or "",
        "event_type": row.get("event_type") or "",
        "timestamp": timestamp.isoformat(),
        "protocol": row.get("protocol") or "",
        "chain": row.get("chain") or "",
        "token0": row.get("token0") or "",
        "token1": row.get("token1") or "",
        "amount0": row.get("amount0") or "",
        "amount1": row.get("amount1") or "",
        "value_usd": row.get("value_usd") or "",
        "tick_lower": row.get("tick_lower"),
        "tick_upper": row.get("tick_upper"),
        "liquidity": row.get("liquidity") or "",
        "in_range": row.get("in_range"),
        "fees_token0": row.get("fees_token0") or "",
        "fees_token1": row.get("fees_token1") or "",
        "leverage": row.get("leverage") or "",
        "entry_price": row.get("entry_price") or "",
        "mark_price": row.get("mark_price") or "",
        "unrealized_pnl": row.get("unrealized_pnl") or "",
        "is_long": row.get("is_long"),
        "tx_hash": row.get("tx_hash") or "",
        "gas_usd": row.get("gas_usd") or "",
        "ledger_entry_id": row.get("ledger_entry_id") or "",
        "protocol_fees_usd": row.get("protocol_fees_usd") or "",
        "attribution_json": row.get("attribution_text") or "{}",
        "attribution_version": int(row.get("attribution_version") or 0),
    }


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
        # Per-instance set so multi-instance setups don't cross-suppress
        # warnings (CodeRabbit review). One-shot WARN per (method, identity)
        # — see _unimplemented_warn for the visibility rationale.
        self._unimplemented_logged: set[tuple[str, str]] = set()

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

        # Recalculate checksum. The WARM backend computes its own canonical
        # checksum from the serialized state body before committing (see
        # SQLiteStore.save and PostgresStore.save), and writes state_data +
        # checksum in the same atomic transaction -- so the on-disk row is
        # always self-consistent. See module docstring -- VIB-3156.
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
            Snapshot ID on success. Raises :class:`AccountingPersistenceError`
            on backend write failure, missing WARM backend, or unsupported
            backend so the runner can halt the cycle in live mode (VIB-3157).
            Paper/dry-run suppression is handled upstream by the runner.
        """
        if not self._initialized:
            await self.initialize()

        strategy_id = getattr(snapshot, "strategy_id", "") or ""

        if not self._warm:
            logger.error("Cannot save portfolio snapshot: no WARM backend configured")
            raise AccountingPersistenceError(
                write_kind=AccountingWriteKind.SNAPSHOT,
                strategy_id=strategy_id,
                message="No WARM backend configured for portfolio snapshot",
            )

        if not hasattr(self._warm, "save_portfolio_snapshot"):
            logger.error("WARM backend does not support portfolio snapshot storage")
            raise AccountingPersistenceError(
                write_kind=AccountingWriteKind.SNAPSHOT,
                strategy_id=strategy_id,
                message="WARM backend does not support portfolio snapshot storage",
            )

        start = time.perf_counter()
        try:
            result = await self._warm.save_portfolio_snapshot(snapshot)  # type: ignore[attr-defined]
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "save_portfolio_snapshot", latency, True)
            return result
        except AccountingPersistenceError:
            # Backend already raised a typed accounting error -- don't double-wrap.
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(
                StateTier.WARM, "save_portfolio_snapshot", latency, False, "AccountingPersistenceError"
            )
            raise
        except Exception as e:
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "save_portfolio_snapshot", latency, False, str(e))
            logger.error(f"Failed to save portfolio snapshot: {e}")
            raise AccountingPersistenceError(
                write_kind=AccountingWriteKind.SNAPSHOT,
                strategy_id=strategy_id,
                cause=e,
            ) from e

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
            ``True`` on success. Raises :class:`AccountingPersistenceError`
            on backend write failure, missing WARM backend, or unsupported
            backend so the runner can halt the cycle in live mode (VIB-3157).
            Paper/dry-run suppression is handled upstream by the runner.
        """
        if not self._initialized:
            await self.initialize()

        strategy_id = getattr(metrics, "strategy_id", "") or ""

        if not self._warm:
            logger.error("Cannot save portfolio metrics: no WARM backend configured")
            raise AccountingPersistenceError(
                write_kind=AccountingWriteKind.METRICS,
                strategy_id=strategy_id,
                message="No WARM backend configured for portfolio metrics",
            )

        if not hasattr(self._warm, "save_portfolio_metrics"):
            logger.error("WARM backend does not support portfolio metrics storage")
            raise AccountingPersistenceError(
                write_kind=AccountingWriteKind.METRICS,
                strategy_id=strategy_id,
                message="WARM backend does not support portfolio metrics storage",
            )

        start = time.perf_counter()
        try:
            result = await self._warm.save_portfolio_metrics(metrics)  # type: ignore[attr-defined]
        except AccountingPersistenceError:
            # Backend already raised a typed accounting error -- don't
            # double-wrap, but still record the failure in tier metrics so
            # observability matches save_ledger_entry / save_portfolio_snapshot.
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(
                StateTier.WARM,
                "save_portfolio_metrics",
                latency,
                False,
                "AccountingPersistenceError",
            )
            raise
        except Exception as e:
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "save_portfolio_metrics", latency, False, str(e))
            logger.error(f"Failed to save portfolio metrics: {e}")
            raise AccountingPersistenceError(
                write_kind=AccountingWriteKind.METRICS,
                strategy_id=strategy_id,
                cause=e,
            ) from e

        # VIB-3157: a ``False`` return from the backend is a write failure.
        # Done OUTSIDE the try-except so the raise here doesn't get caught
        # by the AccountingPersistenceError passthrough above (which would
        # double-record the failure metric). The old path returned the raw
        # bool; downstream (runner_state) only escalates typed accounting
        # errors, so a silent False would have slipped through.
        latency = (time.perf_counter() - start) * 1000
        if not result:
            self._record_metrics(
                StateTier.WARM,
                "save_portfolio_metrics",
                latency,
                False,
                "backend_returned_false",
            )
            raise AccountingPersistenceError(
                write_kind=AccountingWriteKind.METRICS,
                strategy_id=strategy_id,
                message="WARM backend save_portfolio_metrics returned False",
            )
        self._record_metrics(StateTier.WARM, "save_portfolio_metrics", latency, True)
        return True

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

    # =========================================================================
    # Transaction Ledger (VIB-2402)
    # =========================================================================

    async def save_ledger_entry(self, entry: "LedgerEntry") -> None:
        """Save a transaction ledger entry to the WARM backend.

        Raises :class:`AccountingPersistenceError` on backend write failure,
        missing WARM backend, or unsupported backend so the runner can halt
        the cycle in live mode (VIB-3157). Paper/dry-run suppression is
        handled upstream by the runner.

        Args:
            entry: LedgerEntry to persist.
        """
        if not self._initialized:
            await self.initialize()

        strategy_id = getattr(entry, "strategy_id", "") or ""

        if not self._warm:
            logger.error("Cannot save ledger entry: no WARM backend configured")
            raise AccountingPersistenceError(
                write_kind=AccountingWriteKind.LEDGER,
                strategy_id=strategy_id,
                message="No WARM backend configured for transaction ledger",
            )

        if not hasattr(self._warm, "save_ledger_entry"):
            logger.error("WARM backend does not support transaction ledger")
            raise AccountingPersistenceError(
                write_kind=AccountingWriteKind.LEDGER,
                strategy_id=strategy_id,
                message="WARM backend does not support transaction ledger",
            )

        start = time.perf_counter()
        try:
            await self._warm.save_ledger_entry(entry)  # type: ignore[attr-defined]
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "save_ledger_entry", latency, True)
        except AccountingPersistenceError:
            # Backend already raised a typed accounting error -- don't double-wrap.
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "save_ledger_entry", latency, False, "AccountingPersistenceError")
            raise
        except Exception as e:
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "save_ledger_entry", latency, False, str(e))
            # VIB-3157: surface as typed error so the runner can halt the cycle in live
            # mode. Mode-aware suppression (paper/dry-run) happens upstream, never here
            # -- the backend write either completed or it didn't.
            logger.error("Failed to save ledger entry for %s: %s", strategy_id, e)
            raise AccountingPersistenceError(
                write_kind=AccountingWriteKind.LEDGER,
                strategy_id=strategy_id,
                cause=e,
            ) from e

    # =========================================================================
    # Atomic ledger + position_registry + handle commit (VIB-4197 / T11)
    # =========================================================================

    async def save_ledger_and_registry(
        self,
        *,
        ledger: "LedgerEntry",
        registry: "RegistryRow",
        handle: "HandleMapping | None" = None,
    ) -> None:
        """Atomic single-transaction commit of ledger + registry + handle.

        Per blueprint 28 §4.1. Delegates to the SQLite backend's
        ``save_ledger_and_registry_atomic`` method which wraps all three
        writes in one ``BEGIN IMMEDIATE`` ... ``COMMIT``. Idempotent on
        ``(deployment_id, chain, primitive, physical_identity_hash)`` with
        a strict monotone status-priority guard.

        This is the runtime registry-mode write path. The function-level
        primitive at :func:`almanak.framework.accounting.commit.save_ledger_and_registry`
        validates inputs and dispatches here for ``mode='registry'`` calls;
        ``mode='accounting_only'`` callers use :meth:`save_ledger_entry`
        directly. Callers MUST go through one of those two surfaces — see
        ``tests/unit/state/test_position_registry_no_writers.py`` for the
        anti-bypass guard.

        Failure contract: any backend error (CHECK violation, OperationalError,
        etc.) is wrapped as :class:`AccountingPersistenceError` with
        ``write_kind=ACCOUNTING`` so the runner's existing fail-closed
        pipeline (VIB-3157 / VIB-3762) handles it. The transaction is rolled
        back by the backend method before the exception propagates; no
        partial state lands on disk.

        Args:
            ledger: ``LedgerEntry`` for ``transaction_ledger``.
            registry: ``RegistryRow`` for ``position_registry``.
            handle: Optional ``HandleMapping`` (handle column on
                ``position_registry``; no separate table per blueprint 28
                §4.2). May also be encoded directly on ``registry.handle``.
        """
        if not self._initialized:
            await self.initialize()

        strategy_id = getattr(ledger, "strategy_id", "") or ""

        if not self._warm:
            logger.error("Cannot save ledger+registry: no WARM backend configured")
            raise AccountingPersistenceError(
                write_kind=AccountingWriteKind.ACCOUNTING,
                strategy_id=strategy_id,
                message="No WARM backend configured for ledger+registry atomic commit",
            )

        if not hasattr(self._warm, "save_ledger_and_registry_atomic"):
            logger.error("WARM backend does not support save_ledger_and_registry_atomic")
            raise AccountingPersistenceError(
                write_kind=AccountingWriteKind.ACCOUNTING,
                strategy_id=strategy_id,
                message=(
                    "WARM backend does not support atomic ledger+registry commit "
                    "(hosted Postgres path ships in T19 / VIB-4205)"
                ),
            )

        start = time.perf_counter()
        try:
            await self._warm.save_ledger_and_registry_atomic(  # type: ignore[attr-defined]
                ledger,
                registry,
                handle,
            )
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "save_ledger_and_registry", latency, True)
        except AccountingPersistenceError:
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(
                StateTier.WARM,
                "save_ledger_and_registry",
                latency,
                False,
                "AccountingPersistenceError",
            )
            raise
        except Exception as e:
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(
                StateTier.WARM,
                "save_ledger_and_registry",
                latency,
                False,
                str(e),
            )
            logger.error(
                "Failed to save ledger+registry atomically for %s: %s",
                strategy_id,
                e,
            )
            raise AccountingPersistenceError(
                write_kind=AccountingWriteKind.ACCOUNTING,
                strategy_id=strategy_id,
                cause=e,
            ) from e

    # =========================================================================
    # position_registry / migration_state passthroughs (VIB-4198 / T12)
    # =========================================================================
    #
    # Local-SQLite-only delegates to the WARM backend's typed methods. The
    # hosted Postgres equivalents land in T19 (VIB-4205) — the framework
    # path here raises ``NotImplementedError`` from the backend if the WARM
    # backend doesn't support the call (e.g., a remote-only gateway state
    # manager). The runner's ``_enforce_or_run_cutover`` catches that case
    # and degrades to the legacy accounting_only path with an ERROR log.

    async def get_position_registry_open_rows(
        self,
        deployment_id: str,
        *,
        chain: str | None = None,
        primitive: str | None = None,
        accounting_category: str | None = None,
    ) -> list[dict]:
        """Return the OPEN ``position_registry`` rows for a deployment.

        Backed by the WARM backend's typed read. Used by:

        - ``runner.get_open_lp_positions_from_registry`` (UniV3 LP path
          today; broadened by future cutovers).
        - Teardown's pre-flight ("what's open?") check for cutover-flipped
          primitives.

        Audit M3 (CodeRabbit): on a backend that does not implement
        cutover storage (``GatewayStateManager`` until T19/VIB-4205
        ships the Postgres equivalent), this method raises
        :class:`CutoverStorageNotSupported` rather than silently
        returning ``[]``. A silent ``[]`` is indistinguishable from
        "fresh DB, no rows" — the boot guard would interpret the
        empty result as "registry is the source of truth and it is
        empty", potentially marking still-open positions as gone. The
        cutover boot guard catches this exception and chooses degrade
        vs hard refusal based on whether the cutover is meant to be
        active for this build.
        """
        if not self._initialized:
            await self.initialize()
        if not self._warm or not hasattr(self._warm, "get_position_registry_open_rows"):
            from almanak.framework.migration import CutoverStorageNotSupported

            raise CutoverStorageNotSupported(
                f"WARM backend {type(self._warm).__name__ if self._warm else 'None'} "
                "does not implement get_position_registry_open_rows; cutover storage "
                "is unavailable on this backend (T19/VIB-4205 lands the hosted "
                "equivalent)."
            )
        return await self._warm.get_position_registry_open_rows(
            deployment_id,
            chain=chain,
            primitive=primitive,
            accounting_category=accounting_category,
        )

    async def insert_position_registry_row_if_absent(self, *, row: Any) -> bool:
        """Backfill insert (``INSERT … ON CONFLICT DO NOTHING``).

        Idempotent under restart. Used by
        :class:`almanak.framework.migration.BackfillReader`. Raises
        :class:`CutoverStorageNotSupported` on backends that don't
        implement the typed write — see ``get_position_registry_open_rows``
        for the rationale.
        """
        if not self._initialized:
            await self.initialize()
        if not self._warm or not hasattr(self._warm, "insert_position_registry_row_if_absent"):
            from almanak.framework.migration import CutoverStorageNotSupported

            raise CutoverStorageNotSupported(
                f"WARM backend {type(self._warm).__name__ if self._warm else 'None'} "
                "does not implement insert_position_registry_row_if_absent."
            )
        return await self._warm.insert_position_registry_row_if_absent(row=row)

    async def upsert_migration_state(
        self,
        *,
        deployment_id: str,
        primitive: str,
        cutover_key: str,
    ) -> None:
        """Idempotent insert of a baseline migration_state row.

        Raises :class:`CutoverStorageNotSupported` on backends that
        don't implement migration_state — silent no-op would let the
        boot guard's read return ``None`` and trigger
        ``RegistryCutoverNotDeployedError`` even when the build's
        intent is "this backend doesn't support cutover storage yet".
        """
        if not self._initialized:
            await self.initialize()
        if not self._warm or not hasattr(self._warm, "upsert_migration_state"):
            from almanak.framework.migration import CutoverStorageNotSupported

            raise CutoverStorageNotSupported(
                f"WARM backend {type(self._warm).__name__ if self._warm else 'None'} "
                "does not implement upsert_migration_state."
            )
        await self._warm.upsert_migration_state(
            deployment_id=deployment_id,
            primitive=primitive,
            cutover_key=cutover_key,
        )

    async def get_migration_state(
        self,
        *,
        deployment_id: str,
        primitive: str,
        cutover_key: str,
    ) -> Any | None:
        """Return the parsed migration_state row, or None when missing.

        Raises :class:`CutoverStorageNotSupported` on backends that
        don't implement migration_state. Returning ``None`` on an
        unsupported backend would be indistinguishable from "row not
        yet created", which the boot guard treats as
        ``RegistryCutoverNotDeployedError`` — wrong error class, wrong
        recovery path.
        """
        if not self._initialized:
            await self.initialize()
        if not self._warm or not hasattr(self._warm, "get_migration_state"):
            from almanak.framework.migration import CutoverStorageNotSupported

            raise CutoverStorageNotSupported(
                f"WARM backend {type(self._warm).__name__ if self._warm else 'None'} "
                "does not implement get_migration_state."
            )
        return await self._warm.get_migration_state(
            deployment_id=deployment_id,
            primitive=primitive,
            cutover_key=cutover_key,
        )

    async def update_migration_state(
        self,
        *,
        deployment_id: str,
        primitive: str,
        cutover_key: str,
        backfill_started_at: str | None = None,
        rows_synthesized: int | None = None,
        rows_skipped_already_present: int | None = None,
    ) -> None:
        if not self._initialized:
            await self.initialize()
        if not self._warm or not hasattr(self._warm, "update_migration_state"):
            from almanak.framework.migration import CutoverStorageNotSupported

            raise CutoverStorageNotSupported(
                f"WARM backend {type(self._warm).__name__ if self._warm else 'None'} "
                "does not implement update_migration_state."
            )
        await self._warm.update_migration_state(
            deployment_id=deployment_id,
            primitive=primitive,
            cutover_key=cutover_key,
            backfill_started_at=backfill_started_at,
            rows_synthesized=rows_synthesized,
            rows_skipped_already_present=rows_skipped_already_present,
        )

    async def mark_backfill_complete(
        self,
        *,
        deployment_id: str,
        primitive: str,
        cutover_key: str,
        rows_synthesized: int,
        rows_skipped_already_present: int,
        backfill_completed_at: str,
    ) -> None:
        if not self._initialized:
            await self.initialize()
        if not self._warm or not hasattr(self._warm, "mark_backfill_complete"):
            from almanak.framework.migration import CutoverStorageNotSupported

            raise CutoverStorageNotSupported(
                f"WARM backend {type(self._warm).__name__ if self._warm else 'None'} "
                "does not implement mark_backfill_complete. A silent no-op here "
                "would let the runner re-run the full backfill on every restart."
            )
        await self._warm.mark_backfill_complete(
            deployment_id=deployment_id,
            primitive=primitive,
            cutover_key=cutover_key,
            rows_synthesized=rows_synthesized,
            rows_skipped_already_present=rows_skipped_already_present,
            backfill_completed_at=backfill_completed_at,
        )

    async def get_position_events_filtered(
        self,
        *,
        deployment_id: str,
        position_types: frozenset[str],
    ) -> list[dict]:
        """Read the deployment's ``position_events`` rows whose
        ``position_type`` is in the filter set.

        Used by the backfill driver loop. Raises
        :class:`CutoverStorageNotSupported` on backends that don't
        implement the typed read — silent ``[]`` would let the
        backfill complete with zero synthesized rows on a deployment
        that actually has historical positions.
        """
        if not self._initialized:
            await self.initialize()
        if not self._warm or not hasattr(self._warm, "get_position_events_filtered"):
            from almanak.framework.migration import CutoverStorageNotSupported

            raise CutoverStorageNotSupported(
                f"WARM backend {type(self._warm).__name__ if self._warm else 'None'} "
                "does not implement get_position_events_filtered. Silent [] would "
                "look like a deployment with zero historical positions."
            )
        return await self._warm.get_position_events_filtered(
            deployment_id=deployment_id,
            position_types=position_types,
        )

    async def get_ledger_entries(
        self,
        strategy_id: str,
        since: "datetime | None" = None,
        intent_type: str | None = None,
        limit: int = 100,
        before: "datetime | None" = None,
    ) -> list:
        """Query transaction ledger entries.

        Args:
            strategy_id: Strategy to query.
            since: Only entries after this timestamp.
            intent_type: Filter by intent type.
            limit: Maximum entries to return.
            before: Only entries strictly older than this timestamp
                (paginated trade-tape cursor). When set, the SQL filter
                runs at the backend rather than post-fetch in Python so
                callers can never receive a "newest N rows that don't
                match the cursor" empty page.

        Returns:
            List of LedgerEntry objects, newest first.
        """
        if not self._initialized:
            await self.initialize()

        if not self._warm:
            return []

        if not hasattr(self._warm, "get_ledger_entries"):
            return []

        start = time.perf_counter()
        try:
            # Backends may not yet accept the ``before`` kwarg. Detect support
            # via signature inspection so a real TypeError from inside the
            # backend (e.g., a bug raising TypeError post-execution) is NOT
            # swallowed into a silently-uncursored read — that would produce
            # duplicate/looping pages on the trade tape.
            supports_before = True
            if before is not None:
                try:
                    import inspect

                    supports_before = (
                        "before" in inspect.signature(self._warm.get_ledger_entries).parameters  # type: ignore[attr-defined]
                    )
                except (TypeError, ValueError):
                    supports_before = False
                # Fail closed when the caller asked for a strict-cursor
                # read but the backend can't honour it. Falling through
                # to an uncursored fetch silently breaks the
                # "strictly older than ``before``" contract and produces
                # duplicate / looping pages on the trade tape.
                if not supports_before:
                    raise RuntimeError(
                        "Warm backend get_ledger_entries() does not support "
                        "the 'before' pagination cursor; refusing to fall "
                        "back to an uncursored read which would produce "
                        "duplicate or looping trade-tape pages."
                    )
            if before is not None:
                result = await self._warm.get_ledger_entries(  # type: ignore[attr-defined]
                    strategy_id, since, intent_type, limit, before=before
                )
            else:
                result = await self._warm.get_ledger_entries(strategy_id, since, intent_type, limit)  # type: ignore[attr-defined]
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "get_ledger_entries", latency, True)
            return result
        except Exception as e:
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "get_ledger_entries", latency, False, str(e))
            logger.error(f"Failed to get ledger entries: {e}")
            return []

    async def sum_ledger_gas_usd(
        self,
        deployment_id: str,
        strategy_id: str | None = None,
    ) -> Decimal:
        """Σ transaction_ledger.gas_usd for a deployment (VIB-4225 ACC-02).

        Delegates to the WARM backend's aggregator. Returns ``Decimal("0")``
        on no rows, no warm backend, or unsupported backend (the runner's
        ``_build_metrics_for_snapshot`` reads ``hasattr`` first; this fallback
        guards against an old backend that pre-dates the aggregator method).
        Raises :class:`AccountingPersistenceError` so the runner halts the
        cycle in live mode (VIB-3762 contract).
        """
        if not self._initialized:
            await self.initialize()
        if not self._warm or not hasattr(self._warm, "sum_ledger_gas_usd"):
            return Decimal("0")
        start = time.perf_counter()
        try:
            result = await self._warm.sum_ledger_gas_usd(deployment_id, strategy_id)
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "sum_ledger_gas_usd", latency, True)
            return result
        except AccountingPersistenceError:
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "sum_ledger_gas_usd", latency, False, "AccountingPersistenceError")
            raise
        except NotImplementedError:
            # CodeRabbit thread #6: hosted-mode contract depends on
            # ``GatewayStateManager.sum_ledger_gas_usd`` surfacing
            # ``NotImplementedError`` as the typed "hosted unsupported"
            # signal. Wrapping it here as ``AccountingPersistenceError``
            # would shadow the type-narrow catch in
            # ``runner_state._populate_gas_spent_usd``. Propagate unchanged.
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "sum_ledger_gas_usd", latency, False, "NotImplementedError")
            raise
        except Exception as e:
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "sum_ledger_gas_usd", latency, False, str(e))
            logger.error("Failed to sum ledger gas_usd for %s: %s", deployment_id, e)
            raise AccountingPersistenceError(
                write_kind=AccountingWriteKind.METRICS,
                strategy_id=strategy_id or deployment_id,
                cause=e,
            ) from e

    # -------------------------------------------------------------------------
    # PositionEvent delegation (VIB-3204 audit fix)
    # -------------------------------------------------------------------------
    # ``StrategyRunner`` emits PositionEvents after every successful intent
    # and ``pnl_attributor`` reads them back for CLOSE-time IL attribution
    # (VIB-3205). Historically the runner called ``state_manager.save_position_event``
    # directly but StateManager never grew a delegation — every call silently
    # raised AttributeError and was swallowed by the runner's outer
    # try/except, leaving entry_state / attribution_json permanently empty
    # (and thus compute_impermanent_loss returning None for every LP close).
    # These methods delegate to the warm backend.

    async def save_position_event(self, event: "PositionEvent") -> bool:
        """Persist a PositionEvent (OPEN/CLOSE/COLLECT_FEES/SNAPSHOT)."""
        if not self._initialized:
            await self.initialize()
        if not self._warm or not hasattr(self._warm, "save_position_event"):
            return False
        start = time.perf_counter()
        try:
            result = await self._warm.save_position_event(event)
            latency = (time.perf_counter() - start) * 1000
            # CodeRabbit round-4: record success metric from the backend's
            # actual return, not a hard-coded True. Backends return False
            # on silent no-ops; we want those to register as failures so
            # observability reflects reality.
            ok = bool(result)
            self._record_metrics(
                StateTier.WARM,
                "save_position_event",
                latency,
                ok,
                None if ok else "backend_returned_false",
            )
            return ok
        except Exception as e:
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "save_position_event", latency, False, str(e))
            logger.error(f"Failed to save position event: {e}")
            return False

    async def save_accounting_event(self, event: Any) -> bool:
        """Persist a typed accounting event (LendingAccountingEvent, etc.) to the warm backend.

        Delegates to the backend's save_accounting_event when supported (SQLiteStore).
        Returns False when the backend does not yet support accounting events
        (e.g. GatewayStateManager before the metrics-database migration).
        """
        if not self._initialized:
            await self.initialize()
        if not self._warm or not hasattr(self._warm, "save_accounting_event"):
            return False
        start = time.perf_counter()
        try:
            result = await self._warm.save_accounting_event(event)
            latency = (time.perf_counter() - start) * 1000
            ok = bool(result)
            self._record_metrics(
                StateTier.WARM, "save_accounting_event", latency, ok, None if ok else "backend_returned_false"
            )
            return ok
        except Exception as e:
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "save_accounting_event", latency, False, str(e))
            logger.error("Failed to save accounting event: %s", e)
            raise

    def get_accounting_events_sync(
        self,
        deployment_id: str,
        position_key: str | None = None,
    ) -> list[dict]:
        """Synchronous accounting event query — delegates to the warm backend.

        Used by PortfolioValuer (synchronous) to enrich PositionValue with
        cost_basis_usd / unrealized_pnl_usd / realized_pnl_usd at snapshot time.
        Returns [] when no warm backend or the backend predates this method.
        No LIMIT is applied: accurate cost basis requires the full event history.
        """
        if not self._warm or not hasattr(self._warm, "get_accounting_events_sync"):
            self._unimplemented_warn("get_accounting_events_sync", deployment_id)
            return []
        try:
            return self._warm.get_accounting_events_sync(
                deployment_id=deployment_id,
                position_key=position_key,
            )
        except Exception:
            logger.debug("get_accounting_events_sync failed", exc_info=True)
            return []

    def get_position_events_sync(
        self,
        deployment_id: str,
        position_id: str | None = None,
        position_type: str | None = None,
        event_type: str | None = None,
    ) -> list[dict]:
        """Synchronous position event query — delegates to the warm backend.

        Used by PortfolioValuer (synchronous) to enrich LP/PERP PositionValue
        objects with cost_basis_usd at snapshot time by looking up the OPEN event.
        Returns [] when no warm backend or the backend predates this method.
        """
        if not self._warm or not hasattr(self._warm, "get_position_events_sync"):
            self._unimplemented_warn("get_position_events_sync", deployment_id)
            return []
        try:
            return self._warm.get_position_events_sync(
                deployment_id=deployment_id,
                position_id=position_id,
                position_type=position_type,
                event_type=event_type,
            )
        except Exception:
            logger.debug("get_position_events_sync failed", exc_info=True)
            return []

    async def get_accounting_events_for_dashboard(
        self,
        deployment_id: str,
        position_key: str | None = None,
    ) -> list[dict]:
        """Async-context accounting event query for the dashboard service (VIB-3933).

        Distinct from :meth:`get_accounting_events_sync` (which PortfolioValuer
        calls synchronously from inside the snapshot pipeline). The dashboard
        service is async and must not block the event loop on Postgres I/O,
        so it goes through this async sibling.

        Dispatch:
          - PostgresStore exposes ``get_accounting_events`` (async) — preferred.
          - SQLiteStore exposes ``get_accounting_events_sync`` (sync) — wrapped
            in ``run_in_executor`` so the local-mode path does not block the
            running event loop either.

        Returns ``[]`` and emits a one-shot WARN if the backend supports
        neither (Phase 0 visibility for VIB-3933).
        """
        if not self._initialized:
            await self.initialize()
        if not self._warm:
            return []

        # Dispatch order matters (VIB-3933 review finding #2): the dashboard
        # contract is timestamp-ASC + no LIMIT (full history → cost-basis
        # FIFO replay). SQLiteStore exposes that as ``get_accounting_events_sync``;
        # its async sibling uses DESC + LIMIT 500 and is the wrong shape here.
        # PostgresStore has only the async ``get_accounting_events`` (asyncpg
        # is async-only); we wrote that one to ASC and pass an effectively-
        # unbounded limit to mirror the sync semantics.
        # Therefore: prefer ``_sync`` first (SQLite parity), fall through to
        # async (PostgresStore).
        if hasattr(self._warm, "get_accounting_events_sync"):
            import asyncio

            loop = asyncio.get_event_loop()
            try:
                return await loop.run_in_executor(
                    None,
                    lambda: self._warm.get_accounting_events_sync(  # type: ignore[union-attr]
                        deployment_id=deployment_id,
                        position_key=position_key,
                    ),
                )
            except Exception:
                logger.debug("get_accounting_events_sync (executor) failed", exc_info=True)
                return []

        if hasattr(self._warm, "get_accounting_events"):
            try:
                # Effectively-unbounded limit; PostgresStore's async method
                # is ASC, mirroring the sync contract above.
                return await self._warm.get_accounting_events(
                    deployment_id=deployment_id,
                    position_key=position_key,
                    limit=_UNLIMITED_QUERY_LIMIT,
                )
            except Exception:
                logger.debug("get_accounting_events (async) failed", exc_info=True)
                return []

        self._unimplemented_warn("get_accounting_events_for_dashboard", deployment_id)
        return []

    async def get_position_events_for_dashboard(
        self,
        deployment_id: str,
        position_id: str | None = None,
        position_type: str | None = None,
        event_type: str | None = None,
    ) -> list[dict]:
        """Async-context position event query for the dashboard service (VIB-3933).

        See :meth:`get_accounting_events_for_dashboard` for the dispatch
        rationale. PostgresStore exposes the async ``get_position_events_dict``;
        SQLiteStore exposes the sync ``get_position_events_sync`` which we
        invoke through ``run_in_executor``.
        """
        if not self._initialized:
            await self.initialize()
        if not self._warm:
            return []

        # See get_accounting_events_for_dashboard for the dispatch-order
        # rationale (VIB-3933 review finding #2). Prefer SQLite's sync
        # contract; PostgresStore's async ``get_position_events_dict`` is
        # ASC + no-LIMIT to mirror it.
        if hasattr(self._warm, "get_position_events_sync"):
            import asyncio

            loop = asyncio.get_event_loop()
            try:
                return await loop.run_in_executor(
                    None,
                    lambda: self._warm.get_position_events_sync(  # type: ignore[union-attr]
                        deployment_id=deployment_id,
                        position_id=position_id,
                        position_type=position_type,
                        event_type=event_type,
                    ),
                )
            except Exception:
                logger.debug("get_position_events_sync (executor) failed", exc_info=True)
                return []

        if hasattr(self._warm, "get_position_events_dict"):
            try:
                return await self._warm.get_position_events_dict(
                    deployment_id=deployment_id,
                    position_id=position_id,
                    position_type=position_type,
                    event_type=event_type,
                )
            except Exception:
                logger.debug("get_position_events_dict failed", exc_info=True)
                return []

        self._unimplemented_warn("get_position_events_for_dashboard", deployment_id)
        return []

    # --- Phase 0 visibility helper (VIB-3933) -----------------------------
    #
    # Until VIB-3933 the silent-fallthrough on missing PostgresStore methods
    # rendered empty Senior-Quant headers as if they were a measured zero.
    # This helper makes the gap visible: one WARN per (method, identity) pair
    # so we don't spam logs but also don't hide regressions. The same helper
    # is wired into the sync ``_sync`` paths above so the visibility applies
    # whether the call ends up here from PortfolioValuer or the dashboard.
    # ``_unimplemented_logged`` is initialised per-instance in ``__init__``
    # so multi-instance setups don't cross-suppress.

    def _unimplemented_warn(self, method: str, identity: str) -> None:
        """One-shot WARN when the warm backend lacks ``method``."""
        backend_name = type(self._warm).__name__ if self._warm else "None"
        key = (method, identity or "<empty>")
        if key in self._unimplemented_logged:
            return
        self._unimplemented_logged.add(key)
        logger.warning(
            "StateManager: warm backend %s does not implement %s — "
            "returning empty result for identity=%s. This is a silent "
            "fallthrough; if this is hosted Postgres, the read path is missing.",
            backend_name,
            method,
            identity or "<empty>",
        )

    async def update_position_attribution(
        self,
        event_id: str,
        attribution_json: str,
        attribution_version: int,
        deployment_id: str = "",
    ) -> bool:
        """Partial update of attribution_json + attribution_version on a PositionEvent.

        ``deployment_id`` is forwarded to the warm backend so the GSM client
        can pass it through to the gateway proto request as defense-in-depth
        wire-level scope. SQLite ignores it (UUID event_id is globally
        unique); see ``SQLiteStore.update_position_attribution`` for the
        rationale.
        """
        if not self._initialized:
            await self.initialize()
        if not self._warm or not hasattr(self._warm, "update_position_attribution"):
            return False
        start = time.perf_counter()
        try:
            result = await self._warm.update_position_attribution(
                event_id, attribution_json, attribution_version, deployment_id=deployment_id
            )
            latency = (time.perf_counter() - start) * 1000
            ok = bool(result)
            self._record_metrics(
                StateTier.WARM,
                "update_position_attribution",
                latency,
                ok,
                None if ok else "backend_returned_false",
            )
            return ok
        except Exception as e:
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "update_position_attribution", latency, False, str(e))
            logger.error(f"Failed to update position attribution: {e}")
            return False

    async def get_position_events(
        self,
        strategy_id: str,
        event_type: str | None = None,
        limit: int = 100,
    ) -> list:
        """Query position events for a strategy (newest-first).

        Backend signature is ``(deployment_id, position_id, event_type, limit)``
        — call with keyword args so positional binding can't silently bind
        ``event_type`` to ``position_id``. CodeRabbit round-4 caught this:
        my round-3 forwarding was ``(strategy_id, event_type, limit)`` which
        mis-bound, producing empty results for every caller that passed
        ``event_type`` (e.g. ``recompute_attribution`` filtering CLOSE events).
        """
        if not self._initialized:
            await self.initialize()
        if not self._warm or not hasattr(self._warm, "get_position_events"):
            return []
        start = time.perf_counter()
        try:
            result = await self._warm.get_position_events(
                deployment_id=strategy_id,
                position_id=None,
                event_type=event_type,
                limit=limit,
            )
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "get_position_events", latency, True)
            return result
        except Exception as e:
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "get_position_events", latency, False, str(e))
            logger.error(f"Failed to get position events: {e}")
            return []

    async def get_position_history(self, strategy_id: str, position_id: str) -> list:
        """Fetch full history (timestamp-ASC) for a single position_id."""
        if not self._initialized:
            await self.initialize()
        if not self._warm or not hasattr(self._warm, "get_position_history"):
            return []
        start = time.perf_counter()
        try:
            result = await self._warm.get_position_history(strategy_id, position_id)
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "get_position_history", latency, True)
            return result
        except Exception as e:
            latency = (time.perf_counter() - start) * 1000
            self._record_metrics(StateTier.WARM, "get_position_history", latency, False, str(e))
            logger.error(f"Failed to get position history: {e}")
            return []
