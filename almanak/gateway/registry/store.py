"""Instance registry store for the gateway.

Stores and retrieves strategy instance registrations. Instances are
stored in SQLite for persistence and cached in memory for fast access.

This is the single source of truth for strategy instances. Runners
register instances via DashboardService.RegisterStrategyInstance, and
dashboards list instances via DashboardService.ListStrategies.
"""

import logging
import sqlite3
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from almanak.gateway.validation import resolve_agent_id as _resolve_agent_id

logger = logging.getLogger(__name__)


@dataclass
class StrategyInstance:
    """A registered strategy instance.

    Represents a running or previously-running strategy instance with
    unique ID, display name, status, and heartbeat tracking.
    """

    strategy_id: str  # PK -- always unique (e.g. "uniswap_lp:a1b2c3d4e5f6")
    strategy_name: str  # Display name (e.g. "uniswap_lp")
    template_name: str  # Class name (e.g. "UniswapLPStrategy")
    chain: str
    protocol: str
    wallet_address: str
    config_json: str
    chains: str  # Comma-separated chain list (e.g., "arbitrum,base")
    chain_wallets: str  # JSON-encoded per-chain wallet map (e.g., '{"arbitrum":"0x..."}')
    status: str  # RUNNING | INACTIVE | ERROR | PAUSED | STALE
    archived: bool  # Hidden from dashboard, data retained
    created_at: datetime
    updated_at: datetime
    last_heartbeat_at: datetime
    version: str


class InstanceRegistry:
    """Stores and retrieves strategy instance registrations.

    Instances are stored in SQLite for persistence and cached in memory
    for fast access. The registry is thread-safe.

    Usage:
        registry = InstanceRegistry(db_path="gateway.db")
        registry.initialize()

        instance = StrategyInstance(
            strategy_id="uniswap_lp:abc123",
            strategy_name="uniswap_lp",
            ...
        )
        registry.register(instance)

        instances = registry.list_all()
    """

    def __init__(self, db_path: str | Path):
        """Initialize the instance registry.

        Args:
            db_path: Path to SQLite database file.
        """
        self._db_path = Path(db_path)
        self._lock = threading.RLock()
        self._cache: dict[str, StrategyInstance] = {}
        self._initialized = False

    @property
    def db_path(self) -> Path:
        """Get the database path."""
        return self._db_path

    def initialize(self) -> None:
        """Initialize the store and create database tables if needed."""
        if self._initialized:
            return

        with self._lock:
            self._init_database()
            self._load_from_database()
            self._initialized = True
            logger.info(f"InstanceRegistry initialized (db_path={self._db_path})")

    def _init_database(self) -> None:
        """Create database tables if they don't exist."""
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

        with sqlite3.connect(str(self._db_path)) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS strategy_instances (
                    strategy_id TEXT PRIMARY KEY,
                    strategy_name TEXT NOT NULL,
                    template_name TEXT NOT NULL DEFAULT '',
                    chain TEXT NOT NULL DEFAULT '',
                    protocol TEXT NOT NULL DEFAULT '',
                    wallet_address TEXT NOT NULL DEFAULT '',
                    config_json TEXT,
                    status TEXT NOT NULL DEFAULT 'RUNNING',
                    archived INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    last_heartbeat_at TEXT NOT NULL,
                    version TEXT NOT NULL DEFAULT ''
                )
            """)
            # Migration: add chains and chain_wallets columns if missing
            try:
                conn.execute("ALTER TABLE strategy_instances ADD COLUMN chains TEXT NOT NULL DEFAULT ''")
            except sqlite3.OperationalError:
                pass  # column already exists
            try:
                conn.execute("ALTER TABLE strategy_instances ADD COLUMN chain_wallets TEXT NOT NULL DEFAULT ''")
            except sqlite3.OperationalError:
                pass  # column already exists
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_instances_status
                ON strategy_instances(status)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_instances_archived
                ON strategy_instances(archived)
            """)
            conn.commit()

    def _load_from_database(self) -> None:
        """Load instances from database into cache."""
        if not self._db_path.exists():
            return

        with sqlite3.connect(str(self._db_path)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT strategy_id, strategy_name, template_name, chain, protocol,
                       wallet_address, config_json, status, archived,
                       created_at, updated_at, last_heartbeat_at, version,
                       chains, chain_wallets
                FROM strategy_instances
            """)

            for row in cursor:
                instance = StrategyInstance(
                    strategy_id=row["strategy_id"],
                    strategy_name=row["strategy_name"],
                    template_name=row["template_name"] or "",
                    chain=row["chain"] or "",
                    protocol=row["protocol"] or "",
                    wallet_address=row["wallet_address"] or "",
                    config_json=row["config_json"] or "",
                    chains=row["chains"] or "",
                    chain_wallets=row["chain_wallets"] or "",
                    status=row["status"],
                    archived=bool(row["archived"]),
                    created_at=datetime.fromisoformat(row["created_at"]),
                    updated_at=datetime.fromisoformat(row["updated_at"]),
                    last_heartbeat_at=datetime.fromisoformat(row["last_heartbeat_at"]),
                    version=row["version"] or "",
                )
                self._cache[instance.strategy_id] = instance

            if self._cache:
                logger.info(f"Loaded {len(self._cache)} strategy instances from database")

    def register(self, instance: StrategyInstance) -> bool:
        """Register or re-register a strategy instance.

        Uses INSERT OR REPLACE so re-registering after restart works.

        Args:
            instance: The strategy instance to register.

        Returns:
            True if this was a new registration, False if re-registration.
        """
        if not self._initialized:
            self.initialize()

        # In deployed mode, normalise the instance key to AGENT_ID
        instance.strategy_id = _resolve_agent_id(instance.strategy_id)

        with self._lock:
            already_existed = instance.strategy_id in self._cache
            self._cache[instance.strategy_id] = instance
            self._persist_instance(instance)

        logger.info(
            f"{'Re-registered' if already_existed else 'Registered'} "
            f"strategy instance: {instance.strategy_id} ({instance.strategy_name})"
        )
        return not already_existed

    def _persist_instance(self, instance: StrategyInstance) -> None:
        """Persist a single instance to the database."""
        with sqlite3.connect(str(self._db_path)) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO strategy_instances
                (strategy_id, strategy_name, template_name, chain, protocol,
                 wallet_address, config_json, status, archived,
                 created_at, updated_at, last_heartbeat_at, version,
                 chains, chain_wallets)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    instance.strategy_id,
                    instance.strategy_name,
                    instance.template_name,
                    instance.chain,
                    instance.protocol,
                    instance.wallet_address,
                    instance.config_json,
                    instance.status,
                    1 if instance.archived else 0,
                    instance.created_at.isoformat(),
                    instance.updated_at.isoformat(),
                    instance.last_heartbeat_at.isoformat(),
                    instance.version,
                    instance.chains,
                    instance.chain_wallets,
                ),
            )
            conn.commit()

    def update_status(self, strategy_id: str, status: str, reason: str = "") -> bool:
        """Update the status of a strategy instance.

        Args:
            strategy_id: The strategy instance ID.
            status: New status (RUNNING, INACTIVE, ERROR, PAUSED).
            reason: Optional reason for the status change.

        Returns:
            True if instance was found and updated.
        """
        if not self._initialized:
            self.initialize()

        strategy_id = _resolve_agent_id(strategy_id)

        with self._lock:
            instance = self._cache.get(strategy_id)
            if instance is None:
                return False

            now = datetime.now(UTC)
            instance.status = status
            instance.updated_at = now
            instance.last_heartbeat_at = now
            self._persist_instance(instance)

        logger.info(f"Updated status for {strategy_id}: {status}" + (f" ({reason})" if reason else ""))
        return True

    def heartbeat(self, strategy_id: str) -> bool:
        """Update the heartbeat timestamp for a strategy instance.

        Args:
            strategy_id: The strategy instance ID.

        Returns:
            True if instance was found and updated.
        """
        if not self._initialized:
            self.initialize()

        strategy_id = _resolve_agent_id(strategy_id)

        recovered = False
        with self._lock:
            instance = self._cache.get(strategy_id)
            if instance is None:
                return False

            now = datetime.now(UTC)
            instance.last_heartbeat_at = now
            instance.updated_at = now

            # Recover STALE -> RUNNING: a heartbeat proves the strategy is alive.
            # This is the recovery path after startup reconciliation (VIB-1279).
            recovered = instance.status == "STALE"
            if recovered:
                instance.status = "RUNNING"

            # Persist heartbeat (and status recovery if applicable) to DB
            with sqlite3.connect(str(self._db_path)) as conn:
                conn.execute(
                    """
                    UPDATE strategy_instances
                    SET last_heartbeat_at = ?, updated_at = ?, status = ?
                    WHERE strategy_id = ?
                    """,
                    (now.isoformat(), now.isoformat(), instance.status, strategy_id),
                )
                conn.commit()

        if recovered:
            logger.info("Heartbeat received from STALE instance %s — recovered to RUNNING", strategy_id)
        return True

    def reconcile_stale_on_startup(self) -> int:
        """Mark all RUNNING entries as STALE on gateway startup.

        Called once during gateway boot to clear stale RUNNING entries from
        a previous session. Strategies that are actually running will re-register
        on their next heartbeat and transition back to RUNNING.

        Returns:
            Number of entries marked STALE.
        """
        if not self._initialized:
            self.initialize()

        with self._lock:
            now = datetime.now(UTC)
            stale_ids = [sid for sid, inst in self._cache.items() if inst.status == "RUNNING"]

            if not stale_ids:
                return 0

            # Batch UPDATE in SQLite
            placeholders = ",".join("?" * len(stale_ids))
            with sqlite3.connect(str(self._db_path)) as conn:
                conn.execute(
                    f"UPDATE strategy_instances SET status = 'STALE', updated_at = ?"
                    f" WHERE strategy_id IN ({placeholders}) AND status = 'RUNNING'",
                    [now.isoformat(), *stale_ids],
                )
                conn.commit()

            # Update in-memory cache
            for sid in stale_ids:
                self._cache[sid].status = "STALE"
                self._cache[sid].updated_at = now

        logger.warning(
            "Startup reconciliation: marked %d RUNNING instance(s) as STALE "
            "(they will recover on next heartbeat if still running): %s",
            len(stale_ids),
            stale_ids,
        )
        return len(stale_ids)

    def enforce_heartbeat_ttl(self, stale_threshold_seconds: int = 300) -> int:
        """Mark RUNNING entries with expired heartbeats as STALE persistently.

        Called periodically by the gateway background TTL enforcer task to
        catch mid-session crashes. Unlike `_compute_effective_status()` in
        DashboardService (which is read-time only), this persists the STALE
        status to SQLite so it survives across queries.

        Args:
            stale_threshold_seconds: Seconds without heartbeat before marking STALE.
                Defaults to 300 (5 minutes, matching DashboardService's threshold).

        Returns:
            Number of entries newly marked STALE.
        """
        if not self._initialized:
            self.initialize()

        with self._lock:
            now = datetime.now(UTC)
            stale_ids = []
            for sid, inst in self._cache.items():
                if inst.status != "RUNNING":
                    continue
                heartbeat = inst.last_heartbeat_at
                if heartbeat.tzinfo is None:
                    heartbeat = heartbeat.replace(tzinfo=UTC)
                age = (now - heartbeat).total_seconds()
                if age > stale_threshold_seconds:
                    stale_ids.append(sid)

            if not stale_ids:
                return 0

            placeholders = ",".join("?" * len(stale_ids))
            with sqlite3.connect(str(self._db_path)) as conn:
                conn.execute(
                    f"UPDATE strategy_instances SET status = 'STALE', updated_at = ?"
                    f" WHERE strategy_id IN ({placeholders}) AND status = 'RUNNING'",
                    [now.isoformat(), *stale_ids],
                )
                conn.commit()

            for sid in stale_ids:
                self._cache[sid].status = "STALE"
                self._cache[sid].updated_at = now

        logger.warning(
            "Heartbeat TTL enforcer: marked %d RUNNING instance(s) as STALE (no heartbeat for >%ds): %s",
            len(stale_ids),
            stale_threshold_seconds,
            stale_ids,
        )
        return len(stale_ids)

    def archive(self, strategy_id: str) -> bool:
        """Archive a strategy instance (hidden from dashboard, data retained).

        Args:
            strategy_id: The strategy instance ID.

        Returns:
            True if instance was found and archived.
        """
        if not self._initialized:
            self.initialize()

        with self._lock:
            instance = self._cache.get(strategy_id)
            if instance is None:
                return False

            instance.archived = True
            instance.updated_at = datetime.now(UTC)
            self._persist_instance(instance)

        logger.info(f"Archived strategy instance: {strategy_id}")
        return True

    def unarchive(self, strategy_id: str) -> bool:
        """Unarchive a strategy instance.

        Args:
            strategy_id: The strategy instance ID.

        Returns:
            True if instance was found and unarchived.
        """
        if not self._initialized:
            self.initialize()

        with self._lock:
            instance = self._cache.get(strategy_id)
            if instance is None:
                return False

            instance.archived = False
            instance.updated_at = datetime.now(UTC)
            self._persist_instance(instance)

        logger.info(f"Unarchived strategy instance: {strategy_id}")
        return True

    def purge(self, strategy_id: str) -> bool:
        """Delete a strategy instance from the registry.

        Note: For atomic purge of instance + events, use purge_with_events().

        Args:
            strategy_id: The strategy instance ID.

        Returns:
            True if instance was found and deleted.
        """
        if not self._initialized:
            self.initialize()

        with self._lock:
            if strategy_id not in self._cache:
                return False

            del self._cache[strategy_id]

            with sqlite3.connect(str(self._db_path)) as conn:
                conn.execute(
                    "DELETE FROM strategy_instances WHERE strategy_id = ?",
                    (strategy_id,),
                )
                conn.commit()

        logger.info(f"Purged strategy instance: {strategy_id}")
        return True

    def purge_with_events(self, strategy_id: str) -> bool:
        """Atomically delete a strategy instance and all its timeline events.

        Both tables must be in the same SQLite database for this to work
        in a single transaction.

        Args:
            strategy_id: The strategy instance ID.

        Returns:
            True if instance was found and purged.
        """
        if not self._initialized:
            self.initialize()

        with self._lock:
            if strategy_id not in self._cache:
                return False

            # DB operations first -- only remove from cache after successful commit
            with sqlite3.connect(str(self._db_path)) as conn:
                conn.execute(
                    "DELETE FROM strategy_instances WHERE strategy_id = ?",
                    (strategy_id,),
                )
                try:
                    conn.execute(
                        "DELETE FROM timeline_events WHERE strategy_id = ?",
                        (strategy_id,),
                    )
                except sqlite3.OperationalError as e:
                    if "no such table" not in str(e):
                        raise
                conn.commit()

            del self._cache[strategy_id]

        logger.info(f"Purged strategy instance and events: {strategy_id}")
        return True

    def get(self, strategy_id: str) -> StrategyInstance | None:
        """Get a strategy instance by ID.

        Args:
            strategy_id: The strategy instance ID.

        Returns:
            StrategyInstance or None if not found.
        """
        if not self._initialized:
            self.initialize()

        strategy_id = _resolve_agent_id(strategy_id)

        with self._lock:
            return self._cache.get(strategy_id)

    def list_all(self, include_archived: bool = False) -> list[StrategyInstance]:
        """List all strategy instances.

        Args:
            include_archived: If True, include archived instances.

        Returns:
            List of StrategyInstance objects.
        """
        if not self._initialized:
            self.initialize()

        with self._lock:
            instances = list(self._cache.values())
            if not include_archived:
                instances = [i for i in instances if not i.archived]
            return instances

    def close(self) -> None:
        """Close the store and release resources."""
        with self._lock:
            self._cache.clear()
            self._initialized = False


# =============================================================================
# Singleton accessor
# =============================================================================

_instance_registry: InstanceRegistry | None = None
_instance_registry_lock = threading.Lock()


def get_instance_registry(db_path: str | Path | None = None) -> InstanceRegistry:
    """Get the default instance registry (singleton).

    Thread-safe via double-checked locking.

    Args:
        db_path: Path to SQLite database. Only used on first call.

    Returns:
        Shared InstanceRegistry instance.
    """
    global _instance_registry
    if _instance_registry is None:
        with _instance_registry_lock:
            if _instance_registry is None:
                if db_path is None:
                    from almanak.gateway.core.settings import DEFAULT_GATEWAY_DB_PATH

                    db_path = DEFAULT_GATEWAY_DB_PATH
                _instance_registry = InstanceRegistry(db_path=db_path)
                _instance_registry.initialize()
    return _instance_registry


def reset_instance_registry() -> None:
    """Reset the instance registry singleton.

    Useful for testing.
    """
    global _instance_registry
    if _instance_registry is not None:
        _instance_registry.close()
        _instance_registry = None
