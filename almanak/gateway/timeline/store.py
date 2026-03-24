"""Timeline event store for the gateway.

Stores and retrieves timeline events for strategies. Events are
persisted to SQLite (local) or PostgreSQL (deployed) and cached
in memory for fast access.

This is the single source of truth for timeline events. Strategies
record events via ObserveService.RecordTimelineEvent, and dashboards
read events via DashboardService.GetTimeline.

Deployed-mode identifier contract:
    In K8s, the platform injects ``AGENT_ID`` (a UUID) into every container.
    The SDK strategy runner uses ``strategy.strategy_id`` (e.g.
    ``"uniswap_rsi:abc123"``).  To keep metrics_db consistent with the
    lifecycle tables (which already resolve to AGENT_ID), the PostgreSQL
    backend applies the same ``_resolve_agent_id()`` mapping used by
    ``PostgresLifecycleStore``.  SQLite / in-memory mode does NOT resolve
    because ``AGENT_ID`` is not set in local dev.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import threading
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

if TYPE_CHECKING:
    import asyncpg

logger = logging.getLogger(__name__)


@dataclass
class TimelineEvent:
    """Timeline event stored in gateway.

    Represents a single event in a strategy's timeline, such as a
    transaction, state change, or error.
    """

    event_id: str
    strategy_id: str
    timestamp: datetime
    event_type: str
    description: str
    tx_hash: str | None = None
    chain: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "event_id": self.event_id,
            "strategy_id": self.strategy_id,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "event_type": self.event_type,
            "description": self.description,
            "tx_hash": self.tx_hash,
            "chain": self.chain,
            "details": self.details,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TimelineEvent:
        """Create from dictionary."""
        timestamp = data.get("timestamp")
        if isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp)
        elif timestamp is None:
            timestamp = datetime.now(UTC)

        return cls(
            event_id=data.get("event_id", str(uuid4())),
            strategy_id=data.get("strategy_id", ""),
            timestamp=timestamp,
            event_type=data.get("event_type", "CUSTOM"),
            description=data.get("description", ""),
            tx_hash=data.get("tx_hash"),
            chain=data.get("chain"),
            details=data.get("details") or {},
        )


class TimelineStore:
    """Stores and retrieves timeline events.

    Events are persisted to SQLite (local dev) or PostgreSQL (deployed)
    and cached in memory for fast access. The store is thread-safe.

    Backend selection:
    - If ``database_url`` is provided: PostgreSQL via asyncpg (deployed mode).
      Table DDL is managed by ``gateway.database.ensure_schema()``.
    - If ``db_path`` is provided: SQLite file (local development).
    - If neither: in-memory only (no persistence).

    Usage:
        # Local development (SQLite)
        store = TimelineStore(db_path="timeline.db")
        store.initialize()

        # Deployed mode (PostgreSQL)
        store = TimelineStore(database_url="postgres://...")
        store.initialize()

        # Add event
        event = TimelineEvent(
            event_id=str(uuid4()),
            strategy_id="my-strategy",
            timestamp=datetime.now(UTC),
            event_type="TRADE",
            description="Swapped 100 USDC for ETH",
            tx_hash="0x123...",
            chain="arbitrum",
        )
        store.add_event(event)

        # Get events
        events = store.get_events("my-strategy", limit=50)
    """

    def __init__(
        self,
        db_path: str | Path | None = None,
        database_url: str | None = None,
    ):
        """Initialize the timeline store.

        Args:
            db_path: Path to SQLite database file (local development).
            database_url: PostgreSQL connection URL (deployed mode).
                If both are provided, database_url takes precedence.
        """
        self._db_path = Path(db_path) if db_path else None
        self._database_url = database_url
        self._lock = threading.RLock()
        self._cache: dict[str, list[TimelineEvent]] = defaultdict(list)
        self._initialized = False

        # PostgreSQL asyncpg pool + background event loop (only when database_url is set)
        self._pg_pool: asyncpg.Pool | None = None
        self._pg_loop: asyncio.AbstractEventLoop | None = None
        self._pg_thread: threading.Thread | None = None
        self._pg_schema: str | None = None

    @property
    def _uses_postgres(self) -> bool:
        return self._database_url is not None

    def initialize(self) -> None:
        """Initialize the store and create database tables if needed."""
        if self._initialized:
            return

        with self._lock:
            if self._initialized:
                return
            if self._uses_postgres:
                self._init_postgres()
                self._load_from_postgres()
            elif self._db_path:
                self._init_sqlite()
                self._load_from_sqlite()
            self._initialized = True
            backend = (
                "PostgreSQL" if self._uses_postgres else f"SQLite ({self._db_path})" if self._db_path else "memory"
            )
            logger.info(f"TimelineStore initialized (backend={backend})")

    # =========================================================================
    # Deployed-mode identifier resolution
    # =========================================================================

    @staticmethod
    def _resolve_agent_id(strategy_id: str) -> str:
        """Map SDK strategy_id to platform AGENT_ID in deployed mode.

        Same contract as ``PostgresLifecycleStore._resolve_agent_id()``:
        when the ``AGENT_ID`` env var is set (K8s pods), all metrics_db
        keys use that value so lifecycle, state, and timeline tables are
        consistent.  In local dev (no env var), passthrough.
        """
        env_agent_id = os.environ.get("AGENT_ID")
        if env_agent_id is None:
            return strategy_id
        resolved = env_agent_id.strip()
        return resolved or strategy_id

    # =========================================================================
    # PostgreSQL backend (deployed mode)
    # =========================================================================

    def _init_postgres(self) -> None:
        """Initialize asyncpg pool on a dedicated background event loop."""
        from almanak.gateway.database import _strip_schema_param

        assert self._database_url is not None  # guaranteed by _uses_postgres check
        clean_url, self._pg_schema = _strip_schema_param(self._database_url)
        self._database_url_clean = clean_url

        loop = asyncio.new_event_loop()
        self._pg_loop = loop

        def _run() -> None:
            asyncio.set_event_loop(loop)
            loop.run_forever()

        self._pg_thread = threading.Thread(target=_run, daemon=True, name="pg-timeline-loop")
        self._pg_thread.start()

        try:
            self._pg_submit(self._async_init_pool())
        except Exception:
            # Clean up loop/thread so retries don't leak resources
            if self._pg_loop:
                self._pg_loop.call_soon_threadsafe(self._pg_loop.stop)
            if self._pg_thread:
                self._pg_thread.join(timeout=5)
            self._pg_pool = None
            self._pg_loop = None
            self._pg_thread = None
            raise

    async def _async_init_pool(self) -> None:
        import asyncpg

        schema = self._pg_schema

        async def _init_connection(conn: asyncpg.Connection) -> None:
            if schema:
                await conn.fetchval(
                    "SELECT pg_catalog.set_config('search_path', $1, false)",
                    schema,
                )

        self._pg_pool = await asyncpg.create_pool(
            self._database_url_clean,
            min_size=1,
            max_size=5,
            init=_init_connection,
            statement_cache_size=0,
        )
        # Table DDL is managed by ensure_schema() at gateway startup.

    def _pg_submit(self, coro: Any) -> Any:
        """Submit coroutine to the background event loop and wait for result."""
        assert self._pg_loop is not None, "PostgreSQL event loop not initialized"
        future = asyncio.run_coroutine_threadsafe(coro, self._pg_loop)
        return future.result(timeout=30)

    def _load_from_postgres(self) -> None:
        """Load events from PostgreSQL into the in-memory cache.

        Events in PostgreSQL are keyed by the resolved agent_id (platform
        AGENT_ID in deployed mode).  The cache is keyed the same way so
        callers that resolve before lookup will find their data.
        """
        try:
            events = self._pg_submit(self._async_load_events())
            for event in events:
                # Cache key = agent_id from PostgreSQL (already resolved)
                self._cache[event.strategy_id].append(event)
            if events:
                logger.info(f"Loaded {len(events)} timeline events from PostgreSQL")
        except Exception:
            logger.exception("Failed to load timeline events from PostgreSQL")

    async def _async_load_events(self) -> list[TimelineEvent]:
        assert self._pg_pool is not None
        async with self._pg_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT event_id, agent_id, timestamp, event_type,
                       description, tx_hash, chain, details_json
                FROM v2_timeline_events
                ORDER BY timestamp DESC
                """
            )
            events = []
            for row in rows:
                details = {}
                if row["details_json"]:
                    if isinstance(row["details_json"], str):
                        try:
                            details = json.loads(row["details_json"])
                        except json.JSONDecodeError:
                            pass
                    elif isinstance(row["details_json"], dict):
                        details = row["details_json"]

                events.append(
                    TimelineEvent(
                        event_id=row["event_id"],
                        strategy_id=row["agent_id"],
                        timestamp=row["timestamp"],
                        event_type=row["event_type"],
                        description=row["description"] or "",
                        tx_hash=row["tx_hash"],
                        chain=row["chain"],
                        details=details,
                    )
                )
            return events

    def _persist_event_postgres(self, event: TimelineEvent, resolved_id: str) -> None:
        """Persist event to PostgreSQL under the resolved agent_id."""
        try:
            self._pg_submit(self._async_persist_event(event, resolved_id))
        except Exception:
            logger.exception(f"Failed to persist timeline event {event.event_id} to PostgreSQL")

    async def _async_persist_event(self, event: TimelineEvent, resolved_id: str) -> None:
        assert self._pg_pool is not None
        details_json = json.dumps(event.details) if event.details else None
        async with self._pg_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO v2_timeline_events
                    (event_id, agent_id, timestamp, event_type, description,
                     tx_hash, chain, details_json)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (event_id) DO UPDATE SET
                    event_type = EXCLUDED.event_type,
                    description = EXCLUDED.description,
                    tx_hash = EXCLUDED.tx_hash,
                    chain = EXCLUDED.chain,
                    details_json = EXCLUDED.details_json
                """,
                event.event_id,
                resolved_id,
                event.timestamp,
                event.event_type,
                event.description,
                event.tx_hash,
                event.chain,
                details_json,
            )

    def _clear_events_postgres(self, resolved_id: str | None) -> None:
        """Clear events from PostgreSQL using the resolved agent_id."""
        try:
            self._pg_submit(self._async_clear_events(resolved_id))
        except Exception:
            logger.exception("Failed to clear timeline events from PostgreSQL")

    async def _async_clear_events(self, resolved_id: str | None) -> None:
        assert self._pg_pool is not None
        async with self._pg_pool.acquire() as conn:
            if resolved_id is not None:
                await conn.execute(
                    "DELETE FROM v2_timeline_events WHERE agent_id = $1",
                    resolved_id,
                )
            else:
                await conn.execute("DELETE FROM v2_timeline_events")

    # =========================================================================
    # SQLite backend (local development)
    # =========================================================================

    def _init_sqlite(self) -> None:
        """Create SQLite database tables if they don't exist."""
        if not self._db_path:
            return

        self._db_path.parent.mkdir(parents=True, exist_ok=True)

        with sqlite3.connect(str(self._db_path)) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS v2_timeline_events (
                    event_id TEXT PRIMARY KEY,
                    strategy_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    description TEXT,
                    tx_hash TEXT,
                    chain TEXT,
                    details_json TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_timeline_strategy_id
                ON v2_timeline_events(strategy_id)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_timeline_timestamp
                ON v2_timeline_events(timestamp DESC)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_timeline_event_type
                ON v2_timeline_events(event_type)
            """)
            conn.commit()

    def _load_from_sqlite(self) -> None:
        """Load events from SQLite into cache."""
        if not self._db_path or not self._db_path.exists():
            return

        with sqlite3.connect(str(self._db_path)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT event_id, strategy_id, timestamp, event_type,
                       description, tx_hash, chain, details_json
                FROM v2_timeline_events
                ORDER BY timestamp DESC
            """)

            for row in cursor:
                details = {}
                if row["details_json"]:
                    try:
                        details = json.loads(row["details_json"])
                    except json.JSONDecodeError:
                        pass

                event = TimelineEvent(
                    event_id=row["event_id"],
                    strategy_id=row["strategy_id"],
                    timestamp=datetime.fromisoformat(row["timestamp"]),
                    event_type=row["event_type"],
                    description=row["description"] or "",
                    tx_hash=row["tx_hash"],
                    chain=row["chain"],
                    details=details,
                )
                self._cache[event.strategy_id].append(event)

            total_events = sum(len(events) for events in self._cache.values())
            if total_events > 0:
                logger.info(f"Loaded {total_events} timeline events from SQLite")

    def _persist_event_sqlite(self, event: TimelineEvent) -> None:
        """Persist a single event to SQLite."""
        if not self._db_path:
            return

        details_json = json.dumps(event.details) if event.details else None

        with sqlite3.connect(str(self._db_path)) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO v2_timeline_events
                (event_id, strategy_id, timestamp, event_type, description,
                 tx_hash, chain, details_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.event_id,
                    event.strategy_id,
                    event.timestamp.isoformat(),
                    event.event_type,
                    event.description,
                    event.tx_hash,
                    event.chain,
                    details_json,
                ),
            )
            conn.commit()

    # =========================================================================
    # Public API (backend-agnostic, reads from in-memory cache)
    # =========================================================================

    def add_event(self, event: TimelineEvent) -> None:
        """Add a new timeline event.

        Args:
            event: The timeline event to store
        """
        if not self._initialized:
            self.initialize()

        # Resolve the cache/DB key: AGENT_ID in deployed mode, passthrough locally
        cache_key = self._resolve_agent_id(event.strategy_id) if self._uses_postgres else event.strategy_id

        with self._lock:
            # Add to cache under resolved key
            self._cache[cache_key].append(event)

            # Sort by timestamp descending (most recent first)
            self._cache[cache_key].sort(key=lambda e: e.timestamp, reverse=True)

            # Persist to database
            if self._uses_postgres:
                self._persist_event_postgres(event, cache_key)
            elif self._db_path:
                self._persist_event_sqlite(event)

        logger.debug(f"Added timeline event: {event.event_type} for {event.strategy_id}")

    def get_events(
        self,
        strategy_id: str,
        limit: int = 50,
        event_type: str | None = None,
        since: datetime | None = None,
    ) -> list[TimelineEvent]:
        """Get timeline events for a strategy.

        Args:
            strategy_id: Strategy identifier (SDK strategy_id; resolved
                to AGENT_ID automatically in deployed mode)
            limit: Maximum number of events to return
            event_type: Optional filter by event type
            since: Optional filter for events after this timestamp

        Returns:
            List of TimelineEvent objects, sorted by timestamp descending
        """
        if not self._initialized:
            self.initialize()

        cache_key = self._resolve_agent_id(strategy_id) if self._uses_postgres else strategy_id

        with self._lock:
            events = self._cache.get(cache_key, [])

            # Apply filters
            if event_type:
                events = [e for e in events if e.event_type == event_type]

            if since:
                events = [e for e in events if e.timestamp > since]

            # Apply limit
            return events[:limit]

    def get_recent_events(
        self,
        limit: int = 100,
    ) -> list[TimelineEvent]:
        """Get most recent events across all strategies.

        Args:
            limit: Maximum number of events to return

        Returns:
            List of TimelineEvent objects, sorted by timestamp descending
        """
        if not self._initialized:
            self.initialize()

        with self._lock:
            # Collect all events
            all_events: list[TimelineEvent] = []
            for events in self._cache.values():
                all_events.extend(events)

            # Sort by timestamp descending
            all_events.sort(key=lambda e: e.timestamp, reverse=True)

            return all_events[:limit]

    def get_strategy_ids(self) -> list[str]:
        """Get all strategy IDs that have timeline events.

        Returns:
            List of strategy IDs
        """
        if not self._initialized:
            self.initialize()

        with self._lock:
            return list(self._cache.keys())

    def clear_events(self, strategy_id: str | None = None) -> None:
        """Clear events from the store.

        Args:
            strategy_id: If provided, only clear events for this strategy.
                        If None, clear all events.
        """
        resolved_id = (
            self._resolve_agent_id(strategy_id) if strategy_id is not None and self._uses_postgres else strategy_id
        )

        with self._lock:
            if resolved_id is not None:
                self._cache.pop(resolved_id, None)
            else:
                self._cache.clear()

            if self._uses_postgres:
                self._clear_events_postgres(resolved_id)
            elif self._db_path:
                with sqlite3.connect(str(self._db_path)) as conn:
                    if strategy_id is not None:
                        conn.execute(
                            "DELETE FROM v2_timeline_events WHERE strategy_id = ?",
                            (strategy_id,),
                        )
                    else:
                        conn.execute("DELETE FROM v2_timeline_events")
                    conn.commit()

    def close(self) -> None:
        """Close the store and release resources."""
        with self._lock:
            self._cache.clear()
            self._initialized = False

            # Close PostgreSQL pool and background thread
            if self._pg_pool and self._pg_loop:
                try:
                    self._pg_submit(self._pg_pool.close())
                except Exception:
                    logger.warning("Failed to close TimelineStore PostgreSQL pool", exc_info=True)
            if self._pg_loop:
                self._pg_loop.call_soon_threadsafe(self._pg_loop.stop)
            if self._pg_thread:
                self._pg_thread.join(timeout=5)
            self._pg_pool = None
            self._pg_loop = None
            self._pg_thread = None


# =============================================================================
# Singleton accessor
# =============================================================================

_timeline_store: TimelineStore | None = None


def get_timeline_store(
    db_path: str | Path | None = None,
    database_url: str | None = None,
) -> TimelineStore:
    """Get the default timeline store (singleton).

    Args:
        db_path: Path to SQLite database (local development).
            Only used on first call.
        database_url: PostgreSQL connection URL (deployed mode).
            Only used on first call. Takes precedence over db_path.

    Returns:
        Shared TimelineStore instance.
    """
    global _timeline_store
    if _timeline_store is None:
        _timeline_store = TimelineStore(db_path=db_path, database_url=database_url)
        _timeline_store.initialize()
    return _timeline_store


def reset_timeline_store() -> None:
    """Reset the timeline store singleton.

    Useful for testing.
    """
    global _timeline_store
    if _timeline_store is not None:
        _timeline_store.close()
        _timeline_store = None
