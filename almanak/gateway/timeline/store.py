"""Timeline event store for the gateway.

Stores and retrieves timeline events for strategies. Events are
persisted to SQLite (local) or PostgreSQL (deployed) and cached
in memory for fast access.

This is the single source of truth for timeline events. Strategies
record events via ObserveService.RecordTimelineEvent, and dashboards
read events via DashboardService.GetTimeline.

Identifier contract (blueprint 29):
    There is one canonical identity — ``deployment_id`` — resolved once at
    runner boot. Hosted: the platform deployment id (``ALMANAK_DEPLOYMENT_ID``);
    local: a wallet+chain hash. The timeline store keys events on whatever
    ``event.deployment_id`` carries (already the canonical ``deployment_id``);
    there is no gateway-side identity translation on either backend.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import threading
import time
from collections import defaultdict
from contextlib import closing
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
    deployment_id: str
    timestamp: datetime
    event_type: str
    description: str
    tx_hash: str | None = None
    chain: str | None = None
    details: dict[str, Any] = field(default_factory=dict)
    cycle_id: str = ""
    phase: str = ""
    # Typed pointer to transaction_ledger.id when the event narrates an executed
    # intent (VIB-4041). The financial truth — gas, amounts, prices, slippage —
    # lives in the ledger row, never in `details`. Empty string when the event
    # is purely lifecycle/UX (e.g. STATE_CHANGE).
    related_ledger_entry_id: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        d: dict[str, Any] = {
            "event_id": self.event_id,
            "deployment_id": self.deployment_id,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "event_type": self.event_type,
            "description": self.description,
            "tx_hash": self.tx_hash,
            "chain": self.chain,
            "details": self.details,
        }
        if self.cycle_id:
            d["cycle_id"] = self.cycle_id
        if self.phase:
            d["phase"] = self.phase
        if self.related_ledger_entry_id:
            d["related_ledger_entry_id"] = self.related_ledger_entry_id
        return d

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
            deployment_id=data.get("deployment_id", ""),
            timestamp=timestamp,
            event_type=data.get("event_type", "CUSTOM"),
            description=data.get("description", ""),
            tx_hash=data.get("tx_hash"),
            chain=data.get("chain"),
            details=data.get("details") or {},
            cycle_id=data.get("cycle_id", ""),
            phase=data.get("phase", ""),
            related_ledger_entry_id=data.get("related_ledger_entry_id", ""),
        )


# Index names created by ``_init_sqlite``; the quarantine must free exactly these.
_SQLITE_INDEX_NAMES = (
    "idx_timeline_deployment_id",
    "idx_timeline_timestamp",
    "idx_timeline_event_type",
    "idx_timeline_related_ledger",
)


class TimelineStore:
    """Stores and retrieves timeline events.

    Events are persisted to SQLite (local dev) or PostgreSQL (deployed)
    and cached in memory for fast access. The store is thread-safe.

    Backend selection:
    - If ``database_url`` is provided: PostgreSQL via asyncpg (deployed mode).
      Table DDL is owned by the ``metrics-database`` repo's Prisma migrations.
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
            deployment_id="my-strategy",
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

    # Deadline for the truncated-history Postgres fallback in get_events.
    # Must stay well under GatewaySettings.timeout (30s, the gRPC client
    # deadline for GetTimeline/GetActivityFeed): a degraded DB has to fail
    # fast enough that degradation to the cached page happens INSIDE the
    # client budget, not as the caller's DEADLINE_EXCEEDED (PR 3560 review).
    HISTORY_FALLBACK_TIMEOUT_SECONDS: float = 5.0

    def __init__(
        self,
        db_path: str | Path | None = None,
        database_url: str | None = None,
        scope_deployment_id: str | None = None,
        startup_load_limit: int = 10000,
    ):
        """Initialize the timeline store.

        Args:
            db_path: Path to SQLite database file (local development).
            database_url: PostgreSQL connection URL (deployed mode).
                If both are provided, database_url takes precedence.
            scope_deployment_id: When set, the Postgres startup load fetches
                only this deployment's events. Hosted gateways serve exactly
                one deployment but share the platform-wide metrics DB, so an
                unscoped load scales with every deployment's history — not
                this pod's.
            startup_load_limit: Hard row cap on the Postgres startup load
                (newest first). Bounds boot memory even if the scoped
                history itself grows without bound.
        """
        self._db_path = Path(db_path) if db_path else None
        self._database_url = database_url
        self._scope_deployment_id = scope_deployment_id
        if startup_load_limit <= 0:
            # GatewaySettings sanitizes its field the same way; this guard
            # covers direct constructor callers so a bad value can't boot
            # an empty-cache gateway (PR 3560 review).
            logger.warning(
                "startup_load_limit must be > 0 (got %d); using default 10000",
                startup_load_limit,
            )
            startup_load_limit = 10000
        self._startup_load_limit = startup_load_limit
        # Set when the boot load hit the row cap: rows older than
        # ``_pg_cache_floor`` may exist only in PostgreSQL, so cache-served
        # reads that could reach below the floor must fall back to the DB.
        self._pg_history_truncated = False
        self._pg_cache_floor: datetime | None = None
        self._lock = threading.RLock()
        self._cache: dict[str, list[TimelineEvent]] = defaultdict(list)
        self._initialized = False

        # PostgreSQL asyncpg pool + background event loop (only when database_url is set)
        self._pg_pool: asyncpg.Pool | None = None
        self._pg_loop: asyncio.AbstractEventLoop | None = None
        self._pg_thread: threading.Thread | None = None
        self._pg_schema: str | None = None
        # VIB-4041: detected at init against the live Postgres backend. The
        # column is owned by the metrics-database repo's Prisma migrations
        # (see CLAUDE.md "Database schema ownership"); this flag lets the
        # gateway round-trip the field whenever the column is present and
        # gracefully omit it when running against an older schema.
        self._pg_supports_related_ledger: bool = False

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
        # Table DDL is owned by the metrics-database repo's Prisma migrations.

        # VIB-4041: detect related_ledger_entry_id column presence so we can
        # round-trip the typed correlation field once metrics-database adds
        # it. Reading information_schema is cheap (one query at boot) and
        # keeps the SDK side production-ready ahead of the cross-repo migration.
        self._pg_supports_related_ledger = await self._async_detect_related_ledger_column()
        if self._pg_supports_related_ledger:
            logger.info("Postgres timeline_events.related_ledger_entry_id is present; round-trip enabled")
        else:
            logger.info(
                "Postgres timeline_events.related_ledger_entry_id is absent; "
                "field will be empty on read and omitted on write until "
                "metrics-database migration lands"
            )

    async def _async_detect_related_ledger_column(self) -> bool:
        """Return True if timeline_events.related_ledger_entry_id exists.

        Uses the active search_path (set per-connection from `_pg_schema`).
        Failure semantics (CodeRabbit on PR #2117):

        * **Column genuinely absent** (information_schema returned no row) →
          ``False``. This is the legitimate pre-VIB-4051 host case. The gate
          flips off; ``related_ledger_entry_id`` is omitted on writes and reads
          back as ``""``.
        * **Infrastructure error** (connection lost, timeout, permission
          denied, asyncpg internal error, …) → propagate. We do NOT silently
          flip the gate off, because that would degrade a deployed gateway to
          "no correlation writes" without the operator noticing — a UX-only
          regression that would survive every SQLite-backed unit test.
          Failing at boot is the correct production behaviour: it surfaces the
          underlying issue (DB unreachable, schema misconfigured, …) instead
          of papering over it.

        Note: the introspection query is a SELECT against
        ``information_schema.columns``. Column-absent does NOT raise
        ``UndefinedColumn`` — it returns ``None``. So no narrow ``except``
        is needed; the absent-column branch is a value check, not an
        exception path.
        """
        assert self._pg_pool is not None
        async with self._pg_pool.acquire() as conn:
            # Scope to current_schema() so a same-named table in another
            # schema can't false-positive the feature flag.
            row = await conn.fetchval(
                """
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = 'timeline_events'
                  AND column_name = 'related_ledger_entry_id'
                  AND table_schema = current_schema()
                LIMIT 1
                """
            )
            return row is not None

    def _pg_submit(self, coro: Any, timeout: float = 30) -> Any:
        """Submit coroutine to the background event loop and wait for result.

        On timeout the in-flight future is cancelled (best-effort) so an
        abandoned query does not keep loading the DB behind the caller's back.
        """
        assert self._pg_loop is not None, "PostgreSQL event loop not initialized"
        future = asyncio.run_coroutine_threadsafe(coro, self._pg_loop)
        try:
            return future.result(timeout=timeout)
        except TimeoutError:
            future.cancel()
            raise

    def _load_from_postgres(self) -> None:
        """Load events from PostgreSQL into the in-memory cache.

        Events are keyed by the canonical ``deployment_id`` (blueprint 29);
        the cache is keyed the same way so callers find their data with no
        identity translation.
        """
        try:
            events = self._pg_submit(self._async_load_events())
            for event in events:
                # Cache key = deployment_id from PostgreSQL (canonical id)
                self._cache[event.deployment_id].append(event)
            if events:
                scope = self._scope_deployment_id or "ALL deployments"
                logger.info(f"Loaded {len(events)} timeline events from PostgreSQL (scope={scope})")
            if len(events) >= self._startup_load_limit:
                self._pg_history_truncated = True
                self._pg_cache_floor = min(e.timestamp for e in events)
                logger.warning(
                    "Timeline startup load hit the %d-row cap; reads that reach "
                    "past the cached window fall back to Postgres on demand",
                    self._startup_load_limit,
                )
        except Exception:
            logger.exception("Failed to load timeline events from PostgreSQL")

    def _pg_select_clause(self) -> str:
        """SELECT projection shared by the boot load and history reads."""
        related_select = (
            ", COALESCE(related_ledger_entry_id, '') as related_ledger_entry_id"
            if self._pg_supports_related_ledger
            else ""
        )
        return f"""
                SELECT event_id, deployment_id, timestamp, event_type,
                       description, tx_hash, chain, details_json,
                       COALESCE(cycle_id, '') as cycle_id,
                       COALESCE(phase, '') as phase
                       {related_select}
                FROM timeline_events
                """

    def _row_to_event(self, row: Any) -> TimelineEvent:
        """Map a timeline_events row to a TimelineEvent."""
        details = {}
        if row["details_json"]:
            if isinstance(row["details_json"], str):
                try:
                    details = json.loads(row["details_json"])
                except json.JSONDecodeError:
                    pass
            elif isinstance(row["details_json"], dict):
                details = row["details_json"]

        return TimelineEvent(
            event_id=row["event_id"],
            deployment_id=row["deployment_id"],
            timestamp=row["timestamp"],
            event_type=row["event_type"],
            description=row["description"] or "",
            tx_hash=row["tx_hash"],
            chain=row["chain"],
            details=details,
            cycle_id=row["cycle_id"],
            phase=row["phase"],
            related_ledger_entry_id=(row["related_ledger_entry_id"] if self._pg_supports_related_ledger else ""),
        )

    async def _async_load_events(self) -> list[TimelineEvent]:
        assert self._pg_pool is not None
        # The metrics DB is shared platform-wide: without the deployment
        # scope this query transfers every deployment's history into one
        # sidecar's memory (August 2026 NAT-cost incident). LIMIT bounds
        # boot memory even for a single long-lived deployment.
        scope_where = "WHERE deployment_id = $1" if self._scope_deployment_id else ""
        args: list[Any] = [self._scope_deployment_id] if self._scope_deployment_id else []
        limit_param = f"${len(args) + 1}"
        args.append(self._startup_load_limit)
        async with self._pg_pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                {self._pg_select_clause()}
                {scope_where}
                ORDER BY timestamp DESC
                LIMIT {limit_param}
                """,
                *args,
            )
            return [self._row_to_event(row) for row in rows]

    async def _async_fetch_events(
        self,
        deployment_id: str,
        limit: int,
        event_type: str | None,
        since: datetime | None,
        before: datetime | None,
    ) -> list[TimelineEvent]:
        """Scoped, filtered history read — the fallback when a page may
        extend past the truncated boot cache (PR 3560 review). Served by
        idx_timeline_events_deployment_time on hosted Postgres."""
        assert self._pg_pool is not None
        clauses = ["deployment_id = $1"]
        args: list[Any] = [deployment_id]
        if event_type is not None:
            args.append(event_type)
            clauses.append(f"event_type = ${len(args)}")
        if since is not None:
            args.append(since)
            clauses.append(f"timestamp > ${len(args)}")
        if before is not None:
            args.append(before)
            clauses.append(f"timestamp < ${len(args)}")
        args.append(limit)
        limit_param = f"${len(args)}"
        async with self._pg_pool.acquire() as conn:
            rows = await conn.fetch(
                f"""
                {self._pg_select_clause()}
                WHERE {" AND ".join(clauses)}
                ORDER BY timestamp DESC
                LIMIT {limit_param}
                """,
                *args,
            )
            return [self._row_to_event(row) for row in rows]

    def _persist_event_postgres(self, event: TimelineEvent, resolved_id: str, timeout: float = 30) -> None:
        """Persist event to PostgreSQL under the canonical deployment_id."""
        try:
            self._pg_submit(self._async_persist_event(event, resolved_id), timeout=timeout)
        except Exception:
            logger.exception(f"Failed to persist timeline event {event.event_id} to PostgreSQL")

    async def _async_persist_event(self, event: TimelineEvent, resolved_id: str) -> None:
        assert self._pg_pool is not None
        details_json = json.dumps(event.details) if event.details else None
        async with self._pg_pool.acquire() as conn:
            if self._pg_supports_related_ledger:
                await conn.execute(
                    """
                    INSERT INTO timeline_events
                        (event_id, deployment_id, timestamp, event_type, description,
                         tx_hash, chain, details_json, cycle_id, phase,
                         related_ledger_entry_id)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    ON CONFLICT (event_id) DO UPDATE SET
                        event_type = EXCLUDED.event_type,
                        description = EXCLUDED.description,
                        tx_hash = EXCLUDED.tx_hash,
                        chain = EXCLUDED.chain,
                        details_json = EXCLUDED.details_json,
                        cycle_id = EXCLUDED.cycle_id,
                        phase = EXCLUDED.phase,
                        related_ledger_entry_id = EXCLUDED.related_ledger_entry_id
                    """,
                    event.event_id,
                    resolved_id,
                    event.timestamp,
                    event.event_type,
                    event.description,
                    event.tx_hash,
                    event.chain,
                    details_json,
                    event.cycle_id,
                    event.phase,
                    event.related_ledger_entry_id or None,
                )
            else:
                await conn.execute(
                    """
                    INSERT INTO timeline_events
                        (event_id, deployment_id, timestamp, event_type, description,
                         tx_hash, chain, details_json, cycle_id, phase)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (event_id) DO UPDATE SET
                        event_type = EXCLUDED.event_type,
                        description = EXCLUDED.description,
                        tx_hash = EXCLUDED.tx_hash,
                        chain = EXCLUDED.chain,
                        details_json = EXCLUDED.details_json,
                        cycle_id = EXCLUDED.cycle_id,
                        phase = EXCLUDED.phase
                    """,
                    event.event_id,
                    resolved_id,
                    event.timestamp,
                    event.event_type,
                    event.description,
                    event.tx_hash,
                    event.chain,
                    details_json,
                    event.cycle_id,
                    event.phase,
                )

    def _clear_events_postgres(self, resolved_id: str | None) -> None:
        """Clear events from PostgreSQL using the canonical deployment_id."""
        try:
            self._pg_submit(self._async_clear_events(resolved_id))
        except Exception:
            logger.exception("Failed to clear timeline events from PostgreSQL")

    async def _async_clear_events(self, resolved_id: str | None) -> None:
        assert self._pg_pool is not None
        async with self._pg_pool.acquire() as conn:
            if resolved_id is not None:
                await conn.execute(
                    "DELETE FROM timeline_events WHERE deployment_id = $1",
                    resolved_id,
                )
            else:
                await conn.execute("DELETE FROM timeline_events")

    # =========================================================================
    # SQLite backend (local development)
    # =========================================================================

    def _init_sqlite(self) -> None:
        """Create SQLite database tables if they don't exist."""
        if not self._db_path:
            return

        self._db_path.parent.mkdir(parents=True, exist_ok=True)

        with sqlite3.connect(str(self._db_path)) as conn:
            self._quarantine_pre_gateway_table(conn)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS timeline_events (
                    event_id TEXT PRIMARY KEY,
                    deployment_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    description TEXT,
                    tx_hash TEXT,
                    chain TEXT,
                    details_json TEXT,
                    cycle_id TEXT DEFAULT '',
                    phase TEXT DEFAULT '',
                    related_ledger_entry_id TEXT DEFAULT '',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Migration (VIB-4722): rename strategy_id -> deployment_id on
            # existing local DBs to match the unified identity column.
            try:
                conn.execute("ALTER TABLE timeline_events RENAME COLUMN strategy_id TO deployment_id")
            except sqlite3.OperationalError:
                pass  # Already renamed (or fresh DB created with deployment_id)
            # Migrate: add cycle_id and phase columns if not present (existing DBs)
            try:
                conn.execute("ALTER TABLE timeline_events ADD COLUMN cycle_id TEXT DEFAULT ''")
            except sqlite3.OperationalError:
                pass  # Column already exists
            try:
                conn.execute("ALTER TABLE timeline_events ADD COLUMN phase TEXT DEFAULT ''")
            except sqlite3.OperationalError:
                pass  # Column already exists
            # VIB-4041 — typed correlation pointer to transaction_ledger.id.
            try:
                conn.execute("ALTER TABLE timeline_events ADD COLUMN related_ledger_entry_id TEXT DEFAULT ''")
            except sqlite3.OperationalError:
                pass  # Column already exists

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_timeline_deployment_id
                ON timeline_events(deployment_id)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_timeline_timestamp
                ON timeline_events(timestamp DESC)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_timeline_event_type
                ON timeline_events(event_type)
            """)
            # VIB-4041 — index for compositor queries that join by ledger id.
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_timeline_related_ledger
                ON timeline_events(related_ledger_entry_id)
                WHERE related_ledger_entry_id IS NOT NULL AND related_ledger_entry_id != ''
            """)
            conn.commit()

    @staticmethod
    def _quarantine_pre_gateway_table(conn: sqlite3.Connection) -> None:
        """Move a pre-gateway SDK ``timeline_events`` table out of the way.

        This store now shares its file with the state backend, whose older
        versions owned a ``timeline_events`` table of a different shape
        (``event_data`` / ``correlation_id``, no ``event_id``). ``CREATE TABLE IF
        NOT EXISTS`` is a no-op against it and the index DDL below then fails on
        the missing ``timestamp`` column, aborting gateway boot for good. Rename
        rather than drop: the rows are unreadable here but they are still the
        operator's, and the rename is what lets this store own the real name.
        """
        row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='timeline_events'").fetchone()
        if row is None:
            return
        columns = {info[1] for info in conn.execute("PRAGMA table_info(timeline_events)")}
        if "event_id" in columns:
            return
        # Every sqlite_master name, not just tables: RENAME TO also collides with
        # a view, and picking a name an index already holds fails just as hard.
        existing = {r[0] for r in conn.execute("SELECT name FROM sqlite_master")}
        quarantined = "timeline_events_pre_gateway"
        suffix = 2
        while quarantined in existing:
            quarantined = f"timeline_events_pre_gateway_{suffix}"
            suffix += 1
        # Indexes follow the table under RENAME, so release exactly the names this
        # store is about to create. A wider match would destroy legacy indexes
        # that collide with nothing, and CREATE INDEX IF NOT EXISTS below is a
        # silent no-op for any name left attached to the quarantined table.
        for index in _SQLITE_INDEX_NAMES:
            conn.execute(f'DROP INDEX IF EXISTS "{index}"')
        conn.execute(f'ALTER TABLE timeline_events RENAME TO "{quarantined}"')
        logger.warning(
            "Renamed pre-gateway timeline_events table in %s to %s: its columns predate the "
            "gateway timeline schema and cannot be read here. No rows were deleted.",
            conn.execute("PRAGMA database_list").fetchone()[2],
            quarantined,
        )

    def _load_from_sqlite(self) -> None:
        """Load events from SQLite into cache."""
        if not self._db_path or not self._db_path.exists():
            return

        with sqlite3.connect(str(self._db_path)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT event_id, deployment_id, timestamp, event_type,
                       description, tx_hash, chain, details_json,
                       cycle_id, phase, related_ledger_entry_id
                FROM timeline_events
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
                    deployment_id=row["deployment_id"],
                    timestamp=datetime.fromisoformat(row["timestamp"]),
                    event_type=row["event_type"],
                    description=row["description"] or "",
                    tx_hash=row["tx_hash"],
                    chain=row["chain"],
                    details=details,
                    cycle_id=row["cycle_id"] or "",
                    phase=row["phase"] or "",
                    related_ledger_entry_id=row["related_ledger_entry_id"] or "",
                )
                self._cache[event.deployment_id].append(event)

            total_events = sum(len(events) for events in self._cache.values())
            if total_events > 0:
                logger.info(f"Loaded {total_events} timeline events from SQLite")

    def _persist_event_sqlite(self, event: TimelineEvent, deadline: float | None = None) -> None:
        """Persist a single event to SQLite."""
        if not self._db_path:
            return

        details_json = json.dumps(event.details) if event.details else None

        with closing(sqlite3.connect(str(self._db_path), timeout=_remaining_wait(deadline, 5.0))) as conn, conn:
            if deadline is not None:
                conn.execute(f"PRAGMA busy_timeout = {int(_remaining_wait(deadline, 5.0) * 1000)}")
            conn.execute(
                """
                INSERT OR REPLACE INTO timeline_events
                (event_id, deployment_id, timestamp, event_type, description,
                 tx_hash, chain, details_json, cycle_id, phase,
                 related_ledger_entry_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.event_id,
                    event.deployment_id,
                    event.timestamp.isoformat(),
                    event.event_type,
                    event.description,
                    event.tx_hash,
                    event.chain,
                    details_json,
                    event.cycle_id,
                    event.phase,
                    event.related_ledger_entry_id,
                ),
            )
            if deadline is not None:
                # Readers can allow INSERT yet block COMMIT's exclusive lock.
                conn.execute(f"PRAGMA busy_timeout = {int(_remaining_wait(deadline, 5.0) * 1000)}")
            conn.commit()

    # =========================================================================
    # Public API (backend-agnostic, reads from in-memory cache)
    # =========================================================================

    def add_event(self, event: TimelineEvent, *, timeout: float | None = None) -> None:
        """Add a new timeline event.

        Args:
            event: The timeline event to store
            timeout: Optional shared budget for lock and database waits. Requires
                initialization at server startup; does not bound OS scheduling or I/O.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        if not self._initialized:
            if timeout is not None:
                raise RuntimeError("Bounded timeline writes require an initialized store")
            self.initialize()

        # One identity (blueprint 29): event.deployment_id is the canonical
        # deployment_id resolved at runner boot — used directly as the key.
        cache_key = event.deployment_id

        acquired = (
            self._lock.acquire() if deadline is None else self._lock.acquire(timeout=_remaining_wait(deadline, 0))
        )
        if not acquired:
            raise TimeoutError("Timeline write lock budget exhausted")
        try:
            # Add to cache under resolved key
            self._cache[cache_key].append(event)

            # Sort by timestamp descending (most recent first)
            self._cache[cache_key].sort(key=lambda e: e.timestamp, reverse=True)

            # Persist to database
            if self._uses_postgres:
                # PostgreSQL logs write failures without raising; cache acceptance is not durability.
                if deadline is None:
                    self._persist_event_postgres(event, cache_key)
                else:
                    self._persist_event_postgres(event, cache_key, timeout=_remaining_wait(deadline, 30))
            elif self._db_path:
                if deadline is None:
                    self._persist_event_sqlite(event)
                else:
                    self._persist_event_sqlite(event, deadline=deadline)

        finally:
            self._lock.release()

        logger.debug(f"Added timeline event: {event.event_type} for {event.deployment_id}")

    def get_events(
        self,
        deployment_id: str,
        limit: int = 50,
        event_type: str | None = None,
        since: datetime | None = None,
        before: datetime | None = None,
    ) -> list[TimelineEvent]:
        """Get timeline events for a strategy.

        Args:
            deployment_id: The canonical deployment_id (blueprint 29 — no
                gateway-side identity translation).
            limit: Maximum number of events to return
            event_type: Optional filter by event type
            since: Optional filter for events after this timestamp
            before: Optional cursor — only events strictly older than this
                timestamp. Pushed down here (not post-fetch) so paginated
                callers can never receive a "newest N rows that don't match
                the cursor" empty page when activity is dense.

        Returns:
            List of TimelineEvent objects, sorted by timestamp descending
        """
        if not self._initialized:
            self.initialize()

        cache_key = deployment_id

        with self._lock:
            events = self._cache.get(cache_key, [])

            # Apply filters
            if event_type:
                events = [e for e in events if e.event_type == event_type]

            if since:
                events = [e for e in events if e.timestamp > since]

            if before is not None:
                events = [e for e in events if e.timestamp < before]

            # Apply limit
            page = events[:limit]

        # Truncated boot cache: a short page whose window is not fully inside
        # the cached range may be missing rows that exist only in PostgreSQL
        # (PR 3560 review — a `before` cursor older than the cache floor used
        # to return an empty page). Read the authoritative page from the DB,
        # outside the lock so a slow DB cannot stall writers.
        if (
            self._pg_history_truncated
            and self._pg_cache_floor is not None
            and len(page) < limit
            and (since is None or since < self._pg_cache_floor)
        ):
            try:
                return self._pg_submit(
                    self._async_fetch_events(deployment_id, limit, event_type, since, before),
                    timeout=self.HISTORY_FALLBACK_TIMEOUT_SECONDS,
                )
            except Exception:
                logger.exception("Postgres history read failed; serving the cached page")

        return page

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

    def get_deployment_ids(self) -> list[str]:
        """Get all deployment IDs that have timeline events.

        Returns:
            List of deployment IDs
        """
        if not self._initialized:
            self.initialize()

        with self._lock:
            return list(self._cache.keys())

    def clear_events(self, deployment_id: str | None = None) -> None:
        """Clear events from the store.

        Args:
            deployment_id: If provided, only clear events for this strategy.
                        If None, clear all events.
        """
        with self._lock:
            if deployment_id is not None:
                self._cache.pop(deployment_id, None)
            else:
                self._cache.clear()

            if self._uses_postgres:
                self._clear_events_postgres(deployment_id)
            elif self._db_path:
                with sqlite3.connect(str(self._db_path)) as conn:
                    if deployment_id is not None:
                        conn.execute(
                            "DELETE FROM timeline_events WHERE deployment_id = ?",
                            (deployment_id,),
                        )
                    else:
                        conn.execute("DELETE FROM timeline_events")
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


def _remaining_wait(deadline: float | None, default: float) -> float:
    if deadline is None:
        return default
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("Timeline write budget exhausted")
    return remaining


_timeline_store: TimelineStore | None = None


def get_initialized_timeline_store() -> TimelineStore:
    """Return the server-owned store without starting database work during execution."""
    if _timeline_store is None or not _timeline_store._initialized:
        raise RuntimeError("Timeline store is not initialized")
    return _timeline_store


def get_timeline_store(
    db_path: str | Path | None = None,
    database_url: str | None = None,
    scope_deployment_id: str | None = None,
    startup_load_limit: int = 10000,
) -> TimelineStore:
    """Get the default timeline store (singleton).

    Args:
        db_path: Path to SQLite database (local development).
            Only used on first call.
        database_url: PostgreSQL connection URL (deployed mode).
            Only used on first call. Takes precedence over db_path.
        scope_deployment_id: Scope the Postgres startup load to one
            deployment (hosted mode). Only used on first call.
        startup_load_limit: Row cap for the Postgres startup load.
            Only used on first call.

    Returns:
        Shared TimelineStore instance.
    """
    global _timeline_store
    if _timeline_store is None:
        _timeline_store = TimelineStore(
            db_path=db_path,
            database_url=database_url,
            scope_deployment_id=scope_deployment_id,
            startup_load_limit=startup_load_limit,
        )
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
