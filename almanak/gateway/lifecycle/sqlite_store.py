"""SQLite-backed lifecycle store for local development and self-hosted setups.

Creates v2_agent_command and v2_agent_state tables in a local .db file.
Single-tenant: one DB file per strategy run.

Follows the same pattern as InstanceRegistry and TimelineStore:
- Schema embedded as Python string constant (LIFECYCLE_SCHEMA_SQL)
- CREATE TABLE IF NOT EXISTS on initialize() (idempotent)
- Thread-safe via threading.RLock()
- Singleton via get_lifecycle_store()
"""

import logging
import sqlite3
import threading
from datetime import UTC, datetime
from pathlib import Path

from .store import AgentCommand, AgentState

logger = logging.getLogger(__name__)


LIFECYCLE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS v2_agent_state (
    agent_id          TEXT PRIMARY KEY,
    state             TEXT NOT NULL,
    state_changed_at  TEXT NOT NULL DEFAULT (datetime('now')),
    last_heartbeat_at TEXT,
    error_message     TEXT,
    iteration_count   INTEGER DEFAULT 0,
    source            TEXT NOT NULL DEFAULT 'gateway'
);

CREATE TABLE IF NOT EXISTS v2_agent_command (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id      TEXT NOT NULL,
    command       TEXT NOT NULL,
    issued_at     TEXT NOT NULL DEFAULT (datetime('now')),
    issued_by     TEXT NOT NULL,
    processed_at  TEXT
);

CREATE INDEX IF NOT EXISTS idx_v2_agent_command_pending
    ON v2_agent_command (agent_id, id DESC)
    WHERE processed_at IS NULL;
"""


class SQLiteLifecycleStore:
    """SQLite-backed lifecycle store for local development and self-hosted setups.

    Creates v2_agent_command and v2_agent_state tables in a local .db file.
    Single-tenant: one DB file per strategy run.

    Follows the same pattern as InstanceRegistry and TimelineStore:
    - Schema embedded as Python string constant (LIFECYCLE_SCHEMA_SQL)
    - CREATE TABLE IF NOT EXISTS on initialize() (idempotent)
    - Thread-safe via threading.RLock()
    - Singleton via get_lifecycle_store()
    """

    def __init__(self, db_path: str | Path):
        self._db_path = Path(db_path)
        self._lock = threading.RLock()
        self._initialized = False

    def initialize(self) -> None:
        if self._initialized:
            return
        with self._lock:
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
            with sqlite3.connect(str(self._db_path)) as conn:
                conn.executescript(LIFECYCLE_SCHEMA_SQL)
                # Migration: add source column to existing databases
                try:
                    conn.execute("ALTER TABLE v2_agent_state ADD COLUMN source TEXT NOT NULL DEFAULT 'gateway'")
                except sqlite3.OperationalError:
                    pass  # Column already exists
                conn.commit()
            self._initialized = True
            logger.info(f"SQLiteLifecycleStore initialized: {self._db_path}")

    def close(self) -> None:
        with self._lock:
            self._initialized = False

    def write_state(
        self,
        agent_id: str,
        state: str,
        error_message: str | None = None,
    ) -> None:
        if not self._initialized:
            self.initialize()
        now = datetime.now(UTC).isoformat()
        with self._lock:
            with sqlite3.connect(str(self._db_path)) as conn:
                conn.execute(
                    """
                    INSERT INTO v2_agent_state
                        (agent_id, state, state_changed_at, last_heartbeat_at, error_message, source)
                    VALUES (?, ?, ?, ?, ?, 'gateway')
                    ON CONFLICT (agent_id) DO UPDATE SET
                        state = excluded.state,
                        state_changed_at = excluded.state_changed_at,
                        last_heartbeat_at = excluded.last_heartbeat_at,
                        error_message = excluded.error_message,
                        source = 'gateway'
                    """,
                    (agent_id, state, now, now, error_message),
                )
                conn.commit()

    def read_state(self, agent_id: str) -> AgentState | None:
        if not self._initialized:
            self.initialize()
        with self._lock:
            with sqlite3.connect(str(self._db_path)) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    "SELECT * FROM v2_agent_state WHERE agent_id = ?",
                    (agent_id,),
                )
                row = cursor.fetchone()
                if row is None:
                    return None
                return AgentState(
                    agent_id=row["agent_id"],
                    state=row["state"],
                    state_changed_at=datetime.fromisoformat(row["state_changed_at"]),
                    last_heartbeat_at=datetime.fromisoformat(row["last_heartbeat_at"])
                    if row["last_heartbeat_at"]
                    else None,
                    error_message=row["error_message"],
                    iteration_count=row["iteration_count"] or 0,
                    source=row["source"] if "source" in row.keys() else "gateway",
                )

    def heartbeat(self, agent_id: str) -> None:
        if not self._initialized:
            self.initialize()
        now = datetime.now(UTC).isoformat()
        with self._lock:
            with sqlite3.connect(str(self._db_path)) as conn:
                conn.execute(
                    """
                    UPDATE v2_agent_state
                    SET last_heartbeat_at = ?, iteration_count = iteration_count + 1
                    WHERE agent_id = ?
                    """,
                    (now, agent_id),
                )
                conn.commit()

    def read_pending_command(self, agent_id: str) -> AgentCommand | None:
        if not self._initialized:
            self.initialize()
        with self._lock:
            with sqlite3.connect(str(self._db_path)) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    """
                    SELECT id, agent_id, command, issued_at, issued_by, processed_at
                    FROM v2_agent_command
                    WHERE agent_id = ? AND processed_at IS NULL
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (agent_id,),
                )
                row = cursor.fetchone()
                if row is None:
                    return None
                return AgentCommand(
                    id=row["id"],
                    agent_id=row["agent_id"],
                    command=row["command"],
                    issued_at=datetime.fromisoformat(row["issued_at"]),
                    issued_by=row["issued_by"],
                    processed_at=None,
                )

    def ack_command(self, command_id: int) -> None:
        if not self._initialized:
            self.initialize()
        now = datetime.now(UTC).isoformat()
        with self._lock:
            with sqlite3.connect(str(self._db_path)) as conn:
                conn.execute(
                    "UPDATE v2_agent_command SET processed_at = ? WHERE id = ?",
                    (now, command_id),
                )
                conn.commit()

    def write_command(self, agent_id: str, command: str, issued_by: str) -> None:
        if not self._initialized:
            self.initialize()
        now = datetime.now(UTC).isoformat()
        with self._lock:
            with sqlite3.connect(str(self._db_path)) as conn:
                conn.execute(
                    """
                    INSERT INTO v2_agent_command (agent_id, command, issued_at, issued_by)
                    VALUES (?, ?, ?, ?)
                    """,
                    (agent_id, command, now, issued_by),
                )
                conn.commit()
