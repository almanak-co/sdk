"""Timeline event store for the gateway.

Stores and retrieves timeline events for strategies. Events are
stored in SQLite for persistence and cached in memory for fast access.

This is the single source of truth for timeline events. Strategies
record events via ObserveService.RecordTimelineEvent, and dashboards
read events via DashboardService.GetTimeline.
"""

import json
import logging
import sqlite3
import threading
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

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
    def from_dict(cls, data: dict[str, Any]) -> "TimelineEvent":
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

    Events are stored in SQLite for persistence and cached in memory
    for fast access. The store is thread-safe.

    Usage:
        store = TimelineStore(db_path="timeline.db")
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

    def __init__(self, db_path: str | Path | None = None):
        """Initialize the timeline store.

        Args:
            db_path: Path to SQLite database file. If None, uses in-memory storage only.
        """
        self._db_path = Path(db_path) if db_path else None
        self._lock = threading.RLock()
        self._cache: dict[str, list[TimelineEvent]] = defaultdict(list)
        self._initialized = False

    def initialize(self) -> None:
        """Initialize the store and create database tables if needed."""
        if self._initialized:
            return

        with self._lock:
            if self._db_path:
                self._init_database()
                self._load_from_database()
            self._initialized = True
            logger.info(f"TimelineStore initialized (db_path={self._db_path})")

    def _init_database(self) -> None:
        """Create database tables if they don't exist."""
        if not self._db_path:
            return

        self._db_path.parent.mkdir(parents=True, exist_ok=True)

        with sqlite3.connect(str(self._db_path)) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS timeline_events (
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
                ON timeline_events(strategy_id)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_timeline_timestamp
                ON timeline_events(timestamp DESC)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_timeline_event_type
                ON timeline_events(event_type)
            """)
            conn.commit()

    def _load_from_database(self) -> None:
        """Load events from database into cache."""
        if not self._db_path or not self._db_path.exists():
            return

        with sqlite3.connect(str(self._db_path)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT event_id, strategy_id, timestamp, event_type,
                       description, tx_hash, chain, details_json
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
                logger.info(f"Loaded {total_events} timeline events from database")

    def add_event(self, event: TimelineEvent) -> None:
        """Add a new timeline event.

        Args:
            event: The timeline event to store
        """
        if not self._initialized:
            self.initialize()

        with self._lock:
            # Add to cache
            self._cache[event.strategy_id].append(event)

            # Sort by timestamp descending (most recent first)
            self._cache[event.strategy_id].sort(key=lambda e: e.timestamp, reverse=True)

            # Persist to database
            if self._db_path:
                self._persist_event(event)

        logger.debug(f"Added timeline event: {event.event_type} for {event.strategy_id}")

    def _persist_event(self, event: TimelineEvent) -> None:
        """Persist a single event to the database."""
        if not self._db_path:
            return

        details_json = json.dumps(event.details) if event.details else None

        with sqlite3.connect(str(self._db_path)) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO timeline_events
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

    def get_events(
        self,
        strategy_id: str,
        limit: int = 50,
        event_type: str | None = None,
        since: datetime | None = None,
    ) -> list[TimelineEvent]:
        """Get timeline events for a strategy.

        Args:
            strategy_id: Strategy identifier
            limit: Maximum number of events to return
            event_type: Optional filter by event type
            since: Optional filter for events after this timestamp

        Returns:
            List of TimelineEvent objects, sorted by timestamp descending
        """
        if not self._initialized:
            self.initialize()

        with self._lock:
            events = self._cache.get(strategy_id, [])

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
        with self._lock:
            if strategy_id:
                self._cache.pop(strategy_id, None)
                if self._db_path:
                    with sqlite3.connect(str(self._db_path)) as conn:
                        conn.execute(
                            "DELETE FROM timeline_events WHERE strategy_id = ?",
                            (strategy_id,),
                        )
                        conn.commit()
            else:
                self._cache.clear()
                if self._db_path:
                    with sqlite3.connect(str(self._db_path)) as conn:
                        conn.execute("DELETE FROM timeline_events")
                        conn.commit()

    def close(self) -> None:
        """Close the store and release resources."""
        # SQLite connections are closed automatically when out of scope
        # Just clear the cache
        with self._lock:
            self._cache.clear()
            self._initialized = False


# =============================================================================
# Singleton accessor
# =============================================================================

_timeline_store: TimelineStore | None = None


def get_timeline_store(db_path: str | Path | None = None) -> TimelineStore:
    """Get the default timeline store (singleton).

    Args:
        db_path: Path to SQLite database. Only used on first call.

    Returns:
        Shared TimelineStore instance.
    """
    global _timeline_store
    if _timeline_store is None:
        _timeline_store = TimelineStore(db_path=db_path)
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
