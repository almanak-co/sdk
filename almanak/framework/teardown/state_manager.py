"""Teardown State Manager for persisting teardown requests.

This manager handles the state-based signaling mechanism for teardowns.
Teardown requests are stored in SQLite/PostgreSQL and checked by strategies
each iteration.

Flow:
1. CLI/Dashboard/Risk Guard writes TeardownRequest to database
2. Strategy's _check_teardown_request() reads this each iteration
3. When found, strategy initiates teardown and updates the request status
4. On completion, request is marked complete/failed

This decoupled design allows multiple triggers:
- CLI: `almanak strat teardown request --strategy <name> --mode graceful`
- Config: Set `teardown.request = true` in strategy config (hot-reload)
- Dashboard: Click "Close Strategy" button
- Risk Guards: Auto-protect triggers when health factor drops
"""

import json
import logging
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from almanak.framework.teardown.models import (
    TeardownAssetPolicy,
    TeardownMode,
    TeardownPhase,
    TeardownRequest,
    TeardownStatus,
)

logger = logging.getLogger(__name__)


class TeardownStateManager:
    """Manages TeardownRequest persistence in SQLite.

    Provides CRUD operations for teardown requests, enabling
    the state-based signaling mechanism for triggering teardowns
    from multiple sources.

    Thread-safe for concurrent access from CLI, dashboard, and strategies.
    """

    def __init__(self, db_path: str | Path | None = None):
        """Initialize the state manager.

        Args:
            db_path: Path to SQLite database. Defaults to 'almanak_state.db'
        """
        self.db_path = Path(db_path) if db_path else Path("almanak_state.db")
        self._init_db()

    def _init_db(self) -> None:
        """Initialize the database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS teardown_requests (
                    strategy_id TEXT PRIMARY KEY,
                    mode TEXT NOT NULL,
                    asset_policy TEXT NOT NULL,
                    target_token TEXT NOT NULL,
                    reason TEXT,
                    requested_at TEXT NOT NULL,
                    requested_by TEXT NOT NULL,
                    status TEXT NOT NULL,
                    acknowledged_at TEXT,
                    started_at TEXT,
                    completed_at TEXT,
                    current_phase TEXT,
                    positions_total INTEGER DEFAULT 0,
                    positions_closed INTEGER DEFAULT 0,
                    positions_failed INTEGER DEFAULT 0,
                    cancel_requested INTEGER DEFAULT 0,
                    cancel_deadline TEXT,
                    error_message TEXT,
                    result_json TEXT,
                    updated_at TEXT NOT NULL
                )
            """)
            conn.commit()
            logger.debug(f"Initialized teardown state database at {self.db_path}")

    def create_request(self, request: TeardownRequest) -> None:
        """Create or replace a teardown request.

        If a request already exists for this strategy, it will be replaced.
        This allows re-triggering teardowns that were cancelled.

        Args:
            request: The teardown request to persist
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO teardown_requests (
                    strategy_id, mode, asset_policy, target_token,
                    reason, requested_at, requested_by, status,
                    acknowledged_at, started_at, completed_at,
                    current_phase, positions_total, positions_closed,
                    positions_failed, cancel_requested, cancel_deadline,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    request.strategy_id,
                    request.mode.value,
                    request.asset_policy.value,
                    request.target_token,
                    request.reason,
                    request.requested_at.isoformat(),
                    request.requested_by,
                    request.status.value,
                    request.acknowledged_at.isoformat() if request.acknowledged_at else None,
                    request.started_at.isoformat() if request.started_at else None,
                    request.completed_at.isoformat() if request.completed_at else None,
                    request.current_phase.value if request.current_phase else None,
                    request.positions_total,
                    request.positions_closed,
                    request.positions_failed,
                    1 if request.cancel_requested else 0,
                    request.cancel_deadline.isoformat() if request.cancel_deadline else None,
                    datetime.now(UTC).isoformat(),
                ),
            )
            conn.commit()
            logger.info(
                f"Created teardown request for {request.strategy_id}: "
                f"mode={request.mode.value}, by={request.requested_by}"
            )

    def get_request(self, strategy_id: str) -> TeardownRequest | None:
        """Get the current teardown request for a strategy.

        Args:
            strategy_id: The strategy ID to look up

        Returns:
            TeardownRequest if one exists, None otherwise
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT * FROM teardown_requests WHERE strategy_id = ?",
                (strategy_id,),
            )
            row = cursor.fetchone()

            if not row:
                return None

            return self._row_to_request(row)

    def get_active_request(self, strategy_id: str) -> TeardownRequest | None:
        """Get an active (non-completed) teardown request.

        Args:
            strategy_id: The strategy ID to look up

        Returns:
            TeardownRequest if an active one exists, None otherwise
        """
        request = self.get_request(strategy_id)
        if request and request.is_active:
            return request
        return None

    def get_pending_requests(self) -> list[TeardownRequest]:
        """Get all pending teardown requests.

        Returns:
            List of teardown requests with status=PENDING
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT * FROM teardown_requests WHERE status = ?",
                (TeardownStatus.PENDING.value,),
            )
            return [self._row_to_request(row) for row in cursor.fetchall()]

    def get_all_active_requests(self) -> list[TeardownRequest]:
        """Get all active teardown requests across all strategies.

        Returns:
            List of all active (non-completed) teardown requests
        """
        terminal_statuses = [
            TeardownStatus.COMPLETED.value,
            TeardownStatus.CANCELLED.value,
            TeardownStatus.FAILED.value,
        ]
        placeholders = ",".join("?" * len(terminal_statuses))

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                f"SELECT * FROM teardown_requests WHERE status NOT IN ({placeholders})",
                terminal_statuses,
            )
            return [self._row_to_request(row) for row in cursor.fetchall()]

    def get_all_requests(self) -> list[TeardownRequest]:
        """Get all teardown requests including completed and cancelled.

        Returns:
            List of all teardown requests regardless of status
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("SELECT * FROM teardown_requests ORDER BY requested_at DESC")
            return [self._row_to_request(row) for row in cursor.fetchall()]

    def update_request(self, request: TeardownRequest) -> None:
        """Update an existing teardown request.

        Args:
            request: The updated teardown request
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                UPDATE teardown_requests SET
                    mode = ?, asset_policy = ?, target_token = ?,
                    status = ?, acknowledged_at = ?, started_at = ?,
                    completed_at = ?, current_phase = ?,
                    positions_total = ?, positions_closed = ?,
                    positions_failed = ?, cancel_requested = ?,
                    cancel_deadline = ?, updated_at = ?
                WHERE strategy_id = ?
                """,
                (
                    request.mode.value,
                    request.asset_policy.value,
                    request.target_token,
                    request.status.value,
                    request.acknowledged_at.isoformat() if request.acknowledged_at else None,
                    request.started_at.isoformat() if request.started_at else None,
                    request.completed_at.isoformat() if request.completed_at else None,
                    request.current_phase.value if request.current_phase else None,
                    request.positions_total,
                    request.positions_closed,
                    request.positions_failed,
                    1 if request.cancel_requested else 0,
                    request.cancel_deadline.isoformat() if request.cancel_deadline else None,
                    datetime.now(UTC).isoformat(),
                    request.strategy_id,
                ),
            )
            conn.commit()
            logger.debug(f"Updated teardown request for {request.strategy_id}: status={request.status.value}")

    def acknowledge_request(self, strategy_id: str) -> TeardownRequest | None:
        """Acknowledge a pending teardown request.

        Called when a strategy picks up the request and begins processing.

        Args:
            strategy_id: The strategy acknowledging the request

        Returns:
            The acknowledged request, or None if not found
        """
        request = self.get_active_request(strategy_id)
        if not request:
            return None

        request.acknowledged_at = datetime.now(UTC)
        request.status = TeardownStatus.CANCEL_WINDOW
        self.update_request(request)

        logger.info(f"Acknowledged teardown request for {strategy_id}")
        return request

    def mark_started(self, strategy_id: str, total_positions: int = 0) -> TeardownRequest | None:
        """Mark a teardown as started (after cancel window).

        Args:
            strategy_id: The strategy ID
            total_positions: Total number of positions to close

        Returns:
            The updated request, or None if not found
        """
        request = self.get_active_request(strategy_id)
        if not request:
            return None

        request.started_at = datetime.now(UTC)
        request.status = TeardownStatus.EXECUTING
        request.current_phase = TeardownPhase.POSITION_CLOSURE
        request.positions_total = total_positions
        self.update_request(request)

        logger.info(f"Started teardown for {strategy_id}: {total_positions} positions")
        return request

    def update_progress(
        self,
        strategy_id: str,
        positions_closed: int,
        positions_failed: int = 0,
        current_phase: TeardownPhase | None = None,
    ) -> TeardownRequest | None:
        """Update teardown progress.

        Args:
            strategy_id: The strategy ID
            positions_closed: Number of positions successfully closed
            positions_failed: Number of positions that failed to close
            current_phase: Current phase of the teardown

        Returns:
            The updated request, or None if not found
        """
        request = self.get_active_request(strategy_id)
        if not request:
            return None

        request.positions_closed = positions_closed
        request.positions_failed = positions_failed
        if current_phase:
            request.current_phase = current_phase
        self.update_request(request)

        return request

    def mark_completed(
        self,
        strategy_id: str,
        result: dict | None = None,
    ) -> TeardownRequest | None:
        """Mark a teardown as completed.

        Args:
            strategy_id: The strategy ID
            result: Optional result details (final balances, costs, etc.)

        Returns:
            The updated request, or None if not found
        """
        request = self.get_active_request(strategy_id)
        if not request:
            return None

        request.status = TeardownStatus.COMPLETED
        request.completed_at = datetime.now(UTC)

        if result:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "UPDATE teardown_requests SET result_json = ? WHERE strategy_id = ?",
                    (json.dumps(result), strategy_id),
                )
                conn.commit()

        self.update_request(request)
        logger.info(f"Completed teardown for {strategy_id}")
        return request

    def mark_failed(
        self,
        strategy_id: str,
        error: str,
    ) -> TeardownRequest | None:
        """Mark a teardown as failed.

        Args:
            strategy_id: The strategy ID
            error: Error message describing the failure

        Returns:
            The updated request, or None if not found
        """
        request = self.get_active_request(strategy_id)
        if not request:
            return None

        request.status = TeardownStatus.FAILED
        request.completed_at = datetime.now(UTC)

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "UPDATE teardown_requests SET error_message = ? WHERE strategy_id = ?",
                (error, strategy_id),
            )
            conn.commit()

        self.update_request(request)
        logger.error(f"Failed teardown for {strategy_id}: {error}")
        return request

    def request_cancel(self, strategy_id: str) -> bool:
        """Request cancellation of a teardown.

        This sets the cancel_requested flag, which will be checked
        by the strategy during the next iteration.

        Args:
            strategy_id: The strategy ID

        Returns:
            True if cancel request was recorded, False if not cancellable
        """
        request = self.get_active_request(strategy_id)
        if not request:
            return False

        if not request.can_cancel:
            logger.warning(f"Cannot cancel teardown for {strategy_id}: past cancel deadline")
            return False

        request.cancel_requested = True
        self.update_request(request)

        logger.info(f"Cancel requested for teardown {strategy_id}")
        return True

    def mark_cancelled(self, strategy_id: str) -> TeardownRequest | None:
        """Mark a teardown as cancelled.

        Args:
            strategy_id: The strategy ID

        Returns:
            The updated request, or None if not found
        """
        request = self.get_active_request(strategy_id)
        if not request:
            return None

        request.status = TeardownStatus.CANCELLED
        request.completed_at = datetime.now(UTC)
        self.update_request(request)

        logger.info(f"Cancelled teardown for {strategy_id}")
        return request

    def delete_request(self, strategy_id: str) -> bool:
        """Delete a teardown request (usually after completion).

        Args:
            strategy_id: The strategy ID

        Returns:
            True if deleted, False if not found
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "DELETE FROM teardown_requests WHERE strategy_id = ?",
                (strategy_id,),
            )
            conn.commit()
            deleted = cursor.rowcount > 0

        if deleted:
            logger.debug(f"Deleted teardown request for {strategy_id}")
        return deleted

    def _row_to_request(self, row: sqlite3.Row) -> TeardownRequest:
        """Convert a database row to a TeardownRequest."""
        return TeardownRequest(
            strategy_id=row["strategy_id"],
            mode=TeardownMode(row["mode"]),
            asset_policy=TeardownAssetPolicy(row["asset_policy"]),
            target_token=row["target_token"],
            reason=row["reason"],
            requested_at=datetime.fromisoformat(row["requested_at"]),
            requested_by=row["requested_by"],
            status=TeardownStatus(row["status"]),
            acknowledged_at=datetime.fromisoformat(row["acknowledged_at"]) if row["acknowledged_at"] else None,
            started_at=datetime.fromisoformat(row["started_at"]) if row["started_at"] else None,
            completed_at=datetime.fromisoformat(row["completed_at"]) if row["completed_at"] else None,
            current_phase=TeardownPhase(row["current_phase"]) if row["current_phase"] else None,
            positions_total=row["positions_total"],
            positions_closed=row["positions_closed"],
            positions_failed=row["positions_failed"],
            cancel_requested=bool(row["cancel_requested"]),
            cancel_deadline=datetime.fromisoformat(row["cancel_deadline"]) if row["cancel_deadline"] else None,
        )


# Singleton instance for easy access
_default_manager: TeardownStateManager | None = None


def get_teardown_state_manager(db_path: str | None = None) -> TeardownStateManager:
    """Get the default TeardownStateManager instance.

    Args:
        db_path: Optional custom database path

    Returns:
        TeardownStateManager instance
    """
    global _default_manager
    if _default_manager is None:
        _default_manager = TeardownStateManager(db_path)
    return _default_manager
