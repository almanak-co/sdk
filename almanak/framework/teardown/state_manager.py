"""Teardown State Manager for persisting teardown requests.

This manager handles the state-based signaling mechanism for teardowns.
Teardown requests are stored in SQLite (local SDK) or PostgreSQL (hosted
platform) and checked by strategies each iteration.

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

Backend topology (VIB-4049):

- Local SDK keeps using SQLite; rows carry a ``deployment_id`` column.
- Hosted gateway uses PostgreSQL; rows carry a ``deployment_id`` column
  too. The Postgres implementation lives in
  ``platform-plugins/almanak_platform/teardown_store.py`` and is loaded
  via the ``almanak.teardown`` entry point. The dataclass field is still
  named ``deployment_id`` end-to-end (VIB-4726 will reconcile the Python
  identifier); both backends bind that value straight to the
  ``deployment_id`` column.

The Protocols below pin the public surface both backends must implement.
``TeardownStateManager`` / ``TeardownStateAdapter`` are now backwards-compat
aliases that point at the SQLite implementations — existing imports keep
working; new code should depend on the Protocols.
"""

import asyncio
import json
import logging
import os
import random
import sqlite3
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from almanak.framework.teardown.models import (
    EscalationLevel,
    TeardownAssetPolicy,
    TeardownMode,
    TeardownPhase,
    TeardownRequest,
    TeardownState,
    TeardownStatus,
)

# ---------------------------------------------------------------------------
# Public protocols (VIB-4049 PR2)
# ---------------------------------------------------------------------------
#
# These describe the methods the SDK / runner / strategy call against the
# teardown state backend. Both the SQLite implementation in this module and
# the Postgres implementation in ``platform-plugins/almanak_platform`` must
# satisfy them. Mode-aware callers depend on the Protocol — not the concrete
# class — so the ``is_hosted()`` short-circuits at the call sites can be
# removed (VIB-4049 PR2 §7).


@runtime_checkable
class TeardownStateManagerProtocol(Protocol):
    """Public surface of the teardown-request store.

    Captures every method the runner / CLI / dashboard call against the
    teardown request channel. SQLite and Postgres backends both implement
    this. Private helpers (``_row_to_request``, ``_resolve_db_path``, etc.)
    are intentionally excluded — they're implementation details, not part
    of the contract.
    """

    def create_request(self, request: TeardownRequest) -> None: ...

    def get_request(self, deployment_id: str) -> TeardownRequest | None: ...

    def get_active_request(self, deployment_id: str) -> TeardownRequest | None: ...

    def get_pending_requests(self) -> list[TeardownRequest]: ...

    def get_all_active_requests(self) -> list[TeardownRequest]: ...

    def get_all_requests(self) -> list[TeardownRequest]: ...

    def update_request(self, request: TeardownRequest) -> None: ...

    def acknowledge_request(self, deployment_id: str) -> TeardownRequest | None: ...

    def mark_started(self, deployment_id: str, total_positions: int = 0) -> TeardownRequest | None: ...

    def update_progress(
        self,
        deployment_id: str,
        positions_closed: int,
        positions_failed: int = 0,
        current_phase: TeardownPhase | None = None,
    ) -> TeardownRequest | None: ...

    def mark_completed(
        self,
        deployment_id: str,
        result: dict | None = None,
    ) -> TeardownRequest | None: ...

    def mark_failed(
        self,
        deployment_id: str,
        error: str,
        *,
        positions_closed: int | None = None,
        positions_failed: int | None = None,
    ) -> TeardownRequest | None:
        """VIB-4542: keyword-only ``positions_closed`` / ``positions_failed``
        accept the failed-path peer to ``mark_completed``'s
        ``result["intents"]`` lift. ``None`` keeps the pre-call counters
        intact (back-compat with legacy call sites that don't track the
        breakdown). Implementations MUST persist non-None values; both
        SQLite and gateway-backed adapters share this contract."""
        ...

    def request_cancel(self, deployment_id: str) -> bool: ...

    def mark_cancelled(self, deployment_id: str) -> TeardownRequest | None: ...

    def delete_request(self, deployment_id: str) -> bool: ...


@runtime_checkable
class TeardownStateAdapterProtocol(Protocol):
    """Public surface of the teardown-execution-state + approval-channel store.

    Same Protocol-as-contract pattern as :class:`TeardownStateManagerProtocol`.
    Implemented by SQLite locally and Postgres in hosted mode (VIB-4049).
    """

    async def save_teardown_state(self, state: TeardownState) -> None: ...

    async def get_teardown_state(self, deployment_id: str) -> TeardownState | None: ...

    async def delete_teardown_state(self, teardown_id: str) -> None: ...

    def create_approval_request(
        self,
        teardown_id: str,
        deployment_id: str,
        level: EscalationLevel | str,
        request_json: str,
        expires_at: str,
    ) -> None: ...

    def get_approval_response(
        self,
        teardown_id: str,
        level: EscalationLevel | str,
    ) -> str | None: ...

    def write_approval_response(
        self,
        teardown_id: str,
        level: EscalationLevel | str,
        response_json: str,
    ) -> bool: ...

    def get_latest_pending_approval(self, deployment_id: str) -> dict[str, Any] | None: ...

    def write_approval_response_by_strategy(
        self,
        deployment_id: str,
        response_json: str,
    ) -> bool: ...


logger = logging.getLogger(__name__)

# Env var overrides the default SQLite path — ensures runner and API processes
# agree on the file they share for teardown state + approval channel.
_DB_PATH_ENV_VAR = "ALMANAK_STATE_DB"

# SQLite connection settings. WAL enables concurrent readers during writes;
# busy_timeout lets writers wait out contention instead of failing immediately.
_SQLITE_BUSY_TIMEOUT_MS = 30_000
_SQLITE_CONNECT_TIMEOUT_S = 30.0

# Retry policy for OperationalError (e.g., transient locks beyond busy_timeout).
_SQLITE_RETRY_ATTEMPTS = 5
_SQLITE_RETRY_BASE_DELAY_S = 0.05  # 50ms base, exponential backoff + jitter


def _open_connection(db_path: str | Path) -> sqlite3.Connection:
    """Open a SQLite connection with WAL + busy_timeout pragmas applied.

    WAL journal mode allows concurrent readers while a writer commits, which
    matters when the runner poll loop and the API approval writer hit the same
    database. busy_timeout lets a blocked writer wait instead of raising
    immediately.
    """
    conn = sqlite3.connect(str(db_path), timeout=_SQLITE_CONNECT_TIMEOUT_S)
    conn.execute(f"PRAGMA busy_timeout = {_SQLITE_BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


def _with_retry[T](operation: Callable[[], T], *, description: str) -> T:
    """Run a synchronous SQLite operation with retry on OperationalError.

    WAL + busy_timeout should handle most contention. This wraps the rare cases
    where busy_timeout is exhausted (e.g., sustained concurrent writers). Retries
    with exponential backoff + jitter to avoid thundering herd.
    """
    last_err: Exception | None = None
    for attempt in range(_SQLITE_RETRY_ATTEMPTS):
        try:
            return operation()
        except sqlite3.OperationalError as e:
            last_err = e
            if attempt == _SQLITE_RETRY_ATTEMPTS - 1:
                break
            delay = _SQLITE_RETRY_BASE_DELAY_S * (2**attempt) + random.uniform(0, 0.05)
            logger.warning(
                "SQLite %s attempt %d/%d failed (%s); retrying in %.3fs",
                description,
                attempt + 1,
                _SQLITE_RETRY_ATTEMPTS,
                e,
                delay,
            )
            time.sleep(delay)
    assert last_err is not None
    logger.error("SQLite %s exhausted retries: %s", description, last_err)
    raise last_err


class SQLiteTeardownStateManager:
    """SQLite-backed teardown-request store (local SDK).

    Provides CRUD operations for teardown requests, enabling the
    state-based signaling mechanism for triggering teardowns from
    multiple sources.

    Thread-safe for concurrent access from CLI, dashboard, and strategies.
    Hosted deployments use :class:`PostgresTeardownStateManager` (loaded via
    the ``almanak.teardown:postgres`` entry point in ``platform-plugins``).
    Both implement :class:`TeardownStateManagerProtocol`.
    """

    def __init__(self, db_path: str | Path | None = None):
        """Initialize the state manager.

        Args:
            db_path: Path to SQLite database. Defaults to 'almanak_state.db',
                falls back to '/tmp/almanak_state.db' if cwd is not writable.
        """
        self.db_path = self._resolve_db_path(db_path)
        self._init_db()

    @staticmethod
    def _resolve_db_path(db_path: str | Path | None) -> Path:
        """Resolve database path, converging runner and API on the same file.

        Precedence (VIB-3761/VIB-3835 — single source of truth, strategy-scoped):
        1. Explicit ``db_path`` argument.
        2. ``almanak.framework.local_paths.local_strategy_db_path()`` — the
           strategy-scoped helper. Honours ``ALMANAK_STATE_DB`` and
           ``ALMANAK_STRATEGY_FOLDER``; refuses the utility-DB fallback.

        **Never CWD-relative.** Runner is documented to be launched from a
        strategy directory, while API/CLI processes are often started from
        the repo root — a CWD-relative default silently opens different
        SQLite files per process, which breaks the approval channel entirely
        (April 29 silent-failure root cause).

        **No utility-DB fallback** (VIB-3835). Teardown is a strategy-scoped
        operation; falling through to the per-user utility DB silently writes
        the request to a file the runner never reads, which was the May 1
        mainnet teardown failure mode. Callers that get ``LocalPathError``
        here must surface it as a CLI error with the remediation hint.
        """
        if db_path is not None:
            return Path(db_path)

        # VIB-3761/VIB-3835: delegate to the strict, strategy-scoped resolver.
        # Hosted mode and "no strategy folder" both raise LocalPathError; the
        # teardown manager is local-only and strategy-scoped, so we let the
        # error propagate to the caller.
        from almanak.framework.local_paths import local_strategy_db_path

        return local_strategy_db_path()

    def _init_db(self) -> None:
        """Initialize the database schema.

        Wrapped in ``_with_retry`` to mirror :meth:`TeardownStateAdapter._init_tables`
        (ALM-2705). The original asymmetry — ``_init_db`` non-retrying while every
        sibling CRUD method retried — let a single transient ``OperationalError``
        (e.g. WAL contention from the gateway lifecycle/accounting writers on the
        same ``<workspace>/almanak_state.db``) leave the ``teardown_requests``
        table un-created. Subsequent ``create_request`` calls then hit
        ``no such table: teardown_requests`` even though every other path on the
        same DB is healthy.

        The ``CREATE TABLE IF NOT EXISTS`` statement is naturally idempotent, so
        retrying is always safe; calling ``_init_db`` repeatedly on an already-
        initialized DB is a no-op.
        """

        def _op() -> None:
            with _open_connection(self.db_path) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS teardown_requests (
                        deployment_id TEXT PRIMARY KEY,
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
                        updated_at TEXT NOT NULL,
                        -- VIB-3951 crash-watchdog columns (local SQLite only).
                        -- owner_pid: OS pid of the runner process that flipped
                        -- this row to 'executing' (NULL until mark_started).
                        -- heartbeat_at: last liveness stamp the owning process
                        -- writes while actively executing the teardown.
                        owner_pid INTEGER,
                        heartbeat_at TEXT
                    )
                """)
                # Migration (VIB-4722): rename strategy_id -> deployment_id on
                # existing local DBs to match the unified identity column.
                try:
                    conn.execute("ALTER TABLE teardown_requests RENAME COLUMN strategy_id TO deployment_id")
                except sqlite3.OperationalError:
                    pass  # Already renamed (or fresh DB created with deployment_id)
                # Migration (VIB-3951): add crash-watchdog columns to existing
                # local DBs. Idempotent — ALTER raises OperationalError when the
                # column already exists; swallow per-column.
                for _col, _decl in (("owner_pid", "INTEGER"), ("heartbeat_at", "TEXT")):
                    try:
                        conn.execute(f"ALTER TABLE teardown_requests ADD COLUMN {_col} {_decl}")
                    except sqlite3.OperationalError:
                        pass  # Already present
                conn.commit()
                logger.debug(f"Initialized teardown state database at {self.db_path}")

        _with_retry(_op, description="init_teardown_requests")

    def create_request(self, request: TeardownRequest) -> None:
        """Create or replace a teardown request.

        If a request already exists for this strategy, it will be replaced.
        This allows re-triggering teardowns that were cancelled.

        Args:
            request: The teardown request to persist
        """
        with _open_connection(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO teardown_requests (
                    deployment_id, mode, asset_policy, target_token,
                    reason, requested_at, requested_by, status,
                    acknowledged_at, started_at, completed_at,
                    current_phase, positions_total, positions_closed,
                    positions_failed, cancel_requested, cancel_deadline,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    request.deployment_id,
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
                f"Created teardown request for {request.deployment_id}: "
                f"mode={request.mode.value}, by={request.requested_by}"
            )

    def get_request(self, deployment_id: str) -> TeardownRequest | None:
        """Get the current teardown request for a strategy.

        Args:
            deployment_id: The deployment ID to look up

        Returns:
            TeardownRequest if one exists, None otherwise
        """
        with _open_connection(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT * FROM teardown_requests WHERE deployment_id = ?",
                (deployment_id,),
            )
            row = cursor.fetchone()

            if not row:
                return None

            return self._row_to_request(row)

    def get_active_request(self, deployment_id: str) -> TeardownRequest | None:
        """Get an active (non-completed) teardown request.

        Args:
            deployment_id: The deployment ID to look up

        Returns:
            TeardownRequest if an active one exists, None otherwise
        """
        request = self.get_request(deployment_id)
        if request and request.is_active:
            return request
        return None

    def get_result_payload(self, deployment_id: str) -> dict | None:
        """Read the terminal ``result_json`` payload for a teardown request.

        VIB-5011: ``mark_completed`` persists a result dict (intent counts,
        mode, duration, and the ``"consolidation"`` summary) into the
        SQLite-only ``result_json`` column, but :class:`TeardownRequest`
        doesn't carry it. This read-only accessor lets the CLI ``--wait``
        terminal print and ``status`` render the consolidation outcome
        ("consolidated N token(s) → USDC" / residual-token warning).

        Returns ``None`` when the row is missing, the column is empty, or the
        JSON is malformed — callers render nothing in that case.
        """
        with _open_connection(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT result_json FROM teardown_requests WHERE deployment_id = ?",
                (deployment_id,),
            )
            row = cursor.fetchone()
        if not row or not row[0]:
            return None
        try:
            payload = json.loads(row[0])
        except (TypeError, ValueError):
            return None
        return payload if isinstance(payload, dict) else None

    def get_pending_requests(self) -> list[TeardownRequest]:
        """Get all pending teardown requests.

        Returns:
            List of teardown requests with status=PENDING
        """
        with _open_connection(self.db_path) as conn:
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

        with _open_connection(self.db_path) as conn:
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
        with _open_connection(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("SELECT * FROM teardown_requests ORDER BY requested_at DESC")
            return [self._row_to_request(row) for row in cursor.fetchall()]

    def update_request(self, request: TeardownRequest) -> None:
        """Update an existing teardown request.

        Args:
            request: The updated teardown request
        """
        with _open_connection(self.db_path) as conn:
            conn.execute(
                """
                UPDATE teardown_requests SET
                    mode = ?, asset_policy = ?, target_token = ?,
                    status = ?, acknowledged_at = ?, started_at = ?,
                    completed_at = ?, current_phase = ?,
                    positions_total = ?, positions_closed = ?,
                    positions_failed = ?, cancel_requested = ?,
                    cancel_deadline = ?, updated_at = ?
                WHERE deployment_id = ?
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
                    request.deployment_id,
                ),
            )
            conn.commit()
            logger.debug(f"Updated teardown request for {request.deployment_id}: status={request.status.value}")

    def acknowledge_request(self, deployment_id: str) -> TeardownRequest | None:
        """Acknowledge a pending teardown request.

        Called when a strategy picks up the request and begins processing.

        Args:
            deployment_id: The strategy acknowledging the request

        Returns:
            The acknowledged request, or None if not found
        """
        request = self.get_active_request(deployment_id)
        if not request:
            return None

        request.acknowledged_at = datetime.now(UTC)
        request.status = TeardownStatus.CANCEL_WINDOW
        self.update_request(request)

        logger.info(f"Acknowledged teardown request for {deployment_id}")
        return request

    def mark_started(self, deployment_id: str, total_positions: int = 0) -> TeardownRequest | None:
        """Mark a teardown as started (after cancel window).

        Args:
            deployment_id: The deployment ID
            total_positions: Total number of positions to close

        Returns:
            The updated request, or None if not found
        """
        request = self.get_active_request(deployment_id)
        if not request:
            return None

        request.started_at = datetime.now(UTC)
        request.status = TeardownStatus.EXECUTING
        request.current_phase = TeardownPhase.POSITION_CLOSURE
        request.positions_total = total_positions
        self.update_request(request)

        # VIB-3951: stamp the owning process pid + an initial heartbeat so the
        # crash watchdog can later distinguish "still being executed by a live
        # process" from "abandoned by a dead/stale process". Written directly
        # (the TeardownRequest dataclass intentionally has no pid/heartbeat
        # fields — these columns are a local-SQLite crash-recovery concern, not
        # part of the cross-process request schema).
        self._stamp_owner(deployment_id, os.getpid(), datetime.now(UTC))

        logger.info(f"Started teardown for {deployment_id}: {total_positions} positions")
        return request

    def _stamp_owner(self, deployment_id: str, owner_pid: int | None, heartbeat_at: datetime | None) -> None:
        """Write the VIB-3951 crash-watchdog columns for a request (local SQLite)."""

        def _op() -> None:
            with _open_connection(self.db_path) as conn:
                conn.execute(
                    "UPDATE teardown_requests SET owner_pid = ?, heartbeat_at = ? WHERE deployment_id = ?",
                    (
                        owner_pid,
                        heartbeat_at.isoformat() if heartbeat_at else None,
                        deployment_id,
                    ),
                )
                conn.commit()

        _with_retry(_op, description="stamp_teardown_owner")

    def heartbeat(self, deployment_id: str) -> None:
        """Refresh the owning process's liveness stamp for an executing teardown.

        Called periodically by the runner while it actively executes teardown
        intents (VIB-3951). A fresh heartbeat keeps the row out of the
        watchdog's stale-by-time bucket; the pid is left untouched so a PID
        liveness check still works.

        **Best-effort at this level.** Per the teardown loud-but-non-blocking
        contract, a heartbeat failure must never interrupt the risk-reducing
        teardown — so the underlying DB write (which can raise on lock
        contention / transient SQLite errors even through ``_with_retry``) is
        caught HERE and logged, not propagated. The contract therefore holds
        regardless of call site; the ``_commit_with_heartbeat`` wrapper keeps
        its own swallow as defense in depth.
        """
        try:
            self._stamp_owner(deployment_id, os.getpid(), datetime.now(UTC))
        except Exception as exc:  # noqa: BLE001 — heartbeat is best-effort
            logger.warning(
                "Teardown heartbeat stamp failed for %s (non-fatal): %s",
                deployment_id,
                exc,
            )

    def update_progress(
        self,
        deployment_id: str,
        positions_closed: int,
        positions_failed: int = 0,
        current_phase: TeardownPhase | None = None,
    ) -> TeardownRequest | None:
        """Update teardown progress.

        Args:
            deployment_id: The deployment ID
            positions_closed: Number of positions successfully closed
            positions_failed: Number of positions that failed to close
            current_phase: Current phase of the teardown

        Returns:
            The updated request, or None if not found
        """
        request = self.get_active_request(deployment_id)
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
        deployment_id: str,
        result: dict | None = None,
    ) -> TeardownRequest | None:
        """Mark a teardown as completed.

        Args:
            deployment_id: The deployment ID
            result: Optional result details (final balances, costs, etc.)
                If ``result["intents"]`` is set, it's lifted onto
                ``positions_closed`` (VIB-3920) so dashboard tabs and the
                §1.2 G5 ship gate can read a non-zero close-count after a
                successful teardown lifecycle. Pre-fix only ``status``
                was updated and ``positions_closed`` always read 0.

        Returns:
            The updated request, or None if not found
        """
        request = self.get_active_request(deployment_id)
        if not request:
            return None

        request.status = TeardownStatus.COMPLETED
        request.completed_at = datetime.now(UTC)

        # VIB-3920 — lift the closed count off the result payload onto
        # the dedicated column. ``intents`` is what TeardownManager emits
        # in `result_json`; same name as the ``intents_succeeded`` field
        # on TeardownResult.
        if result is not None:
            intents_closed = result.get("intents")
            if isinstance(intents_closed, int) and intents_closed >= 0:
                request.positions_closed = intents_closed

        if result:
            with _open_connection(self.db_path) as conn:
                conn.execute(
                    "UPDATE teardown_requests SET result_json = ? WHERE deployment_id = ?",
                    (json.dumps(result), deployment_id),
                )
                conn.commit()

        self.update_request(request)
        logger.info(f"Completed teardown for {deployment_id} (positions_closed={request.positions_closed})")
        return request

    def mark_failed(
        self,
        deployment_id: str,
        error: str,
        *,
        positions_closed: int | None = None,
        positions_failed: int | None = None,
    ) -> TeardownRequest | None:
        """Mark a teardown as failed.

        Args:
            deployment_id: The deployment ID
            error: Error message describing the failure
            positions_closed: (VIB-4542) intents that landed on-chain before
                the failure. ``None`` preserves the row's pre-call value
                (e.g. whatever ``update_progress`` last wrote). The runner
                terminal-failed path passes ``teardown_result.intents_succeeded``
                so postmortem readers can distinguish a teardown that
                failed at intent 2 of 7 from one that failed before any
                intent landed.
            positions_failed: (VIB-4542) intents that reverted on-chain.
                Same None-preserves-prior-value contract.

        Returns:
            The updated request, or None if not found

        Semantic-clash note (VIB-4542 doc Item 6): the column is named
        ``positions_*`` but the runtime counts **intents**. One position
        can be closed by multiple intents (REPAY + WITHDRAW) and one
        intent can affect multiple positions. Follow-up tracks the
        column rename / position-level bookkeeping; out of scope here.
        """
        request = self.get_active_request(deployment_id)
        if not request:
            return None

        request.status = TeardownStatus.FAILED
        request.completed_at = datetime.now(UTC)
        if positions_closed is not None:
            request.positions_closed = positions_closed
        if positions_failed is not None:
            request.positions_failed = positions_failed

        with _open_connection(self.db_path) as conn:
            conn.execute(
                "UPDATE teardown_requests SET error_message = ? WHERE deployment_id = ?",
                (error, deployment_id),
            )
            conn.commit()

        # update_request persists positions_closed / positions_failed (plus
        # status / completed_at) — see the full UPDATE statement above.
        self.update_request(request)
        # Use %s for the counter fields — they default to 0 on the dataclass
        # so %d would also work today, but %s is the safer style: a future
        # schema migration that allows None on those columns won't crash the
        # error-path log (gemini review on PR #2343).
        logger.error(
            "Failed teardown for %s (closed=%s, failed=%s): %s",
            deployment_id,
            request.positions_closed,
            request.positions_failed,
            error,
        )
        return request

    # VIB-3951 — default heartbeat staleness window. A teardown intent can take
    # ~100s+ to estimate gas + confirm on a slow fork; the window is generous so
    # the watchdog never races a live-but-busy runner. The PID-liveness check is
    # the fast path (a dead process is failed immediately); the time window only
    # catches a stale row whose pid was recycled to an unrelated live process.
    _DEFAULT_STALE_HEARTBEAT_SECONDS = 900  # 15 minutes

    def sweep_stale_executing(
        self,
        *,
        stale_after_seconds: int | None = None,
        now: datetime | None = None,
    ) -> int:
        """Re-queue ``executing`` teardown rows abandoned by a dead/stale process.

        VIB-3951 crash watchdog (local SQLite only). A row is considered
        abandoned when EITHER:

        * its ``owner_pid`` names a process that is no longer alive
          (``os.kill(pid, 0)`` raises :class:`ProcessLookupError`), OR
        * its ``heartbeat_at`` is older than ``stale_after_seconds`` (covers
          the pid-recycled-to-an-unrelated-live-process case, and rows written
          before the owner_pid column existed where ``owner_pid IS NULL``).

        Rows whose owning pid is alive AND whose heartbeat is fresh are left
        untouched. A row stamped with the CURRENT process's pid is NOT
        unconditionally exempt: that exemption would only ever fire via PID
        *recycling* (a crashed runner's pid reassigned to this new process) —
        which is exactly the stuck-row case the heartbeat exists to catch. So
        a current-pid row is still run through the abandonment check; only a
        current-pid row with a FRESH heartbeat (the genuine "this very process
        is mid-teardown" case) is exempt. (VIB-3951 CodeRabbit Major 1.)

        **Recovery semantics (teardown risk contract).** An abandoned row is
        re-queued to ``status='pending'``, NOT marked terminal ``failed``. A
        crash mid-unwind leaves residual on-chain risk; the runner's boot-time
        ``should_teardown()`` check re-enters teardown for any *active*
        (non-terminal) request, regenerates intents from current on-chain
        state, and finishes the unwind. Marking ``failed`` (``is_active=False``)
        would defeat that auto-recovery and force a manual re-trigger —
        violating CLAUDE.md §Teardown ("teardown's first job is to remove
        on-chain risk; never block the next risk-reducing intent"). The crash
        is still recorded loudly: a WARNING per row, the crash note stamped onto
        the request ``reason``, and the dead ``owner_pid``/``heartbeat_at``
        cleared so the re-queued row starts clean. The in-flight progress
        counters are reset to 0 (positions are re-counted from on-chain state on
        re-entry; carrying a stale count forward would mislead the dashboard).

        Returns the number of rows re-queued to ``pending``.
        """
        window = self._DEFAULT_STALE_HEARTBEAT_SECONDS if stale_after_seconds is None else stale_after_seconds
        ref_now = now or datetime.now(UTC)

        with _open_connection(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT deployment_id, owner_pid, heartbeat_at FROM teardown_requests WHERE status = ?",
                (TeardownStatus.EXECUTING.value,),
            ).fetchall()

        requeued = 0
        for row in rows:
            deployment_id = row["deployment_id"]
            owner_pid = row["owner_pid"]
            heartbeat_at = row["heartbeat_at"]

            # No current-pid shortcut (Major 1): a current-pid row is only the
            # genuine "this process owns it" case when its heartbeat is fresh,
            # which _is_executing_row_abandoned already returns False for. A
            # current-pid row with a STALE heartbeat is a recycled-pid orphan
            # and must be requeued like any other.
            if not self._is_executing_row_abandoned(
                owner_pid=owner_pid,
                heartbeat_at=heartbeat_at,
                ref_now=ref_now,
                window_seconds=window,
            ):
                continue

            # Compare-and-swap (Major 2): only count + log a row we actually
            # transitioned. If the row changed since the snapshot (another
            # process committed a heartbeat / status flip), the CAS no-ops and
            # we leave it alone rather than clobbering live state.
            if self._requeue_abandoned_executing(
                deployment_id,
                owner_pid=owner_pid,
                heartbeat_at=heartbeat_at,
            ):
                requeued += 1

        return requeued

    def _requeue_abandoned_executing(
        self,
        deployment_id: str,
        *,
        owner_pid: int | None,
        heartbeat_at: str | None,
    ) -> bool:
        """Compare-and-swap re-queue of one abandoned ``executing`` row (VIB-3951).

        Resets the row to a clean re-triggerable state: ``status='pending'``,
        ``started_at`` / ``acknowledged_at`` / ``current_phase`` cleared,
        progress counters zeroed, crash-watchdog stamps cleared, and the crash
        recorded in ``reason`` for postmortem. Stays ``is_active=True`` so the
        runner's ``should_teardown()`` re-enters teardown on boot and finishes
        the unwind.

        **Compare-and-swap (Major 2):** the caller decided from a snapshot
        ``(owner_pid, heartbeat_at)`` read in a separate connection. Between
        that read and this UPDATE another process may have committed a fresh
        heartbeat or a terminal status flip. The UPDATE therefore guards on
        ``status='executing' AND owner_pid IS ? AND heartbeat_at IS ?`` (the
        observed snapshot) so a row that changed since is NOT clobbered. SQLite
        ``IS`` is used (not ``=``) so a snapshot ``NULL`` matches a stored
        ``NULL`` (``= NULL`` is never true). Returns ``True`` only when a row
        was actually transitioned; the WARNING is emitted only on a real
        requeue. Values are passed as bound parameters — no f-string SQL
        interpolation of row values.
        """
        crash_note = (
            f"Teardown crash watchdog: runner process (pid={owner_pid}) is gone or "
            "stale while status='executing'. Re-queued to 'pending' so the runner "
            "auto-re-enters teardown on boot and finishes unwinding any residual "
            "on-chain positions."
        )

        result = {"rowcount": 0}

        def _op() -> None:
            with _open_connection(self.db_path) as conn:
                cursor = conn.execute(
                    """
                    UPDATE teardown_requests SET
                        status = ?,
                        reason = ?,
                        acknowledged_at = NULL,
                        started_at = NULL,
                        completed_at = NULL,
                        current_phase = NULL,
                        positions_closed = 0,
                        positions_failed = 0,
                        cancel_requested = 0,
                        cancel_deadline = NULL,
                        owner_pid = NULL,
                        heartbeat_at = NULL,
                        updated_at = ?
                    WHERE deployment_id = ?
                      AND status = ?
                      AND owner_pid IS ?
                      AND heartbeat_at IS ?
                    """,
                    (
                        TeardownStatus.PENDING.value,
                        crash_note,
                        datetime.now(UTC).isoformat(),
                        deployment_id,
                        TeardownStatus.EXECUTING.value,
                        owner_pid,
                        heartbeat_at,
                    ),
                )
                result["rowcount"] = cursor.rowcount
                conn.commit()

        _with_retry(_op, description="requeue_abandoned_teardown")
        if result["rowcount"] > 0:
            logger.warning(
                "Teardown crash watchdog: re-queued abandoned teardown for %s "
                "(pid=%s gone/stale) to 'pending' for auto-recovery on boot",
                deployment_id,
                owner_pid,
            )
            return True
        # CAS no-op: the row changed since the snapshot — leave it untouched.
        logger.debug(
            "Teardown crash watchdog: requeue CAS no-op for %s (row changed since snapshot; pid=%s, heartbeat=%s)",
            deployment_id,
            owner_pid,
            heartbeat_at,
        )
        return False

    @staticmethod
    def _is_executing_row_abandoned(
        *,
        owner_pid: int | None,
        heartbeat_at: str | None,
        ref_now: datetime,
        window_seconds: int,
    ) -> bool:
        """True when an ``executing`` row's owner is dead or its heartbeat stale."""
        # Dead-process fast path: a named pid that no longer exists is abandoned.
        if owner_pid is not None:
            try:
                os.kill(int(owner_pid), 0)
            except ProcessLookupError:
                return True  # process is gone
            except PermissionError:
                # Process exists but is owned by another user — treat as alive,
                # fall through to the heartbeat-staleness check.
                pass
            except (OverflowError, ValueError):
                # Malformed pid — fall through to the time-based check.
                pass

        # Time-based staleness: a missing/old heartbeat means abandoned. Rows
        # written before the owner_pid column existed (owner_pid IS NULL,
        # heartbeat_at IS NULL) are abandoned by definition once stuck.
        if not heartbeat_at:
            return True
        try:
            hb = datetime.fromisoformat(heartbeat_at)
        except ValueError:
            return True  # unparseable stamp — treat as stale
        if hb.tzinfo is None:
            hb = hb.replace(tzinfo=UTC)
        return ref_now - hb > timedelta(seconds=window_seconds)

    def request_cancel(self, deployment_id: str) -> bool:
        """Request cancellation of a teardown.

        This sets the cancel_requested flag, which will be checked
        by the strategy during the next iteration.

        Args:
            deployment_id: The deployment ID

        Returns:
            True if cancel request was recorded, False if not cancellable
        """
        request = self.get_active_request(deployment_id)
        if not request:
            return False

        if not request.can_cancel:
            logger.warning(f"Cannot cancel teardown for {deployment_id}: past cancel deadline")
            return False

        request.cancel_requested = True
        self.update_request(request)

        logger.info(f"Cancel requested for teardown {deployment_id}")
        return True

    def mark_cancelled(self, deployment_id: str) -> TeardownRequest | None:
        """Mark a teardown as cancelled.

        Args:
            deployment_id: The deployment ID

        Returns:
            The updated request, or None if not found
        """
        request = self.get_active_request(deployment_id)
        if not request:
            return None

        request.status = TeardownStatus.CANCELLED
        request.completed_at = datetime.now(UTC)
        self.update_request(request)

        logger.info(f"Cancelled teardown for {deployment_id}")
        return request

    def delete_request(self, deployment_id: str) -> bool:
        """Delete a teardown request (usually after completion).

        Args:
            deployment_id: The deployment ID

        Returns:
            True if deleted, False if not found
        """
        with _open_connection(self.db_path) as conn:
            cursor = conn.execute(
                "DELETE FROM teardown_requests WHERE deployment_id = ?",
                (deployment_id,),
            )
            conn.commit()
            deleted = cursor.rowcount > 0

        if deleted:
            logger.debug(f"Deleted teardown request for {deployment_id}")
        return deleted

    def _row_to_request(self, row: sqlite3.Row) -> TeardownRequest:
        """Convert a database row to a TeardownRequest."""
        return TeardownRequest(
            deployment_id=row["deployment_id"],
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


# Mode-aware singleton lives in ``almanak/framework/teardown/__init__.py``
# (VIB-4049 PR2 §4). This module no longer owns ``get_teardown_state_manager``
# directly — the factory must consult ``framework/deployment/mode.py`` and
# ``ALMANAK_GATEWAY_DATABASE_URL`` to decide between SQLite and Postgres,
# which would create a circular import if defined here. See
# ``framework/teardown/__init__.py:get_teardown_state_manager``.


class SQLiteTeardownStateAdapter:
    """SQLite-backed persistence for TeardownManager state and approval channel.

    Implements the ``StateManager`` protocol expected by ``TeardownManager.__init__``
    (``save_teardown_state`` / ``get_teardown_state`` / ``delete_teardown_state``) and
    provides the cross-process approval channel used by the runner's approval
    callback and the teardown API endpoints.

    **Schema**

    - ``teardown_execution_state``: one row per teardown, keyed by ``teardown_id``.
      Persisted so a restarted runner can resume a mid-flight teardown.
    - ``teardown_approvals``: one row per (teardown_id, level) escalation, so each
      level's request/response is preserved independently — a response to level 2
      cannot be clobbered by an INSERT for level 3 (VIB-2927 fix).

    **Cross-process coordination**

    Runner and API/CLI must resolve the same SQLite file, otherwise operator
    approvals written by the API never reach the runner's poll loop. Set the
    ``ALMANAK_STATE_DB`` environment variable in production to make the path
    explicit. The adapter's ``_resolve_db_path`` honours that variable.

    **Concurrency**

    All synchronous SQLite work uses WAL journal mode + busy_timeout, and
    retries on ``OperationalError`` with jittered backoff. Async methods wrap
    the sync I/O in ``asyncio.to_thread`` so the event loop is never blocked.
    """

    def __init__(self, db_path: str | Path | None = None):
        # Fail loudly on invalid types — this path is cross-process state; silently
        # defaulting to CWD on a misconfigured caller would lead to diverging DBs.
        if db_path is not None and not isinstance(db_path, str | Path):
            raise TypeError(
                f"SQLiteTeardownStateAdapter db_path must be str, Path, or None; got {type(db_path).__name__}"
            )
        self.db_path = SQLiteTeardownStateManager._resolve_db_path(db_path)
        self._init_tables()

    # ------------------------------------------------------------------
    # Schema + internal helpers
    # ------------------------------------------------------------------

    def _init_tables(self) -> None:
        def _op():
            with _open_connection(self.db_path) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS teardown_execution_state (
                        teardown_id TEXT PRIMARY KEY,
                        deployment_id TEXT NOT NULL,
                        mode TEXT NOT NULL,
                        status TEXT NOT NULL,
                        total_intents INTEGER NOT NULL,
                        completed_intents INTEGER NOT NULL,
                        current_intent_index INTEGER NOT NULL,
                        started_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        completed_at TEXT,
                        pending_intents_json TEXT,
                        intent_results_json TEXT,
                        cancel_window_until TEXT,
                        config_json TEXT
                    )
                """)
                # Approval requests table for slippage escalation (VIB-2927).
                # Compound PK (teardown_id, level) so each escalation level has its
                # own request/response slot and later levels can't clobber earlier
                # operator responses.
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS teardown_approvals (
                        teardown_id TEXT NOT NULL,
                        level TEXT NOT NULL,
                        deployment_id TEXT NOT NULL,
                        request_json TEXT NOT NULL,
                        response_json TEXT,
                        created_at TEXT NOT NULL,
                        responded_at TEXT,
                        expires_at TEXT NOT NULL,
                        PRIMARY KEY (teardown_id, level)
                    )
                """)
                # Migration (VIB-4722): rename strategy_id -> deployment_id on
                # existing local DBs to match the unified identity column.
                try:
                    conn.execute("ALTER TABLE teardown_execution_state RENAME COLUMN strategy_id TO deployment_id")
                except sqlite3.OperationalError:
                    pass  # Already renamed (or fresh DB created with deployment_id)
                try:
                    conn.execute("ALTER TABLE teardown_approvals RENAME COLUMN strategy_id TO deployment_id")
                except sqlite3.OperationalError:
                    pass  # Already renamed (or fresh DB created with deployment_id)
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_approvals_strategy_pending "
                    "ON teardown_approvals(deployment_id, responded_at)"
                )
                conn.commit()

            # Handle pre-release schema drift: the earlier schema had
            # teardown_id as the sole PRIMARY KEY with no ``level`` column.
            # If we detect that, drop and recreate — this feature is unreleased
            # so there is no production data to preserve.
            self._migrate_legacy_approvals_schema()

        _with_retry(_op, description="init_tables")

    def _migrate_legacy_approvals_schema(self) -> None:
        """Drop + recreate teardown_approvals if it predates the compound PK.

        The pre-release schema used teardown_id as the sole primary key. Rows
        from that schema cannot answer "which escalation level did the operator
        respond to?" and must be migrated. Since this feature has not shipped,
        the migration is a simple drop/recreate rather than a data migration.
        """

        def _op():
            with _open_connection(self.db_path) as conn:
                cursor = conn.execute("PRAGMA table_info(teardown_approvals)")
                columns = {row[1] for row in cursor.fetchall()}
                if "level" in columns:
                    return
                logger.warning(
                    "teardown_approvals table predates compound PK; dropping and recreating "
                    "(pre-release feature, no data loss). Path: %s",
                    self.db_path,
                )
                conn.execute("DROP TABLE IF EXISTS teardown_approvals")
                conn.execute("""
                    CREATE TABLE teardown_approvals (
                        teardown_id TEXT NOT NULL,
                        level TEXT NOT NULL,
                        deployment_id TEXT NOT NULL,
                        request_json TEXT NOT NULL,
                        response_json TEXT,
                        created_at TEXT NOT NULL,
                        responded_at TEXT,
                        expires_at TEXT NOT NULL,
                        PRIMARY KEY (teardown_id, level)
                    )
                """)
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_approvals_strategy_pending "
                    "ON teardown_approvals(deployment_id, responded_at)"
                )
                conn.commit()

        _with_retry(_op, description="migrate_approvals_schema")

    @staticmethod
    def _level_key(level: EscalationLevel | str) -> str:
        """Serialize an escalation level for DB storage."""
        if isinstance(level, EscalationLevel):
            return level.value
        return str(level)

    # ------------------------------------------------------------------
    # TeardownManager StateManager protocol (async)
    # ------------------------------------------------------------------

    async def save_teardown_state(self, state: TeardownState) -> None:
        """Persist TeardownState to SQLite."""
        await asyncio.to_thread(self._save_teardown_state_sync, state)
        logger.debug("Saved teardown execution state: %s (%s)", state.teardown_id, state.status.value)

    def _save_teardown_state_sync(self, state: TeardownState) -> None:
        def _op():
            with _open_connection(self.db_path) as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO teardown_execution_state (
                        teardown_id, deployment_id, mode, status,
                        total_intents, completed_intents, current_intent_index,
                        started_at, updated_at, completed_at,
                        pending_intents_json, intent_results_json,
                        cancel_window_until, config_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        state.teardown_id,
                        state.deployment_id,
                        state.mode.value,
                        state.status.value,
                        state.total_intents,
                        state.completed_intents,
                        state.current_intent_index,
                        state.started_at.isoformat(),
                        state.updated_at.isoformat(),
                        state.completed_at.isoformat() if state.completed_at else None,
                        state.pending_intents_json,
                        json.dumps(state.intent_results),
                        state.cancel_window_until.isoformat() if state.cancel_window_until else None,
                        state.config_json,
                    ),
                )
                conn.commit()

        _with_retry(_op, description="save_teardown_state")

    async def get_teardown_state(self, deployment_id: str) -> TeardownState | None:
        """Load TeardownState from SQLite by deployment_id."""
        return await asyncio.to_thread(self._get_teardown_state_sync, deployment_id)

    def _get_teardown_state_sync(self, deployment_id: str) -> TeardownState | None:
        def _op() -> TeardownState | None:
            with _open_connection(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute(
                    "SELECT * FROM teardown_execution_state WHERE deployment_id = ? ORDER BY updated_at DESC LIMIT 1",
                    (deployment_id,),
                ).fetchone()

            if not row:
                return None

            # Defensive parse: a corrupted intent_results_json blob must not
            # prevent resumption of an in-flight teardown. Log and fall back
            # to an empty list so the state loads and the runner can retry.
            intent_results: list[Any] = []
            raw_intent_results = row["intent_results_json"]
            if raw_intent_results:
                try:
                    parsed = json.loads(raw_intent_results)
                    if isinstance(parsed, list):
                        intent_results = parsed
                    else:
                        logger.warning(
                            "teardown_execution_state intent_results_json for %s is not a list (%s); "
                            "falling back to empty",
                            row["teardown_id"],
                            type(parsed).__name__,
                        )
                except json.JSONDecodeError:
                    logger.error(
                        "Corrupted intent_results_json for teardown %s — falling back to empty list",
                        row["teardown_id"],
                        exc_info=True,
                    )

            return TeardownState(
                teardown_id=row["teardown_id"],
                deployment_id=row["deployment_id"],
                mode=TeardownMode(row["mode"]),
                status=TeardownStatus(row["status"]),
                total_intents=row["total_intents"],
                completed_intents=row["completed_intents"],
                current_intent_index=row["current_intent_index"],
                started_at=datetime.fromisoformat(row["started_at"]),
                updated_at=datetime.fromisoformat(row["updated_at"]),
                completed_at=datetime.fromisoformat(row["completed_at"]) if row["completed_at"] else None,
                pending_intents_json=row["pending_intents_json"] or "",
                intent_results=intent_results,
                cancel_window_until=(
                    datetime.fromisoformat(row["cancel_window_until"]) if row["cancel_window_until"] else None
                ),
                config_json=row["config_json"] or "",
            )

        return _with_retry(_op, description="get_teardown_state")

    async def delete_teardown_state(self, teardown_id: str) -> None:
        """Remove TeardownState from SQLite."""
        await asyncio.to_thread(self._delete_teardown_state_sync, teardown_id)
        logger.debug("Deleted teardown execution state: %s", teardown_id)

    def _delete_teardown_state_sync(self, teardown_id: str) -> None:
        def _op():
            with _open_connection(self.db_path) as conn:
                conn.execute("DELETE FROM teardown_execution_state WHERE teardown_id = ?", (teardown_id,))
                conn.commit()

        _with_retry(_op, description="delete_teardown_state")

    # ------------------------------------------------------------------
    # Approval mechanism (VIB-2927) — unified channel across runner + API
    # ------------------------------------------------------------------

    def create_approval_request(
        self,
        teardown_id: str,
        deployment_id: str,
        level: EscalationLevel | str,
        request_json: str,
        expires_at: str,
    ) -> None:
        """Write an approval request keyed by (teardown_id, level).

        Each escalation level writes its own row, so a stale response from a
        previous level cannot satisfy the poller waiting for the current level.

        Uses ``INSERT ... ON CONFLICT DO UPDATE ... WHERE response_json IS NULL``
        rather than ``INSERT OR REPLACE`` so an existing operator response is
        preserved. Runner restarts or retry loops that re-emit the same
        ``(teardown_id, level)`` must not wipe an already-landed approval.
        """
        level_key = self._level_key(level)

        def _op():
            with _open_connection(self.db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO teardown_approvals
                        (teardown_id, level, deployment_id, request_json,
                         response_json, created_at, responded_at, expires_at)
                    VALUES (?, ?, ?, ?, NULL, ?, NULL, ?)
                    ON CONFLICT(teardown_id, level) DO UPDATE SET
                        request_json = excluded.request_json,
                        deployment_id = excluded.deployment_id,
                        created_at = excluded.created_at,
                        expires_at = excluded.expires_at
                    WHERE teardown_approvals.response_json IS NULL
                    """,
                    (
                        teardown_id,
                        level_key,
                        deployment_id,
                        request_json,
                        datetime.now(UTC).isoformat(),
                        expires_at,
                    ),
                )
                conn.commit()

        _with_retry(_op, description="create_approval_request")
        logger.info("Created approval request for teardown %s (level %s)", teardown_id, level_key)

    def get_approval_response(
        self,
        teardown_id: str,
        level: EscalationLevel | str,
    ) -> str | None:
        """Return the response JSON for the (teardown_id, level) request, or None."""
        level_key = self._level_key(level)

        def _op() -> str | None:
            with _open_connection(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute(
                    "SELECT response_json FROM teardown_approvals WHERE teardown_id = ? AND level = ?",
                    (teardown_id, level_key),
                ).fetchone()
            if row and row["response_json"]:
                return row["response_json"]
            return None

        return _with_retry(_op, description="get_approval_response")

    def write_approval_response(
        self,
        teardown_id: str,
        level: EscalationLevel | str,
        response_json: str,
    ) -> bool:
        """Write the operator's response for a specific (teardown_id, level) request.

        Returns True if a matching pending request existed; False otherwise.
        """
        level_key = self._level_key(level)

        def _op() -> int:
            with _open_connection(self.db_path) as conn:
                # `response_json IS NULL` ensures only the first responder wins.
                # Two near-simultaneous operators can both hit approve_escalation;
                # the second caller's UPDATE matches zero rows and the API surfaces
                # that as a 409 instead of silently overwriting the first response.
                cursor = conn.execute(
                    """
                    UPDATE teardown_approvals
                    SET response_json = ?, responded_at = ?
                    WHERE teardown_id = ? AND level = ? AND response_json IS NULL
                    """,
                    (
                        response_json,
                        datetime.now(UTC).isoformat(),
                        teardown_id,
                        level_key,
                    ),
                )
                conn.commit()
                return cursor.rowcount

        rowcount = _with_retry(_op, description="write_approval_response")
        if rowcount > 0:
            logger.info("Wrote approval response for teardown %s (level %s)", teardown_id, level_key)
            return True
        logger.warning(
            "No pending approval request found for teardown %s (level %s) — already responded or missing",
            teardown_id,
            level_key,
        )
        return False

    def get_latest_pending_approval(self, deployment_id: str) -> dict[str, Any] | None:
        """Return the oldest unresponded, non-expired approval request for a strategy.

        Used by API/CLI endpoints that only know the deployment_id — they look up
        the currently-pending approval, then write the response keyed by
        (teardown_id, level). Oldest-first so operators respond to the request
        that triggered the alert, even if the escalation loop has advanced.

        Expired rows are excluded even when their response_json is still NULL.
        The runner's approval callback writes a synthetic timeout response on
        expiry, but if a crash or race leaves a row in an expired-unresponded
        state, the API must not treat it as the live pending request.
        """
        now_iso = datetime.now(UTC).isoformat()

        def _op() -> dict[str, Any] | None:
            with _open_connection(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute(
                    """
                    SELECT teardown_id, level, deployment_id, request_json, created_at, expires_at
                    FROM teardown_approvals
                    WHERE deployment_id = ?
                      AND response_json IS NULL
                      AND expires_at > ?
                    ORDER BY created_at ASC LIMIT 1
                    """,
                    (deployment_id, now_iso),
                ).fetchone()
            if not row:
                return None
            return {
                "teardown_id": row["teardown_id"],
                "level": row["level"],
                "deployment_id": row["deployment_id"],
                "request_json": row["request_json"],
                "created_at": row["created_at"],
                "expires_at": row["expires_at"],
            }

        return _with_retry(_op, description="get_latest_pending_approval")

    def write_approval_response_by_strategy(
        self,
        deployment_id: str,
        response_json: str,
    ) -> bool:
        """Write an approval response by deployment_id (convenience for API callers).

        Looks up the oldest pending approval for the strategy and writes to it.
        Returns False if no pending approval exists.
        """
        pending = self.get_latest_pending_approval(deployment_id)
        if pending is None:
            logger.warning("No pending approval for strategy %s", deployment_id)
            return False
        return self.write_approval_response(
            teardown_id=pending["teardown_id"],
            level=pending["level"],
            response_json=response_json,
        )


# ---------------------------------------------------------------------------
# Backwards-compat aliases (VIB-4049 PR2)
# ---------------------------------------------------------------------------
#
# Existing callers, tests, and entrypoints reference these by the old names.
# The concrete SQLite implementations were renamed to make room for the
# Postgres siblings in ``platform-plugins/almanak_platform/teardown_store.py``;
# the aliases below keep the public import path stable. Prefer the Protocols
# (``TeardownStateManagerProtocol`` / ``TeardownStateAdapterProtocol``) in new
# call sites — they describe the mode-agnostic contract.
TeardownStateManager = SQLiteTeardownStateManager
TeardownStateAdapter = SQLiteTeardownStateAdapter
