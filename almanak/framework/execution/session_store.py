"""Execution Session Store for persistence of execution sessions.

This module provides a persistence layer for ExecutionSession objects,
enabling crash recovery by saving and retrieving session state.

The store uses file-based JSON storage with atomic writes to prevent
corruption. Sessions are stored in a configurable directory.

Example:
    from almanak.framework.execution.session_store import ExecutionSessionStore
    from almanak.framework.execution.session import ExecutionSession, ExecutionPhase

    # Create store with custom path
    store = ExecutionSessionStore(storage_path="./state/sessions")

    # Save a session
    session = ExecutionSession(
        session_id="sess_123",
        strategy_id="strategy_a",
        intent_id="intent_456",
        phase=ExecutionPhase.PREPARING,
    )
    store.save(session)

    # Retrieve a session
    retrieved = store.get("sess_123")

    # Get incomplete sessions for recovery
    incomplete = store.get_incomplete_sessions()

    # Mark session complete
    store.mark_complete("sess_123", success=True)
"""

import json
import logging
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from almanak.framework.execution.session import ExecutionSession

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

DEFAULT_STORAGE_PATH = "./state/execution_sessions"
SESSION_FILE_EXTENSION = ".json"


# =============================================================================
# Execution Session Store
# =============================================================================


class ExecutionSessionStore:
    """Persistence layer for ExecutionSession objects.

    Provides file-based JSON storage with atomic writes to prevent corruption.
    Sessions are stored as individual JSON files in the configured directory.

    Features:
        - Atomic writes using temp file + rename pattern
        - File-based storage for simplicity and durability
        - Query for incomplete sessions (for recovery)
        - Mark sessions as complete

    Attributes:
        storage_path: Path to the directory where sessions are stored

    Example:
        store = ExecutionSessionStore(storage_path="./state/sessions")

        # Save session
        store.save(session)

        # Get session by ID
        session = store.get("sess_123")

        # Get all incomplete sessions
        incomplete = store.get_incomplete_sessions()

        # Mark complete
        store.mark_complete("sess_123", success=True)
    """

    def __init__(self, storage_path: str = DEFAULT_STORAGE_PATH) -> None:
        """Initialize the ExecutionSessionStore.

        Args:
            storage_path: Path to the directory where sessions will be stored.
                         Created if it doesn't exist.
        """
        self._storage_path = Path(storage_path)
        self._ensure_storage_directory()
        logger.debug(f"ExecutionSessionStore initialized at {self._storage_path}")

    def _ensure_storage_directory(self) -> None:
        """Ensure the storage directory exists."""
        self._storage_path.mkdir(parents=True, exist_ok=True)

    def _session_file_path(self, session_id: str) -> Path:
        """Get the file path for a session.

        Args:
            session_id: Session identifier

        Returns:
            Path to the session file
        """
        return self._storage_path / f"{session_id}{SESSION_FILE_EXTENSION}"

    def save(self, session: ExecutionSession) -> None:
        """Persist a session to storage.

        Uses atomic writes (write to temp file, then rename) to prevent
        corruption if the process crashes during write.

        Args:
            session: ExecutionSession to persist

        Raises:
            OSError: If unable to write to storage
        """
        # Update timestamp before saving
        session.touch()

        file_path = self._session_file_path(session.session_id)
        session_dict = session.to_dict()
        json_content = json.dumps(session_dict, indent=2)

        # Atomic write: write to temp file, then rename
        # Use the same directory for temp file to ensure same filesystem (for rename)
        fd, temp_path = tempfile.mkstemp(
            suffix=".tmp",
            prefix=f"{session.session_id}_",
            dir=self._storage_path,
        )
        try:
            with os.fdopen(fd, "w") as f:
                f.write(json_content)
                f.flush()
                os.fsync(f.fileno())

            # Atomic rename (POSIX guarantees this is atomic on same filesystem)
            os.rename(temp_path, file_path)

            logger.debug(
                f"Saved session {session.session_id} to {file_path} "
                f"(phase={session.phase.value}, completed={session.completed})"
            )
        except Exception:
            # Clean up temp file on failure
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise

    def get(self, session_id: str) -> ExecutionSession | None:
        """Retrieve a session by ID.

        Args:
            session_id: Session identifier

        Returns:
            ExecutionSession if found, None otherwise
        """
        file_path = self._session_file_path(session_id)

        if not file_path.exists():
            logger.debug(f"Session {session_id} not found at {file_path}")
            return None

        try:
            with open(file_path) as f:
                data = json.load(f)

            session = ExecutionSession.from_dict(data)
            logger.debug(f"Retrieved session {session_id} (phase={session.phase.value}, completed={session.completed})")
            return session
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse session file {file_path}: {e}")
            return None
        except Exception as e:
            logger.error(f"Failed to read session {session_id}: {e}")
            return None

    def get_incomplete_sessions(self) -> list[ExecutionSession]:
        """Get all sessions that are not in a terminal state.

        Returns sessions where `completed=False`, which indicates
        they may need recovery or continuation.

        Returns:
            List of incomplete ExecutionSession objects
        """
        incomplete: list[ExecutionSession] = []

        try:
            for file_path in self._storage_path.glob(f"*{SESSION_FILE_EXTENSION}"):
                try:
                    with open(file_path) as f:
                        data = json.load(f)

                    session = ExecutionSession.from_dict(data)

                    # Check if session is not complete
                    if not session.is_terminal():
                        incomplete.append(session)
                        logger.debug(
                            f"Found incomplete session {session.session_id} "
                            f"(phase={session.phase.value}, attempt={session.attempt_number})"
                        )
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse session file {file_path}: {e}")
                except Exception as e:
                    logger.warning(f"Failed to read session file {file_path}: {e}")
        except Exception as e:
            logger.error(f"Failed to scan storage directory: {e}")

        # Sort by created_at (oldest first) for deterministic recovery order
        incomplete.sort(key=lambda s: s.created_at)

        logger.info(f"Found {len(incomplete)} incomplete sessions")
        return incomplete

    def mark_complete(self, session_id: str, success: bool) -> bool:
        """Mark a session as complete.

        Loads the session, marks it as complete with the given success status,
        and saves it back to storage.

        Args:
            session_id: Session identifier
            success: Whether the session completed successfully

        Returns:
            True if session was found and marked complete, False otherwise
        """
        session = self.get(session_id)

        if session is None:
            logger.warning(f"Cannot mark session {session_id} complete: not found")
            return False

        session.mark_complete(success)
        self.save(session)

        logger.info(f"Marked session {session_id} as complete (success={success}, phase={session.phase.value})")
        return True

    def delete(self, session_id: str) -> bool:
        """Delete a session from storage.

        Args:
            session_id: Session identifier

        Returns:
            True if session was deleted, False if not found
        """
        file_path = self._session_file_path(session_id)

        if not file_path.exists():
            logger.debug(f"Cannot delete session {session_id}: not found")
            return False

        try:
            os.remove(file_path)
            logger.info(f"Deleted session {session_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete session {session_id}: {e}")
            return False

    def get_all_sessions(self) -> list[ExecutionSession]:
        """Get all sessions from storage.

        Returns:
            List of all ExecutionSession objects
        """
        sessions: list[ExecutionSession] = []

        try:
            for file_path in self._storage_path.glob(f"*{SESSION_FILE_EXTENSION}"):
                try:
                    with open(file_path) as f:
                        data = json.load(f)

                    session = ExecutionSession.from_dict(data)
                    sessions.append(session)
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse session file {file_path}: {e}")
                except Exception as e:
                    logger.warning(f"Failed to read session file {file_path}: {e}")
        except Exception as e:
            logger.error(f"Failed to scan storage directory: {e}")

        # Sort by created_at
        sessions.sort(key=lambda s: s.created_at)
        return sessions

    def get_sessions_by_strategy(self, strategy_id: str) -> list[ExecutionSession]:
        """Get all sessions for a specific strategy.

        Args:
            strategy_id: Strategy identifier

        Returns:
            List of ExecutionSession objects for the strategy
        """
        all_sessions = self.get_all_sessions()
        return [s for s in all_sessions if s.strategy_id == strategy_id]

    def cleanup_old_sessions(
        self,
        max_age_seconds: int = 86400 * 7,  # 7 days
        keep_incomplete: bool = True,
    ) -> int:
        """Clean up old completed sessions.

        Args:
            max_age_seconds: Maximum age of sessions to keep (default 7 days)
            keep_incomplete: If True, never delete incomplete sessions

        Returns:
            Number of sessions deleted
        """
        deleted_count = 0
        now = datetime.now(UTC)

        for session in self.get_all_sessions():
            # Skip incomplete sessions if keep_incomplete is True
            if keep_incomplete and not session.is_terminal():
                continue

            # Calculate age
            age_seconds = (now - session.updated_at).total_seconds()

            if age_seconds > max_age_seconds:
                if self.delete(session.session_id):
                    deleted_count += 1
                    logger.debug(f"Cleaned up old session {session.session_id} (age={age_seconds / 86400:.1f} days)")

        if deleted_count > 0:
            logger.info(f"Cleaned up {deleted_count} old sessions")

        return deleted_count

    @property
    def storage_path(self) -> Path:
        """Get the storage path.

        Returns:
            Path to the storage directory
        """
        return self._storage_path

    def __repr__(self) -> str:
        """String representation."""
        return f"ExecutionSessionStore(storage_path={self._storage_path!r})"


# =============================================================================
# Exports
# =============================================================================


__all__ = [
    "ExecutionSessionStore",
    "DEFAULT_STORAGE_PATH",
]
