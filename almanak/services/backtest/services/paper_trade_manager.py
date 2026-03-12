"""In-memory paper trading session tracker.

Wraps BackgroundPaperTrader lifecycle for HTTP use. Each session gets a
unique session_id and maps to a BackgroundPaperTrader process.

For v1, sessions are tracked in-memory — lost on restart (same as backtest
jobs). Persistence is deferred.
"""

from __future__ import annotations

import logging
import threading
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from almanak.services.backtest.models import (
    PaperTradeLiveMetrics,
    PaperTradeSessionStatus,
    ProgressInfo,
)

logger = logging.getLogger(__name__)


@dataclass
class SessionState:
    """Internal state of a paper trading session."""

    session_id: str
    status: PaperTradeSessionStatus
    strategy_id: str
    chain: str
    progress: ProgressInfo = field(default_factory=ProgressInfo)  # type: ignore[arg-type]
    metrics: PaperTradeLiveMetrics = field(default_factory=PaperTradeLiveMetrics)
    pid: int | None = None
    error: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    stopped_at: datetime | None = None
    state_dir: Path | None = None


class PaperTradeManager:
    """Thread-safe paper trading session tracker.

    Manages the lifecycle of paper trading sessions. Each session wraps a
    BackgroundPaperTrader subprocess.
    """

    def __init__(self, max_sessions: int = 2, max_total: int = 500, state_base_dir: str | None = None) -> None:
        self._sessions: dict[str, SessionState] = {}
        self._lock = threading.Lock()
        self._max_sessions = max_sessions
        self._max_total = max_total
        self._state_base_dir = Path(state_base_dir) if state_base_dir else Path.home() / ".almanak" / "paper"

    def create_session(self, strategy_id: str, chain: str) -> str:
        """Create a new paper trading session and return its ID."""
        session_id = f"pt_{uuid.uuid4().hex[:12]}"
        with self._lock:
            active = sum(
                1
                for s in self._sessions.values()
                if s.status in (PaperTradeSessionStatus.STARTING, PaperTradeSessionStatus.RUNNING)
            )
            if active >= self._max_sessions:
                raise RuntimeError(
                    f"Max concurrent paper sessions ({self._max_sessions}) reached. Stop a running session first."
                )
            state_dir = self._state_base_dir / session_id
            state_dir.mkdir(parents=True, exist_ok=True)
            self._sessions[session_id] = SessionState(
                session_id=session_id,
                status=PaperTradeSessionStatus.STARTING,
                strategy_id=strategy_id,
                chain=chain,
                state_dir=state_dir,
            )
            self._evict_completed_unlocked()
        return session_id

    def get_session(self, session_id: str) -> SessionState | None:
        """Get session state by ID, or None if not found."""
        with self._lock:
            return self._sessions.get(session_id)

    def mark_running(self, session_id: str, pid: int) -> None:
        """Transition session to RUNNING with the background process PID."""
        with self._lock:
            if session := self._sessions.get(session_id):
                session.status = PaperTradeSessionStatus.RUNNING
                session.pid = pid

    def update_progress(
        self,
        session_id: str,
        percent: float,
        current_step: str = "",
        eta_seconds: int | None = None,
    ) -> None:
        """Update progress for a running session."""
        with self._lock:
            if session := self._sessions.get(session_id):
                session.progress = ProgressInfo(
                    percent=percent,
                    current_step=current_step,
                    eta_seconds=eta_seconds,
                )

    def update_metrics(self, session_id: str, metrics: PaperTradeLiveMetrics) -> None:
        """Update live metrics for a running session."""
        with self._lock:
            if session := self._sessions.get(session_id):
                session.metrics = metrics

    def mark_stopped(self, session_id: str) -> None:
        """Mark session as stopped."""
        with self._lock:
            if session := self._sessions.get(session_id):
                session.status = PaperTradeSessionStatus.STOPPED
                session.stopped_at = datetime.now(UTC)

    def mark_failed(self, session_id: str, error: str) -> None:
        """Mark session as failed with error message."""
        with self._lock:
            if session := self._sessions.get(session_id):
                session.status = PaperTradeSessionStatus.FAILED
                session.error = error
                session.stopped_at = datetime.now(UTC)

    @property
    def active_count(self) -> int:
        """Number of active (starting + running) sessions."""
        with self._lock:
            return sum(
                1
                for s in self._sessions.values()
                if s.status in (PaperTradeSessionStatus.STARTING, PaperTradeSessionStatus.RUNNING)
            )

    def _evict_completed_unlocked(self) -> None:
        """Evict oldest stopped/failed sessions when total exceeds max_total. Caller must hold _lock."""
        if len(self._sessions) <= self._max_total:
            return
        finished = sorted(
            (
                s
                for s in self._sessions.values()
                if s.status in (PaperTradeSessionStatus.STOPPED, PaperTradeSessionStatus.FAILED)
            ),
            key=lambda s: s.created_at,
        )
        to_remove = len(self._sessions) - self._max_total
        for session in finished[:to_remove]:
            del self._sessions[session.session_id]
