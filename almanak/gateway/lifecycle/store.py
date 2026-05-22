"""LifecycleStore protocol and data models for agent lifecycle persistence.

Defines the abstract interface that both SQLite (public SDK) and
PostgreSQL (private platform plugin) backends implement.

Keyed by deployment_id only -- platform-level queries are handled
by the orchestration layer, not the lifecycle store.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, runtime_checkable


@dataclass
class AgentState:
    """Current state of an agent."""

    deployment_id: str
    state: str  # INITIALIZING, RUNNING, PAUSED, ERROR, STOPPING, TERMINATED
    state_changed_at: datetime
    last_heartbeat_at: datetime | None = None
    error_message: str | None = None
    iteration_count: int = 0
    source: str = "gateway"  # 'gateway' or 'platform' — tracks who last wrote the state
    # almanak SDK version loaded inside the strategy process. Written only when
    # the strategy reports it on lifecycle state writes; ``None`` means either
    # not reported yet or the row predates the column.
    running_almanak_version: str | None = None


@dataclass
class AgentCommand:
    """A command issued to an agent."""

    id: int
    deployment_id: str
    command: str  # PAUSE, RESUME, STOP
    issued_at: datetime
    issued_by: str
    processed_at: datetime | None = None


@runtime_checkable
class LifecycleStore(Protocol):
    """Protocol for agent lifecycle persistence (commands + state).

    Both SQLite (public) and PostgreSQL (private plugin) implement this.
    Keyed by deployment_id only -- platform-level queries are handled
    by the orchestration layer, not the lifecycle store.
    """

    def initialize(self) -> None: ...
    def close(self) -> None: ...

    # State
    def write_state(
        self,
        deployment_id: str,
        state: str,
        error_message: str | None = None,
        running_almanak_version: str | None = None,
    ) -> None: ...

    def read_state(self, deployment_id: str) -> AgentState | None: ...

    def heartbeat(self, deployment_id: str) -> None: ...

    # Commands
    def read_pending_command(self, deployment_id: str) -> AgentCommand | None: ...
    def ack_command(self, command_id: int) -> None: ...
    def write_command(self, deployment_id: str, command: str, issued_by: str) -> None: ...
