"""LifecycleStore protocol and data models for agent lifecycle persistence.

Defines the abstract interface that both SQLite (public SDK) and
PostgreSQL (private platform plugin) backends implement.

Keyed by agent_id only -- platform-level queries are handled
by the orchestration layer, not the lifecycle store.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, runtime_checkable


@dataclass
class AgentState:
    """Current state of an agent."""

    agent_id: str
    state: str  # INITIALIZING, RUNNING, PAUSED, ERROR, STOPPING, TERMINATED
    state_changed_at: datetime
    last_heartbeat_at: datetime | None = None
    error_message: str | None = None
    iteration_count: int = 0
    source: str = "gateway"  # 'gateway' or 'platform' — tracks who last wrote the state


@dataclass
class AgentCommand:
    """A command issued to an agent."""

    id: int
    agent_id: str
    command: str  # PAUSE, RESUME, STOP
    issued_at: datetime
    issued_by: str
    processed_at: datetime | None = None


@runtime_checkable
class LifecycleStore(Protocol):
    """Protocol for agent lifecycle persistence (commands + state).

    Both SQLite (public) and PostgreSQL (private plugin) implement this.
    Keyed by agent_id only -- platform-level queries are handled
    by the orchestration layer, not the lifecycle store.
    """

    def initialize(self) -> None: ...
    def close(self) -> None: ...

    # State
    def write_state(
        self,
        agent_id: str,
        state: str,
        error_message: str | None = None,
    ) -> None: ...

    def read_state(self, agent_id: str) -> AgentState | None: ...

    def heartbeat(self, agent_id: str) -> None: ...

    # Commands
    def read_pending_command(self, agent_id: str) -> AgentCommand | None: ...
    def ack_command(self, command_id: int) -> None: ...
    def write_command(self, agent_id: str, command: str, issued_by: str) -> None: ...
