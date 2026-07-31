"""Canonical agent lifecycle vocabulary shared by framework and gateway.

Lifecycle values are persisted as text and cross protobuf boundaries as strings,
but control flow inside the SDK uses the typed values in this module. Historical
and platform-owned values remain decodable so shared rows can be consumed safely;
they are deliberately not SDK-writable or enqueueable.
"""

from __future__ import annotations

from enum import StrEnum

__all__ = [
    "LifecycleCommand",
    "LifecycleState",
    "LifecycleStateSource",
    "LifecycleValueError",
    "parse_lifecycle_command",
    "parse_lifecycle_source",
    "parse_lifecycle_state",
    "require_enqueueable_command",
    "require_writable_state",
]


class LifecycleValueError(ValueError):
    """A persisted, protobuf, or API lifecycle value is not valid."""

    def __init__(self, field_name: str, value: object, *, reason: str = "unknown") -> None:
        self.field_name = field_name
        self.value = value
        self.reason = reason
        super().__init__(f"{reason} lifecycle {field_name}: {value!r}")


class LifecycleState(StrEnum):
    """Agent lifecycle states stored in ``agent_state.state``.

    ``PAUSED`` is historical and read-only. ``V2_PREPARING`` and
    ``V2_DEPLOYING`` are platform-owned hand-off states. They remain members
    because the SDK reads the shared lifecycle row during hosted boot, but no
    SDK writer may persist any of these read-only values.
    """

    INITIALIZING = "INITIALIZING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    TEARING_DOWN = "TEARING_DOWN"
    TERMINATED = "TERMINATED"
    ERROR = "ERROR"
    PAUSED = "PAUSED"
    V2_PREPARING = "V2_PREPARING"
    V2_DEPLOYING = "V2_DEPLOYING"

    @property
    def is_historical(self) -> bool:
        """Whether the state is retained only to decode historical rows."""
        return self is LifecycleState.PAUSED

    @property
    def is_platform_owned(self) -> bool:
        """Whether the platform, rather than the SDK, owns this state."""
        return self in {LifecycleState.V2_PREPARING, LifecycleState.V2_DEPLOYING}

    @property
    def is_writable(self) -> bool:
        """Whether current SDK code may persist this state."""
        return not self.is_historical and not self.is_platform_owned


class LifecycleCommand(StrEnum):
    """Commands stored in ``agent_command.command``.

    ``PAUSE`` and ``RESUME`` are retained solely for historical queued rows.
    They are safe no-ops in the runner and cannot be newly enqueued.
    """

    STOP = "STOP"
    PAUSE = "PAUSE"
    RESUME = "RESUME"

    @property
    def is_enqueueable(self) -> bool:
        """Whether current SDK code may enqueue this command."""
        return self is LifecycleCommand.STOP


class LifecycleStateSource(StrEnum):
    """Owner that last wrote an ``agent_state`` row."""

    GATEWAY = "gateway"
    PLATFORM = "platform"


def _parse_enum[LifecycleEnum: StrEnum](
    value: object,
    enum_type: type[LifecycleEnum],
    field: str,
) -> LifecycleEnum:
    if isinstance(value, enum_type):
        return value
    if not isinstance(value, str):
        raise LifecycleValueError(field, value)
    try:
        return enum_type(value)
    except ValueError as exc:
        raise LifecycleValueError(field, value) from exc


def parse_lifecycle_state(value: object) -> LifecycleState:
    """Parse a state at a persistence, protobuf, or API boundary."""
    return _parse_enum(value, LifecycleState, "state")


def parse_lifecycle_command(value: object) -> LifecycleCommand:
    """Parse a current or historical command at an untrusted boundary."""
    return _parse_enum(value, LifecycleCommand, "command")


def parse_lifecycle_source(value: object) -> LifecycleStateSource:
    """Parse a state source at a persistence boundary."""
    return _parse_enum(value, LifecycleStateSource, "source")


def require_writable_state(state: LifecycleState) -> LifecycleState:
    """Validate an internal state before persistence."""
    if not isinstance(state, LifecycleState):
        raise LifecycleValueError("state", state, reason="untyped")
    if not state.is_writable:
        reason = "platform-owned" if state.is_platform_owned else "retired"
        raise LifecycleValueError("state", state.value, reason=reason)
    return state


def require_enqueueable_command(command: LifecycleCommand) -> LifecycleCommand:
    """Validate an internal command before enqueueing it."""
    if not isinstance(command, LifecycleCommand):
        raise LifecycleValueError("command", command, reason="untyped")
    if not command.is_enqueueable:
        raise LifecycleValueError("command", command.value, reason="retired")
    return command
