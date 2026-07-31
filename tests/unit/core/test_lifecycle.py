"""Canonical agent lifecycle vocabulary contract (ALM-3080)."""

from __future__ import annotations

import pytest

from almanak.core.lifecycle import (
    LifecycleCommand,
    LifecycleState,
    LifecycleStateSource,
    LifecycleValueError,
    parse_lifecycle_command,
    parse_lifecycle_source,
    parse_lifecycle_state,
    require_enqueueable_command,
    require_writable_state,
)


def test_lifecycle_vocabulary_is_exhaustive_and_wire_stable() -> None:
    assert {state.value for state in LifecycleState} == {
        "INITIALIZING",
        "RUNNING",
        "STOPPING",
        "TEARING_DOWN",
        "TERMINATED",
        "ERROR",
        "PAUSED",
        "V2_PREPARING",
        "V2_DEPLOYING",
    }
    assert {command.value for command in LifecycleCommand} == {"STOP", "PAUSE", "RESUME"}
    assert {source.value for source in LifecycleStateSource} == {"gateway", "platform"}


def test_current_and_historical_members_are_explicit() -> None:
    assert {state for state in LifecycleState if state.is_writable} == {
        LifecycleState.INITIALIZING,
        LifecycleState.RUNNING,
        LifecycleState.STOPPING,
        LifecycleState.TEARING_DOWN,
        LifecycleState.TERMINATED,
        LifecycleState.ERROR,
    }
    assert not LifecycleState.PAUSED.is_writable
    assert LifecycleState.PAUSED.is_historical
    assert not LifecycleState.PAUSED.is_platform_owned
    assert {state for state in LifecycleState if state.is_platform_owned} == {
        LifecycleState.V2_PREPARING,
        LifecycleState.V2_DEPLOYING,
    }
    assert all(not state.is_writable for state in LifecycleState if state.is_platform_owned)
    assert {command for command in LifecycleCommand if command.is_enqueueable} == {LifecycleCommand.STOP}
    assert not LifecycleCommand.PAUSE.is_enqueueable
    assert not LifecycleCommand.RESUME.is_enqueueable


def test_boundary_parsers_accept_exact_current_and_historical_values() -> None:
    assert parse_lifecycle_state("TEARING_DOWN") is LifecycleState.TEARING_DOWN
    assert parse_lifecycle_state("PAUSED") is LifecycleState.PAUSED
    assert parse_lifecycle_state("V2_DEPLOYING") is LifecycleState.V2_DEPLOYING
    assert parse_lifecycle_command("STOP") is LifecycleCommand.STOP
    assert parse_lifecycle_command("PAUSE") is LifecycleCommand.PAUSE
    assert parse_lifecycle_source("platform") is LifecycleStateSource.PLATFORM


@pytest.mark.parametrize(
    ("parser", "value"),
    [
        (parse_lifecycle_state, "running"),
        (parse_lifecycle_state, "UNKNOWN"),
        (parse_lifecycle_command, "TEARDOWN"),
        (parse_lifecycle_source, "runner"),
        (parse_lifecycle_command, object()),
    ],
)
def test_boundary_parsers_reject_unknown_or_normalized_values(parser, value: object) -> None:
    with pytest.raises(LifecycleValueError):
        parser(value)


def test_write_guards_reject_historical_and_untyped_values() -> None:
    assert require_writable_state(LifecycleState.RUNNING) is LifecycleState.RUNNING
    assert require_enqueueable_command(LifecycleCommand.STOP) is LifecycleCommand.STOP

    with pytest.raises(LifecycleValueError, match="retired lifecycle state"):
        require_writable_state(LifecycleState.PAUSED)
    with pytest.raises(LifecycleValueError, match="platform-owned lifecycle state"):
        require_writable_state(LifecycleState.V2_PREPARING)
    with pytest.raises(LifecycleValueError, match="retired lifecycle command"):
        require_enqueueable_command(LifecycleCommand.PAUSE)
    with pytest.raises(LifecycleValueError, match="untyped lifecycle state"):
        require_writable_state("RUNNING")  # type: ignore[arg-type]
    with pytest.raises(LifecycleValueError, match="untyped lifecycle command"):
        require_enqueueable_command("STOP")  # type: ignore[arg-type]
