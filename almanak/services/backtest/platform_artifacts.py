"""Versioned terminal-artifact contract for platform backtest jobs.

``result.json`` is diagnostic data, not a success marker.  A runner writes the
small ``terminal.json`` certificate only after ``result.json`` is durable.  The
platform can therefore recover a lost callback without guessing the strategy
outcome from object existence or from the Cloud Run process exit code.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from almanak._version import __version__

PLATFORM_BACKTEST_ARTIFACT_CONTRACT_VERSION = 1
PLATFORM_BACKTEST_TERMINAL_SCHEMA_VERSION = 1
PLATFORM_BACKTEST_TERMINAL_KIND = "almanak.backtest.terminal"


class PlatformBacktestOutcome(StrEnum):
    """Terminal strategy outcomes understood by the platform."""

    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


def build_terminal_manifest(
    *,
    backtest_id: str,
    outcome: PlatformBacktestOutcome,
    result_uri: str,
    result_generation: str,
    commit_sha: str,
    error_message: str | None,
) -> dict[str, Any]:
    """Build the immutable terminal certificate uploaded after ``result.json``."""
    if not result_generation:
        raise ValueError("result_generation is required")
    if outcome is PlatformBacktestOutcome.FAILED and (not isinstance(error_message, str) or not error_message.strip()):
        raise ValueError("failed terminal certificate requires an error message")
    if outcome is PlatformBacktestOutcome.COMPLETED and error_message is not None:
        raise ValueError("completed terminal certificate cannot contain an error message")
    manifest: dict[str, Any] = {
        "schema_version": PLATFORM_BACKTEST_TERMINAL_SCHEMA_VERSION,
        "kind": PLATFORM_BACKTEST_TERMINAL_KIND,
        "backtest_id": backtest_id,
        "backtest_outcome": outcome.value,
        "result_uri": result_uri,
        "result_generation": result_generation,
        "sdk_version": __version__,
        "commit_sha": commit_sha,
        "error_message": error_message,
        "created_at": datetime.now(UTC).isoformat(),
    }
    return manifest


def _require_matching_field(value: dict[str, Any], field: str, expected: Any, error: str) -> None:
    """Require one certificate field to match the runner-owned value."""
    if value.get(field) != expected:
        raise ValueError(error)


def _require_non_empty_string(value: dict[str, Any], field: str, error: str) -> None:
    """Require one certificate field to contain a non-empty string."""
    field_value = value.get(field)
    if not isinstance(field_value, str) or not field_value:
        raise ValueError(error)


def _validated_outcome(value: dict[str, Any]) -> PlatformBacktestOutcome:
    """Parse the closed terminal-outcome vocabulary."""
    raw_outcome = value.get("backtest_outcome")
    if not isinstance(raw_outcome, str):
        raise ValueError("terminal certificate outcome is invalid")
    try:
        return PlatformBacktestOutcome(raw_outcome)
    except ValueError as exc:
        raise ValueError("terminal certificate outcome is invalid") from exc


def _validate_error_message(value: dict[str, Any], outcome: PlatformBacktestOutcome) -> None:
    """Enforce the outcome-specific diagnostic contract."""
    error_message = value.get("error_message")
    if outcome is PlatformBacktestOutcome.FAILED:
        if not isinstance(error_message, str) or not error_message.strip():
            raise ValueError("failed terminal certificate is missing an error message")
    elif error_message is not None:
        raise ValueError("completed terminal certificate contains an error message")


def validate_terminal_manifest(
    value: Any,
    *,
    backtest_id: str,
    result_uri: str,
    commit_sha: str,
) -> dict[str, Any]:
    """Validate and return an existing certificate for retry-safe callback delivery."""
    if not isinstance(value, dict):
        raise ValueError("terminal certificate must be a JSON object")
    _require_matching_field(
        value,
        "schema_version",
        PLATFORM_BACKTEST_TERMINAL_SCHEMA_VERSION,
        "terminal certificate schema is unsupported",
    )
    _require_matching_field(value, "kind", PLATFORM_BACKTEST_TERMINAL_KIND, "terminal certificate kind is invalid")
    _require_matching_field(value, "backtest_id", backtest_id, "terminal certificate identity does not match this run")
    _require_matching_field(value, "commit_sha", commit_sha, "terminal certificate identity does not match this run")
    _require_matching_field(value, "result_uri", result_uri, "terminal certificate result URI does not match this run")
    _require_non_empty_string(value, "result_generation", "terminal certificate result generation is missing")
    _require_matching_field(
        value, "sdk_version", __version__, "terminal certificate SDK version does not match this runner"
    )
    outcome = _validated_outcome(value)
    _validate_error_message(value, outcome)
    return dict(value)
