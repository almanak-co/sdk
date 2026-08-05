from __future__ import annotations

import pytest

from almanak._version import __version__
from almanak.services.backtest.platform_artifacts import (
    PLATFORM_BACKTEST_TERMINAL_KIND,
    PLATFORM_BACKTEST_TERMINAL_SCHEMA_VERSION,
    PlatformBacktestOutcome,
    build_terminal_manifest,
    validate_terminal_manifest,
)

_BACKTEST_ID = "run-1"
_COMMIT_SHA = "a" * 40
_RESULT_URI = "gs://bucket/backtests/run-1/result.json"


def _manifest(
    outcome: PlatformBacktestOutcome = PlatformBacktestOutcome.COMPLETED,
    *,
    error_message: str | None = None,
) -> dict[str, object]:
    return build_terminal_manifest(
        backtest_id=_BACKTEST_ID,
        outcome=outcome,
        result_uri=_RESULT_URI,
        result_generation="42",
        commit_sha=_COMMIT_SHA,
        error_message=error_message,
    )


def _validate(manifest: object) -> dict[str, object]:
    return validate_terminal_manifest(
        manifest,
        backtest_id=_BACKTEST_ID,
        result_uri=_RESULT_URI,
        commit_sha=_COMMIT_SHA,
    )


def test_terminal_manifest_round_trips_through_strict_validation() -> None:
    manifest = _manifest()

    validated = _validate(manifest)

    assert validated == manifest
    assert validated["backtest_outcome"] == "COMPLETED"


def test_failed_terminal_manifest_round_trips_with_its_error_message() -> None:
    manifest = _manifest(
        PlatformBacktestOutcome.FAILED,
        error_message="historical price provider failed",
    )

    validated = _validate(manifest)

    assert validated["backtest_outcome"] == "FAILED"
    assert validated["error_message"] == "historical price provider failed"


def test_terminal_manifest_rejects_failed_outcome_without_diagnostics() -> None:
    with pytest.raises(ValueError, match="requires an error message"):
        build_terminal_manifest(
            backtest_id="run-1",
            outcome=PlatformBacktestOutcome.FAILED,
            result_uri="gs://bucket/backtests/run-1/result.json",
            result_generation="42",
            commit_sha="a" * 40,
            error_message=None,
        )


def test_terminal_manifest_rejects_completed_outcome_with_diagnostics() -> None:
    with pytest.raises(ValueError, match="cannot contain an error message"):
        build_terminal_manifest(
            backtest_id=_BACKTEST_ID,
            outcome=PlatformBacktestOutcome.COMPLETED,
            result_uri=_RESULT_URI,
            result_generation="42",
            commit_sha=_COMMIT_SHA,
            error_message="must not be present",
        )


def test_terminal_manifest_rejects_missing_result_generation() -> None:
    with pytest.raises(ValueError, match="result_generation is required"):
        build_terminal_manifest(
            backtest_id=_BACKTEST_ID,
            outcome=PlatformBacktestOutcome.COMPLETED,
            result_uri=_RESULT_URI,
            result_generation="",
            commit_sha=_COMMIT_SHA,
            error_message=None,
        )


def test_terminal_manifest_rejects_non_object_certificate() -> None:
    with pytest.raises(ValueError, match="must be a JSON object"):
        _validate([])


@pytest.mark.parametrize(
    ("field", "invalid_value", "message"),
    [
        ("schema_version", PLATFORM_BACKTEST_TERMINAL_SCHEMA_VERSION + 1, "schema is unsupported"),
        ("kind", f"{PLATFORM_BACKTEST_TERMINAL_KIND}.other", "kind is invalid"),
        ("backtest_id", "run-2", "identity does not match"),
        ("commit_sha", "b" * 40, "identity does not match"),
        ("result_uri", "gs://bucket/backtests/other-run/result.json", "result URI does not match"),
        ("result_generation", "", "result generation is missing"),
        ("result_generation", 42, "result generation is missing"),
        ("sdk_version", f"{__version__}-other", "SDK version does not match"),
        ("backtest_outcome", None, "outcome is invalid"),
        ("backtest_outcome", "CANCELLED", "outcome is invalid"),
    ],
)
def test_terminal_manifest_rejects_invalid_contract_field(
    field: str,
    invalid_value: object,
    message: str,
) -> None:
    manifest = _manifest()
    manifest[field] = invalid_value

    with pytest.raises(ValueError, match=message):
        _validate(manifest)


@pytest.mark.parametrize("invalid_error", [None, "   ", 123])
def test_terminal_manifest_rejects_failed_certificate_without_valid_diagnostics(invalid_error: object) -> None:
    manifest = _manifest(PlatformBacktestOutcome.FAILED, error_message="initial failure")
    manifest["error_message"] = invalid_error

    with pytest.raises(ValueError, match="missing an error message"):
        _validate(manifest)


def test_terminal_manifest_rejects_completed_certificate_with_diagnostics() -> None:
    manifest = _manifest()
    manifest["error_message"] = "contradictory failure"

    with pytest.raises(ValueError, match="contains an error message"):
        _validate(manifest)
