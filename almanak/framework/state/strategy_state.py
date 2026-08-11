"""Durable strategy-state replacement without clobbering runner state.

``strategy_state`` is one physical row shared by strategy-authored state and
framework/runner control data.  Strategy writes therefore replace only the
reserved user-state envelope; runner keys such as ``execution_progress`` stay
as an overlay in the same CAS-protected row.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from .state_manager import StateConflictError, StateData, StateNotFoundError

STRATEGY_USER_STATE_KEY = "__framework_strategy_user_state__"
"""Reserved top-level key containing the exact strategy-authored state."""

STATE_OWNERSHIP_VERSION_KEY = "__framework_state_ownership_version__"
STATE_OWNERSHIP_VERSION = 1

# One physical row has several writers.  This inventory is intentionally
# closed: adding a new top-level runner field requires adding it here and to
# the inventory regression test.  Everything else in a legacy flat row is
# strategy-authored state and migrates into the user envelope.
RUNNER_OWNED_STATE_KEYS = frozenset(
    {
        "execution_progress",
        "recovered_sessions",
        "copy_trading_state",
        "vault_state",
        "last_iteration",
        "total_iterations",
        "successful_iterations",
        "consecutive_errors",
        "total_value_usd",
        "value_confidence",
        "valuation_source",
        "external_provider",
        "external_total_value_usd",
        "framework_total_value_usd",
        "reconciliation_status",
        "capital_flows",
    }
)

_FRAMEWORK_STATE_PREFIX = "__framework_"
_FRAMEWORK_STATE_SUFFIX = "__"
_DEFAULT_CAS_ATTEMPTS = 3


class StateValuePreconditionError(RuntimeError):
    """A compare-and-mutate refused because the durable value changed."""


def is_framework_state_key(key: Any) -> bool:
    """Return whether ``key`` belongs to a framework component namespace."""
    return bool(
        isinstance(key, str) and key.startswith(_FRAMEWORK_STATE_PREFIX) and key.endswith(_FRAMEWORK_STATE_SUFFIX)
    )


def runner_state_value(row_state: Mapping[str, Any], key: str, default: Any = None) -> Any:
    """Read one runner-owned value across the envelope migration boundary.

    The short-lived pre-version envelope implementation could write a newer
    ``vault_state`` inside the envelope while retaining an older top-level
    copy.  Prefer that envelope value only for an unversioned row; ownership-v1
    rows have exactly one authoritative top-level runner value.
    """
    if key not in RUNNER_OWNED_STATE_KEYS:
        raise KeyError(f"{key!r} is not registered as runner-owned state")
    if row_state.get(STATE_OWNERSHIP_VERSION_KEY) == STATE_OWNERSHIP_VERSION:
        return row_state.get(key, default)
    envelope = row_state.get(STRATEGY_USER_STATE_KEY)
    if key == "vault_state" and isinstance(envelope, Mapping) and key in envelope:
        return envelope[key]
    return row_state.get(key, default)


def _same_json_value(left: Any, right: Any) -> bool:
    """Compare JSON values without Python's ``True == 1`` coercion."""
    try:
        return json.dumps(left, sort_keys=True, separators=(",", ":")) == json.dumps(
            right, sort_keys=True, separators=(",", ":")
        )
    except (TypeError, ValueError):
        return type(left) is type(right) and left == right


def split_strategy_persistent_state(row_state: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return ``(user_state, framework_state)`` from an envelope or legacy row.

    Envelope rows are authoritative: deleted user keys do not reappear from
    stale legacy top-level fields.  A row written by an older SDK has no
    envelope, so all non-framework fields retain the historical read contract
    until the next strategy write migrates it.
    """
    if STRATEGY_USER_STATE_KEY in row_state:
        envelope = row_state[STRATEGY_USER_STATE_KEY]
        if not isinstance(envelope, dict):
            raise ValueError(f"{STRATEGY_USER_STATE_KEY} must contain a JSON object")
        user_state = dict(envelope)
    else:
        user_state = {
            key: value
            for key, value in row_state.items()
            if key not in RUNNER_OWNED_STATE_KEYS and not is_framework_state_key(key)
        }

    framework_state = {
        key: value for key, value in row_state.items() if key != STRATEGY_USER_STATE_KEY and is_framework_state_key(key)
    }
    return user_state, framework_state


async def replace_strategy_persistent_state(
    state_manager: Any,
    deployment_id: str,
    user_state: Mapping[str, Any],
    *,
    framework_state: Mapping[str, Any] | None = None,
    runner_state: Mapping[str, Any] | None = None,
    max_attempts: int = _DEFAULT_CAS_ATTEMPTS,
) -> StateData:
    """Replace strategy-owned state while preserving the latest runner row.

    Each retry reloads the complete row before applying the user envelope and
    framework-owned overlay.  Consequently a concurrent runner write is
    preserved rather than resurrected or dropped.  ``max_attempts`` is bounded
    so a deployment-identity violation cannot spin forever.
    """
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")

    replacement = {key: value for key, value in user_state.items() if key not in RUNNER_OWNED_STATE_KEYS}
    # ``vault_state`` historically travelled through the strategy callback,
    # but is runner-owned.  Promote it into its one authoritative top-level
    # slot while migrating rather than leaving two independently mutable
    # copies.  No other runner key may be supplied by strategy state.
    promoted_vault_state = user_state.get("vault_state")
    framework_overlay = dict(framework_state or {})
    invalid_framework_keys = [key for key in framework_overlay if not is_framework_state_key(key)]
    if invalid_framework_keys:
        raise ValueError(f"framework_state contains non-framework keys: {sorted(invalid_framework_keys)!r}")
    runner_overlay = dict(runner_state or {})
    invalid_runner_keys = [key for key in runner_overlay if key not in RUNNER_OWNED_STATE_KEYS]
    if invalid_runner_keys:
        raise ValueError(f"runner_state contains unregistered keys: {sorted(invalid_runner_keys)!r}")
    last_conflict: StateConflictError | None = None

    for _attempt in range(max_attempts):
        try:
            current = await state_manager.load_state(deployment_id)
            if current is None:
                raise StateNotFoundError(deployment_id)
            expected_version: int | None = current.version
            # Preserve only explicitly-owned runner/component state.  This is
            # also the one-way legacy migration: old strategy keys move into
            # the envelope and their stale top-level duplicates are removed.
            next_row = {
                key: value
                for key, value in current.state.items()
                if key in RUNNER_OWNED_STATE_KEYS or is_framework_state_key(key)
            }
            schema_version = current.schema_version
        except StateNotFoundError:
            expected_version = None
            next_row = {}
            schema_version = 1

        # Replacement, not update: keys absent from ``replacement`` are
        # intentionally deleted from strategy-owned state.
        next_row[STRATEGY_USER_STATE_KEY] = dict(replacement)
        if promoted_vault_state is not None:
            next_row["vault_state"] = promoted_vault_state
        next_row.update(runner_overlay)
        next_row[STATE_OWNERSHIP_VERSION_KEY] = STATE_OWNERSHIP_VERSION
        next_row.update(framework_overlay)
        candidate = StateData(
            deployment_id=deployment_id,
            version=1 if expected_version is None else expected_version,
            state=next_row,
            schema_version=schema_version,
        )
        try:
            return await state_manager.save_state(candidate, expected_version=expected_version)
        except StateConflictError as exc:
            last_conflict = exc

    assert last_conflict is not None
    raise last_conflict


async def merge_runner_state_values(
    state_manager: Any,
    deployment_id: str,
    values: Mapping[str, Any],
    *,
    max_attempts: int = _DEFAULT_CAS_ATTEMPTS,
) -> StateData:
    """CAS-merge registered runner values without changing strategy state."""
    overlay = dict(values)
    invalid_keys = [key for key in overlay if key not in RUNNER_OWNED_STATE_KEYS]
    if invalid_keys:
        raise ValueError(f"runner state contains unregistered keys: {sorted(invalid_keys)!r}")
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")

    last_conflict: StateConflictError | None = None
    for _attempt in range(max_attempts):
        try:
            current = await state_manager.load_state(deployment_id)
            if current is None:
                raise StateNotFoundError(deployment_id)
            expected_version: int | None = current.version
            next_row = dict(current.state)
            schema_version = current.schema_version
        except StateNotFoundError:
            expected_version = None
            next_row = {}
            schema_version = 1
        next_row.update(overlay)
        candidate = StateData(
            deployment_id=deployment_id,
            version=1 if expected_version is None else expected_version,
            state=next_row,
            schema_version=schema_version,
        )
        try:
            return await state_manager.save_state(candidate, expected_version=expected_version)
        except StateConflictError as exc:
            last_conflict = exc
    assert last_conflict is not None
    raise last_conflict


async def compare_and_delete_state_value(
    state_manager: Any,
    deployment_id: str,
    key: str,
    expected_value: Any,
    *,
    max_attempts: int = _DEFAULT_CAS_ATTEMPTS,
) -> StateData:
    """Delete ``key`` only while its complete durable value still matches.

    Intended for authorized release of a replay marker.  A CAS conflict
    reloads the newest row and rechecks the marker before retrying, so an
    unrelated concurrent write is preserved while a changed marker identity
    or version refuses release.
    """
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")

    last_conflict: StateConflictError | None = None
    for _attempt in range(max_attempts):
        current = await state_manager.load_state(deployment_id)
        if current is None:
            raise StateValuePreconditionError(f"state row {deployment_id!r} does not exist")
        actual_value = current.state.get(key)
        if key not in current.state or not _same_json_value(actual_value, expected_value):
            raise StateValuePreconditionError(
                f"state value {key!r} changed for {deployment_id}; refusing compare-and-delete"
            )
        next_row = dict(current.state)
        del next_row[key]
        candidate = StateData(
            deployment_id=deployment_id,
            version=current.version,
            state=next_row,
            schema_version=current.schema_version,
        )
        try:
            return await state_manager.save_state(candidate, expected_version=current.version)
        except StateConflictError as exc:
            last_conflict = exc

    assert last_conflict is not None
    raise last_conflict


async def compare_and_replace_state_value(
    state_manager: Any,
    deployment_id: str,
    key: str,
    expected_value: Any,
    replacement_value: Any,
    *,
    max_attempts: int = _DEFAULT_CAS_ATTEMPTS,
) -> StateData:
    """Replace one owned value only while its complete durable value matches."""
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")

    last_conflict: StateConflictError | None = None
    for _attempt in range(max_attempts):
        current = await state_manager.load_state(deployment_id)
        if current is None:
            raise StateValuePreconditionError(f"state row {deployment_id!r} does not exist")
        actual_value = current.state.get(key)
        if key not in current.state or not _same_json_value(actual_value, expected_value):
            raise StateValuePreconditionError(
                f"state value {key!r} changed for {deployment_id}; refusing compare-and-replace"
            )
        next_row = dict(current.state)
        next_row[key] = replacement_value
        candidate = StateData(
            deployment_id=deployment_id,
            version=current.version,
            state=next_row,
            schema_version=current.schema_version,
        )
        try:
            return await state_manager.save_state(candidate, expected_version=current.version)
        except StateConflictError as exc:
            last_conflict = exc

    assert last_conflict is not None
    raise last_conflict


__all__ = [
    "STRATEGY_USER_STATE_KEY",
    "STATE_OWNERSHIP_VERSION",
    "STATE_OWNERSHIP_VERSION_KEY",
    "RUNNER_OWNED_STATE_KEYS",
    "StateValuePreconditionError",
    "compare_and_delete_state_value",
    "compare_and_replace_state_value",
    "is_framework_state_key",
    "merge_runner_state_values",
    "runner_state_value",
    "replace_strategy_persistent_state",
    "split_strategy_persistent_state",
]
