"""Immutable receipt identity for failed-attempt ledger persistence."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any


def _payload(row: Mapping[str, Any]) -> dict[str, Any]:
    value = row.get("extracted_data_json")
    if isinstance(value, str | bytes):
        try:
            value = json.loads(value)
        except (ValueError, UnicodeDecodeError):
            return {}
    return value if isinstance(value, dict) else {}


def validate_failed_attempt_row(row: Mapping[str, Any]) -> bool:
    """Reject malformed markers instead of admitting an unprotected attempt."""
    payload = _payload(row)
    if "failed_attempt" not in payload:
        return False
    marker = payload["failed_attempt"]
    if (
        not isinstance(marker, dict)
        or marker.get("schema_version") != 1
        or marker.get("ledger_entry_id") != row.get("id")
        or not isinstance(marker.get("receipts"), list)
        or not marker["receipts"]
        or row.get("success") not in (False, 0)
    ):
        raise ValueError("Invalid failed-attempt ledger marker")
    return True


def verify_failed_attempt_replay(existing: Mapping[str, Any], incoming: Mapping[str, Any]) -> bool:
    """Keep the first durable valuation; refuse changed or removed receipt identity.

    True means the original row must be retained without any update. Ordinary
    ledger rows preserve their existing upsert behavior when neither is marked.
    """
    old_payload, new_payload = _payload(existing), _payload(incoming)
    if "failed_attempt" not in old_payload and "failed_attempt" not in new_payload:
        return False
    if not validate_failed_attempt_row(existing) or not validate_failed_attempt_row(incoming):
        raise ValueError("Cannot replace or remove a failed-attempt ledger marker")
    identity = ("id", "deployment_id", "chain", "protocol", "intent_type", "success", "tx_hash", "gas_used")
    if (
        any(existing.get(key) != incoming.get(key) for key in identity)
        or old_payload["failed_attempt"] != new_payload["failed_attempt"]
        or old_payload.get("compiler_evidence") != new_payload.get("compiler_evidence")
        or old_payload.get("execution_intent_id") != new_payload.get("execution_intent_id")
    ):
        raise ValueError("Conflicting immutable failed-attempt ledger evidence")
    return True
