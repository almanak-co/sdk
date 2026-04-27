"""Deterministic UUIDv5 event ID generation for accounting events.

UUIDv5 gives:
- UUID-format output — passes gateway UUID validation at SaveAccountingEvent
- Idempotent writes — same inputs always produce the same ID, safe for ON CONFLICT
- No collision across deployments — deployment_id is part of the key

Namespace: UUID namespace DNS (RFC 4122 §4.3) — a stable, public namespace
that does not require a private secret.
"""

from __future__ import annotations

import json
import uuid

_NS = uuid.NAMESPACE_DNS


def make_accounting_event_id(
    deployment_id: str,
    cycle_id: str,
    intent_type: str,
    tx_hash: str,
    position_key: str = "",
) -> str:
    """Return a deterministic UUIDv5 string for an accounting event.

    Inputs must uniquely identify the event: same (deployment, cycle, type, tx, position)
    always returns the same UUID so retried writes are idempotent ON CONFLICT.

    tx_hash is normalised to lowercase to preserve idempotency across mixed-case sources.
    JSON serialisation avoids separator collisions when inputs contain ``:``.
    """
    key = json.dumps(
        [deployment_id, cycle_id, intent_type, tx_hash.lower(), position_key],
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return str(uuid.uuid5(_NS, key))
