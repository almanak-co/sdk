"""Enso aggregator protocol capabilities for intent validation."""

from __future__ import annotations

from typing import Any

PROTOCOL_CAPABILITIES: dict[str, dict[str, Any]] = {
    "enso": {
        "operations": ["swap"],
    },
}
