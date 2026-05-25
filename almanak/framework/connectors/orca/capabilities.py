"""Orca Whirlpools protocol capabilities for intent validation."""

from __future__ import annotations

from typing import Any

PROTOCOL_CAPABILITIES: dict[str, dict[str, Any]] = {
    "orca_whirlpools": {
        "type": "clmm",
        "operations": ["lp_open", "lp_close"],
    },
}
