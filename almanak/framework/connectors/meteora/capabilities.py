"""Meteora DLMM protocol capabilities for intent validation."""

from __future__ import annotations

from typing import Any

PROTOCOL_CAPABILITIES: dict[str, dict[str, Any]] = {
    "meteora_dlmm": {
        "type": "dlmm",
        "operations": ["lp_open", "lp_close"],
    },
}
