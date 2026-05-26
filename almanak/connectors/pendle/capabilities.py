"""Pendle protocol capabilities for intent validation."""

from __future__ import annotations

from typing import Any

PROTOCOL_CAPABILITIES: dict[str, dict[str, Any]] = {
    "pendle": {
        "operations": ["swap", "lp_open", "lp_close", "withdraw"],
        "supports_pt_yt": True,
        "supports_maturity": True,
    },
}
