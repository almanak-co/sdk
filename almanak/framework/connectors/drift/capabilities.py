"""Drift protocol capabilities for intent validation."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

PROTOCOL_CAPABILITIES: dict[str, dict[str, Any]] = {
    "drift": {
        "supports_leverage": True,
        "max_leverage": Decimal("20"),
        "min_leverage": Decimal("1"),
        "operations": ["perp_open", "perp_close"],
    },
}
