"""GMX V2 protocol capabilities for intent validation."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

PROTOCOL_CAPABILITIES: dict[str, dict[str, Any]] = {
    "gmx_v2": {
        "supports_leverage": True,
        "max_leverage": Decimal("100"),
        "min_leverage": Decimal("1.1"),
        "operations": ["perp_open", "perp_close"],
    },
}
