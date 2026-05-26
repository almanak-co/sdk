"""Spark protocol capabilities for intent validation."""

from __future__ import annotations

from typing import Any

# Spark is an Aave V3 fork; same capability shape. Stable rate is deprecated
# on Spark (most assets disabled), only ``variable`` is current.
PROTOCOL_CAPABILITIES: dict[str, dict[str, Any]] = {
    "spark": {
        "supports_interest_rate_mode": True,
        "interest_rate_modes": ["variable"],
        "supports_collateral_toggle": True,
        "operations": ["supply", "withdraw", "borrow", "repay"],
    },
}
