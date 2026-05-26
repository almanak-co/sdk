"""Radiant V2 protocol capabilities for intent validation."""

from __future__ import annotations

from typing import Any

# Radiant V2 is an Aave V2 fork; stable rate is deprecated, only ``variable``
# remains.
PROTOCOL_CAPABILITIES: dict[str, dict[str, Any]] = {
    "radiant_v2": {
        "supports_interest_rate_mode": True,
        "interest_rate_modes": ["variable"],
        "supports_collateral_toggle": True,
        "operations": ["supply", "withdraw", "borrow", "repay"],
    },
}
