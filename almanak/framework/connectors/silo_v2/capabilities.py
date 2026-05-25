"""Silo V2 protocol capabilities for intent validation."""

from __future__ import annotations

from typing import Any

# Silo V2 uses isolated (token0, token1) pairs; every deposit is collateral so
# the toggle is unavailable.
PROTOCOL_CAPABILITIES: dict[str, dict[str, Any]] = {
    "silo_v2": {
        "supports_interest_rate_mode": False,
        "supports_collateral_toggle": False,
        "operations": ["supply", "withdraw", "borrow", "repay"],
    },
}
