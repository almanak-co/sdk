"""Benqi protocol capabilities for intent validation."""

from __future__ import annotations

from typing import Any

PROTOCOL_CAPABILITIES: dict[str, dict[str, Any]] = {
    "benqi": {
        "supports_interest_rate_mode": False,
        "supports_collateral_toggle": True,
        "operations": ["supply", "withdraw", "borrow", "repay"],
    },
}
