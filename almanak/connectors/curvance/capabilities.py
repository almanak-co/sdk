"""Curvance protocol capabilities for intent validation."""

from __future__ import annotations

from typing import Any

# Curvance v1: every supply posts as collateral (no toggle), and each
# operation routes through a per-market cToken / BorrowableCToken pair so the
# market identifier is required.
PROTOCOL_CAPABILITIES: dict[str, dict[str, Any]] = {
    "curvance": {
        "supports_interest_rate_mode": False,
        "supports_collateral_toggle": False,
        "requires_market_id": True,
        "operations": ["supply", "withdraw", "borrow", "repay"],
    },
}
