"""Kamino protocol capabilities for intent validation."""

from __future__ import annotations

from typing import Any

# Kamino lending on Solana: no rate-mode selection, no collateral toggle.
PROTOCOL_CAPABILITIES: dict[str, dict[str, Any]] = {
    "kamino": {
        "supports_interest_rate_mode": False,
        "supports_collateral_toggle": False,
        "operations": ["supply", "withdraw", "borrow", "repay"],
    },
}
