"""Polymarket prediction-market capabilities for intent validation."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

# Polymarket has no compiler.py yet — prediction intents take a different path
# than swap/lp/lending. The capabilities live here regardless so the validator
# layer can pick them up via the registry the same way every other connector
# is wired.
PROTOCOL_CAPABILITIES: dict[str, dict[str, Any]] = {
    "polymarket": {
        "operations": ["prediction_buy", "prediction_sell", "prediction_redeem"],
        "min_price": Decimal("0.01"),
        "max_price": Decimal("0.99"),
        "order_types": ["market", "limit"],
        "time_in_force": ["GTC", "IOC", "FOK"],
        "collateral_token": "USDC",
    },
}
