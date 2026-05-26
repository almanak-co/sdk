"""Morpho Blue protocol capabilities for intent validation.

Both ``morpho`` (legacy alias) and ``morpho_blue`` (canonical) keys point at
the same capability shape — Morpho Blue is an isolated-market lending protocol
where every market identifier resolves to a single ``(loan, collateral, oracle,
irm, lltv)`` tuple. The connector accepts both keys for backward compatibility
with strategies authored before the canonical name landed.
"""

from __future__ import annotations

from typing import Any

# Define once, alias second key to the same dict so ``PROTOCOL_CAPABILITIES``
# returns the exact same value-dict for both lookups. Tests that mutate the
# dict (e.g. interest_rate_modes) observe the mutation through either key.
_MORPHO_BLUE: dict[str, Any] = {
    "supports_interest_rate_mode": False,
    "supports_collateral_toggle": True,  # supports both collateral and loan-token supply
    "requires_market_id": True,
    "operations": ["supply", "withdraw", "borrow", "repay"],
}

PROTOCOL_CAPABILITIES: dict[str, dict[str, Any]] = {
    "morpho": _MORPHO_BLUE,
    "morpho_blue": _MORPHO_BLUE,
}
