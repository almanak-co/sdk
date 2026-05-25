"""Aave V3 protocol capabilities for intent validation.

See ``almanak.framework.connectors.capabilities_registry`` and
``almanak.framework.intents.vocabulary.PROTOCOL_CAPABILITIES`` for the
framework-side aggregator that exposes these to validators.
"""

from __future__ import annotations

from typing import Any

# Lending: pooled (Aave V3). ``interest_rate_modes`` lists rate modes the
# connector still accepts; stable rate is deprecated on Aave V3 (most assets
# disabled) so only ``variable`` is current.
PROTOCOL_CAPABILITIES: dict[str, dict[str, Any]] = {
    "aave_v3": {
        "supports_interest_rate_mode": True,
        "interest_rate_modes": ["variable"],
        "supports_collateral_toggle": True,
        "operations": ["supply", "withdraw", "borrow", "repay"],
    },
}
