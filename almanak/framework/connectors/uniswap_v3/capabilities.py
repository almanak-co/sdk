"""Uniswap V3 protocol capabilities for intent validation."""

from __future__ import annotations

from typing import Any

# Capability entry covers the canonical ``uniswap_v3`` protocol identifier.
# V3 forks (sushiswap_v3, pancakeswap_v3, agni_finance) share the connector's
# compiler but are not currently listed in PROTOCOL_CAPABILITIES — adding them
# would expand the public surface and is a separate decision.
PROTOCOL_CAPABILITIES: dict[str, dict[str, Any]] = {
    "uniswap_v3": {
        "operations": ["swap", "lp_open", "lp_close"],
    },
}
