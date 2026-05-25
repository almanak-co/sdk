"""Raydium CLMM protocol capabilities for intent validation."""

from __future__ import annotations

from typing import Any

# ``raydium_clmm`` is the protocol identifier strategies pass through Intent;
# the connector directory is ``raydium`` because Raydium has historically
# shipped both AMM v4 and CLMM under the same umbrella SDK.
PROTOCOL_CAPABILITIES: dict[str, dict[str, Any]] = {
    "raydium_clmm": {
        "type": "clmm",
        "operations": ["lp_open", "lp_close"],
    },
}
