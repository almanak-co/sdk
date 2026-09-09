"""Connector-owned MetaMorpho representative vault metadata."""

from __future__ import annotations

from typing import Final

# One representative per chain — synthetic intents and rate-history resolve
# against this table. Do not replace the Base v1 row with the V2 vault.
METAMORPHO_VAULTS: Final[dict[str, dict[str, str]]] = {
    "ethereum": {
        "vault": "0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB",  # Steakhouse USDC
        "underlying": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",  # USDC
    },
    "base": {
        "vault": "0xc1256Ae5FF1cf2719D4937adb3bbCCab2E00A2Ca",  # Moonwell USDC v1
        "underlying": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",  # USDC
    },
}

# Extra vaults permission discovery must authorize in addition to the
# per-chain representative. Steakhouse Prime USDC is the Base V2 vault
# exercised by the V2 intent tests.
METAMORPHO_PERMISSION_VAULTS: Final[tuple[dict[str, str], ...]] = (
    {
        "chain": "base",
        "vault": "0xbeef0e0834849aCC03f0089F01f4F1Eeb06873C9",
        "underlying": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
    },
)

__all__ = ["METAMORPHO_PERMISSION_VAULTS", "METAMORPHO_VAULTS"]
