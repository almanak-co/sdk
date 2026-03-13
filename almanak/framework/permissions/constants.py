"""Shared constants for the permission system."""

from __future__ import annotations

from typing import Final

# Well-known MetaMorpho vault addresses.
# Used by both synthetic_intents.py (to build synthetic VaultDeposit/Redeem)
# and morpho_vault/permission_hints.py (to build static ERC-4626 permissions).
METAMORPHO_VAULTS: Final[dict[str, dict[str, str]]] = {
    "ethereum": {
        "vault": "0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB",  # Steakhouse USDC
        "underlying": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",  # USDC
    },
    "base": {
        "vault": "0xc1256Ae5FF1cf2719D4937adb3bbCCab2E00A2Ca",  # Moonwell USDC
        "underlying": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",  # USDC
    },
}
