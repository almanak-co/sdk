"""MetaMorpho ERC-4626 vault capabilities for intent validation.

The ``metamorpho`` protocol name covers Morpho's curator-managed ERC-4626
vaults (deposit / redeem only — re-allocation is curator-side). The connector
directory is ``morpho_vault`` but strategies and validators reference the
on-chain product name ``metamorpho``.
"""

from __future__ import annotations

from typing import Any

PROTOCOL_CAPABILITIES: dict[str, dict[str, Any]] = {
    "metamorpho": {
        "operations": ["vault_deposit", "vault_redeem"],
        "supports_erc4626": True,
    },
}
