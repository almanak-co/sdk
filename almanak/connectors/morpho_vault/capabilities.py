"""MetaMorpho ERC-4626 vault capabilities for intent validation.

The ``metamorpho`` protocol name covers Morpho's curator-managed ERC-4626
vaults of both generations — MetaMorpho v1 and Morpho Vault V2 — (deposit /
redeem only; re-allocation is curator-side). The connector directory is
``morpho_vault`` but strategies and validators reference the on-chain product
name ``metamorpho``.
"""

from __future__ import annotations

from typing import Any

PROTOCOL_CAPABILITIES: dict[str, dict[str, Any]] = {
    "metamorpho": {
        "operations": ["vault_deposit", "vault_redeem"],
        "supports_erc4626": True,
        # Generations the connector round-trips (deposit AND redeem/teardown),
        # detected on-chain per vault address — see ``sdk.detect_vault_version``.
        # V2 redeem is liquidity-simulated before send; the penalised
        # ``forceDeallocate`` escape hatch is not issued by the connector.
        "vault_versions": ["v1", "v2"],
    },
}
