"""Lagoon Vault Connector.

This module provides a low-level SDK and adapter for interacting with Lagoon vault
contracts (ERC-7540) through the gateway's RPC service.

Supported operations:
- Read vault state (total assets, pending deposits/redemptions, share price)
- Read storage slots (proposed total assets, silo address)
- Verify vault contract version
- Build ActionBundles for vault write operations (propose, settle)
- Deploy new Lagoon vaults via factory contracts

Example:
    from almanak.framework.connectors.lagoon import LagoonVaultSDK, LagoonVaultAdapter

    sdk = LagoonVaultSDK(gateway_client, chain="ethereum")
    adapter = LagoonVaultAdapter(sdk)

    from almanak.framework.connectors.lagoon import LagoonVaultDeployer, VaultDeployParams

    deployer = LagoonVaultDeployer()
"""

from .adapter import LagoonVaultAdapter
from .deployer import LagoonVaultDeployer, VaultDeployParams, VaultDeployResult
from .receipt_parser import LagoonReceiptParser
from .sdk import (
    LagoonVaultSDK,
)

__all__ = [
    "LagoonReceiptParser",
    "LagoonVaultAdapter",
    "LagoonVaultDeployer",
    "LagoonVaultSDK",
    "VaultDeployParams",
    "VaultDeployResult",
]

# Connector registration (VIB-4298). The registry powers the (connector,
# intent, chain) coverage gate in scripts/ci/check_connector_registry.py
# and will be consumed by PR 2's intent-test coverage check.
from almanak.framework.connectors.registry import register_connector  # noqa: E402
from almanak.framework.intents.vocabulary import IntentType  # noqa: E402

register_connector(
    name="lagoon",
    intents=(
        IntentType.VAULT_DEPOSIT,
        IntentType.VAULT_REDEEM,
    ),
    chains=(
        "ethereum",
        "base",
    ),
)
