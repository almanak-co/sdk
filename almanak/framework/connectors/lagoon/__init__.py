"""Lagoon Vault Connector.

This module provides a low-level SDK and adapter for interacting with Lagoon vault
contracts (ERC-7540) through the gateway's RPC service.

Supported operations:
- Read vault state (total assets, pending deposits/redemptions, share price)
- Read storage slots (proposed total assets, silo address)
- Verify vault contract version
- Build ActionBundles for vault write operations (propose, settle)
- Deploy new Lagoon vaults via factory contracts

The SDK / adapter / deployer / receipt parser are consumed by the ``ax``
vault commands (``almanak/framework/agent_tools/executor.py``) and the
vault lifecycle code (``almanak/framework/vault/lifecycle.py``).

Strategy-layer ``VaultDepositIntent`` / ``VaultRedeemIntent`` routing is
currently blocked at Pydantic vocabulary validation (only ``metamorpho`` is
registered via ``register_vault_adapter``). The connector ships the
operator-side surface today; user-side ``requestDeposit`` / ``requestRedeem``
ActionBundle building is tracked in VIB-4307. The intent-coverage gate is
satisfied by the blocker-invariant tests in
``tests/intents/{ethereum,base}/test_lagoon_vault.py`` — those flip to a
real 4-layer on-chain test the moment the vault adapter lands in
``register_vault_adapter``.

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
# and the intent-test coverage check in scripts/ci/check_intent_coverage.py.
#
# The intent-coverage gate is satisfied today by the blocker-invariant tests
# in tests/intents/{ethereum,base}/test_lagoon_vault.py, which credit the
# four (lagoon, VAULT_DEPOSIT/VAULT_REDEEM, ethereum/base) triples by
# asserting that VaultDepositIntent / VaultRedeemIntent currently reject
# protocol="lagoon" at Pydantic validation (VIB-4307). Those assertions flip
# the moment the lagoon vault adapter lands in register_vault_adapter,
# forcing the next engineer to replace them with a real 4-layer on-chain
# test — see blueprints/05-connectors.md §Connector Registration.
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
