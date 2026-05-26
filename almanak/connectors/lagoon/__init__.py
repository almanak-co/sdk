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
    from almanak.connectors.lagoon import LagoonVaultSDK, LagoonVaultAdapter

    sdk = LagoonVaultSDK(gateway_client, chain="ethereum")
    adapter = LagoonVaultAdapter(sdk)

    from almanak.connectors.lagoon import LagoonVaultDeployer, VaultDeployParams

    deployer = LagoonVaultDeployer()
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import LagoonVaultAdapter
    from .deployer import (
        LagoonVaultDeployer,
        VaultDeployParams,
        VaultDeployResult,
    )
    from .receipt_parser import LagoonReceiptParser
    from .sdk import LagoonVaultSDK

__all__ = [
    "LagoonReceiptParser",
    "LagoonVaultAdapter",
    "LagoonVaultDeployer",
    "LagoonVaultSDK",
    "VaultDeployParams",
    "VaultDeployResult",
]

_LAZY: dict[str, tuple[str, str]] = {
    "LagoonReceiptParser": (".receipt_parser", "LagoonReceiptParser"),
    "LagoonVaultAdapter": (".adapter", "LagoonVaultAdapter"),
    "LagoonVaultDeployer": (".deployer", "LagoonVaultDeployer"),
    "LagoonVaultSDK": (".sdk", "LagoonVaultSDK"),
    "VaultDeployParams": (".deployer", "VaultDeployParams"),
    "VaultDeployResult": (".deployer", "VaultDeployResult"),
}

_registered = False


def _register_once() -> None:
    """Fire ``register_connector`` once on first strategy-side access.

    Deferred so importing the connector's gateway-side surface during
    gateway boot does not pull ``framework.intents.vocabulary`` into the
    partially-initialised config-init chain (VIB-4835).
    """
    global _registered
    if _registered:
        return
    _registered = True
    try:
        from almanak.connectors._strategy_base.registry import register_connector
        from almanak.framework.intents.vocabulary import IntentType

        register_connector(
            name="lagoon", intents=(IntentType.VAULT_DEPOSIT, IntentType.VAULT_REDEEM), chains=("ethereum", "base")
        )
    except Exception:
        _registered = False
        raise


def __getattr__(name: str) -> Any:
    """PEP 562 lazy attribute access."""
    if name not in _LAZY:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    submodule, attr = _LAZY[name]
    import importlib

    module = importlib.import_module(submodule, package=__name__)
    value = getattr(module, attr)
    globals()[name] = value
    _register_once()
    return value
