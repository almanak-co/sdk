"""Strategy-side vault-tool provider for Lagoon (VIB-4860 / W8).

Publishes the construction factories the agent-tool vault handlers
(``deploy_vault`` / ``settle_vault`` / ``get_vault_state`` /
``approve_vault_underlying`` / ``deposit_vault`` / ``teardown_vault``) need:
the ``LagoonVaultSDK`` / ``LagoonVaultDeployer`` / ``LagoonVaultAdapter``
handles and the ``VaultDeployParams`` dataclass type.

Prior to W8 the executor imported ``almanak.connectors.lagoon.{sdk,deployer,
adapter}`` at 8 sites inside those handlers. W8 routes the *construction*
through :class:`VaultToolCapability` so the executor no longer imports the
connector — it resolves the capability once at the top of each handler (1:1
with the previous ``LagoonVaultSDK(...)`` / ``LagoonVaultDeployer(...)`` /
``LagoonVaultAdapter(...)`` construction site).

Policy-gate + teardown-ordering boundary (AGENTS.md mandate)
============================================================

These factories are **construction-only and pure**: they return SDK /
deployer / adapter *handles* and the params *type*. They MUST NOT call the
gateway, sign, or touch the agent-tools policy gate. The executor still owns
the gateway client (passed into ``build_sdk`` / ``build_deployer``) and the
policy gate (which runs in ``_execute_inner`` *before* any vault handler,
and again for each sub-tool the teardown state machine re-dispatches through
``self.execute(...)``). The crash-recovery ordering of the
``_TeardownContext`` settlement/teardown state machine is untouched: W8 only
swaps *where the SDK handle comes from* (a registry lookup instead of a
local import), not *when* it is constructed or how progress is saved.

Byte-equivalence (VIB-4860)
===========================

* ``build_sdk(client, chain)`` ← ``LagoonVaultSDK(client, chain=chain)``.
* ``build_deployer(client)`` ← ``LagoonVaultDeployer(gateway_client=client)``.
  The pre-W8 ``approve_vault_underlying`` handler constructed
  ``LagoonVaultDeployer()`` with no client; passing ``client=None`` here
  reproduces that exactly (the ctor default is ``gateway_client=None``).
* ``build_adapter(sdk)`` ← ``LagoonVaultAdapter(sdk)`` (the optional
  ``token_resolver`` ctor arg keeps its default, as before).
* ``deploy_params_type()`` ← ``VaultDeployParams`` (the executor builds an
  instance with the validated request fields, as before).
* ``parse_deploy_receipt(receipt)`` ← ``LagoonVaultDeployer.parse_deploy_receipt``
  (a ``@staticmethod``) — surfaced here so the deploy handler need not name
  the connector class to parse the deployment receipt.

Gateway-boundary note
=====================

Strategy-side. The Lagoon SDK / deployer / adapter build ``ActionBundle``s
the executor then submits through the gateway; the imports here pull no
gateway-side connector code. ``almanak/framework/agent_tools`` is a
strategy-side root per ``tests/static/test_strategy_import_boundary.py``.
"""

from __future__ import annotations

from typing import Any, ClassVar

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.vault_tool_registry import (
    VaultToolCapability,
    VaultToolConnector,
)

# The vault tool names this connector backs (metadata for diagnostics /
# completeness checks). 1:1 with the action+state vault handlers in the
# executor that route through this capability.
_LAGOON_VAULT_TOOL_KEYS = frozenset(
    {
        "deploy_vault",
        "settle_vault",
        "get_vault_state",
        "approve_vault_underlying",
        "deposit_vault",
        "teardown_vault",
    }
)


class LagoonVaultToolConnector(VaultToolConnector, VaultToolCapability):
    """Construction factories for the Lagoon vault agent tools."""

    protocol: ClassVar[ProtocolName] = ProtocolName("lagoon")
    kind: ClassVar[ProtocolKind] = ProtocolKind.VAULT

    def vault_tool_keys(self) -> frozenset[str]:
        return _LAGOON_VAULT_TOOL_KEYS

    def build_sdk(self, gateway_client: Any, chain: str) -> Any:
        from almanak.connectors.lagoon.sdk import LagoonVaultSDK

        return LagoonVaultSDK(gateway_client, chain=chain)

    def build_deployer(self, gateway_client: Any) -> Any:
        from almanak.connectors.lagoon.deployer import LagoonVaultDeployer

        return LagoonVaultDeployer(gateway_client=gateway_client)

    def build_adapter(self, sdk: Any) -> Any:
        from almanak.connectors.lagoon.adapter import LagoonVaultAdapter

        return LagoonVaultAdapter(sdk)

    def deploy_params_type(self) -> type:
        from almanak.connectors.lagoon.deployer import VaultDeployParams

        return VaultDeployParams

    def parse_deploy_receipt(self, receipt: dict[str, Any]) -> Any:
        """Parse a deployment receipt → ``VaultDeployResult`` (1:1 with the
        connector's ``@staticmethod LagoonVaultDeployer.parse_deploy_receipt``).
        """
        from almanak.connectors.lagoon.deployer import LagoonVaultDeployer

        return LagoonVaultDeployer.parse_deploy_receipt(receipt)


__all__ = ["LagoonVaultToolConnector"]
