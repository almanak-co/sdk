"""Strategy-side vault-tool capability registry (VIB-4860 / W8).

Sibling of :mod:`almanak.connectors._strategy_base.agent_read_registry`,
scoped to the genuinely *protocol-named* agent tools — the Lagoon vault
lifecycle tools (``deploy_vault`` / ``settle_vault`` / ``get_vault_state``
/ ``approve_vault_underlying`` / ``deposit_vault`` / ``teardown_vault``).

Why a separate registry from agent-read
=======================================

Unlike the generic read verbs (``get_pool_state`` takes a ``protocol``
parameter and works for every CL DEX), the vault tools are specific to a
vault protocol: they construct a ``LagoonVaultSDK`` / ``LagoonVaultDeployer``
/ ``LagoonVaultAdapter`` and drive a crash-recovery state machine. Prior to
W8 the executor imported ``almanak.connectors.lagoon.{sdk,deployer,adapter}``
at 8 sites inside those handlers.

W8 routes the SDK/deployer/adapter *construction* through this registry so
the executor no longer imports the connector. The handlers resolve the
capability once at the top of the handler — 1:1 with the previous
``LagoonVaultSDK(...)`` construction site — and the crash-recovery ordering
of the teardown / settlement state machine is preserved byte-for-byte
(plan §8: "Keep the SDK handle resolution at the top of each handler; do
not restructure the state machine").

PolicyEngine boundary (AGENTS.md mandate)
=========================================

The factory methods are **construction-only and pure**: they return SDK /
deployer / adapter *handles* and the ``VaultDeployParams`` type. They MUST
NOT call the gateway, sign, or touch ``PolicyEngine``. The executor still
owns the gateway round-trip (it passes its own ``gateway_client`` into the
factory) and the policy gate (which runs in ``_execute_inner`` before any
vault handler). The anti-bypass static guard
(``tests/static/test_agent_tools_policy_anti_bypass.py``) enforces this.

What lives here
===============

* :class:`VaultToolCapability` — a ``@runtime_checkable`` Protocol a vault
  connector declares. Construction factories (the executor supplies its
  own ``gateway_client``):

    - ``vault_tool_keys() -> frozenset[str]`` — the vault tool names the
      connector backs (metadata, for diagnostics / completeness checks).
    - ``build_sdk(gateway_client, chain)`` — the vault SDK handle.
    - ``build_deployer(gateway_client)`` — the vault deployer handle.
    - ``build_adapter(sdk)`` — the vault adapter handle wrapping an SDK.
    - ``deploy_params_type()`` — the ``VaultDeployParams`` dataclass *type*
      (the executor builds an instance with the request fields).

* :class:`VaultToolConnector` — carrier base (``ProtocolName`` +
  ``ProtocolKind``), mirroring :class:`AgentReadConnector`.

* :class:`VaultToolRegistry` — same shape as
  :class:`AgentReadToolRegistry`.

* :data:`STRATEGY_VAULT_TOOL_REGISTRY` — the single in-process instance.
  Registered by :mod:`almanak.connectors._strategy_agent_tool_registry`.

Gateway-boundary note
=====================

Strategy-side. The vault SDK / deployer / adapter are themselves
strategy-side connector modules (they build ``ActionBundle``s the executor
then submits through the gateway); the *type* imports here pull no
gateway-side code. ``almanak/framework/agent_tools`` is a strategy-side
root per ``tests/static/test_strategy_import_boundary.py``.
"""

from __future__ import annotations

from typing import Any, ClassVar, Protocol, TypeVar, runtime_checkable

from almanak.connectors._base.types import ProtocolKind, ProtocolName

__all__ = [
    "STRATEGY_VAULT_TOOL_REGISTRY",
    "VaultToolCapability",
    "VaultToolConnector",
    "VaultToolRegistry",
    "VaultToolRegistryError",
]


class VaultToolRegistryError(Exception):
    """Registry contract violation (collision, unknown key, etc.)."""


@runtime_checkable
class VaultToolCapability(Protocol):
    """Connector publishes construction factories for its vault tools.

    Construction-only and pure — NO RPC, NO signing, NO gateway round-trip,
    NO PolicyEngine. The factories return *handles*; the executor owns the
    gateway client it passes in and the policy gate above dispatch.

    Contract
    --------

    * ``vault_tool_keys() -> frozenset[str]`` — the vault tool names this
      connector backs (e.g. ``frozenset({"deploy_vault", "settle_vault",
      ...})``). Non-empty.

    * ``build_sdk(gateway_client, chain) -> Any`` — return a vault SDK
      handle bound to ``gateway_client`` on ``chain``. 1:1 with the
      previous ``LagoonVaultSDK(client, chain=chain)`` site.

    * ``build_deployer(gateway_client) -> Any`` — return a vault deployer
      handle. 1:1 with ``LagoonVaultDeployer(gateway_client=client)``.

    * ``build_adapter(sdk) -> Any`` — return a vault adapter wrapping
      ``sdk``. 1:1 with ``LagoonVaultAdapter(sdk)``.

    * ``deploy_params_type() -> type`` — the deploy-params dataclass type
      (the executor builds an instance with the validated request fields).

    * ``parse_deploy_receipt(receipt) -> Any`` — parse a deployment receipt
      dict into the connector's deploy-result object (the executor reads
      ``.vault_address`` off it). 1:1 with the connector's
      ``@staticmethod`` deploy-receipt parser; surfaced here so the deploy
      handler need not import the connector class.
    """

    def vault_tool_keys(self) -> frozenset[str]: ...

    def build_sdk(self, gateway_client: Any, chain: str) -> Any: ...

    def build_deployer(self, gateway_client: Any) -> Any: ...

    def build_adapter(self, sdk: Any) -> Any: ...

    def deploy_params_type(self) -> type: ...

    def parse_deploy_receipt(self, receipt: dict[str, Any]) -> Any: ...


class VaultToolConnector:
    """Base class for strategy-side vault-tool connector instances.

    Mirrors :class:`AgentReadConnector`. Required class attributes:
    ``protocol`` (``ProtocolName``, registry key) and ``kind``
    (``ProtocolKind``).
    """

    protocol: ClassVar[ProtocolName]
    kind: ClassVar[ProtocolKind]


T = TypeVar("T")


class VaultToolRegistry:
    """In-process registry of strategy-side vault-tool connectors.

    Same shape as :class:`AgentReadToolRegistry`. ``register`` rejects
    classes / missing-capability / empty-keys; ``lookup`` resolves a
    canonical ``protocol`` to its :class:`VaultToolCapability`.
    """

    def __init__(self) -> None:
        self._connectors: dict[ProtocolName, VaultToolConnector] = {}

    def register(self, connector: VaultToolConnector) -> None:
        """Register a connector instance. Collision on protocol raises."""
        if not isinstance(connector, VaultToolConnector):
            raise VaultToolRegistryError(
                "register() expects a VaultToolConnector instance, got "
                f"{type(connector).__qualname__!s} ({connector!r}); did you "
                "forget to instantiate the class?"
            )
        if not isinstance(connector, VaultToolCapability):
            raise VaultToolRegistryError(
                "register() expects a connector implementing VaultToolCapability "
                f"in addition to VaultToolConnector; {type(connector).__qualname__!s} "
                "is missing the mixin / required methods."
            )
        keys = connector.vault_tool_keys()
        if not isinstance(keys, frozenset) or not keys:
            raise VaultToolRegistryError(
                f"{type(connector).__qualname__}.vault_tool_keys() must return a non-empty frozenset, got {keys!r}"
            )
        for key in keys:
            if not isinstance(key, str) or not key:
                raise VaultToolRegistryError(
                    f"{type(connector).__qualname__}.vault_tool_keys() returned an "
                    f"invalid key {key!r} (must be a non-empty str)"
                )
        proto = connector.protocol
        existing = self._connectors.get(proto)
        if existing is not None:
            raise VaultToolRegistryError(
                f"protocol {proto!r} already registered by "
                f"{type(existing).__qualname__}; refusing to overwrite with "
                f"{type(connector).__qualname__}"
            )
        self._connectors[proto] = connector

    def get(self, protocol: ProtocolName) -> VaultToolConnector | None:
        """Return the connector registered under ``protocol`` (or ``None``)."""
        return self._connectors.get(protocol)

    def lookup(self, protocol: str) -> VaultToolCapability | None:
        """Return the :class:`VaultToolCapability` for ``protocol``, or ``None``."""
        connector = self._connectors.get(ProtocolName(protocol))
        if connector is None:
            return None
        if not isinstance(connector, VaultToolCapability):
            return None
        return connector

    def all(self) -> tuple[VaultToolConnector, ...]:
        """Return every registered connector in registration order."""
        return tuple(self._connectors.values())

    def protocols(self) -> frozenset[ProtocolName]:
        """Return every registered protocol name."""
        return frozenset(self._connectors)

    def with_capability(self, capability: type[T]) -> tuple[T, ...]:
        """Return every registered connector implementing ``capability``."""
        return tuple(c for c in self._connectors.values() if isinstance(c, capability))

    def clear(self) -> None:
        """Test helper — clear registrations. NOT used in production paths."""
        self._connectors.clear()


STRATEGY_VAULT_TOOL_REGISTRY: VaultToolRegistry = VaultToolRegistry()
