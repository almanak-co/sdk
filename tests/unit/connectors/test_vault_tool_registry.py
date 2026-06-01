"""Unit tests for the strategy-side ``VaultToolRegistry`` (VIB-4860 / W8).

Mirrors ``tests/unit/connectors/test_agent_read_registry.py``. The Lagoon
production provider's factory equivalence is pinned by
``tests/unit/agent_tools/test_vault_tool_provider.py``; the vault-handler
behaviour by the existing ``test_executor_vault_*`` suites.
"""

from __future__ import annotations

from typing import Any, ClassVar

import pytest

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.vault_tool_registry import (
    VaultToolCapability,
    VaultToolConnector,
    VaultToolRegistry,
    VaultToolRegistryError,
)


class _MockParams:
    pass


class _MockVault(VaultToolConnector, VaultToolCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("mock_vault")
    kind: ClassVar[ProtocolKind] = ProtocolKind.VAULT

    def vault_tool_keys(self) -> frozenset[str]:
        return frozenset({"deploy_vault", "settle_vault"})

    def build_sdk(self, gateway_client: Any, chain: str) -> Any:
        return ("sdk", gateway_client, chain)

    def build_deployer(self, gateway_client: Any) -> Any:
        return ("deployer", gateway_client)

    def build_adapter(self, sdk: Any) -> Any:
        return ("adapter", sdk)

    def deploy_params_type(self) -> type:
        return _MockParams


def test_lookup_returns_capability_and_factories() -> None:
    registry = VaultToolRegistry()
    vault = _MockVault()
    registry.register(vault)

    cap = registry.lookup("mock_vault")
    assert cap is vault
    assert cap.build_sdk("client", "arbitrum") == ("sdk", "client", "arbitrum")
    assert cap.build_deployer("client") == ("deployer", "client")
    assert cap.build_adapter("sdk_handle") == ("adapter", "sdk_handle")
    assert cap.deploy_params_type() is _MockParams


def test_lookup_returns_none_for_unregistered_protocol() -> None:
    registry = VaultToolRegistry()
    registry.register(_MockVault())
    assert registry.lookup("__nope__") is None


def test_register_rejects_class_not_instance() -> None:
    registry = VaultToolRegistry()
    with pytest.raises(VaultToolRegistryError, match="instance, got"):
        registry.register(_MockVault)  # type: ignore[arg-type]


def test_register_rejects_connector_without_capability_mixin() -> None:
    class _MissingCapability(VaultToolConnector):
        protocol: ClassVar[ProtocolName] = ProtocolName("mock_missing_vault")
        kind: ClassVar[ProtocolKind] = ProtocolKind.VAULT

    registry = VaultToolRegistry()
    with pytest.raises(VaultToolRegistryError, match="missing the mixin"):
        registry.register(_MissingCapability())


def test_register_rejects_protocol_collision() -> None:
    registry = VaultToolRegistry()
    registry.register(_MockVault())

    class _Other(_MockVault):
        pass

    with pytest.raises(VaultToolRegistryError, match="already registered"):
        registry.register(_Other())


def test_register_rejects_empty_keys() -> None:
    class _NoKeys(VaultToolConnector, VaultToolCapability):
        protocol: ClassVar[ProtocolName] = ProtocolName("mock_no_keys_vault")
        kind: ClassVar[ProtocolKind] = ProtocolKind.VAULT

        def vault_tool_keys(self) -> frozenset[str]:
            return frozenset()

        def build_sdk(self, gateway_client: Any, chain: str) -> Any:
            return None

        def build_deployer(self, gateway_client: Any) -> Any:
            return None

        def build_adapter(self, sdk: Any) -> Any:
            return None

        def deploy_params_type(self) -> type:
            return _MockParams

    registry = VaultToolRegistry()
    with pytest.raises(VaultToolRegistryError, match="non-empty frozenset"):
        registry.register(_NoKeys())


def test_register_rejects_invalid_key_member() -> None:
    """A non-empty key set containing an invalid member (empty string / non-str)
    is rejected by the per-key validation loop — distinct from the empty-set
    branch covered by ``test_register_rejects_empty_keys`` (CodeRabbit review)."""

    class _BadKeyMember(VaultToolConnector, VaultToolCapability):
        protocol: ClassVar[ProtocolName] = ProtocolName("mock_bad_key_member_vault")
        kind: ClassVar[ProtocolKind] = ProtocolKind.VAULT

        def vault_tool_keys(self) -> frozenset[str]:
            return frozenset({"deploy_vault", ""})  # the "" member is invalid

        def build_sdk(self, gateway_client: Any, chain: str) -> Any:
            return None

        def build_deployer(self, gateway_client: Any) -> Any:
            return None

        def build_adapter(self, sdk: Any) -> Any:
            return None

        def deploy_params_type(self) -> type:
            return _MockParams

        def parse_deploy_receipt(self, receipt: dict[str, Any]) -> Any:
            return None

    registry = VaultToolRegistry()
    with pytest.raises(VaultToolRegistryError, match="invalid key"):
        registry.register(_BadKeyMember())


def test_protocols_and_with_capability() -> None:
    registry = VaultToolRegistry()
    vault = _MockVault()
    registry.register(vault)

    assert registry.protocols() == frozenset({ProtocolName("mock_vault")})
    capable = registry.with_capability(VaultToolCapability)
    assert len(capable) == 1
    assert isinstance(capable[0], _MockVault)
