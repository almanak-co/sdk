"""Unit tests for the strategy-side ``AgentReadToolRegistry`` (VIB-4860 / W8).

Mirrors ``tests/unit/connectors/test_gas_estimate_registry.py`` (W6).
Exercises the Protocol + registry pair in
``almanak/connectors/_strategy_base/agent_read_registry.py`` against mock
connectors; the per-connector address/selector equivalence is pinned by
``tests/unit/agent_tools/test_agent_read_provider_*`` and the decode
byte-equivalence by ``tests/unit/agent_tools/test_read_tool_goldens.py``.
"""

from __future__ import annotations

from typing import ClassVar

import pytest

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.agent_read_registry import (
    AgentReadCapability,
    AgentReadConnector,
    AgentReadRegistryError,
    AgentReadToolRegistry,
)


class _MockUniLike(AgentReadConnector, AgentReadCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("mock_uni")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def agent_read_keys(self) -> frozenset[str]:
        return frozenset({"pool_state", "lp_position"})

    def factory_address(self, chain: str) -> str | None:
        return "0xfactory" if chain == "arbitrum" else None

    def position_manager_address(self, chain: str) -> str | None:
        return "0xnpm" if chain == "arbitrum" else None

    def get_pool_selector(self) -> str:
        return "0x1698ee82"

    def lending_pool_address(self, chain: str) -> str | None:
        return None


class _MockAaveLike(AgentReadConnector, AgentReadCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("mock_aave")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def agent_read_keys(self) -> frozenset[str]:
        return frozenset({"lending_account"})

    def factory_address(self, chain: str) -> str | None:
        return None

    def position_manager_address(self, chain: str) -> str | None:
        return None

    def get_pool_selector(self) -> str:
        return "0x1698ee82"

    def lending_pool_address(self, chain: str) -> str | None:
        return "0xpool" if chain == "arbitrum" else None


def test_lookup_returns_capability() -> None:
    registry = AgentReadToolRegistry()
    uni = _MockUniLike()
    registry.register(uni)

    cap = registry.lookup("mock_uni")
    assert cap is uni
    assert cap.factory_address("arbitrum") == "0xfactory"
    assert cap.position_manager_address("arbitrum") == "0xnpm"
    assert cap.get_pool_selector() == "0x1698ee82"


def test_lookup_returns_none_for_unregistered_protocol() -> None:
    registry = AgentReadToolRegistry()
    registry.register(_MockUniLike())
    assert registry.lookup("__not_registered__") is None


def test_get_returns_connector_or_none() -> None:
    registry = AgentReadToolRegistry()
    uni = _MockUniLike()
    registry.register(uni)
    assert registry.get(ProtocolName("mock_uni")) is uni
    assert registry.get(ProtocolName("__nope__")) is None


def test_register_rejects_class_not_instance() -> None:
    registry = AgentReadToolRegistry()
    with pytest.raises(AgentReadRegistryError, match="instance, got"):
        registry.register(_MockUniLike)  # type: ignore[arg-type]


def test_register_rejects_connector_without_capability_mixin() -> None:
    class _MissingCapability(AgentReadConnector):
        protocol: ClassVar[ProtocolName] = ProtocolName("mock_missing_cap")
        kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    registry = AgentReadToolRegistry()
    with pytest.raises(AgentReadRegistryError, match="missing the mixin"):
        registry.register(_MissingCapability())


def test_register_rejects_protocol_collision() -> None:
    registry = AgentReadToolRegistry()
    registry.register(_MockUniLike())

    class _Other(_MockUniLike):
        pass

    with pytest.raises(AgentReadRegistryError, match="already registered"):
        registry.register(_Other())


def test_register_rejects_empty_keys() -> None:
    class _NoKeys(AgentReadConnector, AgentReadCapability):
        protocol: ClassVar[ProtocolName] = ProtocolName("mock_no_keys")
        kind: ClassVar[ProtocolKind] = ProtocolKind.LP

        def agent_read_keys(self) -> frozenset[str]:
            return frozenset()

        def factory_address(self, chain: str) -> str | None:
            return None

        def position_manager_address(self, chain: str) -> str | None:
            return None

        def get_pool_selector(self) -> str:
            return "0x1698ee82"

        def lending_pool_address(self, chain: str) -> str | None:
            return None

    registry = AgentReadToolRegistry()
    with pytest.raises(AgentReadRegistryError, match="non-empty frozenset"):
        registry.register(_NoKeys())


def test_register_rejects_invalid_key() -> None:
    class _BadKey(AgentReadConnector, AgentReadCapability):
        protocol: ClassVar[ProtocolName] = ProtocolName("mock_bad_key")
        kind: ClassVar[ProtocolKind] = ProtocolKind.LP

        def agent_read_keys(self) -> frozenset[str]:
            return frozenset({""})  # empty-string key

        def factory_address(self, chain: str) -> str | None:
            return None

        def position_manager_address(self, chain: str) -> str | None:
            return None

        def get_pool_selector(self) -> str:
            return "0x1698ee82"

        def lending_pool_address(self, chain: str) -> str | None:
            return None

    registry = AgentReadToolRegistry()
    with pytest.raises(AgentReadRegistryError, match="invalid key"):
        registry.register(_BadKey())


def test_protocols_and_all() -> None:
    registry = AgentReadToolRegistry()
    uni = _MockUniLike()
    aave = _MockAaveLike()
    registry.register(uni)
    registry.register(aave)

    assert registry.protocols() == frozenset({ProtocolName("mock_uni"), ProtocolName("mock_aave")})
    assert registry.all() == (uni, aave)


def test_with_capability_filters_by_protocol_class() -> None:
    registry = AgentReadToolRegistry()
    registry.register(_MockUniLike())

    capable = registry.with_capability(AgentReadCapability)
    assert len(capable) == 1
    assert isinstance(capable[0], _MockUniLike)


def test_lending_lookup_routes_to_lending_connector() -> None:
    registry = AgentReadToolRegistry()
    registry.register(_MockAaveLike())

    cap = registry.lookup("mock_aave")
    assert cap is not None
    assert cap.lending_pool_address("arbitrum") == "0xpool"
    assert cap.lending_pool_address("polygon") is None
