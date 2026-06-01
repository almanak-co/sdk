"""Unit tests for the strategy-side ``GasEstimateConnectorRegistry`` (VIB-4858 / W6).

Exercises the small Protocol + registry pair in
``almanak/connectors/_strategy_base/gas_estimate_registry.py`` against
mock connectors — the byte-equivalence pin in
``tests/unit/intents/test_w6_gas_estimate_byte_equivalence.py`` is the
sibling that validates the production connectors' integers.
"""

from __future__ import annotations

from typing import ClassVar

import pytest

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.gas_estimate_registry import (
    GasEstimateCapability,
    GasEstimateConnector,
    GasEstimateConnectorRegistry,
    GasEstimateRegistryError,
)


class _MockAaveLike(GasEstimateConnector, GasEstimateCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("mock_aave")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def gas_estimate_keys(self) -> frozenset[str]:
        return frozenset({"mock_supply", "mock_borrow"})

    def gas_estimate(self, action: str, chain: str) -> int:
        return {"mock_supply": 300_000, "mock_borrow": 450_000}[action]


class _MockChainAware(GasEstimateConnector, GasEstimateCapability):
    """A connector whose estimate differs per chain."""

    protocol: ClassVar[ProtocolName] = ProtocolName("mock_chain_aware")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def gas_estimate_keys(self) -> frozenset[str]:
        return frozenset({"mock_supply_with_hooks"})

    def gas_estimate(self, action: str, chain: str) -> int:
        # Higher on arbitrum (hooks/incentives), lower elsewhere.
        return 350_000 if chain == "arbitrum" else 200_000


def test_lookup_routes_to_owning_connector() -> None:
    registry = GasEstimateConnectorRegistry()
    registry.register(_MockAaveLike())

    assert registry.lookup("mock_supply", "ethereum") == 300_000
    assert registry.lookup("mock_borrow", "arbitrum") == 450_000


def test_lookup_returns_none_for_unpublished_action() -> None:
    registry = GasEstimateConnectorRegistry()
    registry.register(_MockAaveLike())

    assert registry.lookup("__not_published__", "ethereum") is None


def test_lookup_threads_chain_through_capability() -> None:
    registry = GasEstimateConnectorRegistry()
    registry.register(_MockChainAware())

    assert registry.lookup("mock_supply_with_hooks", "arbitrum") == 350_000
    assert registry.lookup("mock_supply_with_hooks", "polygon") == 200_000


def test_register_rejects_class_not_instance() -> None:
    registry = GasEstimateConnectorRegistry()
    with pytest.raises(GasEstimateRegistryError, match="instance, got"):
        registry.register(_MockAaveLike)  # type: ignore[arg-type]


def test_register_rejects_connector_without_capability_mixin() -> None:
    """Connector inherited from GasEstimateConnector but missing the
    GasEstimateCapability mixin -> fail loud at registration."""

    class _MissingCapability(GasEstimateConnector):
        protocol: ClassVar[ProtocolName] = ProtocolName("mock_missing_cap")
        kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    registry = GasEstimateConnectorRegistry()
    with pytest.raises(GasEstimateRegistryError, match="missing the mixin"):
        registry.register(_MissingCapability())


def test_register_rejects_protocol_collision() -> None:
    registry = GasEstimateConnectorRegistry()
    registry.register(_MockAaveLike())

    class _Other(_MockAaveLike):
        pass

    with pytest.raises(GasEstimateRegistryError, match="already registered"):
        registry.register(_Other())


def test_action_collision_between_connectors_raises_at_first_lookup() -> None:
    """Two connectors claiming the same action -> hard error at lookup time."""

    class _DuplicateClaim(GasEstimateConnector, GasEstimateCapability):
        protocol: ClassVar[ProtocolName] = ProtocolName("mock_duplicate")
        kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

        def gas_estimate_keys(self) -> frozenset[str]:
            return frozenset({"mock_supply"})  # collides with _MockAaveLike

        def gas_estimate(self, action: str, chain: str) -> int:
            return 1

    registry = GasEstimateConnectorRegistry()
    registry.register(_MockAaveLike())
    registry.register(_DuplicateClaim())

    with pytest.raises(GasEstimateRegistryError, match="claimed by both"):
        registry.lookup("mock_supply", "ethereum")


def test_empty_keys_rejected() -> None:
    class _NoKeys(GasEstimateConnector, GasEstimateCapability):
        protocol: ClassVar[ProtocolName] = ProtocolName("mock_no_keys")
        kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

        def gas_estimate_keys(self) -> frozenset[str]:
            return frozenset()

        def gas_estimate(self, action: str, chain: str) -> int:
            return 1

    registry = GasEstimateConnectorRegistry()
    registry.register(_NoKeys())

    with pytest.raises(GasEstimateRegistryError, match="non-empty frozenset"):
        registry.lookup("any_action", "ethereum")


def test_register_invalidates_action_map_cache() -> None:
    """Adding a new connector after a lookup must surface its actions."""
    registry = GasEstimateConnectorRegistry()
    registry.register(_MockAaveLike())
    # Force first map build.
    assert registry.lookup("mock_supply", "ethereum") == 300_000

    # Add a new connector — its keys must be visible.
    registry.register(_MockChainAware())
    assert registry.lookup("mock_supply_with_hooks", "arbitrum") == 350_000


def test_action_owner_returns_connector_instance() -> None:
    registry = GasEstimateConnectorRegistry()
    aave_like = _MockAaveLike()
    registry.register(aave_like)

    owner = registry.action_owner("mock_supply")
    assert owner is aave_like


def test_actions_returns_full_keyset() -> None:
    registry = GasEstimateConnectorRegistry()
    registry.register(_MockAaveLike())
    registry.register(_MockChainAware())

    assert registry.actions() == frozenset(
        {"mock_supply", "mock_borrow", "mock_supply_with_hooks"}
    )


def test_with_capability_filters_by_protocol_class() -> None:
    registry = GasEstimateConnectorRegistry()
    registry.register(_MockAaveLike())

    capable = registry.with_capability(GasEstimateCapability)
    assert len(capable) == 1
    assert isinstance(capable[0], _MockAaveLike)
