"""Tests for connector-owned deferred transaction refresh."""

from __future__ import annotations

from typing import Any, ClassVar

import pytest

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.deferred_refresh_registry import (
    DeferredRefreshCapability,
    DeferredRefreshConnector,
    DeferredRefreshRegistry,
    DeferredRefreshRegistryError,
)


class _RefreshConnector(DeferredRefreshConnector, DeferredRefreshCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("refresh")
    kind: ClassVar[ProtocolKind] = ProtocolKind.SWAP

    def refresh_transaction(
        self,
        metadata: dict[str, Any],
        wallet_address: str,
        *,
        rpc_url: str | None = None,
    ) -> dict[str, Any]:
        return {
            "metadata": metadata,
            "wallet_address": wallet_address,
            "rpc_url": rpc_url,
        }


class _SecondRefreshConnector(_RefreshConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("second_refresh")


class _ConflictingRefreshConnector(_RefreshConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("refresh")


class _NoCapabilityConnector(DeferredRefreshConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("none")
    kind: ClassVar[ProtocolKind] = ProtocolKind.SWAP


def test_register_rejects_classes() -> None:
    registry = DeferredRefreshRegistry()

    with pytest.raises(DeferredRefreshRegistryError, match="did you forget to instantiate"):
        registry.register(_RefreshConnector)  # type: ignore[arg-type]


def test_register_rejects_connector_without_capability() -> None:
    registry = DeferredRefreshRegistry()

    with pytest.raises(DeferredRefreshRegistryError, match="DeferredRefreshCapability"):
        registry.register(_NoCapabilityConnector())


def test_register_is_idempotent_for_same_connector_type() -> None:
    registry = DeferredRefreshRegistry()
    registry.register(_RefreshConnector())
    registry.register(_RefreshConnector())

    assert tuple(type(connector) for connector in registry.all()) == (_RefreshConnector,)


def test_register_rejects_conflicting_protocol_implementations() -> None:
    registry = DeferredRefreshRegistry()
    registry.register(_RefreshConnector())

    with pytest.raises(DeferredRefreshRegistryError, match="already registered"):
        registry.register(_ConflictingRefreshConnector())


def test_lookup_routes_to_owning_connector() -> None:
    registry = DeferredRefreshRegistry()
    registry.register(_RefreshConnector())

    connector = registry.lookup("refresh")

    assert connector is not None
    assert connector.refresh_transaction({"route": "fresh"}, "0xWallet", rpc_url="http://localhost:8545") == {
        "metadata": {"route": "fresh"},
        "wallet_address": "0xWallet",
        "rpc_url": "http://localhost:8545",
    }


def test_lookup_returns_none_for_unknown_protocol() -> None:
    registry = DeferredRefreshRegistry()
    registry.register(_RefreshConnector())

    assert registry.lookup("__unknown__") is None


def test_refresh_transaction_rejects_unknown_protocol() -> None:
    registry = DeferredRefreshRegistry()

    with pytest.raises(DeferredRefreshRegistryError, match="does not publish deferred refresh"):
        registry.refresh_transaction("__unknown__", {}, "0xWallet")


def test_with_capability_filters_by_protocol_class() -> None:
    registry = DeferredRefreshRegistry()
    connector = _RefreshConnector()
    registry.register(connector)
    registry.register(_SecondRefreshConnector())

    capable = registry.with_capability(DeferredRefreshCapability)

    assert capable == (connector, registry.lookup("second_refresh"))
