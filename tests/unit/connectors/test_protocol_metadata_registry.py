"""Tests for the strategy-side protocol metadata registry."""

from __future__ import annotations

from typing import ClassVar

import pytest

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.protocol_metadata_registry import (
    MarketMintMetadata,
    ProtocolMarketMetadata,
    ProtocolMetadataCapability,
    ProtocolMetadataConnector,
    ProtocolMetadataRegistry,
    ProtocolMetadataRegistryError,
    ProtocolTokenMetadata,
)


class _MetadataConnector(ProtocolMetadataConnector, ProtocolMetadataCapability):
    protocol: ClassVar[ProtocolName] = ProtocolName("metadata")
    kind: ClassVar[ProtocolKind] = ProtocolKind.YIELD_TRADING

    def synthetic_tokens(self) -> tuple[ProtocolTokenMetadata, ...]:
        return (
            ProtocolTokenMetadata(
                protocol=str(self.protocol),
                chain="ethereum",
                symbol="PT-EXAMPLE",
                address="0xpt",
                decimals=18,
                family="PT",
            ),
        )

    def market_tokens(self) -> tuple[ProtocolMarketMetadata, ...]:
        return (
            ProtocolMarketMetadata(
                protocol=str(self.protocol),
                chain="ethereum",
                token_symbol="PT-EXAMPLE",
                market_address="0xmarket",
                family="PT",
            ),
        )

    def market_mint_tokens(self) -> tuple[MarketMintMetadata, ...]:
        return (
            MarketMintMetadata(
                protocol=str(self.protocol),
                chain="ethereum",
                market_address="0xmarket",
                mint_token_address="0xunderlying",
            ),
        )


class _NoCapabilityConnector(ProtocolMetadataConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("none")
    kind: ClassVar[ProtocolKind] = ProtocolKind.YIELD_TRADING


class _ConflictingMetadataConnector(_MetadataConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("metadata")


def test_register_rejects_classes() -> None:
    """Registry stores connector instances, not classes."""
    registry = ProtocolMetadataRegistry()

    with pytest.raises(ProtocolMetadataRegistryError, match="did you forget to instantiate"):
        registry.register(_MetadataConnector)  # type: ignore[arg-type]


def test_register_rejects_connector_without_capability() -> None:
    """A protocol-metadata connector must implement the metadata capability."""
    registry = ProtocolMetadataRegistry()

    with pytest.raises(ProtocolMetadataRegistryError, match="ProtocolMetadataCapability"):
        registry.register(_NoCapabilityConnector())


def test_register_is_idempotent_for_same_connector_type() -> None:
    """Module reloads can safely re-register the same connector type."""
    registry = ProtocolMetadataRegistry()
    registry.register(_MetadataConnector())
    registry.register(_MetadataConnector())

    assert tuple(type(connector) for connector in registry.all()) == (_MetadataConnector,)


def test_register_rejects_conflicting_protocol_implementations() -> None:
    """Protocol collisions from different connector types are hard errors."""
    registry = ProtocolMetadataRegistry()
    registry.register(_MetadataConnector())

    with pytest.raises(ProtocolMetadataRegistryError, match="already registered"):
        registry.register(_ConflictingMetadataConnector())


def test_lookup_returns_capability() -> None:
    """Lookup returns the protocol metadata capability view."""
    registry = ProtocolMetadataRegistry()
    connector = _MetadataConnector()
    registry.register(connector)

    assert registry.lookup("metadata") is connector
    assert registry.lookup("missing") is None


def test_synthetic_tokens_are_aggregated_in_registration_order() -> None:
    """Synthetic token metadata is aggregated from registered connectors."""
    registry = ProtocolMetadataRegistry()
    connector = _MetadataConnector()
    registry.register(connector)

    assert registry.synthetic_tokens() == connector.synthetic_tokens()


def test_with_capability_filters_by_protocol_class() -> None:
    """Capability filtering returns registered connectors matching the protocol."""
    registry = ProtocolMetadataRegistry()
    connector = _MetadataConnector()
    registry.register(connector)

    assert registry.with_capability(ProtocolMetadataCapability) == (connector,)
