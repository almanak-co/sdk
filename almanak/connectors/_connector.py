"""Canonical connector self-registration interface."""

from __future__ import annotations

from almanak.connectors._connector_descriptor import (
    CONNECTOR_DESCRIPTOR_REGISTRY,
    CONNECTOR_REGISTRY,
    CapabilitiesSpec,
    Connector,
    ConnectorDescriptor,
    ConnectorDescriptorRegistry,
    ConnectorDiscoveryError,
    ConnectorRegistry,
    DexVolumeDecl,
    FeeModelDecl,
    FundingHistoryDecl,
    ImportRef,
    LendingReadDecl,
    MetadataAmountEncoding,
    PerpsReadDecl,
    StrategyMatrixEntry,
    SupportedChainsSpec,
)

__all__ = [
    "CONNECTOR_DESCRIPTOR_REGISTRY",
    "CONNECTOR_REGISTRY",
    "CapabilitiesSpec",
    "Connector",
    "ConnectorDescriptor",
    "ConnectorDescriptorRegistry",
    "ConnectorDiscoveryError",
    "ConnectorRegistry",
    "DexVolumeDecl",
    "FeeModelDecl",
    "FundingHistoryDecl",
    "ImportRef",
    "LendingReadDecl",
    "MetadataAmountEncoding",
    "PerpsReadDecl",
    "StrategyMatrixEntry",
    "SupportedChainsSpec",
]
