"""Canonical connector self-registration interface."""

from __future__ import annotations

from almanak.connectors._connector_descriptor import (
    CONNECTOR_DESCRIPTOR_REGISTRY,
    CONNECTOR_REGISTRY,
    Connector,
    ConnectorDescriptor,
    ConnectorDescriptorRegistry,
    ConnectorDiscoveryError,
    ConnectorRegistry,
    ImportRef,
)

__all__ = [
    "CONNECTOR_DESCRIPTOR_REGISTRY",
    "CONNECTOR_REGISTRY",
    "Connector",
    "ConnectorDescriptor",
    "ConnectorDescriptorRegistry",
    "ConnectorDiscoveryError",
    "ConnectorRegistry",
    "ImportRef",
]
