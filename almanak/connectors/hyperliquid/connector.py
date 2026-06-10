"""Hyperliquid connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)
from almanak.connectors._strategy_base.protocol_ownership import CapabilitiesSpec, SupportedChainsSpec

CONNECTOR = Connector(
    name="hyperliquid",
    kind=ProtocolKind.PERP,
    gateway_connector=ImportRef(
        module="almanak.connectors.hyperliquid.gateway.provider",
        attribute="HyperliquidGatewayConnector",
        order=15,
    ),
    compiler=ImportRef(
        module="almanak.connectors.hyperliquid.compiler",
        attribute="HyperliquidCompiler",
    ),
    capabilities=CapabilitiesSpec(
        keys=("hyperliquid",),
        module="almanak.connectors.hyperliquid.capabilities",
    ),
    supported_chains=SupportedChainsSpec(
        keys=("hyperliquid",),
        module="almanak.connectors.hyperliquid.supported_chains",
    ),
    primitive=ImportRef(
        module="almanak.connectors.hyperliquid.primitive",
        attribute="PRIMITIVE",
    ),
)

__all__ = ["CONNECTOR"]
