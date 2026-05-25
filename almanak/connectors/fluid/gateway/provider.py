"""Gateway-side connector binding for Fluid (VIB-4810).

Declares the ``GatewayMarketLookupCapability`` so the gateway boot loop
can discover the Fluid fToken metadata lookup without hand-wiring an
import in :mod:`almanak.gateway.services.token_service`.

Phase 1+2 — the capability is declared but ``token_service`` continues
to call ``get_fluid_lookup`` directly. Phase 4 collapses the
per-protocol accessor methods on ``TokenService`` into a registry-driven
loop.
"""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayMarketLookupCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName

from .market_lookup import get_fluid_lookup


class FluidGatewayConnector(GatewayConnector, GatewayMarketLookupCapability):
    """Gateway-side connector for Fluid."""

    protocol: ClassVar[ProtocolName] = ProtocolName("fluid")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def market_lookup(self):
        """Return the awaitable Fluid market-lookup singleton factory."""
        return get_fluid_lookup


__all__ = ["FluidGatewayConnector"]
