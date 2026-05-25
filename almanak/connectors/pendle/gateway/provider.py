"""Gateway-side connector binding for Pendle (VIB-4810).

Declares the ``GatewayMarketLookupCapability`` so the gateway boot loop
can discover the Pendle PT / YT / LP token metadata lookup without
hand-wiring an import in :mod:`almanak.gateway.services.token_service`.

Phase 1+2 — the capability is declared but ``token_service`` continues
to call ``get_pendle_lookup`` directly. Phase 4 collapses the
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

from .market_lookup import get_pendle_lookup


class PendleGatewayConnector(GatewayConnector, GatewayMarketLookupCapability):
    """Gateway-side connector for Pendle."""

    protocol: ClassVar[ProtocolName] = ProtocolName("pendle")
    kind: ClassVar[ProtocolKind] = ProtocolKind.YIELD_TRADING

    def market_lookup(self):
        """Return the awaitable Pendle market-lookup singleton factory."""
        return get_pendle_lookup


__all__ = ["PendleGatewayConnector"]
