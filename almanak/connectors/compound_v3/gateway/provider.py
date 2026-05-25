"""Gateway-side connector binding for Compound v3 (VIB-4810).

Declares the ``GatewayMarketLookupCapability`` so the gateway boot loop
can discover the Compound v3 cToken metadata lookup without hand-wiring
an import in :mod:`almanak.gateway.services.token_service`.

Phase 1+2 — the capability is declared but ``token_service`` continues
to call ``get_compound_lookup`` directly. Phase 4 collapses the
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

from .market_lookup import get_compound_lookup


class CompoundV3GatewayConnector(GatewayConnector, GatewayMarketLookupCapability):
    """Gateway-side connector for Compound v3."""

    protocol: ClassVar[ProtocolName] = ProtocolName("compound_v3")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def market_lookup(self):
        """Return the awaitable Compound market-lookup singleton factory."""
        return get_compound_lookup


__all__ = ["CompoundV3GatewayConnector"]
