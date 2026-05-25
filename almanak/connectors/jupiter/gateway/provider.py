"""Gateway-side connector binding for Jupiter (VIB-4810).

Declares the ``GatewayMarketLookupCapability`` so the gateway boot loop
can discover the Jupiter SPL token metadata lookup (Solana token
registry) without hand-wiring an import in
:mod:`almanak.gateway.services.token_service`.

Phase 1+2 — the capability is declared but ``token_service`` continues
to call ``get_jupiter_lookup`` directly. Phase 4 collapses the
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

from .token_lookup import get_jupiter_lookup


class JupiterGatewayConnector(GatewayConnector, GatewayMarketLookupCapability):
    """Gateway-side connector for Jupiter."""

    protocol: ClassVar[ProtocolName] = ProtocolName("jupiter")
    kind: ClassVar[ProtocolKind] = ProtocolKind.SWAP

    def market_lookup(self):
        """Return the awaitable Jupiter token-lookup singleton factory."""
        return get_jupiter_lookup


__all__ = ["JupiterGatewayConnector"]
