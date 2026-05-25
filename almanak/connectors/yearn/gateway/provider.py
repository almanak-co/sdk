"""Gateway-side connector binding for Yearn (VIB-4810).

Declares the ``GatewayMarketLookupCapability`` so the gateway boot loop
can discover the Yearn vault-token metadata lookup without hand-wiring
an import in :mod:`almanak.gateway.services.token_service`.

Phase 1+2 — the capability is declared but ``token_service`` continues
to call ``get_yearn_lookup`` directly. Phase 4 collapses the
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

from .vault_lookup import get_yearn_lookup


class YearnGatewayConnector(GatewayConnector, GatewayMarketLookupCapability):
    """Gateway-side connector for Yearn."""

    protocol: ClassVar[ProtocolName] = ProtocolName("yearn")
    kind: ClassVar[ProtocolKind] = ProtocolKind.VAULT

    def market_lookup(self):
        """Return the awaitable Yearn vault-lookup singleton factory."""
        return get_yearn_lookup


__all__ = ["YearnGatewayConnector"]
