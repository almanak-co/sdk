"""Gateway-side connector binding for Morpho Vault (VIB-4810).

Declares the ``GatewayMarketLookupCapability`` so the gateway boot loop
can discover the Morpho vault token metadata lookup without hand-wiring
an import in :mod:`almanak.gateway.services.token_service`.

Phase 1+2 — the capability is declared but ``token_service`` continues
to call ``get_morpho_lookup`` directly. Phase 4 collapses the
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

from .vault_lookup import get_morpho_lookup


class MorphoVaultGatewayConnector(GatewayConnector, GatewayMarketLookupCapability):
    """Gateway-side connector for Morpho Vault."""

    protocol: ClassVar[ProtocolName] = ProtocolName("morpho_vault")
    kind: ClassVar[ProtocolKind] = ProtocolKind.VAULT

    def market_lookup(self):
        """Return the awaitable Morpho vault-lookup singleton factory."""
        return get_morpho_lookup


__all__ = ["MorphoVaultGatewayConnector"]
