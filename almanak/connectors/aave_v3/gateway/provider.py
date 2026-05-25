"""Gateway-side connector binding for Aave v3.

Declares the ``GatewayMarketLookupCapability`` so the gateway boot loop
can discover the Aave v3 receipt-token (aToken / vToken) lookup without
hand-wiring an import in :mod:`almanak.gateway.services.token_service`.

Phase 1+2 (VIB-4810) — the capability is declared but ``token_service``
continues to call ``get_aave_lookup`` directly. Phase 4 collapses the
explicit per-protocol accessor methods on ``TokenService`` into a loop
over ``GATEWAY_REGISTRY.capability_providers(GatewayMarketLookupCapability)``.
"""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayMarketLookupCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName

from .market_lookup import get_aave_lookup


class AaveV3GatewayConnector(GatewayConnector, GatewayMarketLookupCapability):
    """Gateway-side connector for Aave v3."""

    protocol: ClassVar[ProtocolName] = ProtocolName("aave_v3")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def market_lookup(self):
        """Return the awaitable Aave market-lookup singleton factory.

        The underlying ``get_aave_lookup`` is a coroutine factory that
        returns a lazily-loaded singleton with disk-cache + retry
        plumbing (see ``ProtocolTokenLookup``). Phase 4 will swap this
        for an ``async`` capability contract; for Phase 1+2 the provider
        method just returns the callable so the capability registration
        is visible without coupling to the lookup's async lifecycle.
        """
        return get_aave_lookup


__all__ = ["AaveV3GatewayConnector"]
