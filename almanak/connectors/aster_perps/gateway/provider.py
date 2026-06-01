"""Gateway-side connector binding for Aster Perps (VIB-4853 / W1).

Minimal Phase-3-scaffold-style binding so Aster Perps (which also
fronts PancakeSwap Perps via the broker id = 2 attribution) can publish
its on-chain contract addresses through :class:`GatewayAddressCapability`
without forcing every consumer to import the connector by name. The
strategy-side connector code (adapter, compiler, SDK, receipt parser)
still lives under ``almanak/connectors/aster_perps/``; this module
contributes the gateway-side address surface only.

Contributes:

* ``GatewayAddressCapability`` — per-chain Aster Perps Diamond router
  address on BSC, moved verbatim from the entries previously held in
  ``almanak.core.contracts``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayAddressCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName

from ..addresses import ASTER_PERPS


class AsterPerpsGatewayConnector(GatewayConnector, GatewayAddressCapability):
    """Gateway-side connector for Aster Perps (BSC)."""

    protocol: ClassVar[ProtocolName] = ProtocolName("aster_perps")
    kind: ClassVar[ProtocolKind] = ProtocolKind.PERP

    def addresses_for(self, chain: str) -> Mapping[str, str]:
        """Return the Aster Perps contract addresses for ``chain`` (or empty)."""
        return ASTER_PERPS.get(chain, {})

    def address_supported_chains(self) -> frozenset[str]:
        """Chains for which Aster Perps addresses are registered."""
        return frozenset(ASTER_PERPS.keys())


__all__ = ["AsterPerpsGatewayConnector"]
