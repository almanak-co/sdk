"""Gateway-side address binding for the legacy Aster (ApolloX) Diamond on BSC.

Publishes the Diamond router through :class:`GatewayAddressCapability` for the
``pancakeswap_perps`` connector, the only connector still trading this venue.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayAddressCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName

from ..addresses import PANCAKESWAP_PERPS


class AsterDiamondGatewayConnector(GatewayConnector, GatewayAddressCapability):
    """Gateway-side connector for PancakeSwap Perps on the Aster Diamond (BSC)."""

    protocol: ClassVar[ProtocolName] = ProtocolName("pancakeswap_perps")
    kind: ClassVar[ProtocolKind] = ProtocolKind.PERP

    def addresses_for(self, chain: str) -> Mapping[str, str]:
        """Return the Diamond contract addresses for ``chain`` (or empty)."""
        return PANCAKESWAP_PERPS.get(chain, {})

    def address_supported_chains(self) -> frozenset[str]:
        """Chains for which Diamond addresses are registered."""
        return frozenset(PANCAKESWAP_PERPS.keys())


__all__ = ["AsterDiamondGatewayConnector"]
