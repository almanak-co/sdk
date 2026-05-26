"""Gateway-side connector binding for Benqi (VIB-4811).

Phase 3 scaffolding — Benqi does not yet have a full strategy-side
connector under ``almanak/connectors/benqi/``. This scaffold exists so
the protocol can publish its CoinGecko slug through
``GatewayPriceIdCapability``.

Contributes:

* ``GatewayPriceIdCapability`` — ``QI`` (governance token, Avalanche).
  Moved verbatim from ``AVALANCHE_TOKEN_IDS`` in
  ``almanak.gateway.data.price.coingecko``.
"""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayPriceIdCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName


class BenqiGatewayConnector(GatewayConnector, GatewayPriceIdCapability):
    """Gateway-side connector for Benqi (Avalanche lending)."""

    protocol: ClassVar[ProtocolName] = ProtocolName("benqi")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def coingecko_ids(self) -> dict[str, str]:
        """CoinGecko slug for the Benqi governance token."""
        return {"QI": "benqi"}

    def dexscreener_ids(self) -> dict[str, dict[str, str]]:
        """QI is an EVM-only token resolved via ``TokenResolver``."""
        return {}


__all__ = ["BenqiGatewayConnector"]
