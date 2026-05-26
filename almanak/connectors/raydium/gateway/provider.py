"""Gateway-side connector binding for Raydium (VIB-4811).

Phase 3 scaffolding — Raydium does not yet have a full strategy-side
connector under ``almanak/connectors/raydium/``. This scaffold exists
so the protocol can publish its CoinGecko slug + DexScreener address
through ``GatewayPriceIdCapability``.

Contributes:

* ``GatewayPriceIdCapability`` — ``RAY`` (governance token, Solana).
  Moved verbatim from ``SOLANA_TOKEN_IDS`` in
  ``almanak.gateway.data.price.coingecko`` and from
  ``_KNOWN_TOKEN_ADDRESSES["solana"]`` in
  ``almanak.gateway.data.price.dexscreener``.
"""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayPriceIdCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName


class RaydiumGatewayConnector(GatewayConnector, GatewayPriceIdCapability):
    """Gateway-side connector for Raydium."""

    protocol: ClassVar[ProtocolName] = ProtocolName("raydium")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def coingecko_ids(self) -> dict[str, str]:
        """CoinGecko slug for the Raydium governance token."""
        return {"RAY": "raydium"}

    def dexscreener_ids(self) -> dict[str, dict[str, str]]:
        """RAY on-chain address for DexScreener Solana lookups."""
        return {"solana": {"RAY": "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R"}}


__all__ = ["RaydiumGatewayConnector"]
