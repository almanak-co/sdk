"""Gateway-side connector binding for Orca (VIB-4811).

Phase 3 scaffolding — Orca does not yet have a full strategy-side
connector under ``almanak/connectors/orca/``. This scaffold exists so
the protocol can publish its CoinGecko slug + DexScreener address
through ``GatewayPriceIdCapability``.

Contributes:

* ``GatewayPriceIdCapability`` — ``ORCA`` (governance token, Solana).
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


class OrcaGatewayConnector(GatewayConnector, GatewayPriceIdCapability):
    """Gateway-side connector for Orca."""

    protocol: ClassVar[ProtocolName] = ProtocolName("orca")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def coingecko_ids(self) -> dict[str, str]:
        """CoinGecko slug for the Orca governance token."""
        return {"ORCA": "orca"}

    def dexscreener_ids(self) -> dict[str, dict[str, str]]:
        """ORCA on-chain address for DexScreener Solana lookups."""
        return {"solana": {"ORCA": "orcaEKTdK7LKz57vaAYr9QeNsVEPfiu6QeMU1kektZE"}}


__all__ = ["OrcaGatewayConnector"]
