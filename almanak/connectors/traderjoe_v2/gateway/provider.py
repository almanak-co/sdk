"""Gateway-side connector binding for Trader Joe V2 (VIB-4811).

Phase 3 scaffolding — Trader Joe V2 has strategy-side intent code
under ``almanak/connectors/traderjoe_v2/`` (pre-migration
layout) but no gateway-side connector class. This scaffold exists so
the protocol can publish its CoinGecko slug through
``GatewayPriceIdCapability``.

Contributes:

* ``GatewayPriceIdCapability`` — ``JOE`` (governance token). Moved
  verbatim from the per-chain ``*_TOKEN_IDS`` dicts in
  ``almanak.gateway.data.price.coingecko`` (present on both Arbitrum
  and Avalanche).
"""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayPriceIdCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName


class TraderJoeV2GatewayConnector(GatewayConnector, GatewayPriceIdCapability):
    """Gateway-side connector for Trader Joe V2."""

    protocol: ClassVar[ProtocolName] = ProtocolName("traderjoe_v2")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def coingecko_ids(self) -> dict[str, str]:
        """CoinGecko slug for the Trader Joe governance token."""
        return {"JOE": "trader-joe"}

    def dexscreener_ids(self) -> dict[str, dict[str, str]]:
        """JOE is an EVM-only token resolved via ``TokenResolver``."""
        return {}


__all__ = ["TraderJoeV2GatewayConnector"]
