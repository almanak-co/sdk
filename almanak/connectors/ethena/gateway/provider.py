"""Gateway-side connector binding for Ethena (VIB-4811).

Phase 3 scaffolding — Ethena does not yet have a full strategy-side
connector under ``almanak/connectors/ethena/``. This scaffold exists so
the protocol can publish its CoinGecko slugs through
``GatewayPriceIdCapability``.

Contributes:

* ``GatewayPriceIdCapability`` — ``USDE`` (Ethena USD synthetic dollar)
  and ``SUSDE`` (staked USDe). Moved verbatim from the per-chain
  ``*_TOKEN_IDS`` dicts in ``almanak.gateway.data.price.coingecko``.
"""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayPriceIdCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName


class EthenaGatewayConnector(GatewayConnector, GatewayPriceIdCapability):
    """Gateway-side connector for Ethena."""

    protocol: ClassVar[ProtocolName] = ProtocolName("ethena")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def coingecko_ids(self) -> dict[str, str]:
        """CoinGecko slugs for Ethena USDe and staked-USDe."""
        return {
            "USDE": "ethena-usde",
            "SUSDE": "ethena-staked-usde",
        }

    def dexscreener_ids(self) -> dict[str, dict[str, str]]:
        """USDe / sUSDe are EVM-only tokens resolved via ``TokenResolver``."""
        return {}


__all__ = ["EthenaGatewayConnector"]
