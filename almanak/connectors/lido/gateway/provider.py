"""Gateway-side connector binding for Lido (VIB-4811).

Phase 3 scaffolding — Lido does not yet have a full strategy-side
connector under ``almanak/connectors/lido/``. This scaffold exists so
the protocol can publish its CoinGecko slugs through
``GatewayPriceIdCapability`` instead of carrying them as hardcoded
entries in ``almanak.gateway.data.price.coingecko``'s per-chain tables.

Contributes:

* ``GatewayPriceIdCapability`` — ``LDO`` (governance token), ``STETH``
  (staked ETH), and ``WSTETH`` (wrapped staked ETH). Moved verbatim
  from the per-chain ``*_TOKEN_IDS`` dicts.
"""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayPriceIdCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName


class LidoGatewayConnector(GatewayConnector, GatewayPriceIdCapability):
    """Gateway-side connector for Lido (liquid staking)."""

    protocol: ClassVar[ProtocolName] = ProtocolName("lido")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def coingecko_ids(self) -> dict[str, str]:
        """CoinGecko slugs for Lido's governance token and staked-ETH receipts."""
        return {
            "LDO": "lido-dao",
            "STETH": "lido-dao-wrapped-staked-eth",
            "WSTETH": "wrapped-steth",
        }

    def dexscreener_ids(self) -> dict[str, dict[str, str]]:
        """Lido tokens are EVM-only and resolved via ``TokenResolver``."""
        return {}


__all__ = ["LidoGatewayConnector"]
