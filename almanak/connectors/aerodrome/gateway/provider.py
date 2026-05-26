"""Gateway-side connector binding for Aerodrome.

Phase 3 (VIB-4811) introduces capability-keyed dispatch at the gateway
boundary. Aerodrome contributes:

* ``GatewayPoolHistoryCapability`` — pool history is supported on Base
  only. Previously this was the single ``("base", "aerodrome")`` entry
  in ``almanak.gateway.services.pool_history_service.SUPPORTED_POOL_PAIRS``.
* ``GatewayDefillamaSlugCapability`` — DefiLlama project slug
  (``"aerodrome-v2"`` plus the ``"aerodrome_slipstream"`` alias).
* ``GatewayPriceIdCapability`` — AERO CoinGecko slug
  (``aerodrome-finance``).
"""

from __future__ import annotations

from typing import ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayDefillamaSlugCapability,
    GatewayPoolHistoryCapability,
    GatewayPriceIdCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName


class AerodromeGatewayConnector(
    GatewayConnector,
    GatewayPoolHistoryCapability,
    GatewayDefillamaSlugCapability,
    GatewayPriceIdCapability,
):
    """Gateway-side connector for Aerodrome."""

    protocol: ClassVar[ProtocolName] = ProtocolName("aerodrome")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def pool_history_supported_chains(self) -> frozenset[str]:
        """Aerodrome lives on Base only."""
        return frozenset({"base"})

    def defillama_slug(self) -> str | None:
        """DefiLlama project slug for Aerodrome V2."""
        return "aerodrome-v2"

    def defillama_slug_aliases(self) -> dict[str, str]:
        """Aerodrome's Slipstream concentrated-liquidity product rides the
        same connector but DefiLlama tracks it as a separate project.

        Returning the alias here keeps the dispatcher single-pass: the
        pool-analytics matcher resolves ``protocol="aerodrome_slipstream"``
        to ``"aerodrome-slipstream"`` without a second dispatch table.
        """
        return {"aerodrome_slipstream": "aerodrome-slipstream"}

    def coingecko_ids(self) -> dict[str, str]:
        """CoinGecko slug for the Aerodrome governance token."""
        return {"AERO": "aerodrome-finance"}

    def dexscreener_ids(self) -> dict[str, dict[str, str]]:
        """AERO is an EVM-only token resolved via ``TokenResolver``."""
        return {}


__all__ = ["AerodromeGatewayConnector"]
