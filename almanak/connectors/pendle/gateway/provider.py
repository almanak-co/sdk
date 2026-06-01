"""Gateway-side connector binding for Pendle (VIB-4810).

Declares the ``GatewayMarketLookupCapability`` so the gateway boot loop
can discover the Pendle PT / YT / LP token metadata lookup without
hand-wiring an import in :mod:`almanak.gateway.services.token_service`.

Phase 1+2 — the capability is declared but ``token_service`` continues
to call ``get_pendle_lookup`` directly. Phase 4 collapses the
per-protocol accessor methods on ``TokenService`` into a registry-driven
loop.

Phase 3 (VIB-4811) adds ``GatewayPriceIdCapability`` — the PENDLE
governance token's CoinGecko slug (``pendle``). Moved verbatim from
``almanak.gateway.data.price.coingecko``'s per-chain token-id tables.

W1 (VIB-4853) adds:

* ``GatewayAddressCapability`` — per-chain Pendle Router + market
  factory + dynamic ``market_*`` addresses, moved verbatim from
  ``almanak.core.contracts``. Non-connector callers (teardown
  discovery, ContractRegistry, CLI support matrix) resolve Pendle
  addresses through this capability instead of importing the dict by
  name.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayAddressCapability,
    GatewayMarketLookupCapability,
    GatewayPriceIdCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName

from ..addresses import PENDLE
from .market_lookup import get_pendle_lookup


class PendleGatewayConnector(
    GatewayConnector,
    GatewayAddressCapability,
    GatewayMarketLookupCapability,
    GatewayPriceIdCapability,
):
    """Gateway-side connector for Pendle."""

    protocol: ClassVar[ProtocolName] = ProtocolName("pendle")
    kind: ClassVar[ProtocolKind] = ProtocolKind.YIELD_TRADING

    def addresses_for(self, chain: str) -> Mapping[str, str]:
        """Return the Pendle contract addresses for ``chain`` (or empty).

        Includes the dynamic ``market_*`` / ``pt_*`` / ``yt_*`` / ``sy_*``
        entries — the strategy-side ``ContractRegistry`` scans for keys
        with the ``market_`` prefix to register per-market routers.
        """
        return PENDLE.get(chain, {})

    def address_supported_chains(self) -> frozenset[str]:
        """Chains for which Pendle addresses are registered."""
        return frozenset(PENDLE.keys())

    def market_lookup(self):
        """Return the awaitable Pendle market-lookup singleton factory."""
        return get_pendle_lookup

    def coingecko_ids(self) -> dict[str, str]:
        """CoinGecko slug for the Pendle governance token."""
        return {"PENDLE": "pendle"}

    def dexscreener_ids(self) -> dict[str, dict[str, str]]:
        """PENDLE is an EVM-only token resolved via ``TokenResolver``."""
        return {}

    # The CLI support matrix renders Pendle as a single ``yield`` row
    # (overriding the strategy-side SWAP/LP/WITHDRAW intent → category
    # derivation). The override lives on the strategy-side manifest's
    # ``matrix_entries`` field (see ``almanak/connectors/pendle/__init__.py``).


__all__ = ["PendleGatewayConnector"]
