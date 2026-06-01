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

W7-followup (VIB-4870) adds:

* ``GatewayDexVolumeCapability`` — daily trading-volume history via the
  Aerodrome ``pairDayDatas`` (Solidly-style) subgraph. Migrates
  ``framework/backtesting/pnl/providers/dex/aerodrome_volume.py``. Egress
  now runs on the ``RateHistoryService`` servicer's shared HTTP session.
  Aerodrome does not expose Uniswap-style ``observe()`` TWAP (it's a
  Solidly fork), so it implements volume only — not
  ``GatewayDexTwapCapability``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayAddressCapability,
    GatewayDefillamaSlugCapability,
    GatewayDexVolumeCapability,
    GatewayPoolHistoryCapability,
    GatewayPriceIdCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName

from ..addresses import AERODROME

# W7-followup / VIB-4870 — Aerodrome daily-volume subgraph IDs.
# Migrated verbatim from
# ``framework/backtesting/pnl/providers/dex/aerodrome_volume.py``.
# Aerodrome is a Solidly fork (Base-native) → ``pairDayDatas`` schema.
_AERODROME_VOLUME_SUBGRAPH_IDS: dict[str, str] = {
    "base": "GENunSHWLBXm59mBSgPzQ8metBEp9YDfdqwFr91Av1UM",
}


class AerodromeGatewayConnector(
    GatewayConnector,
    GatewayAddressCapability,
    GatewayPoolHistoryCapability,
    GatewayDefillamaSlugCapability,
    GatewayPriceIdCapability,
    GatewayDexVolumeCapability,
):
    """Gateway-side connector for Aerodrome."""

    protocol: ClassVar[ProtocolName] = ProtocolName("aerodrome")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def addresses_for(self, chain: str) -> Mapping[str, str]:
        """Return the Aerodrome contract addresses for ``chain`` (or empty)."""
        return AERODROME.get(chain, {})

    def address_supported_chains(self) -> frozenset[str]:
        """Chains for which Aerodrome addresses are registered."""
        return frozenset(AERODROME.keys())

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

    # ---------------------------------------------------------------------
    # GatewayDexVolumeCapability (VIB-4870 / W7-followup)
    # ---------------------------------------------------------------------

    def dex_name(self) -> str:
        """DEX identifier — matches the legacy ``"aerodrome"`` provider key."""
        return "aerodrome"

    def volume_supported_chains(self) -> frozenset[str]:
        """Chains with a registered Aerodrome volume subgraph (Base only)."""
        return frozenset(_AERODROME_VOLUME_SUBGRAPH_IDS)

    async def fetch_volume_history(
        self,
        servicer: Any,
        *,
        chain: str,
        pool_address: str,
        start_ts: int,
        end_ts: int,
        interval_secs: int,
    ) -> Any:
        """Daily trading-volume history via the Aerodrome ``pairDayDatas`` subgraph.

        Migrated from
        ``framework/backtesting/pnl/providers/dex/aerodrome_volume.py``.
        Aerodrome (Solidly fork) uses ``pairDayDatas`` filtered by
        ``pairAddress`` with the ``dailyVolumeUSD`` field — distinct from
        the Uniswap V3 ``poolDayDatas`` / ``volumeUSD`` shape.
        """
        from almanak.gateway.services._dex_volume_subgraph import (
            DexVolumeSubgraphSpec,
            fetch_dex_volume_history,
        )

        return await fetch_dex_volume_history(
            servicer,
            DexVolumeSubgraphSpec(
                dex_name="aerodrome",
                subgraph_ids=dict(_AERODROME_VOLUME_SUBGRAPH_IDS),
                entity="pairDayDatas",
                id_field="pairAddress",
                volume_field="dailyVolumeUSD",
                source="aerodrome_subgraph",
            ),
            chain=chain,
            pool_address=pool_address,
            start_ts=start_ts,
            end_ts=end_ts,
            interval_secs=interval_secs,
        )


__all__ = ["AerodromeGatewayConnector"]
