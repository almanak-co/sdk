"""Gateway-side connector binding for PancakeSwap V3 (VIB-4811 / VIB-4817).

PancakeSwap V3 has strategy-side intent code under
``almanak/connectors/pancakeswap_v3/`` (pre-migration layout)
and a gateway-side connector class that publishes its DefiLlama slug,
CoinGecko slug, and DexScreener identifiers.

Contributes:

* ``GatewayPriceIdCapability`` — ``CAKE`` (governance token, BSC).
  Moved verbatim from ``BSC_TOKEN_IDS`` in
  ``almanak.gateway.data.price.coingecko``.
* ``GatewayDefillamaSlugCapability`` — DefiLlama project slug
  (``"pancakeswap-amm-v3"``). VIB-4817 moves this off the
  ``_PROTOCOL_TO_LLAMA_TODO_FALLBACK`` dict in
  ``almanak.gateway.services.pool_analytics_service`` onto the connector.

W7 (VIB-4859) adds:

* ``GatewayDexTwapCapability`` — PancakeSwap V3 is a Uniswap V3 fork
  that exposes the identical ``observe(secondsAgos)`` ABI on each pool.
  The body reuses the shared ``observe()`` codec helpers from the
  Uniswap V3 connector since both pool contracts encode tick cumulatives
  the same way.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayAddressCapability,
    GatewayDefillamaSlugCapability,
    GatewayDexTwapCapability,
    GatewayDexVolumeCapability,
    GatewayPriceIdCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName

from ..addresses import PANCAKESWAP_V3

# W7-followup / VIB-4870 — PancakeSwap V3 daily-volume subgraph IDs.
# Migrated verbatim from
# ``framework/backtesting/pnl/providers/dex/pancakeswap_v3_volume.py``.
# PancakeSwap V3 is a Uniswap V3 fork → identical ``poolDayDatas`` schema.
_PANCAKESWAP_V3_VOLUME_SUBGRAPH_IDS: dict[str, str] = {
    "ethereum": "CJYGNhb7RvnhfBDjqpRnD3oxgyhibzc7fkAMa38YV3oS",
    "arbitrum": "251MHFNN1rwjErXD2efWMpNS73SANZN8Ua192zw6iXve",
    "bsc": "Hv1GncLY5docZoGtXjo4kwbTvxm3MAhVZqBZE4sUT9eZ",
    "base": "BHWNsedAHtmTCzXxCCDfhPmm6iN9rxUhoRHdHKyujic3",
}


class PancakeSwapV3GatewayConnector(
    GatewayConnector,
    GatewayAddressCapability,
    GatewayPriceIdCapability,
    GatewayDefillamaSlugCapability,
    GatewayDexTwapCapability,
    GatewayDexVolumeCapability,
):
    """Gateway-side connector for PancakeSwap V3."""

    protocol: ClassVar[ProtocolName] = ProtocolName("pancakeswap_v3")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def addresses_for(self, chain: str) -> Mapping[str, str]:
        """Return the PancakeSwap V3 contract addresses for ``chain`` (or empty)."""
        return PANCAKESWAP_V3.get(chain, {})

    def address_supported_chains(self) -> frozenset[str]:
        """Chains for which PancakeSwap V3 addresses are registered."""
        return frozenset(PANCAKESWAP_V3.keys())

    def coingecko_ids(self) -> dict[str, str]:
        """CoinGecko slug for the PancakeSwap governance token."""
        return {"CAKE": "pancakeswap-token"}

    def dexscreener_ids(self) -> dict[str, dict[str, str]]:
        """CAKE is an EVM-only token resolved via ``TokenResolver``."""
        return {}

    def defillama_slug(self) -> str | None:
        """DefiLlama project slug for PancakeSwap V3."""
        return "pancakeswap-amm-v3"

    def defillama_slug_aliases(self) -> dict[str, str]:
        return {}

    # ---------------------------------------------------------------------
    # GatewayDexTwapCapability (VIB-4859 / W7)
    # ---------------------------------------------------------------------

    def dex_name(self) -> str:
        """DEX identifier (matches the legacy ``Dex.PANCAKESWAP_V3`` string)."""
        return "pancakeswap_v3"

    def twap_supported_chains(self) -> frozenset[str]:
        """Chains where PancakeSwap V3 ``observe()`` is queryable.

        Equal to the chains the connector ships addresses for — wherever
        we have a PCS V3 deployment, the pool exposes ``observe`` (it's
        the standard V3 pool ABI inherited from Uniswap V3).
        """
        return frozenset(PANCAKESWAP_V3.keys())

    async def fetch_twap(
        self,
        servicer: Any,
        *,
        chain: str,
        pool_address: str,
        secs_ago_start: int,
        secs_ago_end: int,
        as_of_block: int | None = None,
    ) -> Any:
        """Fetch a single TWAP observation via ``observe(secondsAgos)``.

        PancakeSwap V3 is a Uniswap V3 fork — the pool ABI is identical
        for ``observe`` / ``slot0`` / ``token0`` / ``token1`` / ``decimals``.
        We reuse the shared TWAP-observation helper from the gateway-side
        connector foundation rather than duplicating the request/decode pipeline.
        """
        from almanak.connectors._base.v3_gateway_twap import fetch_v3_twap_observation

        return await fetch_v3_twap_observation(
            servicer,
            chain=chain,
            pool_address=pool_address,
            secs_ago_start=secs_ago_start,
            secs_ago_end=secs_ago_end,
            as_of_block=as_of_block,
            protocol="pancakeswap_v3",
        )

    async def fetch_twap_series(
        self,
        servicer: Any,
        *,
        chain: str,
        pool_address: str,
        start_ts: int,
        end_ts: int,
        interval_secs: int,
    ) -> Any:
        """TWAP series — block-by-block bisect fan-out tracked in VIB-4870."""
        from almanak.gateway.services.rate_history_service import RateHistoryUnavailable

        raise RateHistoryUnavailable(
            "pancakeswap_v3",
            "TWAP series surface tracked in VIB-4870 (deferred protocols ticket)",
        )

    # ---------------------------------------------------------------------
    # GatewayDexVolumeCapability (VIB-4870 / W7-followup)
    # ---------------------------------------------------------------------

    def volume_supported_chains(self) -> frozenset[str]:
        """Chains with a registered PancakeSwap V3 volume subgraph."""
        return frozenset(_PANCAKESWAP_V3_VOLUME_SUBGRAPH_IDS)

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
        """Daily trading-volume history via the V3 ``poolDayDatas`` subgraph.

        Migrated from
        ``framework/backtesting/pnl/providers/dex/pancakeswap_v3_volume.py``.
        """
        from almanak.gateway.services._dex_volume_subgraph import (
            DexVolumeSubgraphSpec,
            fetch_dex_volume_history,
        )

        return await fetch_dex_volume_history(
            servicer,
            DexVolumeSubgraphSpec(
                dex_name="pancakeswap_v3",
                subgraph_ids=dict(_PANCAKESWAP_V3_VOLUME_SUBGRAPH_IDS),
                entity="poolDayDatas",
                id_field="pool",
                volume_field="volumeUSD",
                source="pancakeswap_v3_subgraph",
            ),
            chain=chain,
            pool_address=pool_address,
            start_ts=start_ts,
            end_ts=end_ts,
            interval_secs=interval_secs,
        )


__all__ = ["PancakeSwapV3GatewayConnector"]
