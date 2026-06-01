"""Gateway-side connector binding for SushiSwap V3 (VIB-4853 / W1).

Minimal Phase-3-scaffold-style binding so SushiSwap V3 can publish its
on-chain contract addresses through :class:`GatewayAddressCapability`
without forcing every consumer to import the connector by name.

Contributes:

* ``GatewayAddressCapability`` — per-chain SushiSwap V3 contract
  addresses (swap router, factory, position manager, quoter). Moved
  verbatim from the entries previously held in
  ``almanak.core.contracts``.

W7 (VIB-4859) adds:

* ``GatewayDexTwapCapability`` — SushiSwap V3 is a Uniswap V3 fork that
  exposes the identical ``observe(secondsAgos)`` ABI on each pool. The
  body reuses the shared ``observe()`` codec helpers from the Uniswap V3
  connector since both pool contracts encode tick cumulatives the same way.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayAddressCapability,
    GatewayDexTwapCapability,
    GatewayDexVolumeCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName

from ..addresses import SUSHISWAP_V3

# W7-followup / VIB-4870 — SushiSwap V3 daily-volume subgraph IDs.
# Migrated verbatim from
# ``framework/backtesting/pnl/providers/dex/sushiswap_v3_volume.py``.
# SushiSwap V3 is a Uniswap V3 fork → identical ``poolDayDatas`` schema.
# Only Ethereum has a registered deployment in the pre-W7 provider.
_SUSHISWAP_V3_VOLUME_SUBGRAPH_IDS: dict[str, str] = {
    "ethereum": "2tGWMrDha4164KkFAfkU3rDCtuxGb4q1emXmFdLLzJ8x",
}


class SushiSwapV3GatewayConnector(
    GatewayConnector,
    GatewayAddressCapability,
    GatewayDexTwapCapability,
    GatewayDexVolumeCapability,
):
    """Gateway-side connector for SushiSwap V3."""

    protocol: ClassVar[ProtocolName] = ProtocolName("sushiswap_v3")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def addresses_for(self, chain: str) -> Mapping[str, str]:
        """Return the SushiSwap V3 contract addresses for ``chain`` (or empty)."""
        return SUSHISWAP_V3.get(chain, {})

    def address_supported_chains(self) -> frozenset[str]:
        """Chains for which SushiSwap V3 addresses are registered."""
        return frozenset(SUSHISWAP_V3.keys())

    # ---------------------------------------------------------------------
    # GatewayDexTwapCapability (VIB-4859 / W7)
    # ---------------------------------------------------------------------

    def dex_name(self) -> str:
        """DEX identifier (matches the legacy ``Dex.SUSHISWAP_V3`` string)."""
        return "sushiswap_v3"

    def twap_supported_chains(self) -> frozenset[str]:
        """Chains where SushiSwap V3 ``observe()`` is queryable.

        Equal to the chains the connector ships addresses for — wherever
        we have a Sushi V3 deployment, the pool exposes ``observe`` (it's
        the standard V3 pool ABI inherited from Uniswap V3).
        """
        return frozenset(SUSHISWAP_V3.keys())

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

        SushiSwap V3 is a Uniswap V3 fork — the pool ABI is identical
        for ``observe`` / ``slot0`` / ``token0`` / ``token1`` / ``decimals``.
        We reuse the shared TWAP-observation helper from the Uniswap V3
        connector rather than duplicating the request/decode pipeline.
        """
        from almanak.connectors.uniswap_v3.gateway.provider import _fetch_uniswap_v3_twap_observation

        return await _fetch_uniswap_v3_twap_observation(
            servicer,
            chain=chain,
            pool_address=pool_address,
            secs_ago_start=secs_ago_start,
            secs_ago_end=secs_ago_end,
            as_of_block=as_of_block,
            protocol="sushiswap_v3",
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
            "sushiswap_v3",
            "TWAP series surface tracked in VIB-4870 (deferred protocols ticket)",
        )

    # ---------------------------------------------------------------------
    # GatewayDexVolumeCapability (VIB-4870 / W7-followup)
    # ---------------------------------------------------------------------

    def volume_supported_chains(self) -> frozenset[str]:
        """Chains with a registered SushiSwap V3 volume subgraph."""
        return frozenset(_SUSHISWAP_V3_VOLUME_SUBGRAPH_IDS)

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
        ``framework/backtesting/pnl/providers/dex/sushiswap_v3_volume.py``.
        """
        from almanak.gateway.services._dex_volume_subgraph import (
            DexVolumeSubgraphSpec,
            fetch_dex_volume_history,
        )

        return await fetch_dex_volume_history(
            servicer,
            DexVolumeSubgraphSpec(
                dex_name="sushiswap_v3",
                subgraph_ids=dict(_SUSHISWAP_V3_VOLUME_SUBGRAPH_IDS),
                entity="poolDayDatas",
                id_field="pool",
                volume_field="volumeUSD",
                source="sushiswap_v3_subgraph",
            ),
            chain=chain,
            pool_address=pool_address,
            start_ts=start_ts,
            end_ts=end_ts,
            interval_secs=interval_secs,
        )


__all__ = ["SushiSwapV3GatewayConnector"]
