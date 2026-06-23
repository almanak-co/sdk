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

W7-followup (VIB-4870) adds:

* ``GatewayDexVolumeCapability`` — daily trading-volume history via the
  Trader Joe V2 ``lbPairDayDatas`` (Liquidity Book) subgraph. Migrates
  ``framework/backtesting/pnl/providers/dex/traderjoe_v2_volume.py``.
  Trader Joe V2 (Liquidity Book) has no Uniswap-style ``observe()`` TWAP
  primitive, so it implements volume only — not
  ``GatewayDexTwapCapability``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayAddressCapability,
    GatewayDexVolumeCapability,
    GatewayPriceIdCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName

from ..addresses import TRADERJOE_V2

# W7-followup / VIB-4870 — Trader Joe V2 daily-volume subgraph IDs.
# Migrated verbatim from
# ``framework/backtesting/pnl/providers/dex/traderjoe_v2_volume.py``.
# Liquidity Book uses ``lbPairDayDatas`` filtered by the ``lbPair`` field.
_TRADERJOE_V2_VOLUME_SUBGRAPH_IDS: dict[str, str] = {
    "avalanche": "6KD9JYCg2qa3TxNK3tLdhj5zuZTABoLLNcnUZXKG9vuH",
}


class TraderJoeV2GatewayConnector(
    GatewayConnector,
    GatewayAddressCapability,
    GatewayPriceIdCapability,
    GatewayDexVolumeCapability,
):
    """Gateway-side connector for Trader Joe V2."""

    protocol: ClassVar[ProtocolName] = ProtocolName("traderjoe_v2")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def addresses_for(self, chain: str) -> Mapping[str, str]:
        """Return the Trader Joe V2 contract addresses for ``chain`` (or empty)."""
        return TRADERJOE_V2.get(chain, {})

    def address_supported_chains(self) -> frozenset[str]:
        """Chains for which Trader Joe V2 addresses are registered."""
        return frozenset(TRADERJOE_V2.keys())

    def coingecko_ids(self) -> dict[str, str]:
        """CoinGecko slug for the Trader Joe governance token."""
        return {"JOE": "trader-joe"}

    def dexscreener_ids(self) -> dict[str, dict[str, str]]:
        """JOE is an EVM-only token resolved via ``TokenResolver``."""
        return {}

    # ---------------------------------------------------------------------
    # GatewayDexVolumeCapability (VIB-4870 / W7-followup)
    # ---------------------------------------------------------------------

    def dex_name(self) -> str:
        """DEX identifier — matches the legacy ``"traderjoe_v2"`` provider key."""
        return "traderjoe_v2"

    def volume_supported_chains(self) -> frozenset[str]:
        """Chains with a registered Trader Joe V2 volume subgraph (Avalanche)."""
        return frozenset(_TRADERJOE_V2_VOLUME_SUBGRAPH_IDS)

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
        """Daily trading-volume history via the LB ``lbPairDayDatas`` subgraph.

        Migrated from
        ``framework/backtesting/pnl/providers/dex/traderjoe_v2_volume.py``.
        Trader Joe V2 (Liquidity Book) uses ``lbPairDayDatas`` filtered by
        the ``lbPair`` field with the standard ``volumeUSD`` daily field.
        """
        from almanak.gateway.services._dex_volume_subgraph import (
            DexVolumeSubgraphSpec,
            fetch_dex_volume_history,
        )

        return await fetch_dex_volume_history(
            servicer,
            DexVolumeSubgraphSpec(
                dex_name="traderjoe_v2",
                subgraph_ids=dict(_TRADERJOE_V2_VOLUME_SUBGRAPH_IDS),
                # The LB subgraph entity is ``lbpairDayDatas`` (lowercase "p");
                # the ``lbPairDayDatas`` camelCase spelling does not exist on it
                # and failed with a GraphQL schema error for every pair.
                entity="lbpairDayDatas",
                id_field="lbPair",
                volume_field="volumeUSD",
                source="traderjoe_v2_subgraph",
            ),
            chain=chain,
            pool_address=pool_address,
            start_ts=start_ts,
            end_ts=end_ts,
            interval_secs=interval_secs,
        )


__all__ = ["TraderJoeV2GatewayConnector"]
