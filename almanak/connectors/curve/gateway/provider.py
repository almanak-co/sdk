"""Gateway-side connector binding for Curve (VIB-4811 / VIB-4817).

Phase 3 scaffolding — Curve does not yet have a full strategy-side
connector under ``almanak/connectors/curve/``. This scaffold publishes
the protocol's DEX-quote function and TheGraph subgraph endpoints.

Contributes:

* ``GatewayDexQuoteCapability`` — DEX quote function for the
  multi-DEX price service (Ethereum, Arbitrum). The simulation logic
  stays on ``MultiDexPriceService`` (where it shares state with
  siblings); this connector only delegates dispatch.
* ``GatewaySubgraphCapability`` (VIB-4817) — TheGraph subgraph URLs
  for Curve, moved verbatim out of the ``_PENDING_SUBGRAPHS`` dict in
  ``almanak.gateway.integrations.thegraph``.

W7-followup (VIB-4870) adds:

* ``GatewayDexVolumeCapability`` — daily trading-volume history via
  Messari's standardised DEX schema (``liquidityPoolDailySnapshots``).
  Migrates ``framework/backtesting/pnl/providers/dex/curve_volume.py``.
  Messari keys daily rows by ``day`` (days since the Unix epoch) rather
  than a unix-second ``date``; the shared helper's ``time_unit="days"``
  handles the conversion. Curve StableSwap has no Uniswap-style
  ``observe()`` TWAP primitive, so it implements volume only.
"""

from __future__ import annotations

from typing import Any, ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayDexQuoteCapability,
    GatewayDexVolumeCapability,
    GatewaySubgraphCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName

# Curve subgraph URLs — moved verbatim from the ``_PENDING_SUBGRAPHS``
# entries previously held in ``thegraph.py``. Endpoints are sourced from
# the Convex community's curve-volume subgraphs, which historically
# back the analytics surface in the gateway.
_CURVE_SUBGRAPHS: dict[str, str] = {
    "curve-ethereum": "https://api.thegraph.com/subgraphs/name/convex-community/volume-mainnet",
    "curve-arbitrum": "https://api.thegraph.com/subgraphs/name/convex-community/volume-arbitrum",
}

# W7-followup / VIB-4870 — Curve daily-volume subgraph IDs (Messari
# standardised DEX schema). Migrated verbatim from
# ``framework/backtesting/pnl/providers/dex/curve_volume.py``. Note the
# volume coverage (Ethereum, Optimism) differs from the quote coverage
# (Ethereum, Arbitrum) above — the Messari volume subgraphs are deployed
# on a different chain set than the Convex quote subgraphs.
_CURVE_VOLUME_SUBGRAPH_IDS: dict[str, str] = {
    "ethereum": "3fy93eAT56UJsRCEht8iFhfi6wjHWXtZ9dnnbQmvFopF",
    "optimism": "CXDZPduZE6nWuWEkSzWkRoJSSJ6CneSqiDxdnhhURShX",
}


class CurveGatewayConnector(
    GatewayConnector,
    GatewayDexQuoteCapability,
    GatewaySubgraphCapability,
    GatewayDexVolumeCapability,
):
    """Gateway-side connector for Curve."""

    protocol: ClassVar[ProtocolName] = ProtocolName("curve")
    kind: ClassVar[ProtocolKind] = ProtocolKind.SWAP

    def dex_name(self) -> str:
        """DEX identifier — matches the legacy ``Dex.CURVE`` string."""
        return "curve"

    def supported_chains(self) -> frozenset[str]:
        """Chains where Curve quotes are available via the multi-DEX service.

        Matches the historical ``DEX_CHAINS`` entries that listed
        ``"curve"`` (Ethereum, Arbitrum).
        """
        return frozenset({"ethereum", "arbitrum"})

    async def quote(
        self,
        service: Any,
        token_in: str,
        token_out: str,
        amount_in: Any,
    ) -> Any:
        """Delegate to ``MultiDexPriceService._get_curve_quote``."""
        return await service._get_curve_quote(token_in, token_out, amount_in)

    def subgraph_endpoints(self) -> dict[str, str]:
        """TheGraph subgraph URLs for Curve (Ethereum, Arbitrum)."""
        return dict(_CURVE_SUBGRAPHS)

    # ---------------------------------------------------------------------
    # GatewayDexVolumeCapability (VIB-4870 / W7-followup)
    # ---------------------------------------------------------------------

    def volume_supported_chains(self) -> frozenset[str]:
        """Chains with a registered Curve volume subgraph (Ethereum, Optimism)."""
        return frozenset(_CURVE_VOLUME_SUBGRAPH_IDS)

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
        """Daily trading-volume history via Messari ``liquidityPoolDailySnapshots``.

        Migrated from
        ``framework/backtesting/pnl/providers/dex/curve_volume.py``.
        Messari snapshots key on ``day`` (days since epoch) → the shared
        helper's ``time_unit="days"`` converts the request window to/from
        unix seconds.
        """
        from almanak.gateway.services._dex_volume_subgraph import (
            DexVolumeSubgraphSpec,
            fetch_dex_volume_history,
        )

        return await fetch_dex_volume_history(
            servicer,
            DexVolumeSubgraphSpec(
                dex_name="curve",
                subgraph_ids=dict(_CURVE_VOLUME_SUBGRAPH_IDS),
                entity="liquidityPoolDailySnapshots",
                id_field="pool",
                volume_field="dailyVolumeUSD",
                source="curve_messari_subgraph",
                time_field="day",
                time_unit="days",
            ),
            chain=chain,
            pool_address=pool_address,
            start_ts=start_ts,
            end_ts=end_ts,
            interval_secs=interval_secs,
        )


__all__ = ["CurveGatewayConnector"]
