"""Gateway-side connector binding for Balancer v2.

Phase 3 (VIB-4811) introduces a minimal Balancer v2 gateway-side
scaffold so the protocol can publish capability-keyed metadata without
the gateway carrying a hardcoded ``"balancer-v2-*"`` table.

Currently contributes:

* ``GatewaySubgraphCapability`` — TheGraph subgraph URLs (Ethereum,
  Arbitrum). Moved verbatim from
  ``almanak.gateway.integrations.thegraph.DEFAULT_ALLOWED_SUBGRAPHS``.

W7-followup (VIB-4870) adds:

* ``GatewayDexVolumeCapability`` — daily trading-volume history via the
  Balancer V2 ``poolSnapshots`` subgraph. Migrates
  ``framework/backtesting/pnl/providers/dex/balancer_volume.py``. The
  legacy provider keyed routing on the historical ``"balancer"`` string;
  the gateway routes on the canonical ``"balancer_v2"`` ``dex_name`` and
  the framework consumer aliases ``"balancer"`` → ``"balancer_v2"``.
  Balancer V2 weighted pools have no Uniswap-style ``observe()`` TWAP,
  so it implements volume only. Since VIB-5090 a bare 42-char pool
  address is auto-resolved to the full 32-byte pool ID the subgraph
  keys ``poolSnapshots`` by (see ``fetch_volume_history``).

Strategy-side Balancer code (intents, connectors, receipt parsing)
remains unchanged and continues to live wherever it lived previously —
this scaffolding only owns the gateway-side capability surface.
"""

from __future__ import annotations

from typing import Any, ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayDexVolumeCapability,
    GatewaySubgraphCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName

# Balancer v2 subgraph URLs. Moved verbatim from
# ``thegraph.DEFAULT_ALLOWED_SUBGRAPHS``.
_BALANCER_V2_SUBGRAPHS: dict[str, str] = {
    "balancer-v2-ethereum": "https://api.thegraph.com/subgraphs/name/balancer-labs/balancer-v2",
    "balancer-v2-arbitrum": "https://api.thegraph.com/subgraphs/name/balancer-labs/balancer-arbitrum-v2",
}

# W7-followup / VIB-4870 — Balancer V2 daily-volume subgraph IDs.
# Migrated verbatim from
# ``framework/backtesting/pnl/providers/dex/balancer_volume.py``. The
# ``poolSnapshots`` entity keys daily rows by a unix-second ``timestamp``
# and exposes ``swapVolume`` (denominated in the pool's accounting token,
# preserved as-is for byte-equivalence with the pre-W7 provider).
_BALANCER_V2_VOLUME_SUBGRAPH_IDS: dict[str, str] = {
    "ethereum": "C4ayEZP2yTXRAB8vSaTrgN4m9anTe9Mdm2ViyiAuV9TV",
    "arbitrum": "98cQDy6tufTJtshDCuhh9z2kWXsQWBHVh2bqnLHsGAeS",
    "polygon": "H9oPAbXnobBRq1cB3HDmbZ1E8MWQyJYQjT1QDJMrdbNp",
}


class BalancerV2GatewayConnector(
    GatewayConnector,
    GatewaySubgraphCapability,
    GatewayDexVolumeCapability,
):
    """Gateway-side connector for Balancer v2."""

    protocol: ClassVar[ProtocolName] = ProtocolName("balancer_v2")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def subgraph_endpoints(self) -> dict[str, str]:
        """TheGraph subgraph URLs for Balancer v2 (one per supported chain)."""
        return dict(_BALANCER_V2_SUBGRAPHS)

    # The CLI support matrix renders Balancer V2's flash-loan row under
    # the historical ``"balancer"`` matrix-name; the override lives on
    # the strategy-side manifest's ``matrix_entries`` field
    # (see ``almanak/connectors/balancer_v2/__init__.py``).

    # ---------------------------------------------------------------------
    # GatewayDexVolumeCapability (VIB-4870 / W7-followup)
    # ---------------------------------------------------------------------

    def dex_name(self) -> str:
        """DEX identifier — canonical ``"balancer_v2"`` protocol identity."""
        return "balancer_v2"

    def volume_supported_chains(self) -> frozenset[str]:
        """Chains with a registered Balancer V2 volume subgraph."""
        return frozenset(_BALANCER_V2_VOLUME_SUBGRAPH_IDS)

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
        """Daily trading-volume history via the Balancer V2 ``poolSnapshots`` subgraph.

        Migrated from
        ``framework/backtesting/pnl/providers/dex/balancer_volume.py``.
        ``poolSnapshots`` keys daily rows by a unix-second ``timestamp``
        (start of day UTC) and exposes ``swapVolume``.

        VIB-5090: ``poolSnapshots.pool`` is the FULL 32-byte pool ID
        (pool address + pool-type/index suffix), so
        ``resolve_bare_address_pool_id`` is set — a bare 42-char pool
        address is auto-resolved to the full ID via a
        ``pools(where: {address})`` lookup (cached per process); full
        IDs pass through unchanged.
        """
        from almanak.gateway.services._dex_volume_subgraph import (
            DexVolumeSubgraphSpec,
            fetch_dex_volume_history,
        )

        return await fetch_dex_volume_history(
            servicer,
            DexVolumeSubgraphSpec(
                dex_name="balancer_v2",
                subgraph_ids=dict(_BALANCER_V2_VOLUME_SUBGRAPH_IDS),
                entity="poolSnapshots",
                id_field="pool",
                volume_field="swapVolume",
                source="balancer_v2_subgraph",
                time_field="timestamp",
                resolve_bare_address_pool_id=True,
            ),
            chain=chain,
            pool_address=pool_address,
            start_ts=start_ts,
            end_ts=end_ts,
            interval_secs=interval_secs,
        )


__all__ = ["BalancerV2GatewayConnector"]
