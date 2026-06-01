"""Balancer V2 historical volume provider.

**VIB-4859 / W7 (VIB-4870)**: this module is now a thin gRPC client of
the gateway's ``RateHistoryService.GetDexVolumeHistory`` RPC. The
TheGraph subgraph HTTP egress that lived here (in the old shared subgraph
client) has moved into the gateway sidecar — the Balancer V2 connector's
:class:`GatewayDexVolumeCapability` owns the ``poolSnapshots`` subgraph
query (``swapVolume`` keyed by ``pool``, unix-second ``timestamp``). The
strategy container holds no subgraph URLs, no API key, and opens no
socket.

The :class:`BalancerVolumeProvider` public API, the
``BALANCER_SUBGRAPH_IDS`` / ``SUPPORTED_CHAINS`` tables and the
``DATA_SOURCE`` constant are preserved for back-compat.

Routing note: the framework aggregator addresses this DEX by the legacy
string id ``"balancer"``, but the gateway capability registered under the
canonical connector ``dex_name`` ``"balancer_v2"`` — the gRPC request
therefore carries ``"balancer_v2"`` (see ``_GATEWAY_DEX``).
"""

from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal
from typing import Any

from almanak.core.enums import Chain

from ...types import VolumeResult
from ..base import HistoricalVolumeProvider
from ._gateway_volume import fetch_volume_via_gateway

logger = logging.getLogger(__name__)


# =============================================================================
# Balancer V2 Subgraph IDs (preserved for back-compat)
# =============================================================================
#
# The authoritative copies now live on the Balancer V2 connector's
# ``GatewayDexVolumeCapability``.
BALANCER_SUBGRAPH_IDS: dict[Chain, str] = {
    Chain.ETHEREUM: "C4ayEZP2yTXRAB8vSaTrgN4m9anTe9Mdm2ViyiAuV9TV",
    Chain.ARBITRUM: "98cQDy6tufTJtshDCuhh9z2kWXsQWBHVh2bqnLHsGAeS",
    Chain.POLYGON: "H9oPAbXnobBRq1cB3HDmbZ1E8MWQyJYQjT1QDJMrdbNp",
}

# Supported chains for this provider
SUPPORTED_CHAINS: list[Chain] = list(BALANCER_SUBGRAPH_IDS.keys())

# Data source identifier (stamped on each VolumeResult — preserves the
# pre-W7 provenance string for byte-equivalence with backtest fixtures).
DATA_SOURCE = "balancer_v2_subgraph"

# Gateway routing key (the connector's ``GatewayDexVolumeCapability.dex_name``
# is the canonical ``"balancer_v2"``, NOT the legacy ``"balancer"`` alias).
_GATEWAY_DEX = "balancer_v2"


# =============================================================================
# BalancerVolumeProvider (thin gRPC client — VIB-4859 / W7)
# =============================================================================


class BalancerVolumeProvider(HistoricalVolumeProvider):
    """Historical volume provider for Balancer V2 pools — gateway-backed.

    Issues a ``GetDexVolumeHistory`` RPC for daily volume on Ethereum,
    Arbitrum, and Polygon. All TheGraph egress lives gateway-side via
    :class:`GatewayDexVolumeCapability`.
    """

    def __init__(
        self,
        client: Any | None = None,
        fallback_volume: Decimal = Decimal("0"),
        requests_per_minute: int = 100,
    ) -> None:
        """Initialize the Balancer volume provider.

        Args:
            client: Ignored (kept for back-compat). Egress lives gateway-side.
            fallback_volume: Ignored (kept for back-compat). A "no data"
                subgraph raises :class:`DataSourceUnavailable` instead of a
                silent-zero row.
            requests_per_minute: Ignored (kept for back-compat).
        """
        self._fallback_volume = fallback_volume

        logger.debug(
            "Initialized BalancerVolumeProvider (gateway-backed): supported_chains=%s",
            [c.value for c in SUPPORTED_CHAINS],
        )

    @property
    def supported_chains(self) -> list[Chain]:
        """Get the list of supported chains."""
        return SUPPORTED_CHAINS.copy()

    async def close(self) -> None:
        """No-op shutdown hook (no owned client to close)."""
        logger.debug("BalancerVolumeProvider closed")

    async def __aenter__(self) -> BalancerVolumeProvider:
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit: close the client."""
        await self.close()

    async def get_volume(
        self,
        pool_address: str,
        chain: Chain,
        start_date: date,
        end_date: date,
    ) -> list[VolumeResult]:
        """Fetch historical daily volume for a Balancer V2 pool via the gateway.

        Note:
            Balancer V2 subgraphs key snapshots by the full pool ID (address
            + pool-type suffix, 64 hex chars). A bare 42-char address may
            return no rows — the gateway surfaces that as
            :class:`DataSourceUnavailable` rather than a silent-zero row.

        Raises:
            ValueError: If chain is not supported.
            DataSourceUnavailable: gateway unreachable / RPC failed / the
                subgraph returned no or errored data (no silent zero-fill).
        """
        if chain not in BALANCER_SUBGRAPH_IDS:
            raise ValueError(f"Unsupported chain: {chain}. Supported chains: {[c.value for c in SUPPORTED_CHAINS]}")

        return await fetch_volume_via_gateway(
            dex=_GATEWAY_DEX,
            chain=chain,
            pool_address=pool_address,
            start_date=start_date,
            end_date=end_date,
            data_source=DATA_SOURCE,
        )


__all__ = [
    "BalancerVolumeProvider",
    "BALANCER_SUBGRAPH_IDS",
    "SUPPORTED_CHAINS",
    "DATA_SOURCE",
]
