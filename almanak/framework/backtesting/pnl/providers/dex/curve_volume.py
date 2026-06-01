"""Curve Finance historical volume provider.

**VIB-4859 / W7 (VIB-4870)**: this module is now a thin gRPC client of
the gateway's ``RateHistoryService.GetDexVolumeHistory`` RPC. The
TheGraph subgraph HTTP egress that lived here (in the old shared subgraph
client) has moved into the gateway sidecar — the Curve connector's
:class:`GatewayDexVolumeCapability` owns the Messari-schema
``liquidityPoolDailySnapshots`` subgraph query (``dailyVolumeUSD`` keyed
by ``pool``, with the ``day`` field carrying days-since-epoch). The
gateway converts the Messari day-number back to unix seconds, so the
consumer sees the same midnight-UTC timestamps the pre-W7 provider built.
The strategy container holds no subgraph URLs, no API key, and opens no
socket.

The :class:`CurveVolumeProvider` public API, the ``CURVE_SUBGRAPH_IDS`` /
``SUPPORTED_CHAINS`` tables and the ``DATA_SOURCE`` constant are preserved
for back-compat.
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
# Curve Finance Subgraph IDs (Messari schema — preserved for back-compat)
# =============================================================================
#
# The authoritative copies now live on the Curve connector's
# ``GatewayDexVolumeCapability``. Arbitrum / Polygon are on the hosted
# service (deprecated), not the decentralized network yet — Ethereum and
# Optimism only, preserved verbatim from pre-W7.
CURVE_SUBGRAPH_IDS: dict[Chain, str] = {
    Chain.ETHEREUM: "3fy93eAT56UJsRCEht8iFhfi6wjHWXtZ9dnnbQmvFopF",
    Chain.OPTIMISM: "CXDZPduZE6nWuWEkSzWkRoJSSJ6CneSqiDxdnhhURShX",
}

# Supported chains for this provider
SUPPORTED_CHAINS: list[Chain] = list(CURVE_SUBGRAPH_IDS.keys())

# Data source identifier (stamped on each VolumeResult — preserves the
# pre-W7 provenance string for byte-equivalence with backtest fixtures).
DATA_SOURCE = "curve_messari_subgraph"

# Gateway routing key (the connector's ``GatewayDexVolumeCapability.dex_name``).
_GATEWAY_DEX = "curve"


# =============================================================================
# CurveVolumeProvider (thin gRPC client — VIB-4859 / W7)
# =============================================================================


class CurveVolumeProvider(HistoricalVolumeProvider):
    """Historical volume provider for Curve Finance pools — gateway-backed.

    Issues a ``GetDexVolumeHistory`` RPC for daily volume on Ethereum and
    Optimism. All TheGraph egress (and the Messari day-number → unix-second
    conversion) lives gateway-side via :class:`GatewayDexVolumeCapability`.
    """

    def __init__(
        self,
        client: Any | None = None,
        fallback_volume: Decimal = Decimal("0"),
        requests_per_minute: int = 100,
    ) -> None:
        """Initialize the Curve volume provider.

        Args:
            client: Ignored (kept for back-compat). Egress lives gateway-side.
            fallback_volume: Ignored (kept for back-compat). A "no data"
                subgraph raises :class:`DataSourceUnavailable` instead of a
                silent-zero row.
            requests_per_minute: Ignored (kept for back-compat).
        """
        self._fallback_volume = fallback_volume

        logger.debug(
            "Initialized CurveVolumeProvider (gateway-backed): supported_chains=%s",
            [c.value for c in SUPPORTED_CHAINS],
        )

    @property
    def supported_chains(self) -> list[Chain]:
        """Get the list of supported chains."""
        return SUPPORTED_CHAINS.copy()

    async def close(self) -> None:
        """No-op shutdown hook (no owned client to close)."""
        logger.debug("CurveVolumeProvider closed")

    async def __aenter__(self) -> CurveVolumeProvider:
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
        """Fetch historical daily volume for a Curve pool via the gateway.

        Raises:
            ValueError: If chain is not supported.
            DataSourceUnavailable: gateway unreachable / RPC failed / the
                subgraph returned no or errored data (no silent zero-fill).
        """
        if chain not in CURVE_SUBGRAPH_IDS:
            raise ValueError(
                f"Unsupported chain: {chain}. Supported chains: {[c.value for c in SUPPORTED_CHAINS]}. "
                f"Note: Arbitrum and Polygon support pending subgraph migration to decentralized network."
            )

        return await fetch_volume_via_gateway(
            dex=_GATEWAY_DEX,
            chain=chain,
            pool_address=pool_address,
            start_date=start_date,
            end_date=end_date,
            data_source=DATA_SOURCE,
        )


__all__ = [
    "CurveVolumeProvider",
    "CURVE_SUBGRAPH_IDS",
    "SUPPORTED_CHAINS",
    "DATA_SOURCE",
]
