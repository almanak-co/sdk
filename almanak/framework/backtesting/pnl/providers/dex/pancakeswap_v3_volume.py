"""PancakeSwap V3 historical volume provider.

**VIB-4859 / W7 (VIB-4870)**: this module is now a thin gRPC client of
the gateway's ``RateHistoryService.GetDexVolumeHistory`` RPC. The
TheGraph subgraph HTTP egress that lived here (in the old shared subgraph
client) has moved into the gateway sidecar — the PancakeSwap V3 connector's
:class:`GatewayDexVolumeCapability` owns the subgraph deployment IDs +
the daily-volume query. The strategy container holds no subgraph URLs,
no API key, and opens no socket.

The :class:`PancakeSwapV3VolumeProvider` public API, the
``PANCAKESWAP_V3_SUBGRAPH_IDS`` / ``SUPPORTED_CHAINS`` tables and the
``DATA_SOURCE`` constant are preserved for back-compat.
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
# PancakeSwap V3 Subgraph IDs (preserved for back-compat)
# =============================================================================
#
# The authoritative copies now live on the PancakeSwap V3 connector's
# ``GatewayDexVolumeCapability``.
PANCAKESWAP_V3_SUBGRAPH_IDS: dict[Chain, str] = {
    Chain.ETHEREUM: "CJYGNhb7RvnhfBDjqpRnD3oxgyhibzc7fkAMa38YV3oS",
    Chain.ARBITRUM: "251MHFNN1rwjErXD2efWMpNS73SANZN8Ua192zw6iXve",
    Chain.BSC: "Hv1GncLY5docZoGtXjo4kwbTvxm3MAhVZqBZE4sUT9eZ",
    Chain.BASE: "BHWNsedAHtmTCzXxCCDfhPmm6iN9rxUhoRHdHKyujic3",
}

# Supported chains for this provider
SUPPORTED_CHAINS: list[Chain] = list(PANCAKESWAP_V3_SUBGRAPH_IDS.keys())

# Data source identifier (stamped on each VolumeResult — preserves the
# pre-W7 provenance string for byte-equivalence with backtest fixtures).
DATA_SOURCE = "pancakeswap_v3_subgraph"

# Gateway routing key (the connector's ``GatewayDexVolumeCapability.dex_name``).
_GATEWAY_DEX = "pancakeswap_v3"


# =============================================================================
# PancakeSwapV3VolumeProvider (thin gRPC client — VIB-4859 / W7)
# =============================================================================


class PancakeSwapV3VolumeProvider(HistoricalVolumeProvider):
    """Historical volume provider for PancakeSwap V3 pools — gateway-backed.

    Issues a ``GetDexVolumeHistory`` RPC for daily volume on Ethereum,
    Arbitrum, BSC, and Base. All TheGraph egress lives gateway-side via
    :class:`GatewayDexVolumeCapability`.
    """

    def __init__(
        self,
        client: Any | None = None,
        fallback_volume: Decimal = Decimal("0"),
        requests_per_minute: int = 100,
    ) -> None:
        """Initialize the PancakeSwap V3 volume provider.

        Args:
            client: Ignored (kept for back-compat). Egress lives gateway-side.
            fallback_volume: Ignored (kept for back-compat). A "no data"
                subgraph raises :class:`DataSourceUnavailable` instead of a
                silent-zero row.
            requests_per_minute: Ignored (kept for back-compat).
        """
        self._fallback_volume = fallback_volume

        logger.debug(
            "Initialized PancakeSwapV3VolumeProvider (gateway-backed): supported_chains=%s",
            [c.value for c in SUPPORTED_CHAINS],
        )

    @property
    def supported_chains(self) -> list[Chain]:
        """Get the list of supported chains."""
        return SUPPORTED_CHAINS.copy()

    async def close(self) -> None:
        """No-op shutdown hook (no owned client to close)."""
        logger.debug("PancakeSwapV3VolumeProvider closed")

    async def __aenter__(self) -> PancakeSwapV3VolumeProvider:
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
        """Fetch historical daily volume for a PancakeSwap V3 pool via the gateway.

        Raises:
            ValueError: If chain is not supported.
            DataSourceUnavailable: gateway unreachable / RPC failed / the
                subgraph returned no or errored data (no silent zero-fill).
        """
        if chain not in PANCAKESWAP_V3_SUBGRAPH_IDS:
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
    "PancakeSwapV3VolumeProvider",
    "PANCAKESWAP_V3_SUBGRAPH_IDS",
    "SUPPORTED_CHAINS",
    "DATA_SOURCE",
]
