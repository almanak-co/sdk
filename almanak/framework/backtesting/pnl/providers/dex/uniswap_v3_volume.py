"""Uniswap V3 historical volume provider.

**VIB-4859 / W7 (VIB-4870)**: this module is now a thin gRPC client of
the gateway's ``RateHistoryService.GetDexVolumeHistory`` RPC. The
TheGraph subgraph HTTP egress that lived here (in the old shared
subgraph client) has moved into the gateway sidecar — the
Uniswap V3 connector's :class:`GatewayDexVolumeCapability` owns the
subgraph deployment IDs + the daily-volume query and delegates to the
shared gateway-side egress helper. The strategy container holds no
subgraph URLs, no API key, and opens no socket.

The :class:`UniswapV3VolumeProvider` public API, the
``UNISWAP_V3_SUBGRAPH_IDS`` / ``SUPPORTED_CHAINS`` tables and the
``DATA_SOURCE`` constant are preserved for back-compat (callers imported
them by name). The subgraph IDs are no longer load-bearing for the
W7 dispatch — the gateway connector owns the authoritative copies — but
``SUPPORTED_CHAINS`` still gates which chains this provider accepts
before the RPC.

Example:
    from almanak.framework.backtesting.pnl.providers.dex import (
        UniswapV3VolumeProvider,
    )
    from almanak.core.enums import Chain
    from datetime import date

    provider = UniswapV3VolumeProvider()
    async with provider:
        volumes = await provider.get_volume(
            pool_address="0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",
            chain=Chain.ARBITRUM,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31),
        )
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
# Uniswap V3 Subgraph IDs (preserved for back-compat)
# =============================================================================
#
# The authoritative copies now live on the Uniswap V3 connector's
# ``GatewayDexVolumeCapability``; these are kept so callers that imported
# the dict by name don't break and so ``SUPPORTED_CHAINS`` stays derived.
UNISWAP_V3_SUBGRAPH_IDS: dict[Chain, str] = {
    Chain.ETHEREUM: "5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV",
    Chain.ARBITRUM: "FbCGRftH4a3yZugY7TnbYgPJVEv2LvMT6oF1fxPe9aJM",
    Chain.BASE: "43Hwfi3dJSoGpyas9VwNoDAv28rqtbnqUk3EYCRr3j6i",
    Chain.OPTIMISM: "Gc2DPCVq5UkBfyHjZDMbKTc7ynrjoSKxc6sHLKY9Pmjc",
    Chain.POLYGON: "3hCPRGf4z88VC5rsBKU5AA9FBBq5nF3jbKJG7VZCbhjm",
}

# Supported chains for this provider
SUPPORTED_CHAINS: list[Chain] = list(UNISWAP_V3_SUBGRAPH_IDS.keys())

# Data source identifier (stamped on each VolumeResult — preserves the
# pre-W7 provenance string for byte-equivalence with backtest fixtures).
DATA_SOURCE = "uniswap_v3_subgraph"

# Gateway routing key (the connector's ``GatewayDexVolumeCapability.dex_name``).
_GATEWAY_DEX = "uniswap_v3"


# =============================================================================
# UniswapV3VolumeProvider (thin gRPC client — VIB-4859 / W7)
# =============================================================================


class UniswapV3VolumeProvider(HistoricalVolumeProvider):
    """Historical volume provider for Uniswap V3 pools — gateway-backed.

    Issues a ``GetDexVolumeHistory`` RPC for daily volume on Ethereum,
    Arbitrum, Base, Optimism, and Polygon. All TheGraph egress lives
    gateway-side via :class:`GatewayDexVolumeCapability`.

    Example:
        provider = UniswapV3VolumeProvider()
        async with provider:
            volumes = await provider.get_volume(
                pool_address="0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443",
                chain=Chain.ARBITRUM,
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 31),
            )
    """

    def __init__(
        self,
        client: Any | None = None,
        fallback_volume: Decimal = Decimal("0"),
        requests_per_minute: int = 100,
    ) -> None:
        """Initialize the Uniswap V3 volume provider.

        Args:
            client: Ignored (kept for back-compat). Egress lives
                gateway-side; no subgraph HTTP client is constructed.
            fallback_volume: Ignored (kept for back-compat). The pre-W7
                silent-zero fallback row is removed — a "no data" subgraph
                raises :class:`DataSourceUnavailable`.
            requests_per_minute: Ignored (kept for back-compat). Rate
                limiting now lives on the gateway side.
        """
        # Preserved for back-compat introspection; not load-bearing.
        self._fallback_volume = fallback_volume

        logger.debug(
            "Initialized UniswapV3VolumeProvider (gateway-backed): supported_chains=%s",
            [c.value for c in SUPPORTED_CHAINS],
        )

    @property
    def supported_chains(self) -> list[Chain]:
        """Get the list of supported chains."""
        return SUPPORTED_CHAINS.copy()

    async def close(self) -> None:
        """No-op shutdown hook (no owned client to close)."""
        logger.debug("UniswapV3VolumeProvider closed")

    async def __aenter__(self) -> UniswapV3VolumeProvider:
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
        """Fetch historical daily volume for a Uniswap V3 pool via the gateway.

        Args:
            pool_address: The pool contract address (checksummed or lowercase).
            chain: The blockchain the pool is on. Must be one of:
                   ETHEREUM, ARBITRUM, BASE, OPTIMISM, POLYGON.
            start_date: Start of date range (inclusive).
            end_date: End of date range (inclusive).

        Returns:
            List of HIGH-confidence :class:`VolumeResult`, one per daily
            point the gateway returned.

        Raises:
            ValueError: If chain is not supported.
            DataSourceUnavailable: gateway unreachable / RPC failed / the
                subgraph returned no or errored data (no silent zero-fill).
        """
        if chain not in UNISWAP_V3_SUBGRAPH_IDS:
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
    "UniswapV3VolumeProvider",
    "UNISWAP_V3_SUBGRAPH_IDS",
    "SUPPORTED_CHAINS",
    "DATA_SOURCE",
]
