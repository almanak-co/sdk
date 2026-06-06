"""Connector-owned deferred transaction refresh for Enso."""

from __future__ import annotations

import logging
from typing import Any

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.deferred_refresh_registry import (
    DeferredRefreshCapability,
    DeferredRefreshConnector,
)

__all__ = ["ANVIL_MIN_SLIPPAGE_BPS", "EnsoDeferredRefreshConnector"]

logger = logging.getLogger(__name__)

# Minimum slippage (basis points) for on-chain guards on Anvil forks.
# Enso routes are quoted against live mainnet pools, but Anvil fork state
# diverges over time. Without a wider tolerance, the safeRouteSingle
# minAmountOut check reverts because forked pool reserves differ from mainnet.
ANVIL_MIN_SLIPPAGE_BPS = 500  # 5%


def _is_local_rpc(rpc_url: str | None) -> bool:
    """Thin wrapper around the canonical local-RPC detector in simulator.config."""
    from almanak.framework.execution.simulator.config import is_local_rpc

    return is_local_rpc(rpc_url)


class EnsoDeferredRefreshConnector(DeferredRefreshConnector, DeferredRefreshCapability):
    """Refresh stale Enso swap calldata immediately before execution."""

    protocol = ProtocolName("enso")
    kind = ProtocolKind.SWAP

    def refresh_transaction(
        self,
        metadata: dict[str, Any],
        wallet_address: str,
        *,
        rpc_url: str | None = None,
    ) -> dict[str, Any]:
        """Fetch fresh Enso transaction data."""
        self._widen_slippage_for_anvil(metadata, rpc_url)
        return self._refresh_from_adapter(metadata, wallet_address)

    def _refresh_from_adapter(self, metadata: dict[str, Any], wallet_address: str) -> dict[str, Any]:
        """Build the Enso adapter lazily and fetch fresh route data."""
        from .adapter import EnsoAdapter
        from .client import EnsoConfig

        from_token = metadata.get("from_token")
        chain = metadata.get("chain", "")
        if not chain and isinstance(from_token, dict):
            chain = from_token.get("chain", "")
        config = EnsoConfig(
            chain=chain,
            wallet_address=wallet_address,
        )
        adapter = EnsoAdapter(config, allow_placeholder_prices=True)
        return adapter.get_fresh_swap_transaction(metadata)

    def _widen_slippage_for_anvil(self, metadata: dict[str, Any], rpc_url: str | None) -> None:
        """Widen Enso slippage for local Anvil forks before the fresh quote."""
        if not _is_local_rpc(rpc_url):
            return

        route_params = metadata.get("route_params")
        if not isinstance(route_params, dict):
            return
        original_bps = route_params.get("slippage_bps")
        if original_bps is None:
            return
        try:
            current_bps = int(original_bps)
        except (TypeError, ValueError):
            return
        if current_bps >= ANVIL_MIN_SLIPPAGE_BPS:
            return

        route_params["slippage_bps"] = ANVIL_MIN_SLIPPAGE_BPS

        logger.info(
            "Anvil fork detected: widening Enso slippage from %d bps to %d bps",
            current_bps,
            ANVIL_MIN_SLIPPAGE_BPS,
        )
