"""Connector-owned deferred transaction refresh for LiFi."""

from __future__ import annotations

from typing import Any

from almanak.connectors._base.types import ProtocolKind, ProtocolName
from almanak.connectors._strategy_base.deferred_refresh_registry import (
    DeferredRefreshCapability,
    DeferredRefreshConnector,
)

__all__ = ["LiFiDeferredRefreshConnector"]


class LiFiDeferredRefreshConnector(DeferredRefreshConnector, DeferredRefreshCapability):
    """Refresh stale LiFi route calldata immediately before execution."""

    protocol = ProtocolName("lifi")
    kind = ProtocolKind.BRIDGE

    def refresh_transaction(
        self,
        metadata: dict[str, Any],
        wallet_address: str,
        *,
        rpc_url: str | None = None,
    ) -> dict[str, Any]:
        """Fetch fresh LiFi transaction data."""
        _ = rpc_url
        return self._refresh_from_adapter(metadata, wallet_address)

    def _refresh_from_adapter(self, metadata: dict[str, Any], wallet_address: str) -> dict[str, Any]:
        """Build the LiFi adapter lazily and fetch fresh route data."""
        from .adapter import LiFiAdapter
        from .client import LiFiConfig

        route_params = metadata["route_params"]
        config = LiFiConfig(
            chain_id=route_params["from_chain_id"],
            wallet_address=wallet_address,
        )
        adapter = LiFiAdapter(config, allow_placeholder_prices=True)
        return adapter.get_fresh_transaction(metadata)
