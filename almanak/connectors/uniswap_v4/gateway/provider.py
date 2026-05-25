"""Gateway-side connector binding for Uniswap V4.

Declares the ``GatewayPoolKeySeedCapability`` for V4 so the gateway boot
loop can pre-seed the V4 pool_id -> PoolKey derivation cache without
hand-wiring an import in :mod:`almanak.gateway.services.market_service`.

Phase 1+2 (VIB-4810) — the capability is declared but
``market_service`` continues to call ``seed_canonical_pool_keys``
directly. Phase 4 collapses ``market_service`` to iterate
``GATEWAY_REGISTRY.capability_providers(GatewayPoolKeySeedCapability)``.
"""

from __future__ import annotations

from typing import Any, ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayPoolKeySeedCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName

from .canonical_pools import seed_canonical_pool_keys


class UniswapV4GatewayConnector(GatewayConnector, GatewayPoolKeySeedCapability):
    """Gateway-side connector for Uniswap V4."""

    protocol: ClassVar[ProtocolName] = ProtocolName("uniswap_v4")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def seed_pool_keys(self, cache: Any) -> None:
        """Pre-seed canonical PoolKeys into ``cache``.

        Idempotent; safe to call multiple times. See
        :func:`almanak.connectors.uniswap_v4.gateway.canonical_pools.seed_canonical_pool_keys`
        for the full contract.
        """
        seed_canonical_pool_keys(cache)


__all__ = ["UniswapV4GatewayConnector"]
