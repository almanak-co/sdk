"""Gateway-side connector binding for Uniswap V4.

Declares ``GatewayPoolKeyCacheCapability`` so the gateway can hold the
V4 pool_id -> PoolKey derivation cache behind a structural Protocol
(``PoolKeyCacheProtocol``) without importing any V4 symbol in
:mod:`almanak.gateway.services.market_service`. ``build_cache``
constructs the cache instance and pre-seeds it with canonical PoolKeys
in one step.

VIB-4818 — supersedes the original ``GatewayPoolKeySeedCapability``
(VIB-4810). Folding construction + seeding into one method closes the
remaining two ``from almanak.connectors.uniswap_v4...`` imports in
``market_service.py``: the gateway no longer needs to name the cache
class to instantiate it, nor the connector-specific error class to
discriminate refresh failures (the cache raises
``PoolKeyCacheError``, a base type that lives in ``_base``).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import ClassVar

from almanak.connectors._base.gateway_capabilities import (
    GatewayAddressCapability,
    GatewayPoolKeyCacheCapability,
    PoolKeyCacheProtocol,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.types import ProtocolKind, ProtocolName

from ..addresses import UNISWAP_V4
from .canonical_pools import seed_canonical_pool_keys
from .pool_key_cache import V4PoolKeyCache


class UniswapV4GatewayConnector(
    GatewayConnector,
    GatewayAddressCapability,
    GatewayPoolKeyCacheCapability,
):
    """Gateway-side connector for Uniswap V4."""

    protocol: ClassVar[ProtocolName] = ProtocolName("uniswap_v4")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LP

    def addresses_for(self, chain: str) -> Mapping[str, str]:
        """Return the Uniswap V4 contract addresses for ``chain`` (or empty)."""
        return UNISWAP_V4.get(chain, {})

    def address_supported_chains(self) -> frozenset[str]:
        """Chains for which Uniswap V4 addresses are registered."""
        return frozenset(UNISWAP_V4.keys())

    # The CLI support matrix renders Uniswap V4's swap + LP rows on every
    # V4 chain; the chain list comes from the strategy-side manifest's
    # ``matrix_entries`` field (see ``almanak/connectors/uniswap_v4/__init__.py``).

    def build_cache(self, *, network: str) -> PoolKeyCacheProtocol:
        """Construct a V4 pool-key cache and pre-seed canonical PoolKeys.

        Called once per gateway process lifetime by
        :meth:`MarketService._get_pool_key_cache` inside the
        double-checked-lock window so the seed is visible to the first
        ``LookupV4PoolKey`` request without a race window. Seeding is
        idempotent.
        """
        cache = V4PoolKeyCache(network=network)
        seed_canonical_pool_keys(cache)
        return cache


__all__ = ["UniswapV4GatewayConnector"]
