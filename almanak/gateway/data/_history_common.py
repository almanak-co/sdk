"""Shared chain-name maps + Solana-family helper for the two off-chain
pool-history / pool-analytics gateway services (VIB-4727 + POOL-5 / VIB-4753).

Single home for the DefiLlama / CoinGecko Onchain chain-spelling tables so the
``PoolAnalyticsService`` handler (``almanak/gateway/services/pool_analytics_service.py``)
and the ``PoolHistoryDispatcher`` providers (``almanak/gateway/data/pool_history/``)
agree on chain spelling without duplicating the literals. Previously each
service owned its own copy of these maps.

The canonical home for these chain-string spellings is now the per-chain
``ChainDescriptor.external_ids`` mapping on the registry — these module-level
names are **derived compat views** (read-only ``MappingProxyType`` snapshots of
``vendor_chain_map(...)``) kept so existing consumers can keep importing the same
symbols. They are no longer the source of truth (VIB-4851 B1). The import of
``almanak.core.chains`` eagerly registers all chains, so the module-level
snapshot captures the complete registry.

Leaf module: imports only ``almanak.core`` (the chain registry / enums + the
derive helper), so both gateway services can depend on it without an import cycle.

No HTTP egress happens here — this is pure data + a registry lookup helper.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

from almanak.core.chains import ChainRegistry
from almanak.core.chains._helpers import vendor_chain_map
from almanak.core.enums import ChainFamily

#: Chain -> CoinGecko Onchain network slug. Derived compat view (VIB-4851 B1);
#: canonical home is ``ChainDescriptor.external_ids["geckoterminal"]``.
#: INTENTIONALLY the union of this service's historical 9-entry map with the
#: price-layer geckoterminal map — it gains ``mantle`` (9 -> 10 keys). Pinned by
#: ``tests/unit/core/test_external_ids_inversion.py::test_geckoterminal_collapse_is_union_with_mantle``.
_CHAIN_TO_GT_NETWORK: Mapping[str, str] = MappingProxyType(vendor_chain_map("geckoterminal"))

#: Chain -> DefiLlama display name (DefiLlama uses capitalized chain names).
#: Derived compat view (VIB-4851 B1); canonical home is
#: ``ChainDescriptor.external_ids["defillama_display"]`` (byte-identical, 9 keys).
_CHAIN_TO_LLAMA_DISPLAY: Mapping[str, str] = MappingProxyType(vendor_chain_map("defillama_display"))


def is_solana_family(chain: str) -> bool:
    """Return True when ``chain`` resolves to the SOLANA family.

    Uses ``ChainRegistry.try_resolve`` so an unknown chain name silently
    falls through to the EVM branch (matches the legacy ``chain == "solana"``
    contract). Branching on ``descriptor.family`` instead of the chain name
    is the ``ChainDescriptor`` carve-out pattern from blueprint 22 (W3 /
    VIB-4855).
    """
    descriptor = ChainRegistry.try_resolve(chain)
    return descriptor is not None and descriptor.family is ChainFamily.SOLANA


__all__ = [
    "_CHAIN_TO_GT_NETWORK",
    "_CHAIN_TO_LLAMA_DISPLAY",
    "is_solana_family",
]
