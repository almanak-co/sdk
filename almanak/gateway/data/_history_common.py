"""Shared chain-name maps + Solana-family helper for the two off-chain
pool-history / pool-analytics gateway services (VIB-4727 + POOL-5 / VIB-4753).

Single home for the DefiLlama / GeckoTerminal chain-spelling tables so the
``PoolAnalyticsService`` handler (``almanak/gateway/services/pool_analytics_service.py``)
and the ``PoolHistoryDispatcher`` providers (``almanak/gateway/data/pool_history/``)
agree on chain spelling without duplicating the literals. Previously each
service owned its own copy of these maps; the chain-string literals now live
**only here** (the canonical-home rule of the coupling ratchet — see
``docs/internal/blueprints/22-connector-self-containment.md``).

Leaf module: imports only ``almanak.core`` (the chain registry / enums), so
both gateway services can depend on it without an import cycle.

No HTTP egress happens here — this is pure data + a registry lookup helper.
"""

from __future__ import annotations

from almanak.core.chains import ChainRegistry
from almanak.core.enums import ChainFamily

#: Chain -> GeckoTerminal network slug.
_CHAIN_TO_GT_NETWORK: dict[str, str] = {
    "ethereum": "eth",
    "arbitrum": "arbitrum",
    "base": "base",
    "optimism": "optimism",
    "polygon": "polygon_pos",
    "avalanche": "avax",
    "bsc": "bsc",
    "sonic": "sonic",
    "solana": "solana",
}

#: Chain -> DefiLlama display name (DefiLlama uses capitalized chain names).
_CHAIN_TO_LLAMA_DISPLAY: dict[str, str] = {
    "ethereum": "Ethereum",
    "arbitrum": "Arbitrum",
    "base": "Base",
    "optimism": "Optimism",
    "polygon": "Polygon",
    "avalanche": "Avalanche",
    "bsc": "BSC",
    "sonic": "Sonic",
    "solana": "Solana",
}


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
