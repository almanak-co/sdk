"""Shared chain-name maps + Solana-family helper for the two off-chain
pool-history / pool-analytics gateway services (VIB-4727 / VIB-4753).

Single home for the DefiLlama / CoinGecko Onchain chain-spelling tables so the
``PoolAnalyticsService`` handler (``almanak/gateway/services/pool_analytics_service.py``)
and the ``PoolHistoryDispatcher`` providers (``almanak/gateway/data/pool_history/``)
agree on chain spelling without duplicating the literals. Previously each
service owned its own copy of these maps.

Also the single home for the CoinGecko Onchain API base URLs and header
construction — the pool-analytics servicer and the
pool-history OHLCV provider previously each carried their own copies of
the free/pro base-URL pair and the ``x-cg-pro-api-key`` header logic.

The canonical home for these chain-string spellings is each chain descriptor's
typed ``ExternalChainIds`` declaration. These module-level names are **derived compat views**
(read-only ``MappingProxyType`` snapshots of ``integration_chain_map(...)``)
kept so existing consumers can keep importing the same symbols. Metadata
lookup is pure and opens no network resources.

No HTTP egress happens here — this is pure data + a registry lookup helper.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

from almanak.core.chains import ChainRegistry
from almanak.core.enums import ChainFamily
from almanak.integrations.chains import integration_chain_map

#: Chain -> CoinGecko Onchain network slug. Derived compat view (VIB-4851 B1);
#: canonical home is the chain descriptor's typed external-id declaration.
#: INTENTIONALLY the union of this service's historical 9-entry map with the
#: price-layer coingecko_onchain map — it gains ``mantle`` (9 -> 10 keys). Pinned by
#: ``tests/unit/core/test_external_ids_inversion.py::test_coingecko_onchain_collapse_is_union_with_mantle``.
_CHAIN_TO_CG_ONCHAIN_NETWORK: Mapping[str, str] = MappingProxyType(integration_chain_map("coingecko_onchain"))

#: Chain -> DefiLlama display name (DefiLlama uses capitalized chain names).
#: Derived compat view; canonical home is each chain descriptor's typed
#: DeFiLlama-display id (byte-identical, 9 keys).
_CHAIN_TO_LLAMA_DISPLAY: Mapping[str, str] = MappingProxyType(integration_chain_map("defillama_display"))


#: CoinGecko Onchain API bases. The org runs the paid CoinGecko key — keyed
#: requests go to the pro host, keyless fall back to the free host (pool
#: endpoints there reject keyless calls, which surfaces as an honest provider
#: error naming the env var).
_CG_ONCHAIN_FREE_API = "https://api.coingecko.com/api/v3/onchain"
_CG_ONCHAIN_PRO_API = "https://pro-api.coingecko.com/api/v3/onchain"


def coingecko_onchain_api_base(api_key: str | None) -> str:
    """Return the CoinGecko Onchain API base for ``api_key`` (pro when keyed)."""
    return _CG_ONCHAIN_PRO_API if api_key else _CG_ONCHAIN_FREE_API


def coingecko_onchain_headers(api_key: str | None) -> dict[str, str]:
    """Standard CoinGecko Onchain request headers (+ pro key when present)."""
    headers = {"Accept": "application/json", "User-Agent": "Almanak-Gateway/1.0"}
    if api_key:
        headers["x-cg-pro-api-key"] = api_key
    return headers


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
    "_CG_ONCHAIN_FREE_API",
    "_CG_ONCHAIN_PRO_API",
    "_CHAIN_TO_CG_ONCHAIN_NETWORK",
    "_CHAIN_TO_LLAMA_DISPLAY",
    "coingecko_onchain_api_base",
    "coingecko_onchain_headers",
    "is_solana_family",
]
