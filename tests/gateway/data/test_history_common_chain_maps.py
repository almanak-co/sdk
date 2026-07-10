"""Registry-derived chain-map compat views in ``almanak/gateway/data/_history_common.py``.

VIB-4851 B1.3 folded the GeckoTerminal / DefiLlama-display chain-spelling
tables onto ``ChainDescriptor.external_ids`` and replaced the standalone dict
literals with read-only ``MappingProxyType`` views derived via
``vendor_chain_map(...)``. These gateway-side assertions lock the exact map
values the pool-history / pool-analytics providers consume, plus the one
intentional widening: ``_CHAIN_TO_GT_NETWORK`` is now the UNION with the
price-layer GeckoTerminal map and so gains ``mantle`` (the core inversion test
``test_external_ids_inversion::test_geckoterminal_collapse_is_union_with_mantle``
pins the derive; this file pins the consumer-facing compat view + its miss path).
"""

from __future__ import annotations

from types import MappingProxyType

from almanak.gateway.data._history_common import (
    _CHAIN_TO_GT_NETWORK,
    _CHAIN_TO_LLAMA_DISPLAY,
)


class TestGeckoTerminalNetworkMap:
    def test_is_readonly_derived_view(self) -> None:
        assert isinstance(_CHAIN_TO_GT_NETWORK, MappingProxyType)

    def test_key_set_is_union_including_mantle(self) -> None:
        # VIB-4851 B1.3: the derive is the union with the price-layer GeckoTerminal
        # map, so it gains ``mantle`` (was 9 keys, now 10). The intentional widening.
        expected = {
            "ethereum",
            "arbitrum",
            "base",
            "optimism",
            "polygon",
            "avalanche",
            "bsc",
            "sonic",
            "solana",
            "mantle",
            "robinhood",
        }
        assert set(_CHAIN_TO_GT_NETWORK) == expected
        assert _CHAIN_TO_GT_NETWORK["mantle"] == "mantle"  # the gained key

    def test_values(self) -> None:
        assert _CHAIN_TO_GT_NETWORK["ethereum"] == "eth"
        assert _CHAIN_TO_GT_NETWORK["polygon"] == "polygon_pos"
        assert _CHAIN_TO_GT_NETWORK["avalanche"] == "avax"
        assert _CHAIN_TO_GT_NETWORK["solana"] == "solana"

    def test_unsupported_chain_misses(self) -> None:
        # Fallback contract: an unsupported chain is absent so ``.get`` returns
        # None and the provider raises DataSourceUnavailable upstream.
        assert _CHAIN_TO_GT_NETWORK.get("fantom") is None


class TestDefiLlamaDisplayMap:
    def test_is_readonly_derived_view(self) -> None:
        assert isinstance(_CHAIN_TO_LLAMA_DISPLAY, MappingProxyType)

    def test_key_set(self) -> None:
        # Byte-identical to the legacy 9-chain display map (no mantle widening here).
        expected = {
            "ethereum",
            "arbitrum",
            "base",
            "optimism",
            "polygon",
            "avalanche",
            "bsc",
            "sonic",
            "solana",
        }
        assert set(_CHAIN_TO_LLAMA_DISPLAY) == expected

    def test_values_are_capitalised_display_names(self) -> None:
        assert _CHAIN_TO_LLAMA_DISPLAY["ethereum"] == "Ethereum"
        assert _CHAIN_TO_LLAMA_DISPLAY["bsc"] == "BSC"
        assert _CHAIN_TO_LLAMA_DISPLAY["solana"] == "Solana"

    def test_unsupported_chain_misses(self) -> None:
        assert _CHAIN_TO_LLAMA_DISPLAY.get("fantom") is None
