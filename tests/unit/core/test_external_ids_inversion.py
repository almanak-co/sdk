"""Equivalence harness for the VIB-4851 B1 external-id inversion.

Eleven standalone per-vendor chain maps were folded onto typed
``ExternalChainIds`` values implemented by chain descriptors and now project via
``integration_chain_id`` / ``integration_chain_map``:

* CoinGecko ``gateway/data/price/coingecko.py::COINGECKO_PLATFORM_IDS``
* DexScreener ``gateway/data/price/dexscreener.py::CHAIN_TO_DEXSCREENER_PLATFORM``
  **+** ``gateway/services/dexscreener_lookup.py::CHAIN_SLUG_MAP`` (collapsed)
* CoinGecko Onchain ``gateway/data/ohlcv/coingecko_onchain_provider.py::_CHAIN_TO_NETWORK``
  **+** ``gateway/data/_history_common.py::_CHAIN_TO_CG_ONCHAIN_NETWORK`` (collapsed)
* DeFiLlama slug ``framework/data/providers/defillama_provider.py::_CHAIN_TO_LLAMA``
* DeFiLlama display ``gateway/data/_history_common.py::_CHAIN_TO_LLAMA_DISPLAY``
  **+** ``framework/data/yields/aggregator.py::_CHAIN_TO_LLAMA_DISPLAY`` (byte-identical)
* Zerion ``gateway/integrations/zerion.py::ZerionIntegration._CHAIN_IDS``
* Moralis ``gateway/integrations/moralis.py::MoralisIntegration._CHAIN_SLUGS``
* OKX ``gateway/integrations/okx.py::OkxIntegration._CHAIN_IDS``

This test freezes each OLD map verbatim (copied from origin/main, including the
explicit ``"bnb"`` alias keys and OKX's synthetic ``"solana": "501"``) and
asserts the registry-derived lookup reproduces it — proving the *data* is
preserved, not the design. It is the same Class-A/B equivalence harness the
chain-string inversion campaign relies on (see
``tests/unit/core/test_native_symbols_inversion.py``).

The most important assertion per vendor is **bounded widening**: the derived
``integration_chain_map`` must declare support for exactly the chains the legacy
map did (minus pure aliases), plus explicitly pinned and provider-verified
expansions. It must never widen implicitly to chains a vendor lacks.

Three collapses were verified value-identical on every shared chain before the
fold and are pinned here by name:

* DexScreener #2/#3 agree on all 19 canonical chains; #2 additionally carried
  the ``"bnb"`` alias (dropped — ``integration_chain_id`` resolves it via the
  registry). The reconciled key-set is the 19 canonical chains.
* CoinGecko Onchain #4/#5 agree on all 9 shared chains; #4 additionally carried
  ``mantle``. The reconciled key-set is the union (10 chains).
* DeFiLlama display #7/#8 are byte-identical.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError, fields, replace

import pytest

from almanak.core.chains import ChainRegistry, ExternalChainIds, ExternalIdProvider
from almanak.core.chains._helpers import external_chain_id_map
from almanak.integrations.chains import integration_chain_id, integration_chain_map

# --- the 11 OLD maps, frozen verbatim from origin/main (pre-B1) ------------------
#
# Kept exactly as they appeared in the vendor files, INCLUDING the ``"bnb"``
# alias keys and OKX's synthetic ``"solana": "501"`` (501 is NOT the Solana
# EIP-155 chain id — Solana has none; it is an OKX-specific literal).

# 1. CoinGecko platform ids.
FROZEN_COINGECKO: dict[str, str] = {
    "ethereum": "ethereum",
    "arbitrum": "arbitrum-one",
    "optimism": "optimistic-ethereum",
    "base": "base",
    "polygon": "polygon-pos",
    "avalanche": "avalanche",
    "bsc": "binance-smart-chain",
    "sonic": "sonic",
    "mantle": "mantle",
    "berachain": "berachain",
    "monad": "monad",
    "xlayer": "xlayer",
    "zerog": "zerog",
    "linea": "linea",
    "blast": "blast",
    "plasma": "plasma",
    "hyperevm": "hyperevm",
    "robinhood": "robinhood",
}

# 2. DexScreener CHAIN_TO_DEXSCREENER_PLATFORM (carries the "bnb" alias).
FROZEN_DEXSCREENER_PLATFORM: dict[str, str] = {
    "ethereum": "ethereum",
    "arbitrum": "arbitrum",
    "base": "base",
    "optimism": "optimism",
    "polygon": "polygon",
    "bsc": "bsc",
    "bnb": "bsc",
    "avalanche": "avalanche",
    "sonic": "sonic",
    "blast": "blast",
    "linea": "linea",
    "mantle": "mantle",
    "berachain": "berachain",
    "monad": "monad",
    "plasma": "plasma",
    "xlayer": "xlayer",
    "zerog": "zerog",
    "solana": "solana",
    "hyperevm": "hyperevm",
    "robinhood": "robinhood",
}

# 3. DexScreener CHAIN_SLUG_MAP (no "bnb"; same "dexscreener" vendor as #2).
FROZEN_DEXSCREENER_SLUG: dict[str, str] = {
    "ethereum": "ethereum",
    "arbitrum": "arbitrum",
    "optimism": "optimism",
    "base": "base",
    "polygon": "polygon",
    "avalanche": "avalanche",
    "bsc": "bsc",
    "sonic": "sonic",
    "mantle": "mantle",
    "berachain": "berachain",
    "monad": "monad",
    "xlayer": "xlayer",
    "zerog": "zerog",
    "blast": "blast",
    "linea": "linea",
    "plasma": "plasma",
    "solana": "solana",
    "hyperevm": "hyperevm",
    "robinhood": "robinhood",
}

# 4. CoinGecko Onchain _CHAIN_TO_NETWORK (has mantle).
FROZEN_COINGECKO_ONCHAIN_NETWORK: dict[str, str] = {
    "ethereum": "eth",
    "arbitrum": "arbitrum",
    "base": "base",
    "optimism": "optimism",
    "polygon": "polygon_pos",
    "avalanche": "avax",
    "bsc": "bsc",
    "sonic": "sonic",
    "solana": "solana",
    "mantle": "mantle",
    "robinhood": "robinhood",
}

# 5. CoinGecko Onchain _CHAIN_TO_CG_ONCHAIN_NETWORK (no mantle; same "coingecko_onchain" vendor).
FROZEN_COINGECKO_ONCHAIN_GT: dict[str, str] = {
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

# 6. DeFiLlama slug (lowercase).
FROZEN_DEFILLAMA: dict[str, str] = {
    "ethereum": "ethereum",
    "arbitrum": "arbitrum",
    "base": "base",
    "optimism": "optimism",
    "polygon": "polygon",
    "avalanche": "avax",
    "bsc": "bsc",
    "sonic": "sonic",
    "robinhood": "robinhood-chain",
}

# 7. DeFiLlama display (Capitalised) — _history_common.
FROZEN_DEFILLAMA_DISPLAY: dict[str, str] = {
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

# 8. DeFiLlama display — aggregator (must be byte-identical to #7).
FROZEN_DEFILLAMA_DISPLAY_AGG: dict[str, str] = {
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

# 9. Zerion _CHAIN_IDS (carries the "bnb" alias).
FROZEN_ZERION: dict[str, str] = {
    "ethereum": "ethereum",
    "arbitrum": "arbitrum",
    "optimism": "optimism",
    "base": "base",
    "avalanche": "avalanche",
    "polygon": "polygon",
    "bsc": "binance-smart-chain",
    "bnb": "binance-smart-chain",
    "solana": "solana",
    "sonic": "sonic",
    "plasma": "plasma",
}

# 10. Moralis _CHAIN_SLUGS (carries the "bnb" alias; solana INTENTIONALLY absent).
FROZEN_MORALIS: dict[str, str] = {
    "ethereum": "eth",
    "polygon": "polygon",
    "bsc": "bsc",
    "bnb": "bsc",
    "avalanche": "avalanche",
    "arbitrum": "arbitrum",
    "optimism": "optimism",
    "base": "base",
    "sonic": "sonic",
}

# 11. OKX _CHAIN_IDS (carries the "bnb" alias; "solana": "501" is synthetic).
# Legacy ``TENDERLY_CHAIN_SLUGS`` in framework/models/reproduction_bundle.py
# (VIB-4851 CS-4). Tenderly DASHBOARD URL slugs — distinct from the Tenderly
# simulation network id, which is always str(chain_id) by SimulationProfile
# design and is deliberately not stored as an external id.
FROZEN_TENDERLY: dict[str, str] = {
    "ethereum": "mainnet",
    "arbitrum": "arbitrum",
    "optimism": "optimism",
    "polygon": "polygon",
    "base": "base",
    "avalanche": "avalanche",
    "bsc": "bsc",
}


FROZEN_OKX: dict[str, str] = {
    "ethereum": "1",
    "optimism": "10",
    "bsc": "56",
    "bnb": "56",
    "polygon": "137",
    "base": "8453",
    "arbitrum": "42161",
    "avalanche": "43114",
    "sonic": "146",
    "solana": "501",
}

# Pure-alias keys that the descriptor model resolves through the registry
# rather than storing as a separate chain. ``integration_chain_id`` still answers
# these (via ``ChainRegistry.try_resolve``); ``integration_chain_map`` (canonical
# names only) does not.
ALIAS_KEYS = frozenset({"bnb"})

# Provider-verified additions made during the typed inversion. Keeping these
# separate preserves the frozen legacy maps above while making every widening
# explicit and reviewable.
PINNED_VENDOR_WIDENINGS: dict[str, dict[str, str]] = {
    "defillama": {"solana": "solana"},
}


def _frozen_for(vendor: str) -> dict[str, str]:
    """The reconciled OLD map for ``vendor`` (collapses resolved by name)."""
    if vendor == "coingecko":
        return dict(FROZEN_COINGECKO)
    if vendor == "dexscreener":
        # #2 ∪ #3, value-identical on all shared canonical chains; #2 adds "bnb".
        merged = {**FROZEN_DEXSCREENER_SLUG, **FROZEN_DEXSCREENER_PLATFORM}
        return merged
    if vendor == "coingecko_onchain":
        # #4 ∪ #5, value-identical on all 9 shared chains; #4 adds "mantle".
        merged = {**FROZEN_COINGECKO_ONCHAIN_GT, **FROZEN_COINGECKO_ONCHAIN_NETWORK}
        return merged
    if vendor == "defillama":
        return dict(FROZEN_DEFILLAMA)
    if vendor == "defillama_display":
        return dict(FROZEN_DEFILLAMA_DISPLAY)
    if vendor == "zerion":
        return dict(FROZEN_ZERION)
    if vendor == "moralis":
        return dict(FROZEN_MORALIS)
    if vendor == "okx":
        return dict(FROZEN_OKX)
    if vendor == "tenderly":
        return dict(FROZEN_TENDERLY)
    raise AssertionError(f"no frozen map for vendor {vendor!r}")


def _expected_for(vendor: str) -> dict[str, str]:
    """Return legacy coverage plus explicitly pinned provider support."""
    return {**_frozen_for(vendor), **PINNED_VENDOR_WIDENINGS.get(vendor, {})}


ALL_VENDORS = (
    "coingecko",
    "dexscreener",
    "coingecko_onchain",
    "defillama",
    "defillama_display",
    "zerion",
    "moralis",
    "okx",
    "tenderly",
)


# --- per-vendor bounded widening: legacy + pinned support only -----------------


@pytest.mark.parametrize("vendor", ALL_VENDORS)
def test_integration_chain_map_does_not_widen_implicitly(vendor: str) -> None:
    # THE most important invariant: the derive declares support for exactly
    # the legacy canonical chains plus reviewed provider-verified additions.
    expected = _expected_for(vendor)
    expected_chains = set(expected) - ALIAS_KEYS
    assert set(integration_chain_map(vendor)) == expected_chains, (
        f"{vendor}: derived support {set(integration_chain_map(vendor))} != frozen canonical support {expected_chains}"
    )


# --- per-vendor value/format parity (verbatim, incl. case) ----------------------


@pytest.mark.parametrize("vendor", ALL_VENDORS)
def test_external_id_values_match_frozen(vendor: str) -> None:
    # Catches arbitrum-one vs arbitrum (coingecko), eth vs ethereum
    # (coingecko_onchain/moralis), avax vs avalanche (defillama), and the
    # lowercase/Capitalised DeFiLlama split.
    expected = _expected_for(vendor)
    for chain, vid in expected.items():
        assert integration_chain_id(chain, vendor) == vid, (
            f"{vendor}/{chain}: {integration_chain_id(chain, vendor)!r} != {vid!r}"
        )


# --- non-derivable literals: OKX solana 501 is stored, not computed -------------


def test_okx_solana_is_synthetic_literal() -> None:
    # "501" is an OKX-specific id, NOT Solana's chain id (Solana has no EIP-155
    # id and is not indexed by ChainRegistry.by_id). The value must be stored
    # verbatim on the descriptor, never derived from a chain id.
    assert integration_chain_id("solana", "okx") == "501"
    with pytest.raises(ValueError):
        ChainRegistry.by_id(501)


# --- miss / fail-closed semantics -----------------------------------------------


def test_integration_chain_id_misses_fail_closed() -> None:
    # A registered chain a vendor does not support -> None (OKX never listed
    # berachain).
    assert integration_chain_id("berachain", "okx") is None
    # An unregistered chain -> None.
    assert integration_chain_id("not-a-chain", "coingecko") is None
    # An empty vendor -> None.
    assert integration_chain_id("ethereum", "") is None
    # An empty chain -> None.
    assert integration_chain_id("", "coingecko") is None
    # An unknown integration is also a clean miss — guard against any chain
    # silently gaining a provider mapping it lacks.
    assert integration_chain_id("ethereum", "definitely-not-a-vendor") is None


# --- alias resolution: "bnb" resolves to bsc through the registry ---------------


def test_alias_resolves_through_registry() -> None:
    # The legacy maps carried explicit "bnb" keys; the descriptor stores the id
    # on bsc only and resolves the alias via ChainRegistry.try_resolve.
    assert integration_chain_id("bnb", "okx") == "56"
    assert integration_chain_id("bnb", "dexscreener") == "bsc"
    assert integration_chain_id("bnb", "zerion") == "binance-smart-chain"
    assert integration_chain_id("bnb", "moralis") == "bsc"
    # Guard the alias is real (and canonicalises to bsc) so the assertions above
    # are meaningful rather than accidentally passing on a missing chain.
    resolved = ChainRegistry.try_resolve("bnb")
    assert resolved is not None
    assert resolved.name == "bsc"


# --- drift reconciliations, pinned by name --------------------------------------


def test_dexscreener_collapse_keeps_all_canonical_chains() -> None:
    # #2 (CHAIN_TO_DEXSCREENER_PLATFORM) and #3 (CHAIN_SLUG_MAP) agreed on every
    # shared canonical chain; the only structural difference was #2's "bnb"
    # alias. Pin that optimism (present in BOTH) survives the collapse and that
    # the reconciled key-set is exactly the 19 canonical chains.
    assert integration_chain_id("optimism", "dexscreener") == "optimism"
    expected = (set(FROZEN_DEXSCREENER_PLATFORM) | set(FROZEN_DEXSCREENER_SLUG)) - ALIAS_KEYS
    assert set(integration_chain_map("dexscreener")) == expected
    assert len(expected) == 19


def test_coingecko_onchain_collapse_is_union_with_mantle() -> None:
    # #4 (_CHAIN_TO_NETWORK) carried mantle; #5 (_CHAIN_TO_CG_ONCHAIN_NETWORK) did not.
    # The collapse is the union, so BOTH mantle and solana must be present by
    # name (they came from different source maps).
    gt_map = integration_chain_map("coingecko_onchain")
    assert "mantle" in gt_map  # only in #4
    assert "solana" in gt_map  # in both #4 and #5
    assert integration_chain_id("mantle", "coingecko_onchain") == "mantle"
    assert integration_chain_id("solana", "coingecko_onchain") == "solana"
    expected = set(FROZEN_COINGECKO_ONCHAIN_NETWORK) | set(FROZEN_COINGECKO_ONCHAIN_GT)
    assert set(gt_map) == expected


def test_defillama_slug_and_display_share_keys_differ_in_format() -> None:
    # #7 and #8 were byte-identical and already included Solana in the display
    # map. Only the lowercase slug map (#6) gains Solana through
    # PINNED_VENDOR_WIDENINGS["defillama"]. The widening makes both typed
    # projections cover the same chains while preserving their distinct value
    # formats (lowercase provider slug vs capitalised display label).
    assert FROZEN_DEFILLAMA_DISPLAY == FROZEN_DEFILLAMA_DISPLAY_AGG
    slug_map = integration_chain_map("defillama")
    display_map = integration_chain_map("defillama_display")
    assert set(slug_map) == set(_expected_for("defillama"))
    assert set(display_map) == set(FROZEN_DEFILLAMA_DISPLAY)
    # The shared chains differ only in case (slug lowercase, display Capitalised).
    assert integration_chain_id("ethereum", "defillama") == "ethereum"
    assert integration_chain_id("ethereum", "defillama_display") == "Ethereum"
    assert integration_chain_id("bsc", "defillama") == "bsc"
    assert integration_chain_id("bsc", "defillama_display") == "BSC"
    assert integration_chain_id("solana", "defillama") == "solana"
    assert integration_chain_id("solana", "defillama_display") == "Solana"


def test_moralis_omits_solana() -> None:
    # Moralis intentionally has no solana entry; the derive must not invent one.
    assert integration_chain_id("solana", "moralis") is None
    assert "solana" not in integration_chain_map("moralis")


def test_external_chain_ids_are_typed_and_immutable() -> None:
    external_ids = ChainRegistry.get("ethereum").external_ids
    assert external_ids.coingecko == "ethereum"
    assert external_ids.get(ExternalIdProvider.COINGECKO) == "ethereum"
    assert external_ids.get("COINGECKO") == "ethereum"
    assert external_ids.get("not-a-provider") is None
    with pytest.raises(FrozenInstanceError):
        external_ids.coingecko = "changed"  # type: ignore[misc]
    with pytest.raises(TypeError):
        external_ids.as_mapping()["coingecko"] = "changed"  # type: ignore[index]


def test_external_id_schema_matches_frozen_provider_set() -> None:
    assert {field.name for field in fields(ExternalChainIds)} == {provider.value for provider in ExternalIdProvider}
    declared = {provider.value for provider in ExternalIdProvider if external_chain_id_map(provider)}
    assert declared == set(ALL_VENDORS)


def test_chain_descriptor_requires_typed_external_ids() -> None:
    with pytest.raises(TypeError, match="external_ids must be ExternalChainIds"):
        replace(ChainRegistry.get("ethereum"), external_ids={"coingecko": "ethereum"})  # type: ignore[arg-type]


@pytest.mark.parametrize("value", ["", " trailing ", 1])
def test_external_chain_ids_reject_invalid_values(value: object) -> None:
    with pytest.raises(ValueError, match="coingecko"):
        ExternalChainIds(coingecko=value)  # type: ignore[arg-type]
