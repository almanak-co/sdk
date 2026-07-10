"""Equivalence harness for the VIB-4851 B1 external-id inversion.

Eleven standalone per-vendor chain maps were folded onto
``ChainDescriptor.external_ids`` and now derive from the registry via
``external_id_for`` / ``vendor_chain_map``:

* CoinGecko ``gateway/data/price/coingecko.py::COINGECKO_PLATFORM_IDS``
* DexScreener ``gateway/data/price/dexscreener.py::CHAIN_TO_DEXSCREENER_PLATFORM``
  **+** ``gateway/services/dexscreener_lookup.py::CHAIN_SLUG_MAP`` (collapsed)
* GeckoTerminal ``gateway/data/ohlcv/geckoterminal_provider.py::_CHAIN_TO_NETWORK``
  **+** ``gateway/data/_history_common.py::_CHAIN_TO_GT_NETWORK`` (collapsed)
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

The most important assertion per vendor is **anti-widening**: the derived
``vendor_chain_map`` must declare support for *exactly* the chains the legacy
map did (minus pure aliases), never widening to chains a vendor lacks.

Three collapses were verified value-identical on every shared chain before the
fold and are pinned here by name:

* DexScreener #2/#3 agree on all 17 canonical chains; #2 additionally carried
  the ``"bnb"`` alias (dropped — ``external_id_for`` resolves it via the
  registry). The reconciled key-set is the 17 canonical chains.
* GeckoTerminal #4/#5 agree on all 9 shared chains; #4 additionally carried
  ``mantle``. The reconciled key-set is the union (10 chains).
* DeFiLlama display #7/#8 are byte-identical.
"""

from __future__ import annotations

from types import MappingProxyType

import pytest

from almanak.core.chains import ChainRegistry
from almanak.core.chains._descriptor import (
    KNOWN_VENDORS,
    ChainDescriptor,
    GasProfile,
    NativeToken,
)
from almanak.core.chains._helpers import external_id_for, vendor_chain_map
from almanak.core.enums import ChainFamily

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

# 4. GeckoTerminal _CHAIN_TO_NETWORK (has mantle).
FROZEN_GECKOTERMINAL_NETWORK: dict[str, str] = {
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

# 5. GeckoTerminal _CHAIN_TO_GT_NETWORK (no mantle; same "geckoterminal" vendor).
FROZEN_GECKOTERMINAL_GT: dict[str, str] = {
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
# rather than storing as a separate chain. ``external_id_for`` still answers
# these (via ``ChainRegistry.try_resolve``); ``vendor_chain_map`` (canonical
# names only) does not.
ALIAS_KEYS = frozenset({"bnb"})


def _frozen_for(vendor: str) -> dict[str, str]:
    """The reconciled OLD map for ``vendor`` (collapses resolved by name)."""
    if vendor == "coingecko":
        return dict(FROZEN_COINGECKO)
    if vendor == "dexscreener":
        # #2 ∪ #3, value-identical on all shared canonical chains; #2 adds "bnb".
        merged = {**FROZEN_DEXSCREENER_SLUG, **FROZEN_DEXSCREENER_PLATFORM}
        return merged
    if vendor == "geckoterminal":
        # #4 ∪ #5, value-identical on all 9 shared chains; #4 adds "mantle".
        merged = {**FROZEN_GECKOTERMINAL_GT, **FROZEN_GECKOTERMINAL_NETWORK}
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


ALL_VENDORS = (
    "coingecko",
    "dexscreener",
    "geckoterminal",
    "defillama",
    "defillama_display",
    "zerion",
    "moralis",
    "okx",
    "tenderly",
)


# --- per-vendor anti-widening: derived support == frozen support (minus aliases) -


@pytest.mark.parametrize("vendor", ALL_VENDORS)
def test_vendor_chain_map_does_not_widen(vendor: str) -> None:
    # THE most important invariant: the derive declares support for exactly the
    # chains the legacy vendor map covered (canonical only) — never more. A
    # widened map would claim a chain a vendor cannot actually serve.
    frozen = _frozen_for(vendor)
    expected_chains = set(frozen) - ALIAS_KEYS
    assert set(vendor_chain_map(vendor)) == expected_chains, (
        f"{vendor}: derived support {set(vendor_chain_map(vendor))} != frozen canonical support {expected_chains}"
    )


# --- per-vendor value/format parity (verbatim, incl. case) ----------------------


@pytest.mark.parametrize("vendor", ALL_VENDORS)
def test_external_id_values_match_frozen(vendor: str) -> None:
    # Catches arbitrum-one vs arbitrum (coingecko), eth vs ethereum
    # (geckoterminal/moralis), avax vs avalanche (defillama), and the
    # lowercase/Capitalised DeFiLlama split.
    frozen = _frozen_for(vendor)
    for chain, vid in frozen.items():
        assert external_id_for(chain, vendor) == vid, f"{vendor}/{chain}: {external_id_for(chain, vendor)!r} != {vid!r}"


# --- non-derivable literals: OKX solana 501 is stored, not computed -------------


def test_okx_solana_is_synthetic_literal() -> None:
    # "501" is an OKX-specific id, NOT Solana's chain id (Solana has no EIP-155
    # id and is not indexed by ChainRegistry.by_id). The value must be stored
    # verbatim on the descriptor, never derived from a chain id.
    assert external_id_for("solana", "okx") == "501"
    with pytest.raises(ValueError):
        ChainRegistry.by_id(501)


# --- miss / fail-closed semantics -----------------------------------------------


def test_external_id_for_misses_fail_closed() -> None:
    # A registered chain a vendor does not support -> None (OKX never listed
    # berachain).
    assert external_id_for("berachain", "okx") is None
    # An unregistered chain -> None.
    assert external_id_for("not-a-chain", "coingecko") is None
    # An empty vendor -> None.
    assert external_id_for("ethereum", "") is None
    # An empty chain -> None.
    assert external_id_for("", "coingecko") is None
    # A registered chain that declares no external_ids at all is also a clean
    # miss — guard against any chain silently gaining a vendor it lacks.
    assert external_id_for("ethereum", "definitely-not-a-vendor") is None


# --- alias resolution: "bnb" resolves to bsc through the registry ---------------


def test_alias_resolves_through_registry() -> None:
    # The legacy maps carried explicit "bnb" keys; the descriptor stores the id
    # on bsc only and resolves the alias via ChainRegistry.try_resolve.
    assert external_id_for("bnb", "okx") == "56"
    assert external_id_for("bnb", "dexscreener") == "bsc"
    assert external_id_for("bnb", "zerion") == "binance-smart-chain"
    assert external_id_for("bnb", "moralis") == "bsc"
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
    # the reconciled key-set is exactly the 17 canonical chains.
    assert external_id_for("optimism", "dexscreener") == "optimism"
    expected = (set(FROZEN_DEXSCREENER_PLATFORM) | set(FROZEN_DEXSCREENER_SLUG)) - ALIAS_KEYS
    assert set(vendor_chain_map("dexscreener")) == expected
    assert len(expected) == 19


def test_geckoterminal_collapse_is_union_with_mantle() -> None:
    # #4 (_CHAIN_TO_NETWORK) carried mantle; #5 (_CHAIN_TO_GT_NETWORK) did not.
    # The collapse is the union, so BOTH mantle and solana must be present by
    # name (they came from different source maps).
    gt_map = vendor_chain_map("geckoterminal")
    assert "mantle" in gt_map  # only in #4
    assert "solana" in gt_map  # in both #4 and #5
    assert external_id_for("mantle", "geckoterminal") == "mantle"
    assert external_id_for("solana", "geckoterminal") == "solana"
    expected = set(FROZEN_GECKOTERMINAL_NETWORK) | set(FROZEN_GECKOTERMINAL_GT)
    assert set(gt_map) == expected


def test_defillama_slug_and_display_share_keys_differ_in_format() -> None:
    # #7 and #8 were byte-identical; pin that the two DeFiLlama vendor keys
    # cover the same chains BUT carry different value formats (lowercase slug vs
    # Capitalised display) — the reason they are distinct vendor keys.
    assert FROZEN_DEFILLAMA_DISPLAY == FROZEN_DEFILLAMA_DISPLAY_AGG
    slug_map = vendor_chain_map("defillama")
    display_map = vendor_chain_map("defillama_display")
    # defillama (slug) lacks solana; defillama_display has it. They are NOT the
    # same key-set, which is exactly why the format-vs-coverage distinction
    # matters — assert each against its own frozen source rather than each
    # other.
    assert set(slug_map) == set(FROZEN_DEFILLAMA)
    assert set(display_map) == set(FROZEN_DEFILLAMA_DISPLAY)
    # The shared chains differ only in case (slug lowercase, display Capitalised).
    assert external_id_for("ethereum", "defillama") == "ethereum"
    assert external_id_for("ethereum", "defillama_display") == "Ethereum"
    assert external_id_for("bsc", "defillama") == "bsc"
    assert external_id_for("bsc", "defillama_display") == "BSC"


def test_moralis_omits_solana() -> None:
    # Moralis intentionally has no solana entry; the derive must not invent one.
    assert external_id_for("solana", "moralis") is None
    assert "solana" not in vendor_chain_map("moralis")


# --- descriptor-level field mechanics (mirrors the tokens-field precedent) -------


def _descriptor(external_ids: dict[str, str] | None) -> ChainDescriptor:
    """Build a throwaway descriptor with a given external_ids map.

    Never registered into the singleton here — construction does not touch
    ``ChainRegistry`` — so we exercise ``__post_init__`` in isolation without
    disturbing the process-wide registry the inversion assertions above read
    from.
    """
    return ChainDescriptor(
        name="ethereum",
        chain_id=1,
        family=ChainFamily.EVM,
        native=NativeToken(symbol="ETH", name="Ethereum", decimals=18),
        gas=GasProfile(),
        external_ids=external_ids,
    )


def test_external_ids_field_is_frozen_proxy() -> None:
    # Wrapped in MappingProxyType like ``tokens`` so descriptor immutability
    # survives a mutable dict literal at the call site.
    descriptor = ChainRegistry.try_resolve("ethereum")
    assert descriptor is not None
    assert descriptor.external_ids is not None
    assert isinstance(descriptor.external_ids, MappingProxyType)
    with pytest.raises(TypeError):
        descriptor.external_ids["new"] = "x"  # type: ignore[index]


def test_external_ids_vendor_keys_are_lowercased_values_verbatim() -> None:
    # Only the vendor KEY is lowercased; the value is stored verbatim (case is
    # load-bearing for DeFiLlama slug-vs-display).
    descriptor = _descriptor({"CoinGecko": "Arbitrum-One"})
    assert descriptor.external_ids is not None
    assert dict(descriptor.external_ids) == {"coingecko": "Arbitrum-One"}


def test_external_ids_unknown_vendor_raises() -> None:
    # A typo'd vendor must fail loudly at construction rather than silently
    # producing an id no lookup will ever find.
    with pytest.raises(ValueError, match="unknown external_ids vendor"):
        _descriptor({"gecko": "eth"})


def test_external_ids_none_stays_none() -> None:
    # The default / no-vendor-support case is None (not an empty proxy), matching
    # the legacy ``map.get(chain)`` miss semantics.
    assert _descriptor(None).external_ids is None


def test_known_vendors_matches_declared_vendor_set() -> None:
    # The frozen vendor set this test enumerates IS the descriptor's allowlist —
    # guard they stay in lockstep so a newly added vendor key can't slip the
    # anti-widening sweep above.
    assert KNOWN_VENDORS == set(ALL_VENDORS)
