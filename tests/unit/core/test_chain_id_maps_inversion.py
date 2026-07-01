"""Equivalence harness for the VIB-4851 A2 chain_id-map inversion.

The backtesting token registry's hardcoded ``_CHAIN_ID_TO_NAME`` matrix was folded
onto the chain registry and now derives via
``almanak.core.chains._helpers.chain_name_for_id`` (backed by the new non-raising
``ChainRegistry.try_resolve_id``). This test freezes the OLD map verbatim and
asserts the derived lookup reproduces it — proving the data, not the design —
mirroring the A1 ``test_native_symbols_inversion`` harness.

Scope note: the OKX (`_CHAIN_IDS`) and Moralis (`_CHAIN_SLUGS`) maps were NOT
inverted here. They are **vendor** identifiers / support-allowlists (OKX even uses
the synthetic id ``"501"`` for Solana, which the registry cannot reproduce), so the
chain registry cannot own them — they belong to the B1 ``external_ids`` unit. A2
covers only the universal ``chain_id <-> name`` data. Moralis's dead ``_CHAIN_IDS``
removal is guarded by ``tests/gateway/test_portfolio_multi_provider.py``.
"""

from __future__ import annotations

import pytest

from almanak.core.chains import ChainRegistry
from almanak.core.chains._helpers import chain_name_for_id
from almanak.core.enums import ChainFamily

# The OLD token_registry._CHAIN_ID_TO_NAME map, frozen verbatim from origin/main.
FROZEN_TOKEN_REGISTRY_ID_TO_NAME: dict[int, str] = {
    1: "ethereum",
    42161: "arbitrum",
    10: "optimism",
    8453: "base",
    43114: "avalanche",
    137: "polygon",
    56: "bsc",
    146: "sonic",
    9745: "plasma",
    81457: "blast",
    5000: "mantle",
    80094: "berachain",
}


def test_token_registry_ids_reproduced_exactly() -> None:
    for chain_id, name in FROZEN_TOKEN_REGISTRY_ID_TO_NAME.items():
        assert chain_name_for_id(chain_id) == name


def test_derive_is_correct_for_every_registry_evm_chain() -> None:
    # The source of truth is the registry, so the derive holds for EVERY EVM chain,
    # not just the 12 the backtesting map happened to list.
    for d in ChainRegistry.all():
        if d.family is ChainFamily.EVM:
            assert chain_name_for_id(d.chain_id) == d.name


def test_widening_over_the_legacy_subset_is_documented() -> None:
    # The derive covers EVM chains the legacy 12-entry map omitted. Pin the added
    # set by name so the widening is an explicit improvement, not a mystery diff
    # (the A2 analogue of A1's zerog gap). For these chains get_token_info now
    # proceeds to the resolver instead of returning None immediately, but the
    # local token data still lacks them, so the final result is unchanged.
    evm_names = {d.name for d in ChainRegistry.all() if d.family is ChainFamily.EVM}
    added = evm_names - set(FROZEN_TOKEN_REGISTRY_ID_TO_NAME.values())
    assert added == {"linea", "monad", "xlayer", "zerog", "hyperevm"}


def test_unknown_id_returns_none() -> None:
    # The consumer relies on None to fall through to the local TOKEN_REGISTRY;
    # the helper must never raise on an unregistered id.
    assert chain_name_for_id(0) is None  # Solana (chain_id 0, excluded from _by_id)
    assert chain_name_for_id(999999) is None
    assert chain_name_for_id(-1) is None


def test_solana_id_absence_is_real() -> None:
    # Deltas-are-real guard: Solana is registered (chain_id 0) but excluded from
    # by_id, which is exactly why chain_name_for_id(0) is None — matching the old
    # map, which also had no Solana entry.
    assert ChainRegistry.try_resolve("solana").chain_id == 0
    assert ChainRegistry.try_resolve_id(0) is None
    with pytest.raises(ValueError):
        ChainRegistry.by_id(0)
