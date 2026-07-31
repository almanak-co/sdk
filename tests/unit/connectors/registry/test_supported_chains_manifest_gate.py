"""Cross-consumer invariants for unified connector chain support."""

from __future__ import annotations

from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.framework.cli.support_matrix import _build_matrix


def test_connector_owned_matrix_rows_never_exceed_exact_intent_coverage() -> None:
    rows = {(row["name"], row["category"]): set(row["chains"]) for row in _build_matrix()["protocols"]}
    for connector in CONNECTOR_REGISTRY.with_strategy_support():
        entries = connector.strategy_matrix_entries
        if entries is None:
            continue
        for entry in entries:
            expected = {
                chain for intent in entry.intents for chain in (connector.supported_chains_for_intent(intent) or ())
            }
            assert rows.get((entry.matrix_name, entry.category), set()) <= expected


def test_fluid_matrix_keeps_swap_and_lending_scopes_distinct() -> None:
    rows = {(row["name"], row["category"]): set(row["chains"]) for row in _build_matrix()["protocols"]}
    assert rows[("fluid", "swap")] == {
        "arbitrum",
        "base",
        "ethereum",
        "polygon",
    }
    assert rows[("fluid", "lending")] == {"arbitrum", "base"}


def test_agni_mantle_row_is_separate_from_uniswap_v3() -> None:
    """The alias publishes its own rows rather than collapsing into the canonical ones.

    Both protocols really are on Mantle — separate factory / position manager /
    quoter / router — so separateness is proven by the alias staying pinned to its
    single chain while the canonical row carries the full SWAP union, not by
    asserting mantle is absent from uniswap_v3.
    """
    rows = {(row["name"], row["category"]): set(row["chains"]) for row in _build_matrix()["protocols"]}
    assert rows[("agni_finance", "swap")] == {"mantle"}
    assert rows[("agni_finance", "lp")] == {"mantle"}
    assert rows[("agni_finance", "swap")] != rows[("uniswap_v3", "swap")]
    assert rows[("agni_finance", "swap")] < rows[("uniswap_v3", "swap")]
    # uniswap_v3's LP row stays narrower than its swap row: mantle is SWAP-only
    # (no LP intent suite), enforced by `intent_overrides` on the manifest.
    assert "mantle" not in rows[("uniswap_v3", "lp")]
    assert rows[("uniswap_v3", "lp")] < rows[("uniswap_v3", "swap")]


def test_intent_overrides_cannot_be_widened_by_matrix_classification() -> None:
    for connector in CONNECTOR_REGISTRY.with_strategy_support():
        for entry in connector.strategy_matrix_entries or ():
            for intent in entry.intents:
                assert set(connector.supported_chains_for_intent(intent) or ()) <= set(connector.all_supported_chains)
