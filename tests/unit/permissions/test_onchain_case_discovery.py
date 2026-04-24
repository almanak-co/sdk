"""Unit coverage for the Phase F case-discovery helpers.

``discover_cases`` and ``discover_negative_cases`` are the runtime counterpart
to the coverage gate in ``test_onchain_case_coverage.py`` — the gate enforces
that declarations exist, these helpers are what the per-chain runners use at
collection time to materialise the parametrize list. They must agree on which
files count as case files and must honour the same DEFERRED_INTENT_TYPES
semantics (uppercase-compared).

Plan doc: ``docs/internal/zodiac-permission-onchain-coverage-plan.md``.
"""

from __future__ import annotations

import pytest

from tests.intents._permission_onchain_harness import (
    PermissionTestCase,
    discover_cases,
    discover_negative_cases,
)


def test_discover_cases_arbitrum_includes_known_fixtures() -> None:
    """Arbitrum discovery includes the pilot SWAP + LEND anchors currently on main."""
    cases = discover_cases("arbitrum")
    pairs = {(c.protocol, c.intent_type.upper()) for c in cases}

    assert ("uniswap_v3", "SWAP") in pairs, (
        "Pilot SWAP case was expected on arbitrum. If the case file moved chains, "
        "update this anchor — don't just delete it, the runtime and the gate have "
        "to stay in agreement about what's active."
    )
    assert ("aave_v3", "SUPPLY") in pairs, (
        "Phase C LEND SUPPLY anchor missing on arbitrum."
    )


def test_discover_cases_returns_flat_sorted_list() -> None:
    """Return type is a flat list, sorted by (protocol, intent_type) for stable IDs."""
    cases = discover_cases("arbitrum")
    assert isinstance(cases, list)
    assert all(isinstance(c, PermissionTestCase) for c in cases)

    ordered = sorted(cases, key=lambda c: (c.protocol, c.intent_type.upper()))
    assert [(c.protocol, c.intent_type) for c in cases] == [
        (c.protocol, c.intent_type) for c in ordered
    ], (
        "discover_cases must return a deterministically sorted list so parametrize "
        "IDs don't flap between CI runs."
    )


def test_discover_cases_honours_deferred_intent_types() -> None:
    """``DEFERRED_INTENT_TYPES`` drops the intent type from runtime discovery.

    ``pendle.py`` defers SWAP at runtime (PT/YT resolution follow-up) — the
    declaration still exists for the coverage gate, but the runtime must skip it.
    """
    cases = discover_cases("arbitrum")
    pendle_entries = [c for c in cases if c.protocol == "pendle"]
    assert pendle_entries == [], (
        "pendle declares DEFERRED_INTENT_TYPES = ['SWAP']; discover_cases must drop it."
    )


def test_discover_cases_filters_strictly_by_chain() -> None:
    """``chain="bsc"`` does not match ``chain="bnb"`` — exact string equality."""
    bsc_cases = discover_cases("bsc")
    for case in bsc_cases:
        assert case.chain == "bsc", (
            f"Expected every case in discover_cases('bsc') to be chain=='bsc', "
            f"got {case.protocol}-{case.intent_type} on chain={case.chain!r}."
        )


def test_discover_cases_empty_for_unknown_chain() -> None:
    """A chain with zero declared cases returns an empty list, not an error."""
    assert discover_cases("chain-that-does-not-exist") == []


def test_discover_negative_cases_subset_of_discover_cases() -> None:
    """Negative discovery is always a subset of positive discovery on the same chain."""
    positive = discover_cases("arbitrum")
    negative = discover_negative_cases("arbitrum")

    positive_ids = {(c.protocol, c.intent_type, c.chain) for c in positive}
    for case in negative:
        assert (case.protocol, case.intent_type, case.chain) in positive_ids, (
            f"Negative case {case.protocol}-{case.intent_type} on {case.chain} "
            "is not in discover_cases — the two helpers diverged."
        )
        assert case.negative_selector is not None, (
            "discover_negative_cases must only return cases with a non-None "
            f"negative_selector; got {case.protocol}-{case.intent_type}."
        )


@pytest.mark.parametrize(
    "chain",
    ["arbitrum", "base", "ethereum", "optimism", "polygon", "bsc", "avalanche"],
)
def test_discover_cases_does_not_raise_for_supported_chains(chain: str) -> None:
    """Sanity: discovery is side-effect-free for every chain the nightly covers."""
    # Collection alone is the assertion — if a case module raises on import,
    # discover_cases would surface it here before the nightly runners hit it.
    result = discover_cases(chain)
    assert isinstance(result, list)
