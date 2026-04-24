"""Unit coverage for the Phase F case-discovery helpers.

``discover_cases`` and ``discover_negative_cases`` are the runtime counterpart
to the coverage gate in ``test_onchain_case_coverage.py`` — the gate enforces
that declarations exist, these helpers are what the per-chain runners use at
collection time to materialise the parametrize list. They must agree on which
files count as case files and must honour the same DEFERRED_INTENT_TYPES
semantics (uppercase-compared).

``discover_negative_cases`` used to filter by ``case.negative_selector is not
None``. It is now a thin alias for ``discover_cases`` because the negative
runner auto-derives a load-bearing selector from the generated manifest — every
active case is a negative-test candidate and cases that can't derive one skip
cleanly at runtime. The alias stays for back-compat with external callers.

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


def test_discover_negative_cases_matches_discover_cases() -> None:
    """``discover_negative_cases`` returns the same set as ``discover_cases``.

    Post-auto-derivation, every active case is a negative-test candidate —
    the runner decides case-by-case (by introspecting the generated manifest
    at runtime) whether to execute or skip. Static discovery can no longer
    predict that outcome, so the two helpers intentionally return the same
    list on every chain.
    """
    for chain in ("arbitrum", "base", "ethereum", "optimism", "polygon", "bsc", "avalanche"):
        positive = discover_cases(chain)
        negative = discover_negative_cases(chain)
        positive_ids = [(c.protocol, c.intent_type, c.chain) for c in positive]
        negative_ids = [(c.protocol, c.intent_type, c.chain) for c in negative]
        assert positive_ids == negative_ids, (
            f"On {chain}, discover_negative_cases diverged from discover_cases. "
            f"Positive: {positive_ids}. Negative: {negative_ids}. "
            "The two helpers are expected to return the same list post-auto-derivation."
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
