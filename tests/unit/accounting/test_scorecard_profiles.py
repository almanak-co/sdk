"""Invariants for the Accountant Test scorecard-profile registry (G-A foundation).

The registry (``SCORECARD_PROFILES`` in ``accountant_test.py``) replaced three
per-primitive ``if/elif`` ladders. These tests lock the contracts that the
ladders enforced implicitly so a new profile cannot drift from the taxonomy, the
string contract, or the legacy G6 ε math.
"""

from __future__ import annotations

from decimal import Decimal
from typing import get_args

import pytest

from almanak.framework.accounting.accountant_test import (
    SCORECARD_PROFILES,
    ProfileName,
    _profile_for,
)
from almanak.framework.accounting.scorecard_profiles import G6Bases
from almanak.framework.primitives.taxonomy import (
    _LENDING_LIFECYCLE,
    _LP_LIFECYCLE,
    _PERP_LIFECYCLE,
)
from almanak.framework.primitives.types import Primitive

# Canonical taxonomy lifecycle constant expected for each profile's primitive.
# A profile whose canonical_primitive is not in this map needs its lifecycle
# source added here deliberately (so the equivalence below stays meaningful).
_TAXONOMY_LIFECYCLE_BY_PRIMITIVE = {
    Primitive.LP: _LP_LIFECYCLE,
    Primitive.LENDING: _LENDING_LIFECYCLE,
    Primitive.PERP: _PERP_LIFECYCLE,
    # Pendle PT rides the SWAP primitive (taxonomy: PENDLE_PT → Primitive.SWAP).
    # SWAP is atomic — no multi-step lifecycle constant — so the canonical
    # lifecycle is empty; the PT buy→sell round-trip is enforced by the PEN cell
    # pack, not the intent_type lifecycle guard (both legs are SWAP in the ledger).
    Primitive.SWAP: (),
}


def test_every_profile_canonical_primitive_is_real_enum() -> None:
    """Each profile carries a genuine ``Primitive`` enum member — the registry
    is no longer blind to the taxonomy (Blueprint 27 §2.4)."""
    for name, profile in SCORECARD_PROFILES.items():
        assert isinstance(profile.canonical_primitive, Primitive), (
            f"profile {name!r} canonical_primitive is not a Primitive enum member"
        )
        assert profile.name == name, f"profile {name!r} name field disagrees with its key"


def test_profile_lifecycle_matches_taxonomy() -> None:
    """Each profile's explicit ``required_lifecycle`` equals the taxonomy
    lifecycle constant for its canonical primitive.

    This is the §2.4 "taxonomy is the source of truth" guarantee done as a static
    assertion rather than a fragile runtime derivation (a representative-intent
    lookup could grab ``_LP_LIFECYCLE_WITH_FEES`` instead of ``_LP_LIFECYCLE``).
    Drift between the explicit tuple and the taxonomy fails CI loudly.
    """
    for name, profile in SCORECARD_PROFILES.items():
        expected = _TAXONOMY_LIFECYCLE_BY_PRIMITIVE.get(profile.canonical_primitive)
        assert expected is not None, (
            f"profile {name!r} maps to {profile.canonical_primitive!r} which has no "
            "taxonomy lifecycle constant registered in this test — add it deliberately"
        )
        assert profile.required_lifecycle == expected, (
            f"profile {name!r} required_lifecycle {profile.required_lifecycle} != "
            f"taxonomy {expected} for {profile.canonical_primitive}"
        )


def test_profile_keys_match_profilename_literal() -> None:
    """The registry keys and the ``ProfileName`` string contract cannot drift —
    a new entry without widening the ``Literal`` (or vice-versa) fails CI."""
    assert set(SCORECARD_PROFILES) == set(get_args(ProfileName))


def test_profile_for_raises_on_unknown() -> None:
    """``_profile_for`` fails loud on an unknown profile (the former G6 ``else``
    branch silently scored unknowns with perp's ε)."""
    with pytest.raises(ValueError, match="unknown scorecard profile"):
        _profile_for("not_a_profile")


@pytest.mark.parametrize(
    ("profile_name", "bases", "expected_base", "expected_label"),
    [
        # LP scales on notional_traded; debt/perp bases are ignored (zeros here).
        (
            "lp",
            G6Bases(notional_traded=Decimal("123.45"), max_debt=Decimal("999"), max_perp_notional=Decimal("999")),
            Decimal("123.45"),
            "notional_traded",
        ),
        # Looping scales on max(notional_traded, max_debt) — debt dominates here.
        (
            "looping",
            G6Bases(notional_traded=Decimal("100"), max_debt=Decimal("250"), max_perp_notional=Decimal("999")),
            Decimal("250"),
            "max(notional_traded, max_debt_outstanding)",
        ),
        # Looping — notional dominates here.
        (
            "looping",
            G6Bases(notional_traded=Decimal("400"), max_debt=Decimal("250"), max_perp_notional=Decimal("999")),
            Decimal("400"),
            "max(notional_traded, max_debt_outstanding)",
        ),
        # Perp scales on max_perp_notional; trade/debt bases are ignored.
        (
            "perp",
            G6Bases(notional_traded=Decimal("999"), max_debt=Decimal("999"), max_perp_notional=Decimal("77.7")),
            Decimal("77.7"),
            "max_perp_notional",
        ),
    ],
)
def test_g6_eps_selectors_match_legacy(
    profile_name: str, bases: G6Bases, expected_base: Decimal, expected_label: str
) -> None:
    """The per-profile ε selector returns exactly what the former hardcoded
    ``if/elif/else`` ladder produced for the same computed bases."""
    profile = SCORECARD_PROFILES[profile_name]
    scaling_base, scaling_label = profile.eps_scaling(bases)
    assert scaling_base == expected_base
    assert scaling_label == expected_label


def test_g6_eps_pct_values_match_legacy() -> None:
    """The per-profile ε percent matches the former hardcoded values."""
    assert SCORECARD_PROFILES["lp"].eps_pct == Decimal("0.0025")
    assert SCORECARD_PROFILES["looping"].eps_pct == Decimal("0.0010")
    assert SCORECARD_PROFILES["perp"].eps_pct == Decimal("0.0005")
