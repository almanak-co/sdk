"""Characterization test: registry-driven ``materializer_primitive_for`` ≡ old ladder.

The protocol→``Primitive`` dispatch in
:func:`almanak.framework.primitives.taxonomy.materializer_primitive_for` was
refactored from a hard-coded upper-cased if-ladder to a connector-owned
:class:`~almanak.connectors._strategy_base.primitive_registry.PrimitiveRegistry`
(per ``docs/internal/blueprints/22-connector-self-containment.md``). This
function is consumed by the accounting position-state materializer
(``accounting.position_state._classify_position``) and the
Primitive version-stamping system — a misclassification corrupts the books.

This test pins the **exact** equivalence: for every position-type string the
old ladder ever handled (including all alias / short forms and the
non-protocol buckets), the registry-driven result MUST equal the old ladder's
result. The old ladder is preserved verbatim below as the oracle so a future
change to the registry or the generic table that drifts from the historical
classification fails loudly here.

This is a pure dispatch-mechanism refactor: ``Primitive`` enum semantics,
``MATCHING_POLICY_VERSIONS``, and ``PRIMITIVE_VERSIONS`` are unchanged.
"""

from __future__ import annotations

import pytest

from almanak.framework.primitives.taxonomy import materializer_primitive_for
from almanak.framework.primitives.types import Primitive


def _old_ladder(position_type_str: str) -> Primitive | None:
    """Verbatim copy of the pre-refactor hard-coded dispatch ladder (the oracle).

    Mirrors ``materializer_primitive_for`` as it existed before the
    connector-owned registry refactor. Do NOT "simplify" this — it is the
    frozen reference implementation the refactor must reproduce exactly.
    """
    s = position_type_str.upper().strip()
    if s in {"UNI_V4", "UNISWAP_V4"}:
        return Primitive.LP_V4
    if s in {"LP", "UNI_V3", "UNISWAP_V3", "AERODROME", "AERODROME_LP", "TRADERJOE_LP"}:
        return Primitive.LP
    if s in {
        "LENDING",
        "SUPPLY",
        "BORROW",
        "AAVE_V3",
        "AAVE",
        "MORPHO",
        "MORPHO_BLUE",
        "COMPOUND_V3",
        "COMPOUND",
    }:
        return Primitive.LENDING
    if s in {"PERP", "GMX", "GMX_V2", "DRIFT", "HYPERLIQUID"}:
        return Primitive.PERP
    if s in {"VAULT", "ERC4626"}:
        return Primitive.VAULT
    if s in {"STAKE", "STAKING", "STAKED"}:
        return Primitive.STAKING
    if s in {"PREDICTION", "POLYMARKET"}:
        return Primitive.PREDICTION
    if s in {"CEX", "TOKEN", "BALANCE"}:
        return Primitive.UTILITY
    return None


# Every label the old ladder handled, in its canonical declared form.
_LADDER_LABELS: tuple[str, ...] = (
    # LP_V4
    "UNI_V4",
    "UNISWAP_V4",
    # LP
    "LP",
    "UNI_V3",
    "UNISWAP_V3",
    "AERODROME",
    "AERODROME_LP",
    "TRADERJOE_LP",
    # LENDING
    "LENDING",
    "SUPPLY",
    "BORROW",
    "AAVE_V3",
    "AAVE",
    "MORPHO",
    "MORPHO_BLUE",
    "COMPOUND_V3",
    "COMPOUND",
    # PERP
    "PERP",
    "GMX",
    "GMX_V2",
    "DRIFT",
    "HYPERLIQUID",
    # VAULT
    "VAULT",
    "ERC4626",
    # STAKING
    "STAKE",
    "STAKING",
    "STAKED",
    # PREDICTION
    "PREDICTION",
    "POLYMARKET",
    # UTILITY bucket (CEX / TOKEN / BALANCE)
    "CEX",
    "TOKEN",
    "BALANCE",
)

# Casing / whitespace variants the normalization (.upper().strip()) must fold,
# plus strings the ladder did not handle (must resolve to None).
_VARIANT_AND_UNKNOWN_LABELS: tuple[str, ...] = (
    # casing / whitespace variants
    "aave_v3",
    "  Gmx_V2  ",
    "uni_v3",
    "polymarket",
    "  lp  ",
    "Erc4626",
    # unknowns — the ladder returned None for these
    "UNKNOWN",
    "MAKER",
    "",
    "FOO_BAR",
    "CDP",
    "LIQUIDATION",
    "SWAP",
    "BRIDGE",
)


@pytest.mark.parametrize("label", _LADDER_LABELS + _VARIANT_AND_UNKNOWN_LABELS)
def test_registry_result_equals_old_ladder(label: str) -> None:
    """Registry-driven dispatch reproduces the old ladder exactly."""
    assert materializer_primitive_for(label) == _old_ladder(label), (
        f"materializer_primitive_for({label!r}) diverged from the frozen "
        "pre-refactor ladder — this is an accounting-critical regression."
    )


def test_every_ladder_label_resolves_non_none() -> None:
    """Sanity: every label the ladder handled still resolves to a primitive."""
    for label in _LADDER_LABELS:
        assert materializer_primitive_for(label) is not None, (
            f"{label!r} regressed to None — it was a known primitive label."
        )
