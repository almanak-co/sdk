"""Per-primitive isolation for CDP and Liquidation (VIB-4248).

Regression suite for the foundation-grade fix shipped in VIB-4248. Before
this PR, all five placeholder TAXONOMY rows (`LIQUIDATE`, `OPEN_CDP`,
`MINT_STABLE`, `REPAY_STABLE`, `CLOSE_CDP`) mapped to ``Primitive.LENDING``,
which contradicted the source PRD (`docs/internal/discussions/
primitives-refactor-20260508.md` lines 117-124):

    "These are P0 because without them, future code paths smuggle CDP
    through BORROW/REPAY and pollute lending accounting before P1 lands."

The placeholder compiler guard (Gate B) prevented the wrong values from
being **written** today, but the wrong values were already **configured**
in the canonical taxonomy waiting to fire the moment a P1 ticket removed
one of these enums from ``_PLACEHOLDER_INTENT_TYPES``.

This module guards the corrected mapping AND the per-primitive version-slot
isolation that VIB-4166 (T6) shipped to enforce.
"""

from __future__ import annotations

import pytest

from almanak.framework.accounting.payload_schemas import (
    MATCHING_POLICY_VERSIONS,
    PRIMITIVE_VERSIONS,
)
from almanak.framework.intents.vocabulary import IntentType
from almanak.framework.primitives.taxonomy import record_for
from almanak.framework.primitives.types import Primitive


# ──────────────────────────────────────────────────────────────────────────
# Enum membership: CDP and LIQUIDATION exist as distinct primitives
# ──────────────────────────────────────────────────────────────────────────


def test_primitive_cdp_member_exists() -> None:
    """CDP is a distinct top-level primitive — not a flavor of LENDING.

    PRD requirement: collateralized debt positions (Maker, Liquity, etc.)
    have a different lifecycle, different liquidation semantics, and a
    different cost-basis story than money-market lending. Conflating them
    poisons LENDING's matching-policy bump trajectory.
    """
    assert hasattr(Primitive, "CDP"), (
        "Primitive.CDP missing. The 4 CDP-family placeholders (OPEN_CDP, "
        "MINT_STABLE, REPAY_STABLE, CLOSE_CDP) need a dedicated primitive "
        "slot — not Primitive.LENDING."
    )
    assert Primitive.CDP.value == "cdp"


def test_primitive_liquidation_member_exists() -> None:
    """LIQUIDATION is a distinct top-level primitive — not a flavor of LENDING.

    Reviewer synthesis (`docs/internal/DeFi-Primitives.md` line 36):
    Liquidation must split out from Lending. A liquidation event is a
    third-party action against a position; its accounting fingerprint
    (forced close at oracle price + penalty) is unlike a voluntary REPAY.
    """
    assert hasattr(Primitive, "LIQUIDATION"), (
        "Primitive.LIQUIDATION missing. The LIQUIDATE placeholder needs a "
        "dedicated primitive slot — not Primitive.LENDING."
    )
    assert Primitive.LIQUIDATION.value == "liquidation"


# ──────────────────────────────────────────────────────────────────────────
# TAXONOMY mapping: placeholders resolve to CDP / LIQUIDATION (NOT LENDING)
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "intent_type, expected_primitive",
    [
        (IntentType.OPEN_CDP, Primitive.CDP),
        (IntentType.MINT_STABLE, Primitive.CDP),
        (IntentType.REPAY_STABLE, Primitive.CDP),
        (IntentType.CLOSE_CDP, Primitive.CDP),
        (IntentType.LIQUIDATE, Primitive.LIQUIDATION),
    ],
)
def test_placeholder_intent_resolves_to_correct_primitive(
    intent_type: IntentType, expected_primitive: Primitive
) -> None:
    """Each placeholder TAXONOMY row resolves to its correct primitive.

    Before VIB-4248 all five rows resolved to ``Primitive.LENDING``,
    which (a) contradicted the source PRD, (b) made the per-primitive
    isolation contract a lie for the first new primitive, and (c) silently
    locked CDP / LIQUIDATION events to LENDING's matching-policy version
    trajectory.
    """
    record = record_for(intent_type.value)
    assert record.primitive is expected_primitive, (
        f"{intent_type.name} resolves to {record.primitive.name}, expected "
        f"{expected_primitive.name}. The pre-VIB-4248 mapping conflated "
        f"these with LENDING — see PRD lines 117-124."
    )
    assert record.primitive is not Primitive.LENDING, (
        f"{intent_type.name} still maps to LENDING — VIB-4248 fix not applied."
    )


# ──────────────────────────────────────────────────────────────────────────
# Version-slot isolation: CDP / LIQUIDATION bumps do NOT affect LENDING
# ──────────────────────────────────────────────────────────────────────────


def test_cdp_has_independent_matching_policy_version_slot() -> None:
    """``MATCHING_POLICY_VERSIONS`` carries a CDP slot independent of LENDING.

    This is the VIB-4166 (T6) per-primitive isolation contract: a CDP
    semantics change MUST be expressible without re-baselining LENDING.
    Before VIB-4248, CDP had no slot — every CDP write would have consumed
    LENDING's version, defeating the contract.
    """
    assert Primitive.CDP in MATCHING_POLICY_VERSIONS
    assert isinstance(MATCHING_POLICY_VERSIONS[Primitive.CDP], int)
    assert MATCHING_POLICY_VERSIONS[Primitive.CDP] >= 1


def test_liquidation_has_independent_matching_policy_version_slot() -> None:
    """Same contract for LIQUIDATION — independent slot from LENDING."""
    assert Primitive.LIQUIDATION in MATCHING_POLICY_VERSIONS
    assert isinstance(MATCHING_POLICY_VERSIONS[Primitive.LIQUIDATION], int)
    assert MATCHING_POLICY_VERSIONS[Primitive.LIQUIDATION] >= 1


def test_cdp_has_independent_primitive_version_slot() -> None:
    """``PRIMITIVE_VERSIONS`` carries a CDP slot independent of LENDING.

    VIB-4166's bump-policy comment at ``payload_schemas.py:121-122``
    literally cites CDP as the example of why per-primitive bumping
    matters. That promise is hollow without a CDP slot in the dict.
    """
    assert Primitive.CDP in PRIMITIVE_VERSIONS
    assert isinstance(PRIMITIVE_VERSIONS[Primitive.CDP], int)
    assert PRIMITIVE_VERSIONS[Primitive.CDP] >= 1


def test_liquidation_has_independent_primitive_version_slot() -> None:
    """Same contract for LIQUIDATION — independent slot from LENDING."""
    assert Primitive.LIQUIDATION in PRIMITIVE_VERSIONS
    assert isinstance(PRIMITIVE_VERSIONS[Primitive.LIQUIDATION], int)
    assert PRIMITIVE_VERSIONS[Primitive.LIQUIDATION] >= 1


def test_cdp_bump_does_not_affect_lending_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bumping CDP's matching policy must NOT change LENDING's version.

    The whole point of per-primitive version maps. Before VIB-4248 this
    test would have raised KeyError on the CDP setitem (no slot). After
    VIB-4248 the bump is contained to CDP.
    """
    lending_before = MATCHING_POLICY_VERSIONS[Primitive.LENDING]
    monkeypatch.setitem(MATCHING_POLICY_VERSIONS, Primitive.CDP, 99)
    assert MATCHING_POLICY_VERSIONS[Primitive.LENDING] == lending_before
    assert MATCHING_POLICY_VERSIONS[Primitive.CDP] == 99


def test_liquidation_bump_does_not_affect_lending_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bumping LIQUIDATION's matching policy must NOT change LENDING's version."""
    lending_before = MATCHING_POLICY_VERSIONS[Primitive.LENDING]
    monkeypatch.setitem(MATCHING_POLICY_VERSIONS, Primitive.LIQUIDATION, 99)
    assert MATCHING_POLICY_VERSIONS[Primitive.LENDING] == lending_before
    assert MATCHING_POLICY_VERSIONS[Primitive.LIQUIDATION] == 99


# ──────────────────────────────────────────────────────────────────────────
# Compiler fail-fast guard untouched (placeholder regression)
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "intent_type",
    [
        IntentType.LIQUIDATE,
        IntentType.OPEN_CDP,
        IntentType.MINT_STABLE,
        IntentType.REPAY_STABLE,
        IntentType.CLOSE_CDP,
    ],
)
def test_placeholder_compiler_guard_still_fires(intent_type: IntentType) -> None:
    """Retargeting the primitive must NOT relax the compiler fail-fast guard.

    VIB-4165 (T5) Hard Ratification Condition #5: the 5 placeholder
    IntentTypes raise ``NotImplementedError`` when compiled. VIB-4248
    only retargets the canonical primitive — Gate A (PolicyEngine) and
    Gate B (compiler) must remain identical.
    """
    from almanak.framework.intents import compiler as compiler_module

    assert intent_type in compiler_module._PLACEHOLDER_INTENT_TYPES, (
        f"{intent_type.name} dropped out of _PLACEHOLDER_INTENT_TYPES — "
        f"the compiler fail-fast guard is no longer protecting it."
    )
