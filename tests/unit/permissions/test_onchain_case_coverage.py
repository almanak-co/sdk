"""Gate: every ``(protocol, intent_type)`` the manifest generator covers must
have at least one default-on-Zodiac intent test (or an explicit exemption).

Companion to ``test_connector_coverage.py`` (Q1 hint-declaration gate). Where
Q1 asks "did the connector author consider permissions?" this gate asks "is
the generated manifest exercised against a real Zodiac Roles Modifier?"

Authoritative capability source:
    ``almanak.framework.permissions.synthetic_intents.get_protocol_intent_matrix``

Single coverage path (post-Phase-G.4 — case-file dual-path retired):
    An intent test in ``tests/intents/<chain>/test_*.py`` constructs an
    intent matching the pair (e.g. ``SupplyIntent(protocol="aave_v3", ...)``)
    AND neither the enclosing function/class nor an outer scope carries
    ``@pytest.mark.no_zodiac(...)``. The test runs through Safe + Roles
    automatically, so any manifest regression fails the test in the nightly.

Exemption sentinels — connectors that legitimately can't be Zodiac-tested:
    ``tests/intents/zodiac_exemptions/<protocol>.permissions_onchain_exempt``
    (one-line justification inside; the gate skips the pair). The legacy
    ``tests/intents/permission_cases/`` directory was retired in Phase G.4,
    along with the per-chain ``test_permission_onchain.py`` runners and the
    case-file dual-path that previously covered pairs lacking an intent test.

Plan doc: ``docs/internal/zodiac-permission-onchain-coverage-plan.md``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from almanak.framework.permissions.synthetic_intents import get_protocol_intent_matrix
from tests.unit.permissions._marker_discovery import collect_intent_test_coverage

INTENTS_DIR = Path(__file__).resolve().parents[2] / "intents"
EXEMPTIONS_DIR = INTENTS_DIR / "zodiac_exemptions"
_SENTINEL_SUFFIX = ".permissions_onchain_exempt"

# Per-pair deferrals — pairs in the matrix whose intent test is intentionally
# absent and won't be added in this PR. Pre-G.4 these lived as
# ``DEFERRED_INTENT_TYPES`` lists inside each ``permission_cases/<proto>.py``
# module; under the simplified gate the declaration is a single constant in
# this file. Each entry SHOULD link to a follow-up issue.
#
# Adding to this dict is a deliberate, review-checkable action — reviewers
# should prefer "write the intent test" or "exempt the whole protocol via
# a sentinel" over "add a deferral here."
_DEFERRED_PAIRS: dict[tuple[str, str], str] = {
    ("traderjoe_v2", "LP_COLLECT_FEES"): "Requires a ``CollectFeesIntent`` harness branch; see plan doc Phase F P1.",
}


def _is_deferred(protocol: str, intent_type: str) -> str | None:
    return _DEFERRED_PAIRS.get((protocol.lower(), intent_type.upper()))


def _has_exemption(protocol: str) -> bool:
    """True iff a sentinel file exempts ``protocol`` from on-chain coverage."""
    return (EXEMPTIONS_DIR / f"{protocol}{_SENTINEL_SUFFIX}").exists()


def _enumerate_required_pairs() -> list[tuple[str, str]]:
    """Flatten the capability matrix into ``(protocol, intent_type_value)`` pairs."""
    matrix = get_protocol_intent_matrix()
    pairs: list[tuple[str, str]] = []
    for protocol in sorted(matrix):
        for intent_type in sorted(t.value for t in matrix[protocol]):
            pairs.append((protocol, intent_type))
    return pairs


def _intent_test_covers(protocol: str, intent_type: str) -> bool:
    """True iff at least one non-``no_zodiac`` intent test exercises the pair."""
    return (protocol.lower(), intent_type.upper()) in collect_intent_test_coverage()


@pytest.mark.parametrize(
    ("protocol", "intent_type"),
    _enumerate_required_pairs(),
    ids=lambda value: value,
)
def test_protocol_intent_has_onchain_case(protocol: str, intent_type: str) -> None:
    """Each ``(protocol, intent_type)`` must be covered by an intent test
    or an exemption sentinel."""
    if _has_exemption(protocol):
        sentinel = EXEMPTIONS_DIR / f"{protocol}{_SENTINEL_SUFFIX}"
        pytest.skip(
            f"Protocol '{protocol}' opts out of on-chain permission coverage "
            f"via {sentinel.name}. Remove the sentinel and add an intent test "
            f"in tests/intents/<chain>/ before enabling this protocol in a "
            f"Zodiac-gated deployment."
        )

    deferral_reason = _is_deferred(protocol, intent_type)
    if deferral_reason is not None:
        pytest.skip(
            f"Pair ({protocol!r}, {intent_type!r}) is in _DEFERRED_PAIRS: "
            f"{deferral_reason} Remove the entry from _DEFERRED_PAIRS in "
            f"{__file__} when the intent test lands."
        )

    if _intent_test_covers(protocol, intent_type):
        return

    pytest.fail(
        f"Protocol '{protocol}' is covered by the manifest generator "
        f"(supports intent type {intent_type}) but no intent test in "
        f"tests/intents/<chain>/ constructs an intent matching this pair.\n\n"
        f"Add an intent test under tests/intents/<chain>/ that builds an intent like:\n"
        f'    <IntentClass>(protocol="{protocol}", chain="...", ...)\n'
        f"Default-on Zodiac means the test will exercise the manifest\n"
        f"automatically — no marker needed.\n\n"
        f"Or mark the protocol exempt by touching\n"
        f"tests/intents/zodiac_exemptions/{protocol}{_SENTINEL_SUFFIX}\n"
        f"with a one-line justification."
    )


def test_intent_class_to_type_lockstep() -> None:
    """Discovery and harness intent-class tables must agree exactly.

    The discovery scanner uses its copy to recognise intent constructors at
    gate time; the harness uses its copy at execute time as a fallback when
    an Intent instance is missing the ``.intent_type`` attribute. If one
    table grows a new entry without the other, the gate silently un-covers
    that intent class — so we pin equality here.
    """
    from tests.intents._permission_onchain_harness import _INTENT_CLASS_TO_TYPE
    from tests.unit.permissions._marker_discovery import INTENT_CLASS_TO_TYPE

    assert INTENT_CLASS_TO_TYPE == _INTENT_CLASS_TO_TYPE, (
        f"Discovery / harness intent-class tables drifted.\n"
        f"  Only in discovery: {sorted(set(INTENT_CLASS_TO_TYPE) - set(_INTENT_CLASS_TO_TYPE))}\n"
        f"  Only in harness:   {sorted(set(_INTENT_CLASS_TO_TYPE) - set(INTENT_CLASS_TO_TYPE))}\n"
        f"  Different values:  "
        f"{sorted({k for k in INTENT_CLASS_TO_TYPE if k in _INTENT_CLASS_TO_TYPE and INTENT_CLASS_TO_TYPE[k] != _INTENT_CLASS_TO_TYPE[k]})}"
    )
