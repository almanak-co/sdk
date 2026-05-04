"""Gate: every ``(protocol, intent_type)`` the manifest generator covers must
have at least one default-on-Zodiac intent test (or an explicit exemption).

Companion to ``test_connector_coverage.py`` (Q1 hint-declaration gate). Where
Q1 asks "did the connector author consider permissions?" this gate asks "is
the generated manifest exercised against a real Zodiac Roles Modifier?"

Authoritative capability source:
    ``almanak.framework.permissions.synthetic_intents.get_protocol_intent_matrix``

Coverage paths (post-Phase-G opt-out model — each path satisfies a pair):
    1. An intent test in ``tests/intents/<chain>/test_*.py`` constructs an
       intent matching the pair (e.g. ``SupplyIntent(protocol="aave_v3", ...)``)
       AND neither the enclosing function/class nor an outer scope carries
       ``@pytest.mark.no_zodiac(...)``. This is the canonical path: the test
       runs through Safe + Roles automatically, so any manifest regression
       fails the test in the nightly.
    2. A legacy ``tests/intents/permission_cases/<protocol>.py`` declares a
       ``PermissionTestCase`` for the pair. Retained transitionally for pairs
       that don't yet have an intent test (the per-chain runner consumes
       these via the harness). Retired in Phase G.4.

Exemption sentinels (read from BOTH directories during the G.3→G.4 migration
window so the eventual ``git mv`` is a pure no-semantic-change cleanup):
    - ``tests/intents/permission_cases/<protocol>.permissions_onchain_exempt`` (legacy)
    - ``tests/intents/zodiac_exemptions/<protocol>.permissions_onchain_exempt`` (post-G.4)

Plan doc: ``docs/internal/zodiac-permission-onchain-coverage-plan.md``.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from almanak.framework.permissions.synthetic_intents import get_protocol_intent_matrix
from tests.intents._permission_onchain_harness import PermissionTestCase
from tests.unit.permissions._marker_discovery import collect_intent_test_coverage

INTENTS_DIR = Path(__file__).resolve().parents[2] / "intents"
CASES_DIR = INTENTS_DIR / "permission_cases"
EXEMPTIONS_DIR = INTENTS_DIR / "zodiac_exemptions"
_SENTINEL_SUFFIX = ".permissions_onchain_exempt"


def _load_case_module(protocol: str):
    """Load ``permission_cases/<protocol>.py`` by path, returning the module or ``None``."""
    case_file = CASES_DIR / f"{protocol}.py"
    if not case_file.exists():
        return None
    spec = importlib.util.spec_from_file_location(f"_perm_cases.{protocol}", case_file)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load spec for {case_file}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _cases_from(module) -> list[PermissionTestCase]:
    cases = getattr(module, "CASES", None)
    if cases is None:
        raise AssertionError(f"{module.__file__} must export a top-level ``CASES: list[PermissionTestCase]``.")
    if not isinstance(cases, list) or any(not isinstance(c, PermissionTestCase) for c in cases):
        raise AssertionError(f"{module.__file__}.CASES must be a list of PermissionTestCase instances.")
    return cases


def _deferred_from(module) -> frozenset[str]:
    """Return the set of intent types this case file explicitly defers."""
    deferred = getattr(module, "DEFERRED_INTENT_TYPES", ())
    return frozenset(str(t).upper() for t in deferred)


def _exemption_paths(protocol: str) -> list[Path]:
    """Return all sentinel paths that exist for ``protocol`` (legacy + new dirs)."""
    candidates = [
        CASES_DIR / f"{protocol}{_SENTINEL_SUFFIX}",
        EXEMPTIONS_DIR / f"{protocol}{_SENTINEL_SUFFIX}",
    ]
    return [p for p in candidates if p.exists()]


def _has_exemption(protocol: str) -> bool:
    return bool(_exemption_paths(protocol))


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
    """Each ``(protocol, intent_type)`` must be covered by an intent test, a
    case file, or an exemption sentinel.

    Opt-out model: the canonical path is "intent test exists and isn't
    ``no_zodiac``-marked". Case files remain a transitional fallback retired
    in Phase G.4.
    """
    has_sentinel = _has_exemption(protocol)
    case_module = _load_case_module(protocol)

    if case_module is not None and has_sentinel:
        sentinel_locs = ", ".join(str(p) for p in _exemption_paths(protocol))
        pytest.fail(
            f"Protocol '{protocol}' has BOTH a case file "
            f"(permission_cases/{protocol}.py) AND an exemption sentinel "
            f"({sentinel_locs}). These are mutually exclusive — a case file "
            f"means the protocol is covered; the sentinel means it is "
            f"deferred. Delete the sentinel to enable coverage (or delete the "
            f"case file if the protocol should stay exempt)."
        )

    if has_sentinel:
        pytest.skip(
            f"Protocol '{protocol}' opts out of on-chain permission coverage "
            f"via {_exemption_paths(protocol)[0].name}. Remove the sentinel "
            f"and add either an intent test in tests/intents/<chain>/ or a "
            f"case file before enabling this protocol in a Zodiac-gated deployment."
        )

    if _intent_test_covers(protocol, intent_type):
        # Intent-test path satisfies coverage. Sanity-check the case file
        # (if one exists) is internally consistent so a stale entry can't
        # masquerade as covering something it doesn't.
        if case_module is not None:
            cases = _cases_from(case_module)
            stray = [c for c in cases if c.protocol != protocol]
            assert not stray, (
                f"permission_cases/{protocol}.py contains cases with the wrong "
                f"protocol: {[c.protocol for c in stray]}. Every case in this "
                f"file must use protocol='{protocol}'."
            )
        return

    # No intent test exercises this pair. Fall back to the legacy case-file path.
    if case_module is None:
        pytest.fail(
            f"Protocol '{protocol}' is covered by the manifest generator "
            f"(supports intent type {intent_type}) but no intent test in "
            f"tests/intents/<chain>/ constructs an intent matching this pair, "
            f"and no permission_cases/{protocol}.py case file exists.\n\n"
            f"Add either:\n"
            f"  (preferred) an intent test under tests/intents/<chain>/ that\n"
            f"  builds an intent like:\n"
            f'      <IntentClass>(protocol="{protocol}", chain="...", ...)\n'
            f"  Default-on Zodiac means the test will exercise the manifest\n"
            f"  automatically — no marker needed.\n\n"
            f"  (legacy fallback) tests/intents/permission_cases/{protocol}.py\n"
            f"  with a CASES list. Retired in Phase G.4.\n\n"
            f"Or mark the protocol exempt by touching\n"
            f"tests/intents/zodiac_exemptions/{protocol}{_SENTINEL_SUFFIX}\n"
            f"with a one-line justification."
        )

    if intent_type.upper() in _deferred_from(case_module):
        pytest.skip(
            f"Intent type '{intent_type}' for protocol '{protocol}' is "
            f"declared in DEFERRED_INTENT_TYPES in the case file. Add an "
            f"intent test exercising this pair (or a PermissionTestCase) and "
            f"remove the entry from DEFERRED_INTENT_TYPES to activate coverage."
        )

    cases = _cases_from(case_module)
    stray = [c for c in cases if c.protocol != protocol]
    assert not stray, (
        f"permission_cases/{protocol}.py contains cases with the wrong protocol: "
        f"{[c.protocol for c in stray]}. Every case in this file must use protocol='{protocol}'."
    )

    matching = [c for c in cases if c.intent_type.upper() == intent_type.upper()]
    assert matching, (
        f"Protocol '{protocol}' has no intent test exercising '{intent_type}' "
        f"and no PermissionTestCase covering it ({len(cases)} unrelated case(s) "
        f"declared). Add an intent test (preferred) or a PermissionTestCase to "
        f"permission_cases/{protocol}.py, or list the intent type in "
        f"DEFERRED_INTENT_TYPES if coverage is intentionally deferred."
    )


# -----------------------------------------------------------------------------
# G.4 readiness signal
# -----------------------------------------------------------------------------
#
# Phase G.4 (retire the parallel ``permission_cases/*.py`` runtime) cannot land
# cleanly until every matrix pair is exercised by an intent test. This
# baseline-ratchet test surfaces pairs that today still depend on the legacy
# case-file path so:
#
#   - new case-file-only entries (a regression — someone shipped a manifest
#     change without a corresponding intent test) fail loudly here;
#   - healed pairs (an intent test landed for a previously case-file-only
#     pair) force a baseline update so the picture stays accurate;
#   - the baseline doubles as a documented G.4 punch list.
#
# Each baseline entry needs a one-line reason explaining WHY no intent test
# exercises the pair yet. When the set goes empty, G.4 retirement is unblocked.

_LEGACY_ONLY_BASELINE: frozenset[tuple[str, str]] = frozenset(
    {
        # No test_aerodrome_lp.py intent test exists yet — needs one before
        # the manifest's LP path can be exercised under default-on Zodiac.
        ("aerodrome", "LP_OPEN"),
        ("aerodrome", "LP_CLOSE"),
    }
)


def _legacy_only_pairs() -> set[tuple[str, str]]:
    """Pairs satisfied by the legacy case-file path with no intent-test coverage.

    Excludes sentinel-exempt protocols and ``DEFERRED_INTENT_TYPES`` entries —
    those are intentional skips, not drift.
    """
    matrix = get_protocol_intent_matrix()
    required = {(proto.lower(), it.value.upper()) for proto, types in matrix.items() for it in types}
    intent_test_covered = set(collect_intent_test_coverage())
    legacy_only: set[tuple[str, str]] = set()
    for proto, itype in required - intent_test_covered:
        if _has_exemption(proto):
            continue
        case_module = _load_case_module(proto)
        if case_module is None:
            continue
        if itype in _deferred_from(case_module):
            continue
        cases = _cases_from(case_module)
        if any(c.intent_type.upper() == itype for c in cases):
            legacy_only.add((proto, itype))
    return legacy_only


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


def test_marker_migration_baseline_ratchet() -> None:
    """G.4 readiness: case-file-only pairs must not exceed the documented baseline.

    A new entry means a connector regressed onto the legacy path; fix it by
    adding an intent test exercising the pair under default-on Zodiac.

    A healed entry means the baseline is stale; remove it from
    ``_LEGACY_ONLY_BASELINE`` and update the comments.
    """
    legacy_only = _legacy_only_pairs()

    new_drift = legacy_only - _LEGACY_ONLY_BASELINE
    assert not new_drift, (
        f"New (protocol, intent_type) pairs are covered only by the legacy "
        f"case-file path: {sorted(new_drift)}.\n\n"
        f"Add an intent test under tests/intents/<chain>/ that constructs an "
        f"intent matching the pair — default-on Zodiac will exercise the "
        f"manifest automatically. Case-file coverage is the legacy fallback "
        f"retired in Phase G.4.\n\n"
        f"If this drift is intentional (no intent test exists yet for "
        f"documented reasons), add an entry to _LEGACY_ONLY_BASELINE in "
        f"{__file__} with a one-line justification — but prefer fixing "
        f"the gap."
    )

    healed = _LEGACY_ONLY_BASELINE - legacy_only
    assert not healed, (
        f"These pairs are no longer case-file-only (an intent test now covers "
        f"them) — remove from _LEGACY_ONLY_BASELINE in {__file__}: "
        f"{sorted(healed)}"
    )
