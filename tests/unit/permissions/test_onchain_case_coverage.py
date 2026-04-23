"""Gate: every ``(protocol, intent_type)`` the manifest generator covers must
have a corresponding on-chain authorisation test case (or an explicit exemption).

Companion to ``test_connector_coverage.py`` (Q1 hint-declaration gate). Where
Q1 asks "did the connector author consider permissions?" this gate asks "is
the generated manifest exercised against a real Zodiac Roles Modifier?"

Authoritative capability source:
    ``almanak.framework.permissions.synthetic_intents.get_protocol_intent_matrix``

Case declarations:
    ``tests/intents/permission_cases/<protocol>.py`` with ``CASES: list[PermissionTestCase]``

Exemption sentinel:
    ``tests/intents/permission_cases/<protocol>.permissions_onchain_exempt``

Plan doc: ``docs/internal/zodiac-permission-onchain-coverage-plan.md``.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from almanak.framework.permissions.synthetic_intents import get_protocol_intent_matrix
from tests.intents._permission_onchain_harness import PermissionTestCase

CASES_DIR = Path(__file__).resolve().parents[2] / "intents" / "permission_cases"
_SENTINEL_SUFFIX = ".permissions_onchain_exempt"


def _load_case_module(protocol: str):
    """Load ``permission_cases/<protocol>.py`` by path, returning the module or ``None``.

    Uses import-by-path so the gate does not require ``permission_cases`` to
    be a Python package.
    """
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
        raise AssertionError(
            f"{module.__file__} must export a top-level ``CASES: list[PermissionTestCase]``."
        )
    if not isinstance(cases, list) or any(not isinstance(c, PermissionTestCase) for c in cases):
        raise AssertionError(
            f"{module.__file__}.CASES must be a list of PermissionTestCase instances."
        )
    return cases


def _deferred_from(module) -> frozenset[str]:
    """Return the set of intent types this case file explicitly defers.

    A case file that covers some — but not all — of a protocol's intent types
    can list the uncovered ones in ``DEFERRED_INTENT_TYPES``. The gate skips
    those pairs; this avoids the sharp edge where partial coverage forces the
    author to either ship a full sweep or opt the whole protocol out.
    """
    deferred = getattr(module, "DEFERRED_INTENT_TYPES", ())
    return frozenset(str(t).upper() for t in deferred)


def _has_exemption(protocol: str) -> bool:
    return (CASES_DIR / f"{protocol}{_SENTINEL_SUFFIX}").exists()


def _enumerate_required_pairs() -> list[tuple[str, str]]:
    """Flatten the capability matrix into ``(protocol, intent_type_value)`` pairs.

    Sorted for deterministic pytest test-id ordering.
    """
    matrix = get_protocol_intent_matrix()
    pairs: list[tuple[str, str]] = []
    for protocol in sorted(matrix):
        for intent_type in sorted(t.value for t in matrix[protocol]):
            pairs.append((protocol, intent_type))
    return pairs


@pytest.mark.parametrize(
    ("protocol", "intent_type"),
    _enumerate_required_pairs(),
    ids=lambda value: value,
)
def test_protocol_intent_has_onchain_case(protocol: str, intent_type: str) -> None:
    """Each ``(protocol, intent_type)`` must have a test case or exemption."""
    module = _load_case_module(protocol)
    has_sentinel = _has_exemption(protocol)

    if module is not None and has_sentinel:
        pytest.fail(
            f"Protocol '{protocol}' has BOTH a case file "
            f"(permission_cases/{protocol}.py) AND an exemption sentinel "
            f"({protocol}{_SENTINEL_SUFFIX}). These are mutually exclusive — "
            f"a case file means the protocol is covered; the sentinel means it "
            f"is deferred. Delete "
            f"tests/intents/permission_cases/{protocol}{_SENTINEL_SUFFIX} to "
            f"enable coverage (or delete the case file if the protocol should "
            f"stay exempt)."
        )

    if has_sentinel:
        pytest.skip(
            f"Protocol '{protocol}' opts out of on-chain permission coverage via "
            f"{protocol}{_SENTINEL_SUFFIX}. Remove the sentinel and add a case file "
            f"before enabling this protocol in a Zodiac-gated deployment."
        )

    if module is None:
        pytest.fail(
            f"Protocol '{protocol}' is covered by the manifest generator "
            f"(supports intent type {intent_type}) but has no on-chain test cases. "
            f"Create tests/intents/permission_cases/{protocol}.py with:\n\n"
            f"    from tests.intents._permission_onchain_harness import PermissionTestCase\n\n"
            f"    CASES = [\n"
            f"        PermissionTestCase(chain=\"<chain>\", protocol=\"{protocol}\",\n"
            f"                           intent_type=\"{intent_type}\", config={{...}}),\n"
            f"    ]\n\n"
            f"Or mark the protocol exempt by touching "
            f"tests/intents/permission_cases/{protocol}{_SENTINEL_SUFFIX} with a one-line justification."
        )

    if intent_type.upper() in _deferred_from(module):
        pytest.skip(
            f"Intent type '{intent_type}' for protocol '{protocol}' is declared in "
            f"DEFERRED_INTENT_TYPES in the case file. Add a PermissionTestCase and "
            f"remove the entry from DEFERRED_INTENT_TYPES to activate coverage."
        )

    cases = _cases_from(module)

    # Sanity: every declared case must tag itself with the right protocol.
    stray = [c for c in cases if c.protocol != protocol]
    assert not stray, (
        f"permission_cases/{protocol}.py contains cases with the wrong protocol: "
        f"{[c.protocol for c in stray]}. Every case in this file must use protocol='{protocol}'."
    )

    matching = [c for c in cases if c.intent_type.upper() == intent_type.upper()]
    assert matching, (
        f"Protocol '{protocol}' has {len(cases)} case(s) declared, but none cover "
        f"intent type '{intent_type}'. Add a PermissionTestCase with "
        f"intent_type='{intent_type}', or add '{intent_type}' to DEFERRED_INTENT_TYPES "
        f"in the case file if coverage is intentionally deferred."
    )
