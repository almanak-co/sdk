"""Unit tests for the typed ``MatchingPolicy.for_primitive()`` accessor.

VIB-4195 (T09 of multi-position-tracking shred). Wraps the existing
``MATCHING_POLICY_VERSIONS`` dict at
``almanak/framework/accounting/payload_schemas.py``. The dict is NOT
relocated — only wrapped — so these tests assert two structural invariants
in addition to the byte-for-byte equivalence:

1. The accessor returns the same int as the source dict for every
   ``Primitive`` member (round-trip identity).
2. Mutating the source dict (or rebinding the source module attribute)
   is observed by the accessor at call-time — there is no stale-snapshot
   bug. This is what guarantees a future LP v3 → v4 bump applied at
   ``payload_schemas.MATCHING_POLICY_VERSIONS`` propagates uniformly to
   every accessor caller (writer + future T11 ``save_ledger_and_registry``).
3. Non-``Primitive`` inputs raise rather than silently returning a default,
   so a bug that calls the accessor with an ``IntentType`` or a string
   surfaces loudly at the augment chokepoint instead of stamping the
   wrong version.

The card backing these tests is ``docs/internal/uat-cards/VIB-4195.md``;
the test names below mirror the D1/D2/D3 step ids so a card-vs-test audit
is mechanical.
"""

from __future__ import annotations

from enum import Enum

import pytest

from almanak.framework.accounting import payload_schemas
from almanak.framework.accounting.payload_schemas import MATCHING_POLICY_VERSIONS
from almanak.framework.accounting.policy import MatchingPolicy
from almanak.framework.primitives.types import Primitive


# ──────────────────────────────────────────────────────────────────────────
# D1.S1 — every Primitive returns the dict's value byte-for-byte
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("primitive", list(Primitive))
def test_for_primitive_matches_dict_for_every_primitive(primitive: Primitive) -> None:
    """The accessor returns the int the source dict declares for every Primitive."""
    expected = MATCHING_POLICY_VERSIONS[primitive]
    actual = MatchingPolicy.for_primitive(primitive)
    assert actual == expected
    assert type(actual) is int


def test_for_primitive_returns_int_not_bool() -> None:
    """``isinstance(True, int)`` is True in Python — ensure the type is strictly int."""
    value = MatchingPolicy.for_primitive(Primitive.LP)
    assert type(value) is int
    assert not isinstance(value, bool)


# ──────────────────────────────────────────────────────────────────────────
# D1.S2 — dict not relocated; accessor reads source at call-time
# ──────────────────────────────────────────────────────────────────────────


def test_dict_still_lives_in_payload_schemas() -> None:
    """The source-of-truth dict must remain at ``payload_schemas.MATCHING_POLICY_VERSIONS``."""
    assert hasattr(payload_schemas, "MATCHING_POLICY_VERSIONS")
    src = payload_schemas.MATCHING_POLICY_VERSIONS
    assert isinstance(src, dict)
    assert all(isinstance(k, Primitive) for k in src.keys())
    assert all(isinstance(v, int) and not isinstance(v, bool) for v in src.values())


def test_accessor_observes_source_dict_mutations(monkeypatch: pytest.MonkeyPatch) -> None:
    """Mutating the source dict via monkeypatch is observed by the accessor.

    This is the property that VIB-4162's ``test_lp_bump_isolation`` relies
    on — the writer's augment chokepoint reads ``MatchingPolicy.for_primitive``
    at every call, so a per-primitive bump applied via the dict's existing
    identity propagates immediately.
    """
    sentinel = 9007  # arbitrary, distinct from any valid version
    monkeypatch.setitem(MATCHING_POLICY_VERSIONS, Primitive.LP, sentinel)
    assert MatchingPolicy.for_primitive(Primitive.LP) == sentinel


# ──────────────────────────────────────────────────────────────────────────
# D1.S5 — public surface (callable shape) for T11
# ──────────────────────────────────────────────────────────────────────────


def test_for_primitive_is_callable_namespace() -> None:
    """``MatchingPolicy.for_primitive`` is a callable accessor, not an instance method.

    T11's ``save_ledger_and_registry`` (VIB-4197) depends on calling the
    accessor without instantiating ``MatchingPolicy``. Locking the surface
    here so a future "make it an instance method" refactor would be
    detected.
    """
    fn = MatchingPolicy.for_primitive
    assert callable(fn)
    # Calling without instantiation must work.
    assert isinstance(MatchingPolicy.for_primitive(Primitive.LP), int)


# ──────────────────────────────────────────────────────────────────────────
# D3.F1 — wrong-typed inputs raise (no silent default)
# ──────────────────────────────────────────────────────────────────────────


class _UnrelatedEnum(Enum):
    """Enum whose VALUE collides with ``Primitive.LP.value`` but is a different class."""

    LP = "lp"


@pytest.mark.parametrize(
    "bad_input",
    [
        pytest.param("lp", id="string-matches-primitive-value"),
        pytest.param("not-a-primitive", id="string-non-matching"),
        pytest.param(None, id="None"),
        pytest.param(1, id="int"),
        pytest.param(_UnrelatedEnum.LP, id="unrelated-enum"),
        pytest.param(object(), id="arbitrary-object"),
        pytest.param([Primitive.LP], id="list-wrapping-primitive"),
        pytest.param({"lp": 3}, id="dict"),
    ],
)
def test_wrong_typed_input_raises(bad_input: object) -> None:
    """Anything other than a ``Primitive`` must raise ``TypeError``; no silent default.

    The accessor's contract is an explicit ``isinstance(p, Primitive)`` guard
    that raises ``TypeError`` BEFORE the dict lookup (see
    ``almanak/framework/accounting/policy.py:92``). Asserting the specific
    exception type — rather than a broad tuple — locks the public API down
    so a regression that loses the type guard and falls through to an
    incidental ``KeyError`` / ``AttributeError`` for non-colliding bad inputs
    would be caught here.
    """
    with pytest.raises(TypeError, match="Primitive"):
        MatchingPolicy.for_primitive(bad_input)  # type: ignore[arg-type]


# ──────────────────────────────────────────────────────────────────────────
# D3.F2 — Primitive present in enum but absent from dict raises
# ──────────────────────────────────────────────────────────────────────────


def test_missing_primitive_raises_keyerror() -> None:
    """Deleting a key from the source dict surfaces as KeyError, not a silent default."""
    original = dict(payload_schemas.MATCHING_POLICY_VERSIONS)
    try:
        del payload_schemas.MATCHING_POLICY_VERSIONS[Primitive.PERP]
        with pytest.raises(KeyError):
            MatchingPolicy.for_primitive(Primitive.PERP)
    finally:
        payload_schemas.MATCHING_POLICY_VERSIONS.clear()
        payload_schemas.MATCHING_POLICY_VERSIONS.update(original)


# ──────────────────────────────────────────────────────────────────────────
# D3.F6 — silent-error guard: BOTH dict-clear AND attribute-rebind raise
# ──────────────────────────────────────────────────────────────────────────


def test_in_place_clear_surfaces_as_keyerror() -> None:
    """Clearing the source dict must surface as KeyError (no copied snapshot)."""
    original = dict(payload_schemas.MATCHING_POLICY_VERSIONS)
    try:
        payload_schemas.MATCHING_POLICY_VERSIONS.clear()
        with pytest.raises(KeyError):
            MatchingPolicy.for_primitive(Primitive.LP)
    finally:
        payload_schemas.MATCHING_POLICY_VERSIONS.clear()
        payload_schemas.MATCHING_POLICY_VERSIONS.update(original)


def test_module_attribute_rebind_surfaces_as_keyerror() -> None:
    """Rebinding ``payload_schemas.MATCHING_POLICY_VERSIONS`` to a new empty dict
    must also surface as KeyError — the accessor must read through the
    source module at call-time, not via a captured local-name binding.

    A failure here means the accessor did
    ``from .payload_schemas import MATCHING_POLICY_VERSIONS`` and indexed
    that local name, which freezes the dict-object identity at import time
    and silently hides any future migration that replaces the dict object.
    """
    saved_obj = payload_schemas.MATCHING_POLICY_VERSIONS
    try:
        payload_schemas.MATCHING_POLICY_VERSIONS = {}  # type: ignore[misc]
        with pytest.raises(KeyError):
            MatchingPolicy.for_primitive(Primitive.LP)
    finally:
        payload_schemas.MATCHING_POLICY_VERSIONS = saved_obj  # type: ignore[misc]


# ──────────────────────────────────────────────────────────────────────────
# D2.M3 — no parallel raw-dict access path under almanak/ outside
# payload_schemas.py + policy.py (AST sweep)
# ──────────────────────────────────────────────────────────────────────────


def test_no_other_production_code_reads_raw_dict() -> None:
    """No production-code module under ``almanak/`` may reference
    ``MATCHING_POLICY_VERSIONS`` by Name, Attribute, ImportFrom, or
    ``getattr(...)`` outside the source (``payload_schemas.py``) and the
    wrapper (``policy.py``). Tests/fixtures under ``tests/`` are NOT in
    scope — they assert the dict's contract directly, which is intended.
    """
    import ast
    import pathlib

    repo_root = pathlib.Path(__file__).resolve().parents[3]
    almanak_root = repo_root / "almanak"
    allowed = {
        almanak_root / "framework" / "accounting" / "payload_schemas.py",
        almanak_root / "framework" / "accounting" / "policy.py",
    }

    violations: list[tuple[str, int, str]] = []
    for path in almanak_root.rglob("*.py"):
        if path in allowed:
            continue
        src = path.read_text()
        if "MATCHING_POLICY_VERSIONS" not in src:
            continue
        rel = path.relative_to(repo_root).as_posix()
        tree = ast.parse(src, filename=rel)
        for node in ast.walk(tree):
            if isinstance(node, ast.Name) and node.id == "MATCHING_POLICY_VERSIONS":
                violations.append((rel, node.lineno, "bare-name"))
            if isinstance(node, ast.Attribute) and node.attr == "MATCHING_POLICY_VERSIONS":
                violations.append((rel, node.lineno, "attr"))
            if isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    if alias.name == "MATCHING_POLICY_VERSIONS":
                        violations.append((rel, node.lineno, "import"))
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "getattr"
            ):
                for arg in node.args:
                    if isinstance(arg, ast.Constant) and arg.value == "MATCHING_POLICY_VERSIONS":
                        violations.append((rel, node.lineno, "getattr-string"))

    assert not violations, (
        "Production code outside payload_schemas.py + policy.py references "
        "MATCHING_POLICY_VERSIONS — every read should go through "
        "MatchingPolicy.for_primitive(). Violations: " + repr(violations)
    )


# ──────────────────────────────────────────────────────────────────────────
# D1.S4 — writer.py imports MatchingPolicy and does not index the raw dict
# ──────────────────────────────────────────────────────────────────────────


def test_writer_imports_accessor_and_does_not_index_raw_dict() -> None:
    """``almanak/framework/accounting/writer.py`` must (a) not reference
    ``MATCHING_POLICY_VERSIONS`` in any AST-detectable form, (b) import
    ``MatchingPolicy``, and (c) actually call ``MatchingPolicy.for_primitive(...)``.
    """
    import ast
    import pathlib

    repo_root = pathlib.Path(__file__).resolve().parents[3]
    src = (repo_root / "almanak" / "framework" / "accounting" / "writer.py").read_text()
    tree = ast.parse(src)

    bypasses: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id == "MATCHING_POLICY_VERSIONS":
            bypasses.append((node.lineno, "bare-name"))
        if isinstance(node, ast.Attribute) and node.attr == "MATCHING_POLICY_VERSIONS":
            bypasses.append((node.lineno, "attr"))
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.name == "MATCHING_POLICY_VERSIONS":
                    bypasses.append((node.lineno, "import"))
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "getattr"
        ):
            for arg in node.args:
                if isinstance(arg, ast.Constant) and arg.value == "MATCHING_POLICY_VERSIONS":
                    bypasses.append((node.lineno, "getattr-string"))
    assert not bypasses, f"writer.py still references MATCHING_POLICY_VERSIONS at {bypasses}"

    imports_accessor = any(
        isinstance(node, ast.ImportFrom)
        and node.module == "almanak.framework.accounting.policy"
        and any(alias.name == "MatchingPolicy" for alias in node.names)
        for node in ast.walk(tree)
    )
    assert imports_accessor, "writer.py must import MatchingPolicy from accounting.policy"

    calls = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "for_primitive"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "MatchingPolicy"
    ]
    assert calls, "writer.py imports MatchingPolicy but never calls .for_primitive(...)"
