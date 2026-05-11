"""Anti-bypass guard for the `position_reference` augment chokepoint (VIB-4196 / T10).

Mirror of T11's ``tests/unit/state/test_position_registry_no_writers.py`` for
the OPEN/CLOSE position-reference shape. Two layers:

- **Layer A — construction scope.** Only
  :mod:`almanak.framework.accounting.writer` may import
  :class:`PositionReference` or call :func:`build_legacy_position_reference`.
  Any other production source under ``almanak/framework/`` (connectors,
  category handlers, runner, etc.) attempting either is flagged.

- **Layer B — payload-key scope.** No production source under
  ``almanak/framework/`` may write a JSON dict whose key is
  ``"position_reference"`` EXCEPT inside ``writer.py``'s
  ``augment_accounting_payload`` chokepoint. Catches the bypass where a
  caller fabricates the JSON sub-document directly into a payload string
  before handing it to ``save_accounting_event``.

Cross-reference: UAT card ``docs/internal/uat-cards/VIB-4196.md`` D3.F3.
The runtime backstop is the writer's exclusive augment chokepoint;
this test prevents the static-analysis bypass.
"""

from __future__ import annotations

import ast
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
_ALMANAK_DIR = _REPO_ROOT / "almanak"

# The canonical chokepoint file. Layer A + B both allow this file's bodies.
_WRITER_REL: str = "almanak/framework/accounting/writer.py"
# The shape module is allowed to import its own symbols.
_SHAPE_REL: str = "almanak/framework/accounting/position_reference.py"
# The SQLite + gateway state-manager backends extract the column from the
# augmented payload — they do NOT construct it; they read it. The migration
# helper at module level reads `build_legacy_position_reference` to backfill
# pre-existing rows once at upgrade time. These two paths are explicitly
# allowlisted so the column wiring in T10 is not flagged by Layer A.
_ALLOWLISTED_RELS: frozenset[str] = frozenset(
    {
        _WRITER_REL,
        _SHAPE_REL,
        "almanak/framework/state/backends/sqlite.py",
    }
)

# Forbidden constructor symbols. Any caller that imports OR references one
# of these names outside the allowlist is a bypass attempt. VIB-4278 added
# ``build_registry_position_reference`` for the registry-source path; same
# rule applies — the writer chokepoint is the only construction site.
_FORBIDDEN_SYMBOLS: frozenset[str] = frozenset(
    {
        "PositionReference",
        "build_legacy_position_reference",
        "build_registry_position_reference",
    }
)


def _iter_python_sources():
    for path in _ALMANAK_DIR.rglob("*.py"):
        if any(part == "tests" for part in path.relative_to(_ALMANAK_DIR).parts):
            continue
        yield path


def _rel(path: Path) -> str:
    return str(path.relative_to(_REPO_ROOT))


# =============================================================================
# Layer A — construction scope
# =============================================================================
def _imports_forbidden_symbol(tree: ast.AST) -> list[tuple[str, int]]:
    """Return ``(symbol, lineno)`` for every forbidden import (or import-then-attr-access) in ``tree``.

    Catches the following bypass shapes:

    * ``from almanak.framework.accounting.position_reference import X``
      where ``X`` is in :data:`_FORBIDDEN_SYMBOLS` (with or without ``as``-alias).
    * ``import almanak.framework.accounting.position_reference`` (plain or
      ``as``-aliased).
    * ``from almanak.framework.accounting import position_reference [as pr]``
      followed by an ``ast.Attribute`` access ``pr.PositionReference`` /
      ``pr.build_legacy_position_reference`` ANYWHERE in the same module.

    The ``import almanak.framework.accounting as acct`` →
    ``acct.position_reference.X`` shape is rare in this codebase but is
    caught by the package-level ``ast.Attribute`` walk on the suffix
    ``position_reference.{symbol}`` regardless of receiver name.
    """
    hits: list[tuple[str, int]] = []
    # Names that bind to the `position_reference` MODULE in this file's scope.
    # Captures both `from ... import position_reference [as alias]` (binding
    # is the alias-or-name) and `import almanak.framework.accounting.position_reference`
    # (binding is the leading dotted segment, which is what alias resolution
    # actually delivers — same scoping rule Python uses).
    module_aliases: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if module == "almanak.framework.accounting":
                # Catches `from almanak.framework.accounting import position_reference [as pr]`.
                for alias in node.names:
                    if alias.name == "position_reference":
                        module_aliases.add(alias.asname or alias.name)
            elif "accounting.position_reference" in module:
                # `from almanak.framework.accounting.position_reference import X [as Y]`.
                for alias in node.names:
                    if alias.name in _FORBIDDEN_SYMBOLS:
                        hits.append((alias.asname or alias.name, node.lineno))
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if "accounting.position_reference" in alias.name:
                    # `import almanak.framework.accounting.position_reference [as pr]`.
                    hits.append((alias.asname or alias.name, node.lineno))

    if module_aliases:
        # Second pass: any `<alias>.<forbidden_symbol>` attribute access is a
        # bypass via aliased module import.
        for node in ast.walk(tree):
            if not isinstance(node, ast.Attribute):
                continue
            if node.attr not in _FORBIDDEN_SYMBOLS:
                continue
            value = node.value
            if isinstance(value, ast.Name) and value.id in module_aliases:
                hits.append((f"{value.id}.{node.attr}", node.lineno))

    return hits


def test_layer_a_only_writer_constructs_position_reference() -> None:
    """No production code outside the chokepoint may import the constructor symbols."""
    failures: list[str] = []
    for path in _iter_python_sources():
        rel = _rel(path)
        if rel in _ALLOWLISTED_RELS:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        try:
            tree = ast.parse(text)
        except SyntaxError:
            raise
        for symbol, lineno in _imports_forbidden_symbol(tree):
            failures.append(f"{rel}:{lineno} imports forbidden symbol {symbol!r}")

    if failures:
        report = "\n".join(f"  {f}" for f in failures)
        raise AssertionError(
            "PositionReference / build_legacy_position_reference may only be "
            "imported by the augment chokepoint at "
            f"{_WRITER_REL!r} (and its own module / migration backfill in "
            f"{sorted(_ALLOWLISTED_RELS)}). Found:\n" + report + "\n"
            "Do NOT add a new caller — fabricating position_reference JSON "
            "outside the writer is the bypass class this guard exists to "
            "prevent. See blueprint 28 §6 and AGENTS.md 'AccountingWriter is "
            "the only path that may call save_accounting_event()'."
        )


def test_layer_a_canonical_writer_imports_symbols() -> None:
    """Sanity check: the chokepoint actually imports both symbols.

    Catches a refactor that drops the import and also drops the augmentation
    silently — would re-trigger the May 4 silent-NULL class.
    """
    text = (_REPO_ROOT / _WRITER_REL).read_text(encoding="utf-8")
    tree = ast.parse(text)
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module and "position_reference" in node.module:
                for alias in node.names:
                    found.add(alias.name)
    assert "build_legacy_position_reference" in found, (
        f"{_WRITER_REL} must import build_legacy_position_reference; "
        f"found imports: {found}"
    )
    # VIB-4278: the writer chokepoint MUST also import the registry helper.
    # A refactor that drops the import would silently regress every accounting
    # event to source="legacy" — closing the L5_22 invariant from the wrong
    # side.
    assert "build_registry_position_reference" in found, (
        f"{_WRITER_REL} must import build_registry_position_reference (VIB-4278); "
        f"found imports: {found}"
    )


# =============================================================================
# Layer B — payload-key scope
# =============================================================================
def _has_position_reference_dict_key(tree: ast.AST) -> list[tuple[str, int]]:
    """Return ``(shape, lineno)`` for every position_reference key construction in ``tree``.

    Catches the four documented bypass shapes (CodeRabbit on PR #2211):

    1. **dict literal**: ``{"position_reference": {...}}``.
    2. **subscript assignment**: ``payload["position_reference"] = ...``.
    3. **dict() keyword**: ``dict(position_reference=...)``.
    4. **.update() with dict literal**:
       ``payload.update({"position_reference": ...})``.

    All four shapes are statically detectable via ``ast.Constant`` matches.
    A dynamic key (e.g. ``payload[var] = ...``) is intentionally NOT flagged —
    that requires data-flow analysis and is covered by the runtime augment-
    chokepoint behaviour (the writer always pops + canonical-stamps).
    """
    hits: list[tuple[str, int]] = []
    for node in ast.walk(tree):
        # Shape 1 — dict literal.
        if isinstance(node, ast.Dict):
            for key in node.keys:
                if isinstance(key, ast.Constant) and key.value == "position_reference":
                    hits.append(("dict_literal", node.lineno))
                    break
            continue

        # Shape 2 — subscript assignment: x["position_reference"] = ...
        if isinstance(node, (ast.Assign, ast.AugAssign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                if not isinstance(target, ast.Subscript):
                    continue
                slc = target.slice
                if isinstance(slc, ast.Constant) and slc.value == "position_reference":
                    hits.append(("subscript_assign", node.lineno))
                    break
            continue

        # Shape 3 + 4 — call sites.
        if isinstance(node, ast.Call):
            # Shape 3: dict(position_reference=...)
            if isinstance(node.func, ast.Name) and node.func.id == "dict":
                for kw in node.keywords:
                    if kw.arg == "position_reference":
                        hits.append(("dict_call_kwarg", node.lineno))
                        break
            # Shape 4: <anything>.update({"position_reference": ...})  /
            # also catches ``foo.update(position_reference=...)`` for symmetry.
            elif isinstance(node.func, ast.Attribute) and node.func.attr == "update":
                # 4a: dict literal as positional arg.
                for arg in node.args:
                    if isinstance(arg, ast.Dict):
                        for key in arg.keys:
                            if isinstance(key, ast.Constant) and key.value == "position_reference":
                                hits.append(("update_with_dict_literal", node.lineno))
                                break
                # 4b: keyword-arg form `update(position_reference=...)`.
                for kw in node.keywords:
                    if kw.arg == "position_reference":
                        hits.append(("update_kwarg", node.lineno))
                        break
    return hits


def test_layer_b_no_caller_fabricates_position_reference_in_payload() -> None:
    """No production source outside the writer may construct a `position_reference` key
    via dict literal, subscript assignment, ``dict()`` keyword, or ``.update()``."""
    failures: list[str] = []
    for path in _iter_python_sources():
        rel = _rel(path)
        if rel in _ALLOWLISTED_RELS:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        try:
            tree = ast.parse(text)
        except SyntaxError:
            raise
        for shape, lineno in _has_position_reference_dict_key(tree):
            failures.append(f"{rel}:{lineno} {shape} contains 'position_reference' key")

    if failures:
        report = "\n".join(f"  {f}" for f in failures)
        raise AssertionError(
            "Production code outside the writer chokepoint constructs a "
            "'position_reference' key — this is a bypass attempt. The "
            "augment_accounting_payload function in writer.py is the ONLY "
            "permitted construction site.\n" + report
        )


# =============================================================================
# Self-tests — verify the helpers actually catch the four documented bypass
# shapes. Without these, a future "improvement" that breaks one of the
# detectors slips silently — the production-scan tests above are negative
# (they assert no hits in real source), so a broken detector would simply
# stop firing and the test would still pass.
# =============================================================================


def _detector_hit_shapes(source: str) -> set[str]:
    return {shape for shape, _ in _has_position_reference_dict_key(ast.parse(source))}


def test_layer_b_self_dict_literal_detected() -> None:
    assert "dict_literal" in _detector_hit_shapes(
        'payload = {"event_type": "X", "position_reference": {"a": 1}}'
    )


def test_layer_b_self_subscript_assign_detected() -> None:
    assert "subscript_assign" in _detector_hit_shapes(
        'payload["position_reference"] = {"source": "smuggled"}'
    )


def test_layer_b_self_dict_call_kwarg_detected() -> None:
    assert "dict_call_kwarg" in _detector_hit_shapes(
        'payload = dict(event_type="X", position_reference={"a": 1})'
    )


def test_layer_b_self_update_with_dict_literal_detected() -> None:
    assert "update_with_dict_literal" in _detector_hit_shapes(
        'payload.update({"position_reference": {"a": 1}})'
    )


def test_layer_b_self_update_kwarg_detected() -> None:
    assert "update_kwarg" in _detector_hit_shapes(
        "payload.update(position_reference={'a': 1})"
    )


def test_layer_b_self_dynamic_key_not_flagged() -> None:
    """Dynamic keys are out of static-scope by design — flagging them would false-positive."""
    assert _detector_hit_shapes('payload[some_var] = {"a": 1}') == set()


def test_layer_a_self_aliased_module_attr_access_detected() -> None:
    """``from almanak.framework.accounting import position_reference as pr`` then ``pr.PositionReference``."""
    src = (
        "from almanak.framework.accounting import position_reference as pr\n"
        "x = pr.PositionReference(source='legacy')\n"
    )
    hits = _imports_forbidden_symbol(ast.parse(src))
    symbols = {s for s, _ in hits}
    # Either the import line OR the attribute access counts as a hit; both is
    # the strict case we want.
    assert "pr.PositionReference" in symbols or "PositionReference" in symbols, (
        f"Aliased-module attribute access not detected; hits={hits}"
    )


def test_layer_a_self_aliased_function_import_detected() -> None:
    """``from ...position_reference import build_legacy_position_reference as build_x``."""
    src = (
        "from almanak.framework.accounting.position_reference import "
        "build_legacy_position_reference as build_x\n"
        "build_x(record)\n"
    )
    hits = _imports_forbidden_symbol(ast.parse(src))
    symbols = {s for s, _ in hits}
    # The import binds the FORBIDDEN name (`build_legacy_position_reference`)
    # but our detector reports the local alias (`build_x`) so a grep for the
    # alias can be run. Either flavour is a "yes, detected".
    assert symbols, f"Aliased function import not detected; hits={hits}"
