"""D3.F7 — anti-bypass: re-introduced local authoritative maps must FAIL.

Static AST scan of the four migrated consumers. Any non-allowlisted
collection literal whose elements overlap with IntentType values FAILs.
Plus: each module must import from primitives.taxonomy.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from almanak.framework.intents.vocabulary import IntentType

# Allowlist — kept-by-design local maps (event-type derivation only).
_ALLOWLISTED_NAMES = frozenset(
    {
        "INTENT_TO_EVENT_TYPE",  # event-type derivation; T2 keeps this local
        "_PAYLOAD_MODELS",  # payload_schemas (NOT in scope of this scan)
    }
)

_INTENT_VALUES = frozenset({m.value for m in IntentType})

_TARGETS = (
    "almanak/framework/accounting/classifier.py",
    "almanak/framework/observability/position_events.py",
    "almanak/framework/accounting/position_state.py",
    "almanak/framework/teardown/models.py",
)

_REPO_ROOT = Path(__file__).resolve().parents[3]


def _module_path(rel: str) -> Path:
    return _REPO_ROOT / rel


def _string_constants(node: ast.AST) -> set[str]:
    """Walk an AST node and collect every string-literal value."""
    out: set[str] = set()
    for child in ast.walk(node):
        if isinstance(child, ast.Constant) and isinstance(child.value, str):
            out.add(child.value)
    return out


def _enclosing_assign_target(tree: ast.AST, target: ast.AST) -> str | None:
    """Find the name of the Assign target that owns ``target``."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name):
                    for child in ast.walk(node.value):
                        if child is target:
                            return t.id
    return None


def _module_level_assignments(tree: ast.AST) -> list[tuple[str, ast.AST]]:
    """Yield (name, value-AST) for every module-level Assign target."""
    out: list[tuple[str, ast.AST]] = []
    if isinstance(tree, ast.Module):
        for stmt in tree.body:
            if isinstance(stmt, ast.Assign):
                for target in stmt.targets:
                    if isinstance(target, ast.Name):
                        out.append((target.id, stmt.value))
    return out


@pytest.mark.parametrize("rel_path", _TARGETS)
def test_no_local_authoritative_collection_overlaps(rel_path: str) -> None:
    """No MODULE-LEVEL collection literal may overlap IntentType values.

    Function-local string literals (e.g. ``field_map`` inside a helper
    function, or ``("REPAY", "WITHDRAW", "DELEVERAGE")`` inside an
    if-condition) are not "authoritative routing maps" — they are
    operational checks at point-of-use. The class-of-bug T2 prevents
    is module-level dicts / frozensets that re-implement the
    intent-type → category mapping at module scope; those compete with
    the canonical taxonomy and can drift silently.
    """
    src = _module_path(rel_path).read_text()
    tree = ast.parse(src)

    suspicious: list[tuple[str, str, set[str]]] = []
    for owner, value in _module_level_assignments(tree):
        if owner in _ALLOWLISTED_NAMES:
            continue
        if not isinstance(value, ast.Dict | ast.Set | ast.Tuple | ast.List | ast.DictComp | ast.SetComp):
            continue
        keys = _string_constants(value)
        overlap = keys & _INTENT_VALUES
        if not overlap:
            continue
        suspicious.append((rel_path, owner, overlap))

    assert not suspicious, (
        f"module-level authoritative collection literals reintroduced in {rel_path}: "
        f"{suspicious!r}"
    )


@pytest.mark.parametrize(
    "rel_path",
    [
        "almanak/framework/accounting/classifier.py",
        "almanak/framework/observability/position_events.py",
        "almanak/framework/accounting/position_state.py",
    ],
)
def test_imports_from_primitives_taxonomy(rel_path: str) -> None:
    """The three accounting/observability consumers must import from primitives.taxonomy."""
    src = _module_path(rel_path).read_text()
    tree = ast.parse(src)
    imported_from_taxonomy = False
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if module == "almanak.framework.primitives.taxonomy":
                imported_from_taxonomy = True
                break
    assert imported_from_taxonomy, (
        f"{rel_path} does not import from almanak.framework.primitives.taxonomy "
        "(delegation lock — re-implement local routing without this import = bypass)"
    )
