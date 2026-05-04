"""Staleness guard for the Phase 6b-1 coverage omits.

Three modules under ``almanak/framework/accounting/`` are listed in
``[tool.coverage.run] omit`` because they are Track-A2 / Track-B library
code with no production callers (see ``CLAUDE.md`` and
``docs/internal/coverage-improvement-plan.md`` Phase 6b-1).

If a future PR wires any of them into a production code path, leaving them
in ``omit`` would silently under-report coverage on the new caller path.
This test fails loudly the moment that happens — by re-running the same
grep that justified the omit decision in the first place — so the omit
can never silently rot.

Scope: ``almanak/`` excluding the modules themselves and any test
directories. Tests under ``tests/`` are not scanned: it is fine for unit
tests to import the modules directly.
"""

from __future__ import annotations

import ast
import tomllib
from pathlib import Path

import pytest

# Repo root: this file lives at tests/unit/framework/accounting/<this>.py
REPO_ROOT = Path(__file__).resolve().parents[4]
PYPROJECT = REPO_ROOT / "pyproject.toml"
ALMANAK_DIR = REPO_ROOT / "almanak"

# The accounting omits this guard protects. Hard-coded — NOT derived from
# the omit list — because a typo in pyproject.toml that drops one of these
# from the omit list should NOT silently disable the guard for that module.
GUARDED_MODULES = (
    "almanak.framework.accounting.observations",
    "almanak.framework.accounting.receipts",
    "almanak.framework.accounting.typed_columns",
)


def _omit_list_from_pyproject() -> list[str]:
    with PYPROJECT.open("rb") as fh:
        data = tomllib.load(fh)
    return data["tool"]["coverage"]["run"]["omit"]


def _module_file(dotted: str) -> Path:
    """Translate ``almanak.framework.accounting.observations`` → file path."""
    return REPO_ROOT / (dotted.replace(".", "/") + ".py")


def _file_imports_module(tree: ast.AST, module_dotted: str, parent: str, leaf: str) -> bool:
    """Return ``True`` iff the AST imports ``module_dotted`` in any form.

    Recognised forms (all four are equivalent at the import-graph level):

    * ``import almanak.framework.accounting.observations``
    * ``import almanak.framework.accounting.observations as alias``
    * ``from almanak.framework.accounting.observations import X``
    * ``from almanak.framework.accounting import observations`` (incl.
      parenthesised / multi-line variants — the AST normalises these).

    Aliasing on the leaf (``import ... as alias`` / ``from ... import leaf as alias``)
    does not affect detection: ``alias.name`` is the original symbol either way.
    """
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            # ``import a.b.c`` and ``import a.b.c as alias``
            if any(alias.name == module_dotted for alias in node.names):
                return True
        elif isinstance(node, ast.ImportFrom):
            # ``from a.b.c import X``
            if node.module == module_dotted:
                return True
            # ``from a.b import c`` (parenthesised / multi-line both fine —
            # the AST has already normalised whitespace and brackets away).
            if node.module == parent and any(alias.name == leaf for alias in node.names):
                return True
    return False


def _scan_for_imports(module_dotted: str) -> list[Path]:
    """Return non-self ``almanak/**`` files that import ``module_dotted``.

    Detection is AST-based (see :func:`_file_imports_module`) so that
    parenthesised / multi-line ``from parent import (leaf, ...)`` forms are
    caught alongside the single-line variants — a brittle regex would miss
    the multi-line case and silently let a production caller slip in.

    Excludes:

    * The module itself (it can't import itself).
    * Test directories under ``almanak/`` (tests may import freely).
    * Any other module that is *also* in ``GUARDED_MODULES``. The Track-A2
      cluster is a closed set — ``observations.py`` legitimately imports
      ``receipts.py`` because they ship as one unused library. We are
      guarding against a *production* caller crossing into the omit set,
      not against the omit set referencing itself.
    """
    self_file = _module_file(module_dotted)
    intra_omit_files = {_module_file(m) for m in GUARDED_MODULES}
    leaf = module_dotted.rsplit(".", 1)[-1]
    parent = module_dotted.rsplit(".", 1)[0]

    hits: list[Path] = []
    for path in ALMANAK_DIR.rglob("*.py"):
        if path == self_file or path in intra_omit_files:
            continue
        # Skip test directories that may live under almanak/ in the future.
        parts = set(path.parts)
        if "tests" in parts or "test" in parts:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        try:
            tree = ast.parse(text, filename=str(path))
        except SyntaxError:
            # A file that can't be parsed can't import anything we care
            # about; skip it rather than masking the syntax error here.
            continue
        if _file_imports_module(tree, module_dotted, parent, leaf):
            hits.append(path)
    return hits


@pytest.mark.parametrize("module_dotted", GUARDED_MODULES)
def test_guarded_module_is_in_omit_list(module_dotted: str) -> None:
    """If a module is listed in ``GUARDED_MODULES`` it must also be in the
    ``[tool.coverage.run] omit`` list.

    Drift here means someone removed an omit but didn't drop the guard,
    or vice-versa — either way, the two lists must agree.
    """
    omit_paths = {entry.replace("/", ".").removesuffix(".py")
                  for entry in _omit_list_from_pyproject()
                  if entry.endswith(".py")}
    assert module_dotted in omit_paths, (
        f"{module_dotted} is guarded by this test but not present in "
        f"[tool.coverage.run] omit. Either re-add the omit or remove the "
        f"module from GUARDED_MODULES."
    )


@pytest.mark.parametrize("module_dotted", GUARDED_MODULES)
def test_omitted_module_has_no_production_callers(module_dotted: str) -> None:
    """The omit decision was justified by ``grep`` showing zero non-self
    importers. Re-run the same grep here so a future PR cannot silently
    add a production caller while the module remains in ``omit`` (which
    would under-report coverage on the new caller path).

    If this fails, the correct response is one of:

    1. Drop the module from ``[tool.coverage.run] omit`` (and from
       ``GUARDED_MODULES``) and add real coverage for the new caller path.
    2. Refactor so the production caller stops importing it.

    Do NOT add an exemption to widen the allowlist — that defeats the point.
    """
    callers = _scan_for_imports(module_dotted)
    assert callers == [], (
        f"{module_dotted} is in [tool.coverage.run] omit on the assumption "
        f"that it has no production callers, but the following non-test "
        f"almanak/ files import it:\n  "
        + "\n  ".join(str(p.relative_to(REPO_ROOT)) for p in callers)
        + "\n\nEither remove it from the omit list (and from "
        "GUARDED_MODULES in this test) and add real coverage for the new "
        "caller path, or refactor so the production caller stops importing it."
    )


# ---------------------------------------------------------------------------
# Self-tests for the AST-based detector. These lock in the contract that
# parenthesised / multi-line ``from parent import (leaf, ...)`` forms are
# detected — the brittle regex they replaced silently missed exactly that
# case (PR #2044 round-2 review).
# ---------------------------------------------------------------------------


_MOD = "almanak.framework.accounting.observations"
_PARENT, _LEAF = _MOD.rsplit(".", 1)


@pytest.mark.parametrize(
    "source",
    [
        # Direct dotted import, plain.
        f"import {_MOD}\n",
        # Direct dotted import with alias.
        f"import {_MOD} as obs\n",
        # ``from <full> import X``.
        f"from {_MOD} import something\n",
        # ``from <parent> import leaf`` single-line.
        f"from {_PARENT} import {_LEAF}\n",
        # ``from <parent> import leaf as alias``.
        f"from {_PARENT} import {_LEAF} as obs\n",
        # Parenthesised single-leaf.
        f"from {_PARENT} import (\n    {_LEAF},\n)\n",
        # Parenthesised multi-leaf — leaf in the middle.
        f"from {_PARENT} import (\n    foo,\n    {_LEAF},\n    bar,\n)\n",
        # Parenthesised multi-leaf — trailing comma absent on last item.
        f"from {_PARENT} import (\n    foo,\n    {_LEAF}\n)\n",
        # Backslash-continued (legacy style — still valid Python).
        f"from {_PARENT} import \\\n    {_LEAF}\n",
    ],
)
def test_detector_finds_import(source: str) -> None:
    tree = ast.parse(source)
    assert _file_imports_module(tree, _MOD, _PARENT, _LEAF) is True


@pytest.mark.parametrize(
    "source",
    [
        # Sibling module with a name that contains the leaf as a substring.
        f"from {_PARENT} import {_LEAF}_extra\n",
        # Different parent that ends with the same leaf.
        f"from some.other.{_LEAF} import thing\n",
        # Importing a longer dotted path that starts with the same prefix.
        f"import {_MOD}.submodule\n",
        # Plain comment that mentions the module — must NOT match.
        f"# from {_PARENT} import {_LEAF}\n",
        # String literal that mentions the module — must NOT match.
        f's = "from {_PARENT} import {_LEAF}"\n',
        # Empty file.
        "",
    ],
)
def test_detector_rejects_non_import(source: str) -> None:
    tree = ast.parse(source)
    assert _file_imports_module(tree, _MOD, _PARENT, _LEAF) is False
