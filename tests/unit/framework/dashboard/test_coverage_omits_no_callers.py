"""Staleness guard for the Phase W4 (VIB-4081) dashboard coverage omits.

A set of modules under ``almanak/framework/dashboard/`` is listed in
``[tool.coverage.run] omit`` because each one's primary contract is to
produce a ``streamlit.*`` widget tree or a ``plotly.graph_objects.Figure``
— a UI surface that has no feasible unit-test harness, so counting its
lines in the coverage denominator is a measurement error. See
``docs/internal/coverage-improvement-plan.md`` §5 (Structural Blockers) and
the Review Log entry dated 2026-05-06.

If a future PR wires any of these UI-shell modules into a *non-shell*
production code path, leaving them in ``omit`` would silently under-report
coverage on the new caller path. This test fails loudly the moment that
happens — by re-running the same import-graph scan that justified the omit
decision in the first place — so the omit can never silently rot.

Scope: ``almanak/`` excluding (1) the modules themselves, (2) other modules
already covered by an omit pattern (intra-omit cross-imports are fine — the
UI shells form a closed dependency cluster), and (3) test directories.
Tests under ``tests/`` are not scanned: it is fine for unit tests to import
the modules directly.
"""

from __future__ import annotations

import ast
import fnmatch
import functools
import tomllib
from pathlib import Path

import pytest

# Repo root: this file lives at tests/unit/framework/dashboard/<this>.py
REPO_ROOT = Path(__file__).resolve().parents[4]
PYPROJECT = REPO_ROOT / "pyproject.toml"
ALMANAK_DIR = REPO_ROOT / "almanak"

# The W4 dashboard omits this guard protects. Hard-coded — NOT derived from
# the omit list — because a typo in pyproject.toml that drops one of these
# from the omit list should NOT silently disable the guard for that module.
#
# The set is the explicit file-level enumeration of the two globs
# (``dashboard/templates/*``, ``dashboard/plots/*``) plus the single-file
# ``dashboard/components.py`` omit. Any new W4-style omit MUST be added
# here for the guard to apply.
GUARDED_MODULES = (
    # dashboard/templates/* — Streamlit per-primitive dashboards
    "almanak.framework.dashboard.templates",
    "almanak.framework.dashboard.templates.lending_dashboard",
    "almanak.framework.dashboard.templates.lp_dashboard",
    "almanak.framework.dashboard.templates.perp_dashboard",
    "almanak.framework.dashboard.templates.prediction_dashboard",
    "almanak.framework.dashboard.templates.ta_dashboard",
    # dashboard/plots/* — Plotly chart builders
    "almanak.framework.dashboard.plots",
    "almanak.framework.dashboard.plots.base",
    "almanak.framework.dashboard.plots.lending_plots",
    "almanak.framework.dashboard.plots.lp_plots",
    "almanak.framework.dashboard.plots.perp_plots",
    "almanak.framework.dashboard.plots.portfolio_plots",
    "almanak.framework.dashboard.plots.prediction_plots",
    "almanak.framework.dashboard.plots.ta_plots",
    # Single-file Streamlit widget helpers
    "almanak.framework.dashboard.components",
)


@functools.lru_cache(maxsize=1)
def _omit_list_from_pyproject() -> tuple[str, ...]:
    with PYPROJECT.open("rb") as fh:
        data = tomllib.load(fh)
    # Tuple — `lru_cache` returns the same instance to every caller; a list
    # would invite accidental mutation that bleeds into later test runs.
    return tuple(data["tool"]["coverage"]["run"]["omit"])


def _module_file(dotted: str) -> Path:
    """Translate ``almanak.framework.dashboard.templates.lp_dashboard`` → file path.

    Package-form dotted names (e.g. ``almanak.framework.dashboard.plots``)
    resolve to the package's ``__init__.py``.
    """
    candidate = REPO_ROOT / (dotted.replace(".", "/") + ".py")
    if candidate.exists():
        return candidate
    pkg_init = REPO_ROOT / dotted.replace(".", "/") / "__init__.py"
    return pkg_init


def _path_matches_any_omit(path: Path, omit_patterns: tuple[str, ...]) -> bool:
    """Return True iff ``path`` is matched by any omit pattern.

    Coverage.py's omit patterns are fnmatch globs evaluated against the
    file path — see https://coverage.readthedocs.io/en/latest/source.html .
    Patterns are written relative to the repo root.
    """
    rel = path.relative_to(REPO_ROOT).as_posix()
    for pat in omit_patterns:
        if fnmatch.fnmatch(rel, pat):
            return True
    return False


def _file_imports_module(tree: ast.AST, module_dotted: str, parent: str, leaf: str) -> bool:
    """Return ``True`` iff the AST imports ``module_dotted`` in any form.

    Recognised forms (all four are equivalent at the import-graph level):

    * ``import almanak.framework.dashboard.plots.lp_plots``
    * ``import almanak.framework.dashboard.plots.lp_plots as alias``
    * ``from almanak.framework.dashboard.plots.lp_plots import X``
    * ``from almanak.framework.dashboard.plots import lp_plots`` (incl.
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


@functools.lru_cache(maxsize=1)
def _production_asts() -> tuple[tuple[Path, ast.AST], ...]:
    """Walk + parse every non-omit, non-test ``almanak/**/*.py`` once.

    Cached: the parametrized scan runs this once per pytest session instead
    of once per guarded module. With ~15 guarded modules and ~thousands of
    almanak/ files, the difference is whole-test-suite-noticeable.

    Files that fail to read (binary in source tree) or parse (syntax error)
    are skipped silently — a file that can't be parsed can't import the
    guarded module anyway.
    """
    omit_patterns = _omit_list_from_pyproject()
    results: list[tuple[Path, ast.AST]] = []
    for path in ALMANAK_DIR.rglob("*.py"):
        if _path_matches_any_omit(path, omit_patterns):
            continue
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
            continue
        results.append((path, tree))
    return tuple(results)


def _scan_for_imports(module_dotted: str) -> list[Path]:
    """Return non-self ``almanak/**`` files that import ``module_dotted``.

    Detection is AST-based (see :func:`_file_imports_module`) so that
    parenthesised / multi-line ``from parent import (leaf, ...)`` forms are
    caught alongside the single-line variants — a brittle regex would miss
    the multi-line case and silently let a production caller slip in.

    Excludes:

    * The module itself (it can't import itself).
    * Test directories under ``almanak/`` (tests may import freely).
    * Any other module that is *also* covered by an omit pattern. The
      W4 cluster is a closed set — the templates/, plots/, pages/, and
      backtesting/dashboard/ shells legitimately cross-reference each
      other (and the demo strategies pull `dashboard.plots` for their
      own UI). We are guarding against a *production* caller crossing
      *into* the omit set, not against the omit set referencing itself.
    """
    self_file = _module_file(module_dotted)
    leaf = module_dotted.rsplit(".", 1)[-1]
    parent = module_dotted.rsplit(".", 1)[0] if "." in module_dotted else ""

    hits: list[Path] = []
    for path, tree in _production_asts():
        if path == self_file:
            continue
        if _file_imports_module(tree, module_dotted, parent, leaf):
            hits.append(path)
    return hits


@pytest.mark.parametrize("module_dotted", GUARDED_MODULES)
def test_guarded_module_is_in_omit_list(module_dotted: str) -> None:
    """If a module is listed in ``GUARDED_MODULES`` it must also be matched
    by some entry in ``[tool.coverage.run] omit``.

    Drift here means someone removed an omit but didn't drop the guard,
    or vice-versa — either way, the two lists must agree.
    """
    omit_patterns = _omit_list_from_pyproject()
    module_file = _module_file(module_dotted)
    assert module_file.exists(), (
        f"GUARDED_MODULES references {module_dotted} but the resolved file "
        f"{module_file.relative_to(REPO_ROOT)} does not exist. Either fix "
        f"the dotted name or remove it from GUARDED_MODULES."
    )
    assert _path_matches_any_omit(module_file, omit_patterns), (
        f"{module_dotted} (resolved to {module_file.relative_to(REPO_ROOT)}) "
        f"is guarded by this test but no entry in [tool.coverage.run] omit "
        f"matches its path. Either re-add the omit or remove the module from "
        f"GUARDED_MODULES."
    )


@pytest.mark.parametrize("module_dotted", GUARDED_MODULES)
def test_omitted_module_has_no_production_callers(module_dotted: str) -> None:
    """The omit decision was justified by a grep showing zero non-omit
    importers. Re-run the same scan here so a future PR cannot silently
    add a production caller while the module remains in ``omit`` (which
    would under-report coverage on the new caller path).

    If this fails, the correct response is one of:

    1. Drop the module from ``[tool.coverage.run] omit`` (and from
       ``GUARDED_MODULES``) and add real coverage for the new caller path.
    2. Refactor so the production caller stops importing it (e.g. extract
       the pure-data part the caller actually needs into a non-shell
       helper module).

    Do NOT add an exemption to widen the allowlist — that defeats the point.
    """
    callers = _scan_for_imports(module_dotted)
    assert callers == [], (
        f"{module_dotted} is in [tool.coverage.run] omit on the assumption "
        f"that it has no non-omit production callers, but the following "
        f"non-test, non-omit almanak/ files import it:\n  "
        + "\n  ".join(str(p.relative_to(REPO_ROOT)) for p in callers)
        + "\n\nEither remove it from the omit list (and from "
        "GUARDED_MODULES in this test) and add real coverage for the new "
        "caller path, or refactor so the production caller stops importing "
        "it (extract the data the caller needs into a non-shell helper)."
    )


# ---------------------------------------------------------------------------
# Self-tests for the AST-based detector. These lock in the contract that
# parenthesised / multi-line ``from parent import (leaf, ...)`` forms are
# detected — the brittle regex pattern they replaced silently missed exactly
# that case (see the parallel guard at
# ``tests/unit/framework/accounting/test_coverage_omits_no_callers.py``).
# ---------------------------------------------------------------------------


_MOD = "almanak.framework.dashboard.plots.lp_plots"
_PARENT, _LEAF = _MOD.rsplit(".", 1)


@pytest.mark.parametrize(
    "source",
    [
        # Direct dotted import, plain.
        f"import {_MOD}\n",
        # Direct dotted import with alias.
        f"import {_MOD} as plots\n",
        # ``from <full> import X``.
        f"from {_MOD} import something\n",
        # ``from <parent> import leaf`` single-line.
        f"from {_PARENT} import {_LEAF}\n",
        # ``from <parent> import leaf as alias``.
        f"from {_PARENT} import {_LEAF} as plots\n",
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
