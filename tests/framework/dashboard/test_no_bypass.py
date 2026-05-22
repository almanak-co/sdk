"""CI lint: dashboard renderers must not bypass the gateway (VIB-4496 / Phase 4).

This test parses every ``.py`` file under ``almanak/framework/dashboard/``
and asserts:

1. **No direct DB access.** Dashboard code must not import ``sqlite3``,
   ``aiosqlite``, or any internal SQLite store / state-manager from the
   framework or gateway. The dashboard is a read-only view over gateway
   gRPC; direct DB access is the exact tech debt this rewrite removes.

2. **No operator-client leakage.** Renderer modules (everything outside
   the operator-only allowlist) must not import
   ``OperatorDashboardServiceClient`` from the Phase 2 facade
   (``service_client.py``). The two-tier client split (Phase 2 / VIB-4494)
   only enforces typing at the parameter level — this lint enforces it
   at the import level so a renderer can't quietly grab the operator
   class even via local function imports.

Existing pre-cutover violators (see ``_BASELINE_VIOLATIONS``) are
explicitly tracked. Phase 6 deletes them; the lint guarantees no NEW
violators are added in the meantime.

How to handle a lint failure:

* If you genuinely need DB / operator access, you are working in operator-
  scope code — move the file into the allowlist after a design review.
* Otherwise, route the data via a new gateway RPC. The gateway is the
  single source of truth; the dashboard is a renderer.
"""

from __future__ import annotations

import ast
from collections.abc import Iterable
from pathlib import Path

import pytest

# Repo-root-relative scope. Resolved relative to this test file to survive
# `pytest --rootdir` overrides and worktree checkouts.
_DASHBOARD_PACKAGE = Path(__file__).resolve().parents[3] / "almanak" / "framework" / "dashboard"


# Files that ARE allowed to touch operator surfaces because they ARE the
# operator surface. Stored as POSIX-style suffixes (matched against the
# trailing path components) so the allowlist survives directory moves.
_OPERATOR_SCOPE_SUFFIXES: tuple[str, ...] = (
    "almanak/framework/dashboard/sections_operator.py",
    # service_client.py *defines* OperatorDashboardServiceClient and its
    # singleton accessor — by construction it has to reference the symbol.
    # The tightened scanner (Name/Attribute walk) would otherwise flag the
    # class definition itself. Exempt the definer; renderers still can't
    # import from it because the OPERATOR symbol-import check below catches
    # any `from .service_client import OperatorDashboardServiceClient`.
    "almanak/framework/dashboard/service_client.py",
    # Future: operator-only pages will be listed here once they land.
)


# Forbidden imports for DB-bypass detection. The keys are the *importable*
# module prefixes; matching is "name == prefix OR name.startswith(prefix + '.')".
_FORBIDDEN_DB_IMPORTS: tuple[str, ...] = (
    "sqlite3",
    "aiosqlite",
    "almanak.framework.state",  # state manager + sqlite backends
    "almanak.gateway.database",
    "almanak.gateway.lifecycle.sqlite_store",
)


# Forbidden operator-client import names. We match the IMPORTED NAME, not
# just the module, because the operator class is the exact attack surface.
_FORBIDDEN_OPERATOR_NAMES: tuple[str, ...] = ("OperatorDashboardServiceClient",)


# Baseline — pre-existing violations that Phase 6 will remove. Keys are
# (path_suffix, forbidden_import_or_name) tuples. The lint allows exactly
# these; any addition / removal makes the test fail loudly so the diff
# stays visible in review.
_BASELINE_VIOLATIONS: frozenset[tuple[str, str]] = frozenset(
    {
        # pages/detail.py contains legacy direct sqlite reads in three
        # helper functions. Phase 6 deletes them; until then the lint
        # acknowledges them so renderers added today don't inherit the
        # habit by following the existing pattern.
        ("almanak/framework/dashboard/pages/detail.py", "sqlite3"),
        ("almanak/framework/dashboard/pages/detail.py", "almanak.framework.state"),
    }
)


# =============================================================================
# AST scanner
# =============================================================================


def _path_suffix(path: Path) -> str:
    """Return the POSIX-suffix used for allowlist / baseline matching."""
    # Find "almanak/framework/dashboard/..." anchored at the package root so
    # results don't depend on the absolute checkout path or worktree slug. The
    # checkout itself may live under a directory named "almanak", so do not use
    # the first matching path component.
    parts = path.parts
    for idx in range(len(parts) - 2):
        if parts[idx : idx + 3] == ("almanak", "framework", "dashboard"):
            return "/".join(parts[idx:])
    return path.as_posix()  # pragma: no cover


def _is_operator_scope(path: Path) -> bool:
    suffix = _path_suffix(path)
    return any(suffix.endswith(scope) for scope in _OPERATOR_SCOPE_SUFFIXES)


def _matches_forbidden(name: str, forbidden: Iterable[str]) -> str | None:
    """Return the matched forbidden prefix, or None."""
    for prefix in forbidden:
        if name == prefix or name.startswith(prefix + "."):
            return prefix
    return None


def _iter_imports(tree: ast.AST) -> Iterable[tuple[int, str, str | None]]:
    """Yield (lineno, module_name, optional_imported_symbol) for every import.

    Handles both ``import a.b`` (yields ``("a.b", None)``) and
    ``from a.b import c`` (yields ``("a.b", "c")``). ``from . import x``
    (relative) is normalized via ``ImportFrom.module`` which may be None
    for pure-relative imports; we represent those with an empty string
    so the matcher skips them.
    """
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                yield node.lineno, alias.name, None
        elif isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            for alias in node.names:
                yield node.lineno, mod, alias.name


def _scan_file(path: Path) -> list[tuple[str, str, int]]:
    """Return list of (kind, offender, lineno) for forbidden imports in `path`.

    kind is "DB" or "OPERATOR" so the assertion message can group them.
    """
    try:
        tree = ast.parse(path.read_text(), filename=str(path))
    except SyntaxError as exc:  # pragma: no cover
        pytest.fail(f"Syntax error parsing {path}: {exc}")

    findings: list[tuple[str, str, int]] = []
    is_operator = _is_operator_scope(path)
    operator_hits: set[tuple[str, int]] = set()

    for lineno, module_name, imported_symbol in _iter_imports(tree):
        if module_name:
            hit = _matches_forbidden(module_name, _FORBIDDEN_DB_IMPORTS)
            if hit:
                findings.append(("DB", hit, lineno))

        # Operator client may be imported by operator-scope files only.
        if not is_operator and imported_symbol in _FORBIDDEN_OPERATOR_NAMES:
            operator_hits.add((imported_symbol, lineno))

    # Close the module-alias bypass:
    #   import almanak.framework.dashboard.service_client as sc
    #   sc.OperatorDashboardServiceClient(...)
    # The import scan above only sees `from ... import OperatorX`; the
    # attribute access path is invisible there. Walk every Attribute /
    # Name reference and catch the forbidden symbol by usage too.
    if not is_operator:
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr in _FORBIDDEN_OPERATOR_NAMES:
                operator_hits.add((node.attr, node.lineno))
            elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load) and node.id in _FORBIDDEN_OPERATOR_NAMES:
                operator_hits.add((node.id, node.lineno))

    findings.extend(("OPERATOR", offender, lineno) for offender, lineno in sorted(operator_hits, key=lambda x: x[1]))

    return findings


def _enumerate_dashboard_files() -> list[Path]:
    return sorted(p for p in _DASHBOARD_PACKAGE.rglob("*.py") if "__pycache__" not in p.parts)


# =============================================================================
# Sanity checks (so a broken lint doesn't quietly pass)
# =============================================================================


def test_dashboard_package_path_resolves() -> None:
    assert _DASHBOARD_PACKAGE.is_dir(), (
        f"Phase 4 lint cannot locate the dashboard package at {_DASHBOARD_PACKAGE}. Did the repo layout change?"
    )


def test_scanner_finds_known_files() -> None:
    files = _enumerate_dashboard_files()
    assert len(files) > 5, (
        "Expected to find multiple .py files under the dashboard package; got "
        f"{len(files)}. The lint cannot enforce what it cannot see."
    )


def test_baseline_files_exist() -> None:
    """Every file in the baseline must actually exist — otherwise the
    baseline drifted and we are no longer enforcing what we think we are."""
    files = {_path_suffix(p) for p in _enumerate_dashboard_files()}
    for path_suffix, _ in _BASELINE_VIOLATIONS:
        assert path_suffix in files, (
            f"Baseline references {path_suffix}, which does not exist anymore. "
            "Remove the entry from _BASELINE_VIOLATIONS — it indicates the file "
            "was deleted (Phase 6 progress!)."
        )


# =============================================================================
# The lint itself
# =============================================================================


def test_no_db_bypass_in_dashboard() -> None:
    """No new direct DB / state-manager imports in the dashboard package."""
    files = _enumerate_dashboard_files()
    new_violations: list[str] = []

    for path in files:
        suffix = _path_suffix(path)
        for kind, offender, lineno in _scan_file(path):
            if kind != "DB":
                continue
            if (suffix, offender) in _BASELINE_VIOLATIONS:
                continue
            new_violations.append(f"{suffix}:{lineno} imports forbidden DB module '{offender}'")

    if new_violations:
        msg = [
            "Dashboard package contains NEW direct DB / state-manager imports.",
            "The dashboard is a read-only renderer over gateway gRPC — direct",
            "DB access is the exact tech debt this rewrite (VIB-4492) removes.",
            "",
            "Route your data via a new gateway RPC, or — if you are working",
            "on operator-only mutation code — move the file into the",
            "operator scope (see _OPERATOR_SCOPE_SUFFIXES) after a design",
            "review.",
            "",
            "Violations:",
            *(f"  - {v}" for v in sorted(new_violations)),
        ]
        raise AssertionError("\n".join(msg))


def test_no_operator_client_leak_into_renderers() -> None:
    """Operator-only client must not leak into renderer modules."""
    files = _enumerate_dashboard_files()
    leaks: list[str] = []

    for path in files:
        suffix = _path_suffix(path)
        for kind, offender, lineno in _scan_file(path):
            if kind != "OPERATOR":
                continue
            leaks.append(f"{suffix}:{lineno} imports operator-only '{offender}'")

    if leaks:
        msg = [
            "Renderer modules under the dashboard package are importing the",
            "operator-only client. The two-tier facade split (Phase 2 / VIB-4494)",
            "exists specifically to prevent this: renderers take a",
            "DashboardServiceClient parameter and cannot call mutation methods.",
            "",
            "If your code legitimately needs operator surfaces, move the file",
            "into the operator scope (see _OPERATOR_SCOPE_SUFFIXES) — only",
            "explicit operator pages / panels belong there.",
            "",
            "Leaks:",
            *(f"  - {leak}" for leak in sorted(leaks)),
        ]
        raise AssertionError("\n".join(msg))


def test_baseline_has_no_dead_entries() -> None:
    """If a baseline violation is no longer in the source, the baseline lies.

    This forces the baseline to shrink as Phase 6 deletes legacy code — we
    never accidentally hold the line at a fictional violation count.
    """
    files = _enumerate_dashboard_files()
    live_violations: set[tuple[str, str]] = set()
    for path in files:
        suffix = _path_suffix(path)
        for kind, offender, _ in _scan_file(path):
            if kind == "DB":
                live_violations.add((suffix, offender))

    dead = _BASELINE_VIOLATIONS - live_violations
    if dead:
        msg = [
            "_BASELINE_VIOLATIONS lists entries that are no longer present",
            "in the source. Remove them to keep the baseline truthful:",
            *(f"  - {p} → {o}" for p, o in sorted(dead)),
        ]
        raise AssertionError("\n".join(msg))


# =============================================================================
# Documentation / unit coverage of the helper functions themselves so a
# broken matcher doesn't pass via a false-negative.
# =============================================================================


class TestMatcherHelpers:
    @pytest.mark.parametrize(
        "name, prefixes, expected",
        [
            ("sqlite3", ("sqlite3",), "sqlite3"),
            ("aiosqlite", ("sqlite3", "aiosqlite"), "aiosqlite"),
            ("almanak.framework.state.sqlite", ("almanak.framework.state",), "almanak.framework.state"),
            ("almanak.framework.statemachine", ("almanak.framework.state",), None),
            ("os", ("sqlite3",), None),
            ("", ("sqlite3",), None),
        ],
    )
    def test_matches_forbidden(self, name: str, prefixes: tuple[str, ...], expected: str | None) -> None:
        assert _matches_forbidden(name, prefixes) == expected

    def test_path_suffix_strips_above_almanak(self) -> None:
        # Any concrete file under the dashboard package gives a suffix that
        # starts with "almanak/" — independent of the absolute path prefix.
        sample = next(iter(_enumerate_dashboard_files()))
        suffix = _path_suffix(sample)
        assert suffix.startswith("almanak/framework/dashboard/")
        assert not suffix.startswith("/")

    def test_is_operator_scope_recognises_operator_module(self) -> None:
        operator_file = _DASHBOARD_PACKAGE / "sections_operator.py"
        assert _is_operator_scope(operator_file) is True

    def test_is_operator_scope_rejects_renderer_module(self) -> None:
        renderer_file = _DASHBOARD_PACKAGE / "sections_reconciliation.py"
        assert _is_operator_scope(renderer_file) is False

    def test_iter_imports_includes_both_styles(self) -> None:
        tree = ast.parse("import a.b\nimport c\nfrom d.e import f\nfrom d import g, h\nfrom . import i\n")
        imports = list(_iter_imports(tree))
        modules = {(mod, sym) for _, mod, sym in imports}
        assert ("a.b", None) in modules
        assert ("c", None) in modules
        assert ("d.e", "f") in modules
        assert ("d", "g") in modules
        assert ("d", "h") in modules
        # Pure-relative `from . import i` has module=None which we normalise to "".
        assert any(mod == "" and sym == "i" for _, mod, sym in imports)
