"""Schema-only invariant: no production code writes to position_registry yet.

VIB-4190 / T05 ships the table without any production reader or writer; T11
(VIB-4197) introduces the atomic save_ledger_and_registry primitive and is
the first PR that should make this test fail (at which point the test moves
to T11's branch unchanged or gets retired in T12+).

The invariant scans every Python file under almanak/ (excluding tests/ and
the schema definition itself) and rejects any INSERT INTO / UPDATE /
DELETE FROM / SELECT FROM statement targeting position_registry. The
schema definition in almanak/framework/state/backends/sqlite.py is
allow-listed because that's the only place that should mention the table
during T05.
"""

from __future__ import annotations

import re
from pathlib import Path

# Project root — three parents up from this file: tests/unit/state/<this>.
_REPO_ROOT = Path(__file__).resolve().parents[3]
_ALMANAK_DIR = _REPO_ROOT / "almanak"

# The only file allowed to name the table during T05 is the schema source.
_ALLOWLIST: set[Path] = {
    _ALMANAK_DIR / "framework" / "state" / "backends" / "sqlite.py",
}

# Optional quoting (`"position_registry"`, `\`position_registry\``,
# `[position_registry]`) and optional schema qualifier (`main.position_registry`,
# `metrics_db.position_registry`) — both forms must be caught alongside the bare
# identifier (CodeRabbit r3).
_TABLE_REF = r'(?:["`\[])?(?:\w+\.)?position_registry(?:["`\]])?'

# Patterns that constitute a "writer" or "reader" of the table.
_FORBIDDEN_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("INSERT INTO", re.compile(rf"\bINSERT\s+(?:OR\s+\w+\s+)?INTO\s+{_TABLE_REF}", re.I)),
    ("UPDATE", re.compile(rf"\bUPDATE\s+{_TABLE_REF}", re.I)),
    ("DELETE FROM", re.compile(rf"\bDELETE\s+FROM\s+{_TABLE_REF}", re.I)),
    ("SELECT FROM", re.compile(rf"\bFROM\s+{_TABLE_REF}", re.I)),
    # Catches `... JOIN position_registry ...` reads that bypass the FROM-only check
    # (CodeRabbit r1). Production reads via JOIN are rejected with the same
    # severity as direct FROM reads.
    ("JOIN", re.compile(rf"\bJOIN\s+{_TABLE_REF}", re.I)),
]


def _iter_python_sources():
    """Yield Python source files under almanak/ that should not name the table."""
    for path in _ALMANAK_DIR.rglob("*.py"):
        if path in _ALLOWLIST:
            continue
        # Exclude tests under almanak/ if any (none today, but be safe).
        if any(part == "tests" for part in path.relative_to(_ALMANAK_DIR).parts):
            continue
        yield path


def test_no_production_writers_or_readers() -> None:
    """Production code under almanak/ must not yet read or write position_registry."""
    violations: list[tuple[Path, str, int, str]] = []
    for path in _iter_python_sources():
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        for label, pattern in _FORBIDDEN_PATTERNS:
            for match in pattern.finditer(text):
                line_no = text.count("\n", 0, match.start()) + 1
                line = text.splitlines()[line_no - 1] if line_no - 1 < len(text.splitlines()) else ""
                violations.append((path, label, line_no, line.strip()))

    if violations:
        report = "\n".join(
            f"  {p.relative_to(_REPO_ROOT)}:{lineno}: [{label}] {line}"
            for p, label, lineno, line in violations
        )
        raise AssertionError(
            "Found production code that reads or writes position_registry; T05 "
            "is schema-only. Move this code to T11 (VIB-4197) or later:\n" + report
        )


def test_allowlist_path_actually_exists() -> None:
    """Guard: the only allow-listed file must exist (catches ALLOWLIST drift)."""
    for path in _ALLOWLIST:
        assert path.is_file(), (
            f"ALLOWLIST entry {path} does not exist — refresh the allow-list"
        )
