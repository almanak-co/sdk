"""VIB-3872 WI-3 — anti-bypass static guard for the perp settlement lane.

Mirrors ``tests/unit/teardown/test_teardown_accounting_anti_bypass.py``: a
static-analysis guard that the settlement-commit path routes every accounting
write through ``AccountingWriter`` (the sole sanctioned ``save_accounting_event``
chokepoint, CLAUDE.md §Accounting) and never signs/executes on-chain.

Guarded invariants:
- The commit lane writes the ``PERP_SETTLEMENT`` event ONLY via
  ``runner._accounting_processor._writer.write`` (AccountingWriter) — never a raw
  ``save_accounting_event``.
- The reconciler books settlements ONLY through ``commit_perp_settlement`` — it
  never touches ``_writer.write`` or ``save_accounting_event`` directly.
- Neither module calls ``orchestrator.execute*`` (settlement books money that
  already moved — it never signs a new transaction).
"""

from __future__ import annotations

import ast
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
_LANE = _ROOT / "almanak" / "framework" / "runner" / "perp_settlement_commit.py"
_RECONCILER = _ROOT / "almanak" / "framework" / "runner" / "perp_settlement_reconciler.py"


def _tree(path: Path) -> ast.AST:
    return ast.parse(path.read_text(), filename=str(path))


def _attr_calls(tree: ast.AST) -> list[str]:
    """Every ``x.y(...)`` call's attribute name in the tree."""
    names: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            names.append(node.func.attr)
    return names


def _names_referenced(tree: ast.AST) -> set[str]:
    return {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)} | {
        a.attr for a in ast.walk(tree) if isinstance(a, ast.Attribute)
    }


def test_lane_never_calls_save_accounting_event_directly() -> None:
    calls = _attr_calls(_tree(_LANE))
    assert "save_accounting_event" not in calls, (
        "perp_settlement_commit must route the accounting write through AccountingWriter "
        "(processor._writer.write), never a raw save_accounting_event."
    )


def test_lane_writes_through_accounting_writer() -> None:
    refs = _names_referenced(_tree(_LANE))
    assert "write" in refs and "_writer" in refs, (
        "perp_settlement_commit must call runner._accounting_processor._writer.write (the AccountingWriter chokepoint)."
    )


def test_lane_never_executes_onchain() -> None:
    calls = _attr_calls(_tree(_LANE))
    for forbidden in ("execute", "execute_bundle"):
        assert forbidden not in calls, f"perp_settlement_commit must not call orchestrator.{forbidden}(...)"


def test_reconciler_books_only_through_the_lane() -> None:
    tree = _tree(_RECONCILER)
    refs = _names_referenced(tree)
    assert "commit_perp_settlement" in refs, (
        "the reconciler must funnel every settlement through commit_perp_settlement"
    )
    calls = _attr_calls(tree)
    assert "save_accounting_event" not in calls, "the reconciler must not write accounting events directly"
    # The reconciler must not reach into the writer itself — that is the lane's job
    # (keeps the drain-first ordering guard + Empty≠Zero build in one place).
    src = _RECONCILER.read_text()
    assert "_writer.write" not in src, "the reconciler must not call _writer.write directly — route through the lane"


def test_reconciler_never_executes_onchain() -> None:
    calls = _attr_calls(_tree(_RECONCILER))
    for forbidden in ("execute", "execute_bundle"):
        assert forbidden not in calls, f"the reconciler must not call orchestrator.{forbidden}(...)"
