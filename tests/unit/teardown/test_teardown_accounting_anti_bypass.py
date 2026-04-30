"""Static anti-bypass guards for VIB-3773 — tests T14 + T15.

These tests pin the *source* of the teardown lane against re-introduction of
the April-30 silent-failure class:

* T14 (grep) — every ``orchestrator.execute*`` call in
  ``almanak/framework/teardown/teardown_manager.py`` must be followed in
  the same scope by a ``commit_teardown_intent`` invocation, and the
  source order must be ``execute_*`` → ``commit``. A new bypass must
  trip this guard before it ships.
* T15 (AST) — parse ``runner_teardown.py`` and confirm
  ``execute_teardown_via_manager`` brackets the TeardownManager call with
  a pre + post ``capture_snapshot`` invocation AND swaps both cycle-id
  surfaces (``runner._last_cycle_id`` AND ``set_cycle_id``).

These tests run at lint speed (no fixtures, no IO beyond a file read) so
they're cheap to run on every commit.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
TEARDOWN_MANAGER = ROOT / "almanak" / "framework" / "teardown" / "teardown_manager.py"
RUNNER_TEARDOWN = ROOT / "almanak" / "framework" / "runner" / "runner_teardown.py"


# ---------------------------------------------------------------------------
# T14 — execute_bundle (or .execute) is paired with commit_teardown_intent
# ---------------------------------------------------------------------------


def test_t14_orchestrator_execute_paired_with_commit_in_teardown_manager():
    """Every ``self.orchestrator.execute(...)`` call inside
    ``teardown_manager.py`` must be followed by a ``commit_teardown_intent``
    invocation in the same enclosing function. Adding a new bypass would
    fail this test.
    """
    src = TEARDOWN_MANAGER.read_text(encoding="utf-8")
    tree = ast.parse(src, filename=str(TEARDOWN_MANAGER))

    # Walk every function in teardown_manager.py — for each, locate
    # orchestrator.execute / orchestrator.execute_bundle calls and verify
    # the SAME function also references commit_teardown_intent.
    offenders: list[str] = []

    def _func_name(node: ast.AST) -> str:
        return getattr(node, "name", "<unknown>")

    def _calls_orchestrator_execute(node: ast.AST) -> bool:
        for sub in ast.walk(node):
            if isinstance(sub, ast.Attribute):
                attr = sub.attr
                if attr in {"execute", "execute_bundle"}:
                    val = sub.value
                    if isinstance(val, ast.Attribute) and val.attr == "orchestrator":
                        return True
        return False

    def _references_commit(node: ast.AST) -> bool:
        for sub in ast.walk(node):
            if isinstance(sub, ast.Attribute) and sub.attr == "commit_teardown_intent":
                return True
            if isinstance(sub, ast.Attribute) and sub.attr == "commit":
                # ``runner_helpers.commit(...)`` form.
                val = sub.value
                if isinstance(val, ast.Attribute) and val.attr == "runner_helpers":
                    return True
        return False

    # Recurse into nested function defs (the closure ``execute_at_slippage``).
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef | ast.FunctionDef):
            if _calls_orchestrator_execute(node) and not _references_commit(node):
                offenders.append(_func_name(node))

    assert not offenders, (
        f"VIB-3773 anti-bypass guard tripped: {offenders} call orchestrator.execute "
        "without a paired commit_teardown_intent (or runner_helpers.commit). "
        "Every successful on-chain teardown intent must drive the runner's commit "
        "pipeline so ledger / position_event / outbox / accounting_event / sidecar "
        "all fire. See docs/internal/AccountingTeardown.md §4.1."
    )


def test_t14_orchestrator_execute_followed_by_commit_in_source_order():
    """Source-order check: in ``teardown_manager.py``, the line containing
    ``runner_helpers.commit`` must appear AFTER the matching
    ``orchestrator.execute`` call. A bypass that calls commit first and
    then executes would silently drop accounting on later retries.
    """
    src = TEARDOWN_MANAGER.read_text(encoding="utf-8")
    lines = src.splitlines()

    execute_lines = [
        i
        for i, line in enumerate(lines)
        if re.search(r"self\.orchestrator\.execute\b", line)
    ]
    commit_lines = [i for i, line in enumerate(lines) if "runner_helpers.commit" in line]

    assert execute_lines, "Expected at least one orchestrator.execute call in teardown_manager.py"
    assert commit_lines, (
        "Expected at least one runner_helpers.commit call in teardown_manager.py "
        "(VIB-3773 wiring missing)."
    )

    # The first commit must appear AFTER the first execute (we scan top-down).
    assert min(commit_lines) > min(execute_lines), (
        "Source-order anti-bypass tripped: a commit_teardown_intent call is "
        "BEFORE the orchestrator.execute it should follow."
    )


# ---------------------------------------------------------------------------
# T15 — execute_teardown_via_manager has the snapshot brackets + cycle-id swap
# ---------------------------------------------------------------------------


def _find_function(tree: ast.AST, name: str) -> ast.AST:
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef | ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"function {name!r} not found")


def test_t15_execute_teardown_via_manager_has_snapshot_brackets():
    """``execute_teardown_via_manager`` must call ``capture_snapshot``
    twice (pre + post) and the SAME function must also call the
    TeardownManager's executor (``execute_and_verify``).
    """
    src = RUNNER_TEARDOWN.read_text(encoding="utf-8")
    tree = ast.parse(src, filename=str(RUNNER_TEARDOWN))
    fn = _find_function(tree, "execute_teardown_via_manager")

    capture_calls = 0
    seen_execute_and_verify = False
    for sub in ast.walk(fn):
        if isinstance(sub, ast.Attribute):
            if sub.attr == "capture_snapshot":
                capture_calls += 1
            elif sub.attr == "execute_and_verify":
                seen_execute_and_verify = True

    assert seen_execute_and_verify, (
        "execute_teardown_via_manager no longer calls execute_and_verify — "
        "did the manager wiring move? Update this guard."
    )
    assert capture_calls >= 2, (
        f"execute_teardown_via_manager must call capture_snapshot at least "
        f"twice (pre + post). Found {capture_calls}."
    )


def test_t15_execute_teardown_via_manager_swaps_both_cycle_id_surfaces():
    """The function must mutate ``runner._last_cycle_id`` AND call
    ``set_cycle_id`` for the ContextVar surface (P1-4 dual swap).
    """
    src = RUNNER_TEARDOWN.read_text(encoding="utf-8")
    tree = ast.parse(src, filename=str(RUNNER_TEARDOWN))
    fn = _find_function(tree, "execute_teardown_via_manager")

    sets_last_cycle = False
    sets_ctx_cycle = False
    for sub in ast.walk(fn):
        # ``runner._last_cycle_id = ...``
        if isinstance(sub, ast.Assign):
            for tgt in sub.targets:
                if (
                    isinstance(tgt, ast.Attribute)
                    and tgt.attr == "_last_cycle_id"
                ):
                    sets_last_cycle = True
        # ``set_cycle_id(...)``
        if isinstance(sub, ast.Call):
            f = sub.func
            if isinstance(f, ast.Name) and f.id == "set_cycle_id":
                sets_ctx_cycle = True

    assert sets_last_cycle, (
        "execute_teardown_via_manager does not assign runner._last_cycle_id — "
        "the iteration cycle id will leak into teardown rows (P1-4)."
    )
    assert sets_ctx_cycle, (
        "execute_teardown_via_manager does not call set_cycle_id — the "
        "ContextVar surface is not stamped (P1-4 dual swap requirement)."
    )


def test_t15_execute_teardown_inline_swaps_both_cycle_id_surfaces():
    """Same dual-swap requirement for the inline lane. The inline lane
    wraps its own brackets around the loop body."""
    src = RUNNER_TEARDOWN.read_text(encoding="utf-8")
    tree = ast.parse(src, filename=str(RUNNER_TEARDOWN))
    fn = _find_function(tree, "execute_teardown_inline")

    sets_last_cycle = False
    sets_ctx_cycle = False
    for sub in ast.walk(fn):
        if isinstance(sub, ast.Assign):
            for tgt in sub.targets:
                if isinstance(tgt, ast.Attribute) and tgt.attr == "_last_cycle_id":
                    sets_last_cycle = True
        if isinstance(sub, ast.Call):
            f = sub.func
            if isinstance(f, ast.Name) and f.id == "set_cycle_id":
                sets_ctx_cycle = True

    assert sets_last_cycle, (
        "execute_teardown_inline does not assign runner._last_cycle_id — "
        "P1-4 dual-swap requirement."
    )
    assert sets_ctx_cycle, (
        "execute_teardown_inline does not call set_cycle_id — "
        "P1-4 dual-swap requirement."
    )
