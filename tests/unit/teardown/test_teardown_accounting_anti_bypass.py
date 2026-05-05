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
CLI_TEARDOWN = ROOT / "almanak" / "framework" / "cli" / "teardown.py"
# PR #2093: execute_teardown was refactored from a 880-LOC click body
# (CC=89) into a thin orchestrator that delegates to typed helpers in
# teardown_helpers.py. The TeardownManager construction, the
# capture_snapshot bracket calls, and the cycle-id dual-swap all moved
# into ``run_teardown_with_brackets`` (and the manager construction
# also touches ``build_teardown_machinery``). The CLI-execute lane is
# now this set of functions; the AST guards walk all of them as one
# logical scope.
CLI_TEARDOWN_HELPERS = ROOT / "almanak" / "framework" / "cli" / "teardown_helpers.py"
# (file, function_name) pairs covering the CLI-execute lane.
CLI_EXECUTE_LANE: tuple[tuple[Path, str], ...] = (
    (CLI_TEARDOWN, "execute_teardown"),
    (CLI_TEARDOWN_HELPERS, "build_teardown_machinery"),
    (CLI_TEARDOWN_HELPERS, "run_teardown_with_brackets"),
)


def _walk_cli_execute_lane():
    """Yield every AST node reachable from any function in the CLI-execute
    lane, plus a path-name tag for error messages."""
    for path, fn_name in CLI_EXECUTE_LANE:
        src = path.read_text(encoding="utf-8")
        tree = ast.parse(src, filename=str(path))
        fn = _find_function(tree, fn_name)
        for sub in ast.walk(fn):
            yield sub, path.name, fn_name


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

    # Match both `.execute` and `.execute_bundle` so the source-order
    # check stays in sync with the paired-with-commit AST guard above —
    # which already inspects both forms (CR feedback PR #2093).
    execute_lines = [i for i, line in enumerate(lines) if re.search(r"self\.orchestrator\.execute(?:_bundle)?\b", line)]
    commit_lines = [i for i, line in enumerate(lines) if "runner_helpers.commit" in line]

    assert execute_lines, "Expected at least one orchestrator.execute call in teardown_manager.py"
    assert commit_lines, (
        "Expected at least one runner_helpers.commit call in teardown_manager.py (VIB-3773 wiring missing)."
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
        f"execute_teardown_via_manager must call capture_snapshot at least twice (pre + post). Found {capture_calls}."
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
                if isinstance(tgt, ast.Attribute) and tgt.attr == "_last_cycle_id":
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
        "execute_teardown_inline does not assign runner._last_cycle_id — P1-4 dual-swap requirement."
    )
    assert sets_ctx_cycle, "execute_teardown_inline does not call set_cycle_id — P1-4 dual-swap requirement."


# ---------------------------------------------------------------------------
# VIB-3839 — CLI execute_teardown wires runner_helpers + brackets + cycle-id swap
# ---------------------------------------------------------------------------


def test_t_3839_cli_execute_teardown_constructs_manager_with_runner_helpers():
    """The CLI ``execute_teardown`` lane must construct ``TeardownManager``
    with ``runner_helpers=`` populated. Without it, every closing tx lands
    on-chain but the SDK records zero rows in transaction_ledger /
    position_events / portfolio_snapshots / portfolio_metrics /
    accounting_events — the same silent-failure class VIB-3773 closed for
    the runner-loop lane.

    PR #2093: the execute lane is now split across cli/teardown.py and
    cli/teardown_helpers.py. The construction may live in either file —
    the lane is the union ``CLI_EXECUTE_LANE``.
    """
    offending: list[tuple[str, int]] = []
    found_any_construction = False
    for sub, file_name, _fn_name in _walk_cli_execute_lane():
        if isinstance(sub, ast.Call):
            f = sub.func
            is_teardown_manager_ctor = (isinstance(f, ast.Name) and f.id == "TeardownManager") or (
                isinstance(f, ast.Attribute) and f.attr == "TeardownManager"
            )
            if not is_teardown_manager_ctor:
                continue
            found_any_construction = True
            kwarg_names = {kw.arg for kw in sub.keywords if kw.arg}
            if "runner_helpers" not in kwarg_names:
                offending.append((file_name, getattr(sub, "lineno", -1)))

    assert found_any_construction, (
        "execute_teardown lane no longer constructs TeardownManager anywhere "
        "in CLI_EXECUTE_LANE — did the wiring move again? Update CLI_EXECUTE_LANE."
    )
    assert not offending, (
        f"VIB-3839 anti-bypass guard tripped: TeardownManager constructed at "
        f"{offending} without runner_helpers=. Every CLI-driven teardown intent "
        "must drive the commit pipeline (enrich → ledger → outbox+fire → "
        "sidecar). See blueprint 27-accounting.md and CLAUDE.md §Teardown lane "
        "accounting boundary."
    )


def test_t_3839_cli_execute_teardown_brackets_with_capture_snapshot():
    """The CLI execute lane must call ``runner_helpers.capture_snapshot``
    (or a hoisted local that aliases it) at least twice — once before the
    manager runs and once after — so portfolio_snapshots /
    portfolio_metrics rows mark the teardown's start/end.

    Counts call sites (``capture_snapshot(...)``) rather than attribute
    references, so a hoist like ``capture_snapshot = ...runner_helpers.
    capture_snapshot`` followed by two calls satisfies the guard the same
    way as two ``runner_helpers.capture_snapshot(...)`` invocations.
    """
    call_sites = 0
    for sub, _file_name, _fn_name in _walk_cli_execute_lane():
        if not isinstance(sub, ast.Call):
            continue
        f = sub.func
        # ``runner_helpers.capture_snapshot(...)``
        if isinstance(f, ast.Attribute) and f.attr == "capture_snapshot":
            call_sites += 1
        # Hoisted: ``capture_snapshot(...)`` where the name is the helper.
        elif isinstance(f, ast.Name) and f.id == "capture_snapshot":
            call_sites += 1

    assert call_sites >= 2, (
        "VIB-3839 anti-bypass: the CLI execute lane (cli/teardown.py + "
        "cli/teardown_helpers.py per CLI_EXECUTE_LANE) must call "
        f"capture_snapshot at least twice (pre + post). Found {call_sites}."
    )


def test_t_3839_cli_execute_teardown_swaps_both_cycle_id_surfaces():
    """The CLI execute lane must mutate ``runner._last_cycle_id`` AND call
    ``set_cycle_id`` — same dual-swap requirement as the runner-loop lane
    (P1-4). Without this, snapshot/metrics rows would land with the
    iteration's cycle id, splitting attribution across two cycle ids.
    """
    sets_last_cycle = False
    sets_ctx_cycle = False
    for sub, _file_name, _fn_name in _walk_cli_execute_lane():
        if isinstance(sub, ast.Assign):
            for tgt in sub.targets:
                if isinstance(tgt, ast.Attribute) and tgt.attr == "_last_cycle_id":
                    sets_last_cycle = True
        if isinstance(sub, ast.Call):
            f = sub.func
            if isinstance(f, ast.Name) and f.id == "set_cycle_id":
                sets_ctx_cycle = True

    assert sets_last_cycle, (
        "CLI execute lane (CLI_EXECUTE_LANE) does not assign runner._last_cycle_id — "
        "P1-4 dual-swap requirement (snapshot/metrics rows would carry stale cycle id)."
    )
    assert sets_ctx_cycle, (
        "CLI execute lane (CLI_EXECUTE_LANE) does not call set_cycle_id — "
        "P1-4 dual-swap requirement (ContextVar surface unstamped)."
    )
