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
VAULT_LIFECYCLE = ROOT / "almanak" / "framework" / "vault" / "lifecycle.py"
TEARDOWN_COMMIT = ROOT / "almanak" / "framework" / "runner" / "teardown_commit.py"
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
# VIB-4895 — commit_teardown_intent (Lane B) emits position_events
# ---------------------------------------------------------------------------


def test_vib4895_commit_teardown_intent_emits_position_events():
    """``commit_teardown_intent`` (Lane B) must drive a ``position_events``
    write, not just enrich → ledger → outbox → sidecar.

    Before VIB-4895, Lane B (``TeardownManager`` → ``commit_teardown_intent``)
    ran the four-step commit pipeline but NEVER wrote ``position_events``. A
    multi-chain teardown therefore landed LP_CLOSE / PERP_CLOSE / lending-close
    TXs on-chain while writing zero CLOSE rows — silently breaking close-time
    IL/PnL attribution. ``position_events`` is one of the five surfaces
    teardown must populate (CLAUDE.md §Teardown; blueprint 27-accounting.md
    §14.1).

    This guard is static (AST over ``teardown_commit.py``): it asserts the
    module both builds a position event (``build_position_event_from_intent``)
    AND persists it (``save_position_event``). A regression that drops the
    emit — e.g. deleting Step 2b — fails this test before it ships.
    """
    src = TEARDOWN_COMMIT.read_text(encoding="utf-8")
    tree = ast.parse(src, filename=str(TEARDOWN_COMMIT))

    references_build = False
    references_save = False
    for sub in ast.walk(tree):
        # ``build_position_event_from_intent(...)`` — bare name call.
        if isinstance(sub, ast.Name) and sub.id == "build_position_event_from_intent":
            references_build = True
        # ``state_manager.save_position_event(...)`` — attribute access.
        if isinstance(sub, ast.Attribute) and sub.attr == "save_position_event":
            references_save = True

    assert references_build, (
        "VIB-4895 anti-bypass: teardown_commit.py no longer references "
        "build_position_event_from_intent — Lane B teardown closes will write "
        "zero position_events rows (silent IL/PnL break). See blueprint "
        "27-accounting.md §14.1 and CLAUDE.md §Teardown."
    )
    assert references_save, (
        "VIB-4895 anti-bypass: teardown_commit.py no longer references "
        "save_position_event — Lane B teardown closes will write zero "
        "position_events rows (silent IL/PnL break). See blueprint "
        "27-accounting.md §14.1 and CLAUDE.md §Teardown."
    )


def test_vib4895_commit_teardown_intent_position_event_emit_is_caught():
    """The Step 2b position-event emit in ``commit_teardown_intent`` MUST be
    wrapped so a write failure becomes a deferred-log row, never a propagated
    halt (blueprint 27 §14.1 loud-but-never-block contract — teardown's first
    job is removing on-chain risk).

    Static check: the ``_emit_teardown_position_event`` call inside
    ``commit_teardown_intent`` must sit inside a ``try`` whose handler calls
    ``_record(...)`` with the ``"position_event"`` kind. This pins the inverted
    failure semantics against a regression that lets the emit propagate.
    """
    src = TEARDOWN_COMMIT.read_text(encoding="utf-8")
    tree = ast.parse(src, filename=str(TEARDOWN_COMMIT))
    fn = _find_function(tree, "commit_teardown_intent")

    found_guarded_emit = False
    for node in ast.walk(fn):
        if not isinstance(node, ast.Try):
            continue
        # Does this try-body call _emit_teardown_position_event?
        calls_emit = any(
            isinstance(c, ast.Call)
            and (
                (isinstance(c.func, ast.Name) and c.func.id == "_emit_teardown_position_event")
            )
            for stmt in node.body
            for c in ast.walk(stmt)
        )
        if not calls_emit:
            continue
        # Does a handler record the position_event kind?
        records_position_event = any(
            isinstance(c, ast.Call)
            and isinstance(c.func, ast.Name)
            and c.func.id == "_record"
            and c.args
            and isinstance(c.args[0], ast.Constant)
            and c.args[0].value == "position_event"
            for handler in node.handlers
            for stmt in handler.body
            for c in ast.walk(stmt)
        )
        if records_position_event:
            found_guarded_emit = True
            break

    assert found_guarded_emit, (
        "VIB-4895: the _emit_teardown_position_event call in "
        "commit_teardown_intent must be wrapped in try/except that records a "
        "'position_event' deferred-log row via _record(...). Teardown's "
        "loud-but-never-block contract (blueprint 27 §14.1) requires the emit "
        "failure to degrade-but-continue, not propagate and strand the next "
        "risk-reducing intent."
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


def test_vib5011_run_token_consolidation_routes_through_execute_intents():
    """VIB-5011 static guard: ``TeardownManager.run_token_consolidation``
    must execute its planned swaps by REUSING ``_execute_intents`` — never by
    talking to the orchestrator directly. The reuse is what keeps the
    consolidation lane inside the slippage-escalation ladder and the
    per-intent commit pairing the T14 guards enforce; a direct
    ``orchestrator.execute`` call inside this method would be a new bypass
    surface the T14 pairing guard alone might not localize.

    AST over the method body: (a) it references ``_execute_intents``;
    (b) it contains NO attribute access on anything named ``orchestrator``.
    """
    src = TEARDOWN_MANAGER.read_text(encoding="utf-8")
    tree = ast.parse(src, filename=str(TEARDOWN_MANAGER))
    fn = _find_function(tree, "run_token_consolidation")

    references_execute_intents = False
    orchestrator_accesses: list[int] = []
    for sub in ast.walk(fn):
        if isinstance(sub, ast.Attribute):
            if sub.attr == "_execute_intents":
                references_execute_intents = True
            # Any read of `.orchestrator` (e.g. `self.orchestrator.execute`)
            # or attribute access ON an `orchestrator` name.
            if sub.attr == "orchestrator":
                orchestrator_accesses.append(getattr(sub, "lineno", -1))
            val = sub.value
            if isinstance(val, ast.Name) and val.id == "orchestrator":
                orchestrator_accesses.append(getattr(sub, "lineno", -1))

    assert references_execute_intents, (
        "run_token_consolidation no longer routes execution through "
        "_execute_intents — consolidation swaps would lose the slippage "
        "ladder + per-intent commit pairing. See blueprint 14 §4.5 (VIB-5011)."
    )
    assert not orchestrator_accesses, (
        f"run_token_consolidation accesses `orchestrator` directly at lines "
        f"{orchestrator_accesses} — execution must go through _execute_intents "
        "only (VIB-5011 anti-bypass)."
    )


# ---------------------------------------------------------------------------
# VIB-5667 — vault-release execute sites are paired with commit_teardown_intent
# ---------------------------------------------------------------------------


def test_vib5667_release_leg_pairs_execute_with_commit():
    """The vault-release lane (``lifecycle.py``) must route EVERY
    ``self._execution_orchestrator.execute`` through a helper that also calls
    ``commit`` in the same function. Without the pairing, a teardown vault-release
    would transition the vault on-chain (Open->Closing->Closed) while writing zero
    rows to transaction_ledger / accounting_events — the exact silent-failure class
    VIB-3773 closed for the runner-loop lane, re-introduced on a new execute site.

    Static AST guard, two parts:

    1. The single release execute site ``_execute_release_leg`` references BOTH
       ``self._execution_orchestrator.execute`` AND a ``commit(...)`` call.
    2. No other release-lane function (``release_on_teardown`` / ``_release_*``)
       calls ``self._execution_orchestrator.execute`` directly — every release
       execution MUST funnel through ``_execute_release_leg`` so the pairing can
       never be bypassed by adding a new leg. (The settlement lane's own
       ``_execute_*`` methods are out of scope — they belong to the iteration
       lane, which has snapshot-based accounting, not the teardown commit path.)
    """
    src = VAULT_LIFECYCLE.read_text(encoding="utf-8")
    tree = ast.parse(src, filename=str(VAULT_LIFECYCLE))

    def _calls_orchestrator_execute(node: ast.AST) -> bool:
        for sub in ast.walk(node):
            if isinstance(sub, ast.Attribute) and sub.attr == "execute":
                val = sub.value
                if isinstance(val, ast.Attribute) and val.attr == "_execution_orchestrator":
                    return True
        return False

    def _references_commit(node: ast.AST) -> bool:
        for sub in ast.walk(node):
            if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Name) and sub.func.id == "commit":
                return True
        return False

    def _is_release_lane(name: str) -> bool:
        return name == "release_on_teardown" or name.startswith("_release_")

    leg = _find_function(tree, "_execute_release_leg")
    assert _calls_orchestrator_execute(leg), (
        "_execute_release_leg no longer calls self._execution_orchestrator.execute — "
        "did the vault-release execute site move? Update this guard (VIB-5667)."
    )
    assert _references_commit(leg), (
        "VIB-5667 anti-bypass: _execute_release_leg calls orchestrator.execute WITHOUT a "
        "paired commit(...) — every successful release leg must drive the teardown commit "
        "pipeline (ledger / accounting_events / outbox / sidecar). See CLAUDE.md §Teardown."
    )

    direct_offenders = [
        node.name
        for node in ast.walk(tree)
        if isinstance(node, ast.AsyncFunctionDef | ast.FunctionDef)
        and _is_release_lane(node.name)
        and _calls_orchestrator_execute(node)
    ]
    assert not direct_offenders, (
        f"VIB-5667 anti-bypass: release-lane functions {direct_offenders} call "
        "self._execution_orchestrator.execute directly instead of routing through "
        "_execute_release_leg — a bare execute bypasses the commit pairing. Route every "
        "release execution through _execute_release_leg (VIB-5667)."
    )


def test_vib5667_execute_vault_release_threads_commit_into_release():
    """``execute_vault_release`` (runner_teardown.py) must pass a ``commit=`` kwarg
    into ``release_on_teardown`` so the release legs are commit-paired. Without it
    the lifecycle manager executes bundles with no accounting binding."""
    src = RUNNER_TEARDOWN.read_text(encoding="utf-8")
    tree = ast.parse(src, filename=str(RUNNER_TEARDOWN))
    fn = _find_function(tree, "execute_vault_release")

    threads_commit = False
    for sub in ast.walk(fn):
        if isinstance(sub, ast.Call):
            f = sub.func
            is_release_call = isinstance(f, ast.Attribute) and f.attr == "release_on_teardown"
            if is_release_call and any(kw.arg == "commit" for kw in sub.keywords):
                threads_commit = True
    assert threads_commit, (
        "execute_vault_release does not pass commit= into release_on_teardown — the "
        "vault-release legs would execute without driving commit_teardown_intent (VIB-5667)."
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
