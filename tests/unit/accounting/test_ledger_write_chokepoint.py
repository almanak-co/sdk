"""Every ledger write goes through the commit primitive (VIB-6043 leg 2).

The write-time Empty != Zero guard lives in
``almanak.framework.accounting.commit.save_ledger_and_registry``. A guard is
only as good as its coverage: a production path that calls
``state_manager.save_ledger_entry(...)`` directly writes a row the guard never
sees, and the failure mode is silent — an over-claiming success row lands and
nothing complains.

This is a static guard in the same shape as
``tests/unit/teardown/test_teardown_accounting_anti_bypass.py``.
"""

from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
ALMANAK = REPO_ROOT / "almanak"

#: The commit primitive IS the chokepoint — it must call the state manager.
_ALLOWED = {ALMANAK / "framework" / "accounting" / "commit.py"}

#: Storage-layer definitions / gateway servicers implement or delegate the
#: method; they are not callers routing around the guard.
#:
#: ``almanak/gateway`` is allowed here ONLY because BOTH of its ledger write
#: RPCs apply the guard themselves before delegating — they are INDEPENDENT
#: write boundaries, not pass-throughs, and the gateway must not trust that its
#: caller already ran the guard. That is asserted by
#: ``test_the_gateway_rpcs_apply_the_guard_at_their_own_boundary`` below and by
#: ``tests/unit/gateway/test_save_ledger_entry_guard.py``; without those, this
#: allowance would be a hole rather than an exemption.
#:
#: This exemption was a REAL HOLE twice: first for ``SaveLedgerEntry`` (fixed in
#: f99a618) and then for its sibling ``SaveLedgerAndRegistry``, which carries
#: the registry/LP lane and was still persisting caller-supplied success and
#: amounts verbatim in both DB branches. A directory-wide allowance is only ever
#: as good as the assertions that earn it — if you add a new gateway ledger
#: writer, add the matching positive assertion below in the same commit.
_ALLOWED_DIRS = (
    ALMANAK / "framework" / "state",
    ALMANAK / "gateway",
)

#: One documented exception: ``_best_effort_ledger_fallback`` is the durability
#: net for a commit that ALREADY failed. Routing it back through the commit
#: primitive would let a systemic failure there take out the net too — the one
#: thing that path exists to prevent. It therefore writes directly, and applies
#: the guard inline instead (asserted by
#: ``test_the_orphan_fallback_applies_the_guard_inline``).
_ALLOWED_FUNCTIONS = {"_best_effort_ledger_fallback"}

#: EVERY storage-layer method that lands a ``transaction_ledger`` row. Matching
#: only ``save_ledger_entry`` left the atomic/registry writers invisible to this
#: guard — a production caller of ``save_ledger_and_registry`` that skipped
#: ``commit.py`` would have written an unguarded row with no test failing.
_METHODS = frozenset(
    {
        "save_ledger_entry",
        "save_ledger_and_registry",
        "save_ledger_and_registry_atomic",
    }
)


def _direct_calls(tree: ast.AST, *, allowed_functions: set[str] | None = None) -> list[int]:
    """Line numbers of direct ``<something>.<ledger-write-method>(...)`` calls.

    Calls inside a function named in ``allowed_functions`` are skipped.
    """
    allowed = allowed_functions or set()
    skip: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef | ast.FunctionDef) and node.name in allowed:
            for inner in ast.walk(node):
                if isinstance(inner, ast.Call):
                    skip.add(id(inner))
    return [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in _METHODS
        and id(node) not in skip
    ]


def _is_allowed(path: Path) -> bool:
    """Whether ``path`` is exempt from the chokepoint rule.

    Uses ``Path.is_relative_to`` rather than string-prefix matching: a naive
    ``str(path).startswith(str(allowed_dir))`` also exempts any SIBLING whose
    name merely shares the prefix (``almanak/gateway_v2/``, ``almanak/state_x/``),
    silently widening the exemption beyond intent.
    """
    return path in _ALLOWED or any(path.is_relative_to(allowed_dir) for allowed_dir in _ALLOWED_DIRS)


def test_only_the_commit_primitive_writes_ledger_rows():
    offenders: list[str] = []

    for path in sorted(ALMANAK.rglob("*.py")):
        if _is_allowed(path):
            continue
        try:
            tree = ast.parse(path.read_text(), filename=str(path))
        except SyntaxError:  # pragma: no cover — generated/templated files
            continue
        for lineno in _direct_calls(tree, allowed_functions=_ALLOWED_FUNCTIONS):
            offenders.append(f"{path.relative_to(REPO_ROOT)}:{lineno}")

    assert not offenders, (
        "transaction_ledger rows must be written through "
        "`almanak.framework.accounting.commit.save_ledger_and_registry`, which applies the "
        "write-time Empty != Zero guard (VIB-6043). Direct `state_manager.save_ledger_entry(...)` "
        "calls bypass it silently.\n  " + "\n  ".join(offenders)
    )


def test_the_guard_can_actually_see_a_violation():
    """Anti-vacuity: a matcher that matches nothing reads as coverage."""
    source = "async def f(sm, entry):\n    await sm.save_ledger_entry(entry)\n"
    assert _direct_calls(ast.parse(source)) == [2]


def test_the_guard_sees_every_ledger_write_method():
    """Anti-vacuity for the widened matcher — one case per method.

    ``save_ledger_entry`` alone left the atomic/registry writers unguarded;
    pin each name so dropping one from ``_METHODS`` fails here.
    """
    for method in sorted(_METHODS):
        source = f"async def f(sm, entry):\n    await sm.{method}(entry)\n"
        assert _direct_calls(ast.parse(source)) == [2], f"matcher is blind to {method}"


def test_the_commit_primitive_applies_the_guard():
    """Pin that the chokepoint actually CALLS the guard before delegating.

    Assert on calls, not on source text. Both helpers are imported at module
    level in ``commit.py``, so ``"classify_ledger_row" in commit_src`` is
    satisfied by the import statement alone.

    Measured, before this was tightened: deleting the entire guard block from
    ``save_ledger_and_registry`` AND the guard from the orphan fallback left
    **6781 unit tests green** — the money guard had no regression net at either
    framework call site, while the gateway RPCs 30 lines below were already
    protected by exactly the ``_called_names`` check used here. That asymmetry
    is the whole reason this test exists, so do NOT weaken it back to a
    substring: ``save_ledger_and_registry`` is a plausible refactor target, and
    a refactor that drops the classify/apply pair would otherwise ship green
    and silently restore the Safe/PancakeSwap ``success=1 amount_in=''
    amount_out=''`` row this PR exists to stop.
    """
    commit_calls = _called_names(
        _function_node(ALMANAK / "framework" / "accounting" / "commit.py", "save_ledger_and_registry")
    )
    assert {"classify_ledger_row", "apply_degradation"} <= commit_calls, (
        "the commit chokepoint must CALL classify_ledger_row + apply_degradation, not "
        f"merely import them (VIB-6043). Calls found: {sorted(commit_calls)}"
    )


def test_the_orphan_fallback_applies_the_guard_inline():
    """The one allowed direct writer must still CALL classify + degrade first.

    Same trap as the commit primitive above, one level subtler: this function
    imports the two helpers INSIDE its own body
    (``strategy_runner.py``), so even ``ast.dump(fallback)`` contains both
    names via the ``ImportFrom`` node — the substring check passed with every
    call deleted. Assert on ``ast.Call`` nodes instead.
    """
    fallback_calls = _called_names(
        _function_node(ALMANAK / "framework" / "runner" / "strategy_runner.py", "_best_effort_ledger_fallback")
    )
    assert {"classify_ledger_row", "apply_degradation"} <= fallback_calls, (
        "the orphan fallback is the one allowed direct ledger writer, so it must apply the "
        "guard inline — it writes a row the chokepoint never sees (VIB-6043). "
        f"Calls found: {sorted(fallback_calls)}"
    )


def _function_node(path: Path, name: str) -> ast.AST:
    """The AST node of a named function in ``path``.

    Raises ``StopIteration`` if the function is gone — a rename that silently
    exempted the function from its guard test would otherwise read as a pass.
    """
    tree = ast.parse(path.read_text(), filename=str(path))
    return next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.AsyncFunctionDef | ast.FunctionDef) and node.name == name
    )


def _gateway_function_node(name: str) -> ast.AST:
    """The AST node of a named function in the gateway state service."""
    return _function_node(ALMANAK / "gateway" / "services" / "state_service.py", name)


def _gateway_function(name: str) -> str:
    """``ast.dump`` of a named function in the gateway state service."""
    return ast.dump(_gateway_function_node(name))


def _called_names(fn: ast.AST) -> set[str]:
    """Names of functions actually CALLED inside ``fn``.

    Substring checks against ``ast.dump`` are not enough: both guard helpers
    are imported *inside* ``_build_ledger_entry_from_request``, so the import
    statement alone satisfies ``"classify_ledger_row" in dump`` even if every
    call is deleted. This inspects ``ast.Call`` nodes, so removing the calls
    genuinely fails the assertion.
    """
    called: set[str] = set()
    for node in ast.walk(fn):
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Name):
            called.add(node.func.id)
        elif isinstance(node.func, ast.Attribute):
            called.add(node.func.attr)
    return called


def test_the_gateway_rpcs_apply_the_guard_at_their_own_boundary():
    """`almanak/gateway` is allow-listed above — this is what earns that.

    BOTH ledger write RPCs persist the caller's `success` / amounts into the
    PostgreSQL and SQLite branches. Each is a hosted write path AND a trust
    boundary, so each must apply the guard itself rather than assume the
    caller did.

    `SaveLedgerEntry` was flagged by CodeRabbit on PR #3441 and fixed in
    f99a618; its sibling `SaveLedgerAndRegistry` — the registry/LP lane, i.e.
    the VIB-6051 shape — was still unguarded behind this same allowlist entry
    and is fixed here. One assertion per boundary, so removing either guard
    fails this test.
    """
    entry_body = _gateway_function("SaveLedgerEntry")
    assert "degrade_row_fields" in entry_body, (
        "the gateway SaveLedgerEntry RPC must apply the Empty != Zero guard at its own "
        "boundary — otherwise any gRPC client can still write success=True with "
        "unmeasured amounts (VIB-6043)"
    )
    assert entry_body.count("ledger_success") >= 2, "both persistence branches must use the guarded value"

    # SaveLedgerAndRegistry guards at its single construction point, so the
    # object BOTH branches consume is already degraded.
    #
    # Assert on CALLS, not on an ``ast.dump`` substring: the guard helpers are
    # imported inside this function, so a substring check is satisfied by the
    # import alone and passes even with every call deleted.
    builder_calls = _called_names(_gateway_function_node("_build_ledger_entry_from_request"))
    assert {"classify_ledger_row", "apply_degradation"} <= builder_calls, (
        "SaveLedgerAndRegistry builds its LedgerEntry here and both the PostgreSQL "
        "INSERT and the SQLite delegate consume it — without the guard at this "
        "construction point an atomic-RPC caller can persist success=True with "
        f"unmeasured amounts (VIB-6043 leg 2). Calls found: {sorted(builder_calls)}"
    )
    # Same treatment for SaveLedgerEntry's own guard application.
    assert "degrade_row_fields" in _called_names(_gateway_function_node("SaveLedgerEntry")), (
        "SaveLedgerEntry references degrade_row_fields but never calls it"
    )
