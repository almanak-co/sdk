"""Static anti-bypass guard for the vault-settlement accounting lane — VIB-5682.

Mirrors the teardown lane's guard
(``tests/unit/teardown/test_teardown_accounting_anti_bypass.py``), which pins the
teardown source against re-introduction of the "pre-``decide()`` therefore
unaccounted" silent-failure class. Vault settlement (``settleDeposit`` /
``settleRedeem`` / ``updateNewTotalAssets``) is lifecycle-owned and pre-``decide()``:
before VIB-5666 it called ``ExecutionOrchestrator.execute`` directly, so every
settlement tx landed on-chain with **zero** rows in ``transaction_ledger`` /
``accounting_events`` — the identical hole that bit teardown, but firing every
settlement interval.

These tests run at lint speed (no fixtures, no IO beyond a file read):

* SETTLE-1 (AST) — every ``self._execution_orchestrator.execute(...)`` call inside
  ``vault/lifecycle.py`` must sit in a function that also invokes
  ``_emit_settlement_commit``. A new settlement execute that skips the commit
  wiring must trip this guard before it ships.
* SETTLE-2 (source order, per function) — in every function that both executes
  and commits, the first ``_emit_settlement_commit`` call must appear AFTER the
  first ``_execution_orchestrator.execute`` call (execute → commit order); a
  commit-before-execute shape would drop accounting on later retries.
* SETTLE-3 (AST) — ``_emit_settlement_commit`` must actually drive the runner's
  settlement-commit callable (``self._settlement_commit``); a regression that
  stubs the emit into a no-op would pass SETTLE-1/2 while writing zero rows.
"""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
VAULT_LIFECYCLE = ROOT / "almanak" / "framework" / "vault" / "lifecycle.py"
SETTLEMENT_COMMIT = ROOT / "almanak" / "framework" / "runner" / "settlement_commit.py"

# The commit-pairing wrapper every settlement execute must be bracketed by.
_COMMIT_WRAPPER = "_emit_settlement_commit"
# Orchestrator-execute forms the guard treats as a capital-moving settlement tx.
_EXECUTE_ATTRS = {"execute", "execute_bundle"}
# The instance attribute the settlement lane executes through.
_ORCHESTRATOR_ATTR = "_execution_orchestrator"


def _find_function(tree: ast.AST, name: str) -> ast.AST:
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef | ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"function {name!r} not found in {VAULT_LIFECYCLE}")


def _calls_orchestrator_execute(node: ast.AST) -> bool:
    """True if ``node`` contains a ``self._execution_orchestrator.execute(...)``
    (or ``.execute_bundle``) call."""
    for sub in ast.walk(node):
        if isinstance(sub, ast.Attribute) and sub.attr in _EXECUTE_ATTRS:
            val = sub.value
            if isinstance(val, ast.Attribute) and val.attr == _ORCHESTRATOR_ATTR:
                return True
    return False


def _references_emit_commit(node: ast.AST) -> bool:
    """True if ``node`` references the ``_emit_settlement_commit`` wrapper."""
    for sub in ast.walk(node):
        if isinstance(sub, ast.Attribute) and sub.attr == _COMMIT_WRAPPER:
            return True
        if isinstance(sub, ast.Name) and sub.id == _COMMIT_WRAPPER:
            return True
    return False


def _references_teardown_commit(node: ast.AST) -> bool:
    """True if ``node`` invokes a ``commit(...)`` callback — the pairing used by the
    VIB-5667 vault-RELEASE legs (Open->Closing->Closed on teardown).

    Release executes through the SAME orchestrator but is TEARDOWN-lane, not
    settlement-lane: each leg pairs with the ``commit`` callback the runner binds to
    ``commit_teardown_intent`` (ledger -> outbox+fire -> sidecar), NOT
    ``_emit_settlement_commit``. It is guarded end-to-end by the teardown anti-bypass
    test (``tests/unit/teardown/test_teardown_accounting_anti_bypass.py``). Recognize
    that pairing here so this settlement guard does not false-trip on the single
    release execution site (``_execute_release_leg``) while still catching a bare,
    unpaired settlement execute (settlement functions carry no ``commit`` param —
    they use ``_emit_settlement_commit``, so this does not weaken SETTLE-1).
    """
    for sub in ast.walk(node):
        if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Name) and sub.func.id == "commit":
            return True
    return False


def test_settle1_orchestrator_execute_paired_with_emit_commit() -> None:
    """Every ``self._execution_orchestrator.execute(...)`` in ``vault/lifecycle.py``
    must live in a function that also calls ``_emit_settlement_commit``.

    Adding a new settlement execute without the paired commit wiring — the exact
    VIB-5666 regression — fails this test.
    """
    src = VAULT_LIFECYCLE.read_text(encoding="utf-8")
    tree = ast.parse(src, filename=str(VAULT_LIFECYCLE))

    offenders: list[str] = []
    teardown_paired: list[str] = []
    found_any_execute = False
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef | ast.FunctionDef):
            if _calls_orchestrator_execute(node):
                found_any_execute = True
                emit_paired = _references_emit_commit(node)
                if not (emit_paired or _references_teardown_commit(node)):
                    offenders.append(node.name)
                elif not emit_paired:
                    # Paired ONLY via the teardown commit(...) form, not
                    # _emit_settlement_commit — the vault-release lane.
                    teardown_paired.append(node.name)

    assert found_any_execute, (
        "No self._execution_orchestrator.execute call found in vault/lifecycle.py — "
        "did the settlement execution wiring move? Update this guard."
    )
    assert not offenders, (
        f"VIB-5682 settlement anti-bypass guard tripped: {offenders} call "
        "self._execution_orchestrator.execute without a paired _emit_settlement_commit. "
        "Every successful on-chain settlement tx must drive the runner's settlement-commit "
        "pipeline (ledger -> outbox+fire -> sidecar) so transaction_ledger / accounting_events "
        "rows land. See docs/internal/blueprints/27-accounting.md and CLAUDE.md §Teardown "
        "(settlement shares the inverted loud-but-never-block semantics)."
    )
    # Precision guard: the teardown-commit(...) pairing exemption is for the vault-
    # RELEASE lane ONLY. Exactly one function may rely on it — `_execute_release_leg`.
    # If a NEW execute-bearing function pairs via commit(...) instead of
    # _emit_settlement_commit, it is a new lane that needs its own anti-bypass
    # scrutiny (and its own guard), not a silent ride on this exemption.
    assert set(teardown_paired) <= {"_execute_release_leg"}, (
        f"New execute-bearing function(s) {sorted(set(teardown_paired) - {'_execute_release_leg'})} "
        "pair with a teardown commit(...) callback instead of _emit_settlement_commit. Only "
        "_execute_release_leg (the vault-release lane, guarded by the teardown anti-bypass test) "
        "is allowed this exemption — a new one needs its own commit-pairing scrutiny."
    )


def _first_lineno(node: ast.AST, predicate) -> int | None:
    """Smallest lineno of any sub-node matching ``predicate``, or None."""
    linenos = [
        sub.lineno
        for sub in ast.walk(node)
        if predicate(sub) and hasattr(sub, "lineno")
    ]
    return min(linenos) if linenos else None


def test_settle2_execute_followed_by_emit_commit_in_source_order() -> None:
    """Per-function source-order check (AST): in EVERY ``vault/lifecycle.py``
    function that both executes and commits, the first ``_emit_settlement_commit``
    call must appear AFTER the first ``self._execution_orchestrator.execute``
    call. A commit-before-execute shape in ANY leg function would silently drop
    accounting on later retries — a global first-occurrence check would miss a
    mis-ordered later function.
    """
    src = VAULT_LIFECYCLE.read_text(encoding="utf-8")
    tree = ast.parse(src, filename=str(VAULT_LIFECYCLE))

    def _is_execute(sub: ast.AST) -> bool:
        return (
            isinstance(sub, ast.Attribute)
            and sub.attr in _EXECUTE_ATTRS
            and isinstance(sub.value, ast.Attribute)
            and sub.value.attr == _ORCHESTRATOR_ATTR
        )

    def _is_commit_call(sub: ast.AST) -> bool:
        # Invocation sites only — Call whose func references the wrapper.
        if not isinstance(sub, ast.Call):
            return False
        fn = sub.func
        return (isinstance(fn, ast.Attribute) and fn.attr == _COMMIT_WRAPPER) or (
            isinstance(fn, ast.Name) and fn.id == _COMMIT_WRAPPER
        )

    checked = 0
    offenders: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.AsyncFunctionDef | ast.FunctionDef):
            continue
        first_execute = _first_lineno(node, _is_execute)
        first_commit = _first_lineno(node, _is_commit_call)
        if first_execute is None or first_commit is None:
            continue  # pairing itself is SETTLE-1's job
        checked += 1
        if first_commit < first_execute:
            offenders.append(f"{node.name} (commit@{first_commit} < execute@{first_execute})")

    assert checked > 0, (
        "Expected at least one vault/lifecycle.py function with both an "
        "_execution_orchestrator.execute call and an _emit_settlement_commit call "
        "(VIB-5666 wiring missing?)."
    )
    assert not offenders, (
        "Per-function source-order anti-bypass tripped — _emit_settlement_commit is called "
        f"BEFORE the execute it should follow in: {offenders}"
    )


def test_settle3_emit_commit_drives_settlement_commit_callable() -> None:
    """``_emit_settlement_commit`` must actually invoke the runner-injected
    settlement-commit callable (``self._settlement_commit``).

    Without this, SETTLE-1/2 could pass while the emit was a no-op that writes
    zero ledger / accounting rows — the very silent failure this lane exists to
    close. Static AST check over the wrapper body.
    """
    src = VAULT_LIFECYCLE.read_text(encoding="utf-8")
    tree = ast.parse(src, filename=str(VAULT_LIFECYCLE))
    fn = _find_function(tree, _COMMIT_WRAPPER)

    calls_commit = any(
        isinstance(sub, ast.Attribute) and sub.attr == "_settlement_commit" for sub in ast.walk(fn)
    )
    assert calls_commit, (
        "_emit_settlement_commit no longer references self._settlement_commit — the settlement "
        "commit pipeline (ledger -> outbox+fire -> sidecar) is bypassed and settlement txs will "
        "write zero transaction_ledger / accounting_events rows (VIB-5666 regression)."
    )


def test_settlement_commit_module_drives_ledger_and_outbox() -> None:
    """``settlement_commit.py`` (the runner-owned pipeline) must both write the
    ledger row (``_write_ledger_entry``) AND fire the accounting drain
    (``_write_outbox_and_fire_processor``).

    Companion to VIB-4895's teardown guard: a regression that drops either step
    would land settlement txs on-chain while writing zero typed accounting events.
    """
    src = SETTLEMENT_COMMIT.read_text(encoding="utf-8")
    tree = ast.parse(src, filename=str(SETTLEMENT_COMMIT))

    references_ledger = False
    references_outbox = False
    for sub in ast.walk(tree):
        if isinstance(sub, ast.Attribute) and sub.attr == "_write_ledger_entry":
            references_ledger = True
        if isinstance(sub, ast.Attribute) and sub.attr == "_write_outbox_and_fire_processor":
            references_outbox = True

    assert references_ledger, (
        "settlement_commit.py no longer references _write_ledger_entry — settlement txs would "
        "write zero transaction_ledger rows (VIB-5666 regression)."
    )
    assert references_outbox, (
        "settlement_commit.py no longer references _write_outbox_and_fire_processor — settlement "
        "txs would write a ledger row but zero accounting_events (the drain never fires)."
    )
