"""Static AST guard: no payload-side re-summing into ``portfolio_metrics.gas_spent_usd``.

VIB-4225 §8 drift #3 — the gas trail is the responsibility of the
``transaction_ledger`` aggregator at ``runner_state._populate_gas_spent_usd``.
A future commit must NEVER write to ``gas_spent_usd`` from a payload-side
field (``payload.gas_usd`` / ``event.gas_usd`` / ``accounting_events.gas_usd``)
because that would double-count gas already captured at ledger level by the
swap / lending / vault accounting handlers.

Regex grep is brittle (false positives on comments, false negatives on
renamed locals). This guard parses the relevant modules with ``ast`` and
walks ``Assign`` / ``AugAssign`` nodes whose target string contains
``gas_spent_usd``, asserting the value subtree references only ledger-side
sources (``transaction_ledger`` / ``sum_ledger_gas_usd``) — never payload
fields.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]

# Modules in scope: anywhere `gas_spent_usd` could plausibly be assigned.
# Keep this list narrow — over-coverage produces false-positive blockers
# (e.g. dataclass field declarations like ``gas_spent_usd: Decimal = ...``
# are NOT bugs).
SCOPED_MODULES = [
    "almanak/framework/runner/runner_state.py",
    "almanak/framework/accounting/lending_accounting.py",
    "almanak/framework/accounting/vault_accounting.py",
    "almanak/framework/accounting/perp_accounting.py",
    "almanak/framework/accounting/lp_accounting.py",
    "almanak/framework/accounting/pendle_accounting.py",
    "almanak/framework/accounting/pendle_pt_accounting.py",
    "almanak/framework/accounting/pendle_pt_sell_accounting.py",
    "almanak/framework/accounting/pendle_redeem_accounting.py",
]

# Tokens that signal a forbidden payload-side gas read inside a
# `gas_spent_usd = ...` assignment value subtree. Any AST.Name / AST.Attribute
# with one of these in its dotted path is a violation.
FORBIDDEN_TOKENS = frozenset({
    "payload",          # payload.gas_usd
    "event",            # event.gas_usd (typed AccountingEvent)
    "accounting_event", # accounting_event.gas_usd
    "ev",               # common abbreviation
})


def _iter_target_names(target: ast.AST) -> list[str]:
    """Flatten an Assign / AugAssign target into a list of dotted names."""
    if isinstance(target, ast.Tuple):
        names: list[str] = []
        for elt in target.elts:
            names.extend(_iter_target_names(elt))
        return names
    if isinstance(target, ast.Name):
        return [target.id]
    if isinstance(target, ast.Attribute):
        # `metrics.gas_spent_usd` → "metrics.gas_spent_usd"
        chunks = []
        cur: ast.AST = target
        while isinstance(cur, ast.Attribute):
            chunks.append(cur.attr)
            cur = cur.value
        if isinstance(cur, ast.Name):
            chunks.append(cur.id)
        return [".".join(reversed(chunks))]
    return []


def _value_subtree_references(value: ast.AST) -> set[str]:
    """All Name / Attribute leaf names in the subtree."""
    names: set[str] = set()
    for node in ast.walk(value):
        if isinstance(node, ast.Name):
            names.add(node.id)
        elif isinstance(node, ast.Attribute):
            cur: ast.AST = node
            while isinstance(cur, ast.Attribute):
                names.add(cur.attr)
                cur = cur.value
            if isinstance(cur, ast.Name):
                names.add(cur.id)
    return names


@pytest.mark.parametrize("module_path", SCOPED_MODULES)
def test_no_payload_gas_resum_into_gas_spent_usd(module_path: str) -> None:
    """For every assignment to `*.gas_spent_usd`, the value subtree references
    only ledger-side sources, never payload-side fields.
    """
    file_path = REPO_ROOT / module_path
    if not file_path.exists():
        pytest.skip(f"{module_path} not present (legitimately optional module)")
    source = file_path.read_text()
    tree = ast.parse(source, filename=str(file_path))

    violations: list[tuple[int, str, set[str]]] = []
    for node in ast.walk(tree):
        targets: list[ast.AST]
        if isinstance(node, ast.Assign):
            targets = list(node.targets)
        elif isinstance(node, ast.AugAssign):
            targets = [node.target]
        else:
            continue
        for target in targets:
            for name in _iter_target_names(target):
                if not name.endswith("gas_spent_usd"):
                    continue
                refs = _value_subtree_references(node.value)
                forbidden = refs & FORBIDDEN_TOKENS
                if forbidden:
                    violations.append((node.lineno, name, forbidden))

    assert not violations, (
        f"{module_path} writes gas_spent_usd from payload-side fields "
        f"(forbidden tokens {[v[2] for v in violations]} at lines "
        f"{[v[0] for v in violations]}). VIB-4225 §8 drift #3: gas trail "
        f"is sourced from transaction_ledger only — never re-summed from "
        f"category-handler payloads."
    )
