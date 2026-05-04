"""AST-based discovery of ``(protocol, intent_type)`` coverage from intent tests.

Returns a mapping ``(protocol_lower, intent_type_upper) -> list[location]`` where
``location`` is ``"path/to/test.py:line"``. Used by the on-chain coverage gate
to decide whether a matrix pair is exercised by at least one default-on Zodiac
intent test.

Opt-out model (post-Phase-G pivot): every test in ``tests/intents/<chain>/``
runs through Safe + Roles + ``execTransactionWithRole`` by default. A test
"exercises" a ``(protocol, intent_type)`` pair when:

  1. It contains an intent constructor call ``SomeIntent(protocol="...", ...)``
     where the class name maps to a known intent type and ``protocol`` is a
     literal string kwarg.
  2. Neither the enclosing function/method nor its enclosing class carries a
     ``@pytest.mark.no_zodiac(...)`` decorator.

We use AST instead of pytest collection because intent-test conftests pull in
chain-specific imports (Anvil hooks, RPC clients) that can fail to *collect*
on lean dev/CI machines without those binaries — even when we never intend
to *run* the tests, only enumerate their intent shape. AST analysis is robust
to those import errors and runs in well under a second for the full tree.

The discovery handles dynamic forms (computed protocol kwargs, parametrized
markers via ``pytest.param(..., marks=[...])``) by simply ignoring them — if
the kwarg isn't a literal string, the call doesn't contribute coverage. A
test that genuinely needs dynamic protocol selection should also carry a
hand-rolled fallback case file for the gate to honour.
"""

from __future__ import annotations

import ast
import functools
from collections import defaultdict
from pathlib import Path

INTENTS_ROOT = Path(__file__).resolve().parents[2] / "intents"
NO_ZODIAC_MARKER = "no_zodiac"

# Intent class → canonical IntentType.value. Mirrors
# ``_INTENT_CLASS_TO_TYPE`` in ``tests/intents/_permission_onchain_harness.py``;
# kept in sync because the harness uses it at execute-time and the gate uses
# it at discover-time — drift between the two would silently un-cover pairs.
# ``test_marker_discovery_class_map_in_sync`` (in this directory) asserts the
# two tables stay equal; update both whenever a new intent class is added.
INTENT_CLASS_TO_TYPE: dict[str, str] = {
    "SwapIntent": "SWAP",
    "LPOpenIntent": "LP_OPEN",
    "LPCloseIntent": "LP_CLOSE",
    "CollectFeesIntent": "LP_COLLECT_FEES",
    "SupplyIntent": "SUPPLY",
    "WithdrawIntent": "WITHDRAW",
    "BorrowIntent": "BORROW",
    "RepayIntent": "REPAY",
    "PerpOpenIntent": "PERP_OPEN",
    "PerpCloseIntent": "PERP_CLOSE",
    "VaultDepositIntent": "VAULT_DEPOSIT",
    "VaultRedeemIntent": "VAULT_REDEEM",
    "BridgeIntent": "BRIDGE",
    "FlashLoanIntent": "FLASH_LOAN",
}


class MarkerDiscoveryError(RuntimeError):
    """Raised when an AST scan finds something we can't statically resolve."""


def _intent_class_name(call: ast.Call) -> str | None:
    """Return the intent class name if ``call`` is ``SomeIntent(...)``, else ``None``.

    Handles both bare-name (``SwapIntent(...)``) and dotted-attribute
    (``almanak.framework.intents.SwapIntent(...)``) forms.
    """
    func = call.func
    if isinstance(func, ast.Name) and func.id in INTENT_CLASS_TO_TYPE:
        return func.id
    if isinstance(func, ast.Attribute) and func.attr in INTENT_CLASS_TO_TYPE:
        return func.attr
    return None


def _literal_string_kwarg(call: ast.Call, name: str) -> str | None:
    for kw in call.keywords:
        if kw.arg == name and isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
            return kw.value.value
    return None


def _is_no_zodiac_marker_expr(node: ast.expr) -> bool:
    """True iff ``node`` is ``pytest.mark.no_zodiac`` or ``pytest.mark.no_zodiac(...)``."""
    target = node.func if isinstance(node, ast.Call) else node
    return (
        isinstance(target, ast.Attribute)
        and target.attr == NO_ZODIAC_MARKER
        and isinstance(target.value, ast.Attribute)
        and target.value.attr == "mark"
        and isinstance(target.value.value, ast.Name)
        and target.value.value.id == "pytest"
    )


def _has_no_zodiac_decorator(
    node: ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef,
) -> bool:
    """True iff ``node`` carries ``@pytest.mark.no_zodiac(...)``."""
    return any(_is_no_zodiac_marker_expr(deco) for deco in node.decorator_list)


def _module_has_no_zodiac(tree: ast.Module) -> bool:
    """True iff the module sets ``pytestmark = pytest.mark.no_zodiac(...)`` at top level.

    Recognises both single-marker and list-of-markers forms:

        pytestmark = pytest.mark.no_zodiac(reason="...")
        pytestmark = [pytest.mark.no_zodiac(reason="..."), pytest.mark.something_else]
    """
    for stmt in tree.body:
        if not isinstance(stmt, ast.Assign):
            continue
        if not any(isinstance(t, ast.Name) and t.id == "pytestmark" for t in stmt.targets):
            continue
        candidates: list[ast.expr]
        if isinstance(stmt.value, (ast.List, ast.Tuple)):
            candidates = list(stmt.value.elts)
        else:
            candidates = [stmt.value]
        if any(_is_no_zodiac_marker_expr(c) for c in candidates):
            return True
    return False


def _scan_file(path: Path) -> list[tuple[str, str, str]]:
    """Return ``[(protocol_lower, intent_type_upper, location), ...]`` for ``path``.

    Walks every intent constructor call in the file; the enclosing function /
    class chain is checked for ``no_zodiac`` and, if found at any level above
    the call, the call doesn't contribute to coverage.
    """
    try:
        tree = ast.parse(path.read_text(), filename=str(path))
    except SyntaxError as exc:
        raise MarkerDiscoveryError(f"Could not parse {path}: {exc}") from exc

    pairs: list[tuple[str, str, str]] = []
    module_no_zodiac = _module_has_no_zodiac(tree)

    def walk(node: ast.AST, no_zodiac_active: bool) -> None:
        # Decorators on this node may opt the whole subtree out of Zodiac.
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            no_zodiac_active = no_zodiac_active or _has_no_zodiac_decorator(node)

        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.Call):
                cls_name = _intent_class_name(child)
                if cls_name is not None and not no_zodiac_active:
                    protocol = _literal_string_kwarg(child, "protocol")
                    if protocol:
                        pairs.append(
                            (
                                protocol.lower(),
                                INTENT_CLASS_TO_TYPE[cls_name],
                                f"{path}:{child.lineno}",
                            )
                        )
            walk(child, no_zodiac_active)

    walk(tree, module_no_zodiac)
    return pairs


@functools.cache
def collect_intent_test_coverage() -> dict[tuple[str, str], list[str]]:
    """Return ``{(protocol_lower, intent_type_upper): [location, ...]}`` for the full tree.

    Cached for the lifetime of the pytest session — the gate runs once over
    the matrix, calling this once per pytest run.
    """
    if not INTENTS_ROOT.exists():
        return {}
    coverage: dict[tuple[str, str], list[str]] = defaultdict(list)
    for path in sorted(INTENTS_ROOT.rglob("test_*.py")):
        for proto, itype, location in _scan_file(path):
            coverage[(proto, itype)].append(location)
    return dict(coverage)
