"""``AccountingProcessor._dispatch`` is a registry lookup, not category-branching code.

VIB-4163 (T3). Covers UAT card steps:

- D1.S4 — static AST gate forbidding any ``category``-comparing branch except the
  documented ``NO_ACCOUNTING`` and missing-handler guards.
- D1.S5 — black-box behavioural test parameterized over every registered category
  (including ``TRANSFER``), proving ``_dispatch`` calls the registered handler
  exactly once with a ``HandlerContext`` and returns its return value.
- D3.F3 (behavioural prong) — missing-handler-for-classified-category returns ``None``
  and emits an ERROR log line; per-category coverage.
"""

from __future__ import annotations

import ast
import inspect
import logging
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from almanak.framework.accounting import category_handlers as ch
from almanak.framework.accounting.basis import FIFOBasisStore
from almanak.framework.accounting.category_handlers import HandlerContext
from almanak.framework.accounting.processor import AccountingProcessor
from almanak.framework.primitives.types import AccountingCategory


# ─── D1.S4 — static AST gate ─────────────────────────────────────────────────


def _dispatch_method_ast() -> ast.FunctionDef:
    src = Path(inspect.getsourcefile(AccountingProcessor)).read_text()
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == "AccountingProcessor":
            for item in node.body:
                if isinstance(item, ast.FunctionDef | ast.AsyncFunctionDef) and item.name == "_dispatch":
                    return item  # type: ignore[return-value]
    raise AssertionError("AccountingProcessor._dispatch not found")


def _is_category_name(node: ast.AST) -> bool:
    return isinstance(node, ast.Name) and node.id == "category"


def _is_category_attribute(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "category"
    )


def _is_no_accounting_compare(test: ast.AST) -> bool:
    """Recognise ``category == AccountingCategory.NO_ACCOUNTING`` (the carve-out)."""
    if not isinstance(test, ast.Compare):
        return False
    if not _is_category_name(test.left):
        return False
    if not test.comparators:
        return False
    rhs = test.comparators[0]
    if isinstance(rhs, ast.Attribute) and isinstance(rhs.value, ast.Name):
        return rhs.value.id == "AccountingCategory" and rhs.attr == "NO_ACCOUNTING"
    return False


def _is_missing_handler_guard(test: ast.AST) -> bool:
    """Recognise ``handler is None`` / ``not handler`` / ``category not in HANDLERS``."""
    if isinstance(test, ast.Compare):
        # ``category not in HANDLERS`` (NotIn)
        if (
            _is_category_name(test.left)
            and len(test.ops) == 1
            and isinstance(test.ops[0], ast.NotIn)
        ):
            return True
        # ``handler is None`` / ``handler is not None``
        if (
            isinstance(test.left, ast.Name)
            and test.left.id == "handler"
            and len(test.ops) == 1
            and isinstance(test.ops[0], ast.Is | ast.IsNot)
        ):
            return True
    if isinstance(test, ast.UnaryOp) and isinstance(test.op, ast.Not):
        if isinstance(test.operand, ast.Name) and test.operand.id == "handler":
            return True
    return False


def test_dispatch_ast_has_no_category_branching() -> None:
    """``_dispatch`` MUST NOT branch on ``category`` outside the carve-out.

    Carve-out: exactly the documented ``NO_ACCOUNTING`` early-return and the
    missing-handler guard. Any other Compare/Attribute read of ``category`` in
    an If test, or any Match statement on ``category``, FAILS this gate.
    """
    method = _dispatch_method_ast()

    failures: list[str] = []
    seen_subscript = False

    for node in ast.walk(method):
        # Match statements on `category` are forbidden.
        if isinstance(node, ast.Match) and _is_category_name(node.subject):
            failures.append(f"`match category` at line {node.lineno} — registry should be value-based, not pattern")

        # If tests reading `category` (or `category.value` / `category.name`)
        # outside the carve-out are forbidden.
        if isinstance(node, ast.If):
            if _is_no_accounting_compare(node.test) or _is_missing_handler_guard(node.test):
                continue
            for sub in ast.walk(node.test):
                if _is_category_name(sub) or _is_category_attribute(sub):
                    failures.append(
                        f"if-branch at line {node.lineno} reads `category` outside the carve-out"
                    )
                    break

        # The dispatcher must contain at least one HANDLERS subscript / .get(...).
        if isinstance(node, ast.Subscript):
            if isinstance(node.value, ast.Name) and node.value.id == "HANDLERS":
                seen_subscript = True
        if isinstance(node, ast.Attribute) and node.attr == "get":
            if isinstance(node.value, ast.Name) and node.value.id == "HANDLERS":
                seen_subscript = True

        # Lazy imports inside `_dispatch` are forbidden — handler imports must
        # happen at package init.
        if isinstance(node, ast.ImportFrom) and node.module and "category_handlers" in node.module:
            failures.append(
                f"lazy import at line {node.lineno} inside `_dispatch` — handler imports must "
                "happen at package init via category_handlers/__init__.py"
            )

    if not seen_subscript:
        failures.append("`_dispatch` must contain at least one `HANDLERS[...]` or `HANDLERS.get(...)`")

    assert not failures, "\n".join(failures)


# ─── D1.S5 — black-box behavioural test ──────────────────────────────────────


_CLASSIFIABLE_CATEGORIES = sorted(
    (set(ch.HANDLERS.keys()) - {AccountingCategory.NO_ACCOUNTING}),
    key=lambda c: c.value,
)


def _build_processor() -> AccountingProcessor:
    return AccountingProcessor(
        state_manager=None,
        basis_store=FIFOBasisStore(),
        deployment_id="dep-test",
    )


@pytest.mark.parametrize("category", _CLASSIFIABLE_CATEGORIES, ids=lambda c: c.value)
def test_dispatch_calls_registered_handler_for_every_registered_category(
    category: AccountingCategory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """For every registered category, ``_dispatch`` returns the bound handler's value.

    A ``_dispatch`` that contains a ``HANDLERS[...]`` expression but discards the
    call would FAIL: the assertion ``returned is sentinel`` checks return-value
    identity, not just truthiness.
    """
    sentinel = object()
    handler_mock = MagicMock(return_value=sentinel)

    fake_registry = {category: handler_mock}
    monkeypatch.setattr("almanak.framework.accounting.processor.HANDLERS", fake_registry)
    monkeypatch.setattr(
        "almanak.framework.accounting.processor.classify",
        lambda *_args, **_kwargs: category,
    )

    processor = _build_processor()
    outbox = {"id": "ob", "wallet_address": "0xwallet"}
    ledger = {"id": "led", "intent_type": "X", "protocol": "p"}
    result = processor._dispatch(outbox, ledger)

    assert result is sentinel
    assert handler_mock.call_count == 1
    args, kwargs = handler_mock.call_args
    assert kwargs == {}
    assert len(args) == 1
    ctx = args[0]
    assert isinstance(ctx, HandlerContext)
    assert ctx.outbox_row is outbox
    assert ctx.ledger_row is ledger


def test_dispatch_returns_none_for_no_accounting(monkeypatch: pytest.MonkeyPatch) -> None:
    """``NO_ACCOUNTING`` short-circuits without consulting the registry."""
    handler_mock = MagicMock()
    monkeypatch.setattr(
        "almanak.framework.accounting.processor.HANDLERS",
        {AccountingCategory.NO_ACCOUNTING: handler_mock},
    )
    monkeypatch.setattr(
        "almanak.framework.accounting.processor.classify",
        lambda *_args, **_kwargs: AccountingCategory.NO_ACCOUNTING,
    )

    processor = _build_processor()
    result = processor._dispatch({"id": "ob"}, {"id": "led", "intent_type": "HOLD"})
    assert result is None
    assert handler_mock.call_count == 0


def test_dispatch_invokes_handler_exactly_once(monkeypatch: pytest.MonkeyPatch) -> None:
    sentinel = object()
    handler_mock = MagicMock(return_value=sentinel)
    monkeypatch.setattr(
        "almanak.framework.accounting.processor.HANDLERS",
        {AccountingCategory.LP: handler_mock},
    )
    monkeypatch.setattr(
        "almanak.framework.accounting.processor.classify",
        lambda *_args, **_kwargs: AccountingCategory.LP,
    )
    processor = _build_processor()
    processor._dispatch({"id": "ob"}, {"id": "led", "intent_type": "LP_OPEN"})
    assert handler_mock.call_count == 1


# ─── D3.F3 (behavioural prong) — missing handler returns None + ERROR log ────


@pytest.mark.parametrize("category", _CLASSIFIABLE_CATEGORIES, ids=lambda c: c.value)
def test_dispatch_logs_and_returns_none_when_handler_missing(
    category: AccountingCategory,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Per-category: when the registry is missing the bound handler, ``_dispatch``
    returns ``None`` (no ``KeyError``) and emits a single ERROR log line that
    names the category. This is the dispatcher-level silent-error guard.
    """
    monkeypatch.setattr("almanak.framework.accounting.processor.HANDLERS", {})
    monkeypatch.setattr(
        "almanak.framework.accounting.processor.classify",
        lambda *_args, **_kwargs: category,
    )

    processor = _build_processor()
    with caplog.at_level(logging.ERROR, logger="almanak.framework.accounting.processor"):
        result = processor._dispatch({"id": "ob"}, {"id": "led", "intent_type": "X"})
    assert result is None

    error_records = [
        r for r in caplog.records
        if r.levelno == logging.ERROR and r.name == "almanak.framework.accounting.processor"
    ]
    assert error_records, f"expected ERROR log for missing {category.value} handler"
    msg = "\n".join(r.message for r in error_records)
    assert "category=" in msg or "category" in msg.lower()
    assert category.value in msg
    assert "no handler" in msg.lower() or "not registered" in msg.lower() or "missing" in msg.lower()


def test_dispatch_returns_none_for_unmapped_category(monkeypatch: pytest.MonkeyPatch) -> None:
    """Synthetic 'unmapped category' injection: registry has no entry for the
    classified category. Dispatcher must NOT raise ``KeyError``.

    Implemented by reusing the classifier monkeypatch above with an empty
    registry — same code path as the per-category parameterized version, but
    here we explicitly assert the no-KeyError behaviour.
    """
    monkeypatch.setattr("almanak.framework.accounting.processor.HANDLERS", {})
    monkeypatch.setattr(
        "almanak.framework.accounting.processor.classify",
        lambda *_args, **_kwargs: AccountingCategory.LP,
    )
    processor = _build_processor()
    # MUST NOT raise.
    result = processor._dispatch({"id": "ob"}, {"id": "led", "intent_type": "LP_OPEN"})
    assert result is None
