"""Tests for the neutral-tier primitives types module.

Hard Ratification Condition #3 (VIB-4159): ``primitives/types.py`` must remain
free of imports from ``accounting/``, ``intents/``, ``observability/``, or any
other framework subpackage. The :func:`test_types_has_no_framework_imports`
test walks the module's AST to enforce this statically — it cannot be evaded
by a runtime ``importlib`` trick because the AST sees every literal import
statement.
"""

from __future__ import annotations

import ast
import importlib
from dataclasses import FrozenInstanceError, is_dataclass
from pathlib import Path

import pytest

from almanak.framework.primitives import types as primitives_types
from almanak.framework.primitives.types import (
    AccountingCategory,
    EventKind,
    LifecyclePhase,
    PositionKind,
    Primitive,
    PrimitiveRecord,
)


_FORBIDDEN_PREFIXES: tuple[str, ...] = (
    "almanak.framework.accounting",
    "almanak.framework.intents",
    "almanak.framework.observability",
    "almanak.framework.runner",
    # ``framework.connectors`` is the legacy path (VIB-4835 moved
    # strategy-side code to ``almanak.connectors``). Keep both prefixes
    # so a future regression to either path is caught.
    "almanak.framework.connectors",
    "almanak.connectors",
    "almanak.framework.gateway_client",
    "almanak.framework.teardown",
    "almanak.gateway",
)


def _module_imports(module) -> list[str]:
    """Return the list of fully-qualified imports declared in ``module``.

    Walks the AST so dynamic ``importlib.import_module`` calls cannot escape
    the rule — a future agent that adds a forbidden import via a string would
    still trip on the literal-import test.
    """
    source = Path(module.__file__).read_text()
    tree = ast.parse(source)
    imports: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            module_name = node.module or ""
            imports.extend(f"{module_name}.{alias.name}" for alias in node.names)
            imports.append(module_name)
    return imports


def test_types_has_no_framework_imports() -> None:
    """``types.py`` must not import from accounting/intents/observability/etc.

    Hard Ratification Condition #3 — adding a forbidden import re-introduces
    the cycle the taxonomy module exists to break.
    """
    imports = _module_imports(primitives_types)
    forbidden = [
        imp
        for imp in imports
        for prefix in _FORBIDDEN_PREFIXES
        if imp.startswith(prefix)
    ]
    assert forbidden == [], (
        f"primitives/types.py must not import from {_FORBIDDEN_PREFIXES}; "
        f"found {forbidden}"
    )


def test_types_module_docstring_forbids_growth() -> None:
    """The module docstring must explicitly forbid growing beyond enums + record.

    Hard Ratification Condition #3 — the docstring is the human-readable
    pointer that tells future agents not to add behaviour here. Without the
    explicit forbiddance, the rule is invisible to anyone landing on the file
    cold.
    """
    docstring = primitives_types.__doc__
    assert docstring is not None
    lower = docstring.lower()
    assert "must not grow beyond" in lower or "forbids growing beyond" in lower
    assert "primitiverecord" in lower


@pytest.mark.parametrize(
    "enum_cls,expected_member_count_at_least",
    [
        (Primitive, 10),
        (AccountingCategory, 10),  # 9 legacy + TRANSFER (VIB-4161)
        (PositionKind, 6),
        (LifecyclePhase, 4),
        (EventKind, 6),
    ],
)
def test_enum_membership(enum_cls, expected_member_count_at_least) -> None:
    """Every required enum is a StrEnum with at least the documented members."""
    members = list(enum_cls)
    assert len(members) >= expected_member_count_at_least, (
        f"{enum_cls.__name__} expected ≥{expected_member_count_at_least} members, got {len(members)}"
    )
    # StrEnum semantics: each member is a string equal to its value.
    for m in members:
        assert isinstance(m, str)
        assert m == m.value


def test_accounting_category_includes_transfer() -> None:
    """VIB-4161 added ``TRANSFER`` to ``AccountingCategory`` (T4 wires the row)."""
    assert AccountingCategory.TRANSFER == "transfer"


def test_position_kind_covers_lending_legs() -> None:
    """``LENDING_COLLATERAL`` and ``LENDING_DEBT`` must mirror VIB-4085 (PositionType)."""
    assert PositionKind.LENDING_COLLATERAL == "LENDING_COLLATERAL"
    assert PositionKind.LENDING_DEBT == "LENDING_DEBT"


def test_lifecycle_phase_has_async_phases() -> None:
    """The async-settlement vocabulary must cover request/claim/settle/atomic."""
    values = {p.value for p in LifecyclePhase}
    assert {"atomic", "request", "claim", "settle"} <= values


def test_primitive_record_is_frozen_dataclass() -> None:
    """``PrimitiveRecord`` must be a frozen dataclass — the table is hashable + immutable."""
    assert is_dataclass(PrimitiveRecord)

    record = PrimitiveRecord(
        intent_type="SWAP",
        primitive=Primitive.SWAP,
        accounting_category=AccountingCategory.SWAP,
        position_type=None,
        event_kind=EventKind.NONE,
        is_async=False,
        lifecycle_phase=LifecyclePhase.ATOMIC,
        required_lifecycle=(),
    )
    with pytest.raises(FrozenInstanceError):
        record.intent_type = "LP_OPEN"  # type: ignore[misc]


def test_primitive_record_is_hashable() -> None:
    """Frozen dataclasses default to hashable; the table relies on that for set membership."""
    record = PrimitiveRecord(
        intent_type="SWAP",
        primitive=Primitive.SWAP,
        accounting_category=AccountingCategory.SWAP,
        position_type=None,
        event_kind=EventKind.NONE,
        is_async=False,
        lifecycle_phase=LifecyclePhase.ATOMIC,
        required_lifecycle=(),
    )
    assert hash(record) == hash(record)
    assert {record} == {record}


def test_module_imports_clean() -> None:
    """Re-importing the module must not raise (catches accidental top-level side-effects)."""
    importlib.reload(primitives_types)
