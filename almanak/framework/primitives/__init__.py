"""Canonical primitives taxonomy for the Almanak SDK.

This module is the authoritative source of truth for mapping `IntentType` to
its primitive, accounting category, position type, lifecycle, and event kind.

Public API:
    - ``classify(intent_type)`` -> :class:`~types.AccountingCategory`
    - ``position_type_for(intent_type)`` -> :class:`~types.PositionKind` | ``None``
    - ``is_async(intent_type)`` -> ``bool``
    - ``record_for(intent_type)`` -> :class:`~types.PrimitiveRecord`

Downstream consumers (`accounting/classifier.py`,
`observability/position_events.py`, `accounting/position_state.py`,
`teardown/models.py`) are migrated to delegate to this module in T2 (VIB-4163
and follow-ups). T1 ships the module foundation only — no consumer behaviour
changes.

See ``docs/internal/discussions/primitives-refactor-20260508.md`` for the
ratified design and ``blueprints/27-accounting.md`` for accounting context.
"""

from __future__ import annotations

from almanak.framework.primitives.taxonomy import (
    ALIASES,
    TAXONOMY,
    classify,
    is_async,
    position_type_for,
    record_for,
)
from almanak.framework.primitives.types import (
    AccountingCategory,
    EventKind,
    LifecyclePhase,
    PositionKind,
    Primitive,
    PrimitiveRecord,
)

__all__ = [
    "ALIASES",
    "AccountingCategory",
    "EventKind",
    "LifecyclePhase",
    "PositionKind",
    "Primitive",
    "PrimitiveRecord",
    "TAXONOMY",
    "classify",
    "is_async",
    "position_type_for",
    "record_for",
]
