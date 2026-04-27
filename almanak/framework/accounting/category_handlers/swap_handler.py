"""Swap category handler for AccountingProcessor.

Typed models for swap events are added in VIB-3473.
Until then this handler returns None so the outbox row is marked
processed without writing an accounting_events row.
"""

from __future__ import annotations

from typing import Any


def handle_swap(
    outbox_row: dict[str, Any],
    ledger_row: dict[str, Any],
) -> None:
    """Return None — no typed swap model yet (VIB-3473)."""
    return None
