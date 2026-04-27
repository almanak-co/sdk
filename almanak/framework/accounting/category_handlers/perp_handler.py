"""Perp category handler for AccountingProcessor.

Typed models for perp events are added in VIB-3471.
Until then this handler returns None so the outbox row is marked
processed without writing an accounting_events row.
"""

from __future__ import annotations

from typing import Any


def handle_perp(
    outbox_row: dict[str, Any],
    ledger_row: dict[str, Any],
) -> None:
    """Return None — no typed perp model yet (VIB-3471)."""
    return None
