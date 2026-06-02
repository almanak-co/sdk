"""Per-position PnL summary computed from stored accounting events (VIB-3424).

Called synchronously from PortfolioValuer to enrich PositionValue objects with
cost_basis_usd, unrealized_pnl_usd, realized_pnl_usd, entry_timestamp, and
ledger_entry_id pulled from the local accounting_events SQLite table.

None discipline: fields that are missing from a payload are skipped, never
fabricated as zero — consistent with the accounting foundation's UNAVAILABLE
vs Decimal("0") distinction.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation


@dataclass
class PositionPnLSummary:
    """Aggregated PnL state for one position derived from accounting events."""

    cost_basis_usd: Decimal
    realized_pnl_usd: Decimal
    entry_timestamp: str
    latest_timestamp: str
    latest_ledger_entry_id: str


def compute_position_pnl(events: list[dict]) -> PositionPnLSummary | None:
    """Compute a PnL summary from raw accounting_events rows for one position_key.

    events: rows from the accounting_events table (any order; sorted internally).

    Cost basis rules:
      SUPPLY / BORROW            → += principal_delta_usd  (capital added)
      WITHDRAW / REPAY / DELEVERAGE → -= principal_delta_usd  (capital returned or repaid)
      None values are skipped — UNAVAILABLE amounts are never treated as zero.

    Realized PnL rules:
      REPAY / DELEVERAGE → -= interest_delta_usd  (borrow interest paid is a realized cost)
      WITHDRAW           → += interest_delta_usd  (supply yield received is a realized gain)
      None interest_delta_usd is skipped (may be UNAVAILABLE if no BORROW lots).

    VIB-4974: DELEVERAGE is structurally a repay — it routes through the same
    ``basis_store.match_repay`` path as REPAY (lending_accounting.py) and
    carries borrow-side interest.  It therefore belongs on the debt
    (subtract) side for both principal and interest, alongside REPAY.

    Returns None when events is empty.
    """
    if not events:
        return None

    sorted_events = sorted(events, key=lambda e: e.get("timestamp", ""))
    cost_basis = Decimal("0")
    realized_pnl = Decimal("0")

    for ev in sorted_events:
        try:
            payload = json.loads(ev.get("payload_json") or "{}")
        except (json.JSONDecodeError, TypeError):
            continue

        event_type = ev.get("event_type", "")
        principal_raw = payload.get("principal_delta_usd")
        interest_raw = payload.get("interest_delta_usd")

        # Principal accounting — parse failure skips only the principal, not interest.
        if principal_raw is not None:
            try:
                principal = Decimal(str(principal_raw))
                if event_type in ("SUPPLY", "BORROW"):
                    cost_basis += principal
                elif event_type in ("WITHDRAW", "REPAY", "DELEVERAGE"):
                    cost_basis -= principal
            except InvalidOperation:
                pass

        if interest_raw is not None and event_type in ("REPAY", "DELEVERAGE", "WITHDRAW"):
            try:
                interest = Decimal(str(interest_raw))
                if event_type in ("REPAY", "DELEVERAGE"):
                    realized_pnl -= interest  # borrow interest paid is a cost to the borrower
                else:
                    realized_pnl += interest  # supply yield received is a gain
            except InvalidOperation:
                pass

    oldest = sorted_events[0]
    latest = sorted_events[-1]

    return PositionPnLSummary(
        cost_basis_usd=max(cost_basis, Decimal("0")),
        realized_pnl_usd=realized_pnl,
        entry_timestamp=oldest.get("timestamp", ""),
        latest_timestamp=latest.get("timestamp", ""),
        latest_ledger_entry_id=latest.get("ledger_entry_id") or "",
    )
