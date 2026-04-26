"""Pendle yield report section.

Groups PendleAccountingEvents by position_key and surfaces PT/YT/LP
balances, implied APR at entry/current, discount, and realized yield.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

from almanak.framework.accounting.models import PendleAccountingEvent, PendleEventType

from .loader import AccountingData


def _bps_to_pct(bps: int | None) -> Decimal | None:
    if bps is None:
        return None
    return Decimal(bps) / Decimal("100")


@dataclass
class PendlePositionSummary:
    """Summary for a single Pendle market position."""

    position_key: str
    market_id: str
    pt_token: str
    protocol: str
    chain: str

    # Latest PT state
    pt_amount: Decimal | None = None
    pt_price: Decimal | None = None  # price in underlying (0–1 range pre-maturity)
    implied_apr_pct_at_entry: Decimal | None = None  # from first PT_BUY
    implied_apr_pct_latest: Decimal | None = None  # from most recent event
    days_to_maturity: int | None = None
    maturity_timestamp: datetime | None = None

    # Realized yield
    realized_yield_usd: Decimal = Decimal("0")
    is_redeemed: bool = False
    event_count: int = 0


@dataclass
class PendleSection:
    positions: list[PendlePositionSummary] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        return not self.positions


def build_pendle_report(data: AccountingData) -> PendleSection:
    """Build a per-position Pendle yield summary from accounting events."""
    if not data.pendle_events:
        return PendleSection()

    # Events arrive newest-first; reverse to process chronologically.
    by_key: dict[str, list[PendleAccountingEvent]] = {}
    for ev in reversed(data.pendle_events):
        by_key.setdefault(ev.position_key, []).append(ev)

    summaries: list[PendlePositionSummary] = []
    for position_key, events in by_key.items():
        first = events[0]
        summary = PendlePositionSummary(
            position_key=position_key,
            market_id=first.market_id,
            pt_token=first.pt_token,
            protocol=first.identity.protocol,
            chain=first.identity.chain,
            maturity_timestamp=first.maturity_timestamp,
        )

        for ev in events:
            summary.event_count += 1

            # Capture implied APR at entry (first PT_BUY)
            if ev.event_type == PendleEventType.PT_BUY and summary.implied_apr_pct_at_entry is None:
                summary.implied_apr_pct_at_entry = _bps_to_pct(ev.implied_apr_bps)

            # Keep updating latest implied APR
            if ev.implied_apr_bps is not None:
                summary.implied_apr_pct_latest = _bps_to_pct(ev.implied_apr_bps)

            # Latest PT amount and price
            if ev.pt_amount is not None:
                summary.pt_amount = ev.pt_amount
            if ev.pt_price is not None:
                summary.pt_price = ev.pt_price
            if ev.days_to_maturity is not None:
                summary.days_to_maturity = ev.days_to_maturity

            # Accumulate realized yield
            if ev.realized_yield_usd is not None:
                summary.realized_yield_usd += ev.realized_yield_usd

            if ev.event_type == PendleEventType.PT_REDEEM:
                summary.is_redeemed = True

        summaries.append(summary)

    return PendleSection(positions=summaries)
