"""Pendle accounting report provider."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any, ClassVar

from almanak.connectors._strategy_base.accounting_report_registry import (
    AccountingReportCapability,
    AccountingReportConnector,
    AccountingReportSectionCapability,
)
from almanak.framework.accounting.models import PendleAccountingEvent, PendleEventType

_MISSING = "—"
_LINE = "-" * 44

# Connector-local copies keep Pendle rendering self-contained; preserve parity with
# framework accounting report formatters when changing output policy.


def _money(value: Decimal | None, width: int = 8) -> str:
    if value is None:
        return _MISSING
    sign = "-" if value < 0 else " "
    return f"{sign}${abs(value):>{width},.2f}"


def _pct(value: Decimal | None, decimals: int = 2) -> str:
    if value is None:
        return _MISSING
    return f"{value:.{decimals}f}%"


def _decimal_to_json(value: Decimal | None) -> str | None:
    return str(value) if value is not None else None


def _bps_to_pct(bps: int | None) -> Decimal | None:
    if bps is None:
        return None
    return Decimal(bps) / Decimal("100")


def _market_display(market_id: str) -> str:
    return f"{market_id[:16]}…" if len(market_id) > 16 else market_id


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
    pt_price: Decimal | None = None
    implied_apr_pct_at_entry: Decimal | None = None
    implied_apr_pct_latest: Decimal | None = None
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


def _pendle_events_from_data(data: Any) -> list[PendleAccountingEvent]:
    """Read Pendle events from connector-owned data, with legacy mirror fallback."""
    connector_events = getattr(data, "connector_events", {}) or {}
    events = connector_events.get("pendle")
    if events is None:
        events = getattr(data, "pendle_events", [])
    return list(events or [])


def build_pendle_report(data: Any) -> PendleSection:
    """Build a per-position Pendle yield summary from connector-owned accounting events."""
    pendle_events = _pendle_events_from_data(data)
    if not pendle_events:
        return PendleSection()

    # Events arrive newest-first; reverse to process chronologically.
    by_key: dict[str, list[PendleAccountingEvent]] = {}
    for ev in reversed(pendle_events):
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

            if ev.event_type == PendleEventType.PT_BUY and summary.implied_apr_pct_at_entry is None:
                summary.implied_apr_pct_at_entry = _bps_to_pct(ev.implied_apr_bps)

            if ev.implied_apr_bps is not None:
                summary.implied_apr_pct_latest = _bps_to_pct(ev.implied_apr_bps)

            if ev.pt_amount is not None:
                summary.pt_amount = ev.pt_amount
            if ev.pt_price is not None:
                summary.pt_price = ev.pt_price
            if ev.days_to_maturity is not None:
                summary.days_to_maturity = ev.days_to_maturity

            if ev.realized_yield_usd is not None:
                summary.realized_yield_usd += ev.realized_yield_usd

            if ev.event_type == PendleEventType.PT_REDEEM:
                summary.is_redeemed = True

        summaries.append(summary)

    return PendleSection(positions=summaries)


def render_pendle_section(section: PendleSection) -> str:
    if section.is_empty:
        return ""
    lines: list[str] = ["", "Pendle Positions", _LINE]
    for pos in section.positions:
        status = "REDEEMED" if pos.is_redeemed else "ACTIVE"
        lines.append(f"  {pos.pt_token} [{pos.protocol} / {pos.chain}] [{status}]")
        lines.append(f"    Market:        {_market_display(pos.market_id)}")
        if pos.maturity_timestamp:
            lines.append(f"    Maturity:      {pos.maturity_timestamp.date()}")
        if pos.days_to_maturity is not None:
            lines.append(f"    Days to mat.:  {pos.days_to_maturity}")
        if pos.pt_amount is not None:
            lines.append(f"    PT amount:     {pos.pt_amount:.4f}")
        if pos.pt_price is not None:
            lines.append(f"    PT price:      {pos.pt_price:.4f} (underlying)")
        if pos.implied_apr_pct_at_entry is not None:
            lines.append(f"    APR at entry:  {_pct(pos.implied_apr_pct_at_entry)}")
        if pos.implied_apr_pct_latest is not None and pos.implied_apr_pct_latest != pos.implied_apr_pct_at_entry:
            lines.append(f"    APR latest:    {_pct(pos.implied_apr_pct_latest)}")
        if pos.realized_yield_usd != 0:
            lines.append(f"    Realized yld:  {_money(pos.realized_yield_usd)}")
        lines.append(f"    Events:        {pos.event_count}")
        lines.append("")
    return "\n".join(lines)


def pendle_section_to_dict(section: PendleSection) -> dict[str, Any]:
    return {
        "positions": [
            {
                "position_key": p.position_key,
                "market_id": p.market_id,
                "pt_token": p.pt_token,
                "protocol": p.protocol,
                "chain": p.chain,
                "is_redeemed": p.is_redeemed,
                "pt_amount": _decimal_to_json(p.pt_amount),
                "pt_price": _decimal_to_json(p.pt_price),
                "implied_apr_pct_at_entry": _decimal_to_json(p.implied_apr_pct_at_entry),
                "implied_apr_pct_latest": _decimal_to_json(p.implied_apr_pct_latest),
                "days_to_maturity": p.days_to_maturity,
                "maturity_timestamp": p.maturity_timestamp.isoformat() if p.maturity_timestamp else None,
                "realized_yield_usd": str(p.realized_yield_usd),
                "event_count": p.event_count,
            }
            for p in section.positions
        ]
    }


class PendleAccountingReportConnector(
    AccountingReportConnector,
    AccountingReportCapability,
    AccountingReportSectionCapability,
):
    """Deserialize and render Pendle-specific accounting events for reporting."""

    key: ClassVar[str] = "pendle"
    strategy_class: ClassVar[str] = "pendle"
    event_types: ClassVar[frozenset[str]] = frozenset(event_type.value for event_type in PendleEventType)
    section_key: ClassVar[str] = "pendle"
    section_order: ClassVar[int] = 300

    def deserialize_event(self, identity: Any, payload_json: str) -> PendleAccountingEvent:
        return PendleAccountingEvent.from_payload_json(identity, payload_json)

    def build_section(self, data: Any) -> Any:
        return build_pendle_report(data)

    def render_text(self, section: Any, data: Any) -> str:
        return render_pendle_section(section)

    def to_json(self, section: Any) -> dict[str, Any]:
        return pendle_section_to_dict(section)


__all__ = [
    "PendleAccountingReportConnector",
    "PendlePositionSummary",
    "PendleSection",
    "build_pendle_report",
    "pendle_section_to_dict",
    "render_pendle_section",
]
