"""LP economics report section.

Groups LP position_events by position_id and surfaces principal, collected
fees, protocol fees, gas, and net PnL from attribution_json.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any

from .loader import AccountingData


def _dec(v: Any) -> Decimal | None:
    if v is None or v == "":
        return None
    try:
        return Decimal(str(v))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _dec0(v: Any) -> Decimal:
    return _dec(v) or Decimal("0")


def _parse_attribution(raw: str | None) -> dict:
    if not raw or raw == "{}":
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except (json.JSONDecodeError, TypeError):
        return {}


@dataclass
class LPPositionSummary:
    position_id: str
    protocol: str
    chain: str
    token0: str
    token1: str

    # Entry state (from OPEN event)
    entry_value_usd: Decimal | None = None

    # Exit state (from CLOSE event)
    exit_value_usd: Decimal | None = None
    is_closed: bool = False

    # Cumulative fees collected
    fees_token0: Decimal = Decimal("0")
    fees_token1: Decimal = Decimal("0")
    protocol_fees_usd: Decimal = Decimal("0")

    # Gas across all events
    total_gas_usd: Decimal = Decimal("0")

    # Net PnL from PnLAttributor (attribution_json on CLOSE)
    net_pnl_usd: Decimal | None = None
    il_usd: Decimal | None = None  # impermanent loss from attribution

    # Position health at last snapshot
    in_range: bool | None = None


@dataclass
class LPSection:
    positions: list[LPPositionSummary] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        return not self.positions

    @property
    def total_net_pnl_usd(self) -> Decimal | None:
        scored = [p.net_pnl_usd for p in self.positions if p.net_pnl_usd is not None]
        if not scored:
            return None
        return sum(scored, Decimal("0"))

    @property
    def total_gas_usd(self) -> Decimal:
        return sum((p.total_gas_usd for p in self.positions), Decimal("0"))


def build_lp_report(data: AccountingData) -> LPSection:
    """Build per-position LP economics summary from position_events."""
    lp_events = [ev for ev in data.position_events if (ev.get("position_type") or "").upper() == "LP"]
    if not lp_events:
        return LPSection()

    # Group by position_id, newest-first → reverse for chronological
    by_id: dict[str, list[dict]] = {}
    for ev in reversed(lp_events):
        pid = ev.get("position_id") or ""
        if not pid:
            continue
        by_id.setdefault(pid, []).append(ev)

    summaries: list[LPPositionSummary] = []
    for position_id, events in by_id.items():
        first = events[0]
        summary = LPPositionSummary(
            position_id=position_id,
            protocol=first.get("protocol") or "",
            chain=first.get("chain") or "",
            token0=first.get("token0") or "",
            token1=first.get("token1") or "",
        )

        for ev in events:
            event_type = (ev.get("event_type") or "").upper()
            gas = _dec0(ev.get("gas_usd"))
            summary.total_gas_usd += gas

            if event_type == "OPEN" and summary.entry_value_usd is None:
                summary.entry_value_usd = _dec(ev.get("value_usd"))

            if event_type == "CLOSE":
                summary.is_closed = True
                summary.exit_value_usd = _dec(ev.get("value_usd"))
                attribution = _parse_attribution(ev.get("attribution_json"))
                if attribution:
                    net = attribution.get("net_pnl_usd")
                    summary.net_pnl_usd = _dec(net) if net is not None else None
                    il = attribution.get("impermanent_loss_usd")
                    summary.il_usd = _dec(il) if il is not None else None

            if event_type in ("COLLECT_FEES", "CLOSE"):
                summary.fees_token0 += _dec0(ev.get("fees_token0"))
                summary.fees_token1 += _dec0(ev.get("fees_token1"))
                summary.protocol_fees_usd += _dec0(ev.get("protocol_fees_usd"))

            if event_type == "SNAPSHOT":
                in_range = ev.get("in_range")
                if isinstance(in_range, bool):
                    summary.in_range = in_range
                elif isinstance(in_range, int):
                    summary.in_range = bool(in_range)
                elif isinstance(in_range, str):
                    summary.in_range = in_range.lower() not in ("false", "0", "")

        summaries.append(summary)

    return LPSection(positions=summaries)
