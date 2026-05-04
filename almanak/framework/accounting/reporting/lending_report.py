"""Lending carry report section.

Groups LendingAccountingEvents by position_key and surfaces per-position
collateral/debt state, health factor, supply/borrow APR, and net carry.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from almanak.framework.accounting.models import LendingAccountingEvent, LendingEventType

from .loader import AccountingData

_MISSING = "—"


def _bps_to_pct(bps: int | None) -> Decimal | None:
    if bps is None:
        return None
    return Decimal(bps) / Decimal("100")


@dataclass
class LendingPositionSummary:
    """Latest observed state for a single lending position."""

    position_key: str
    protocol: str
    chain: str
    asset: str
    market_id: str

    # Latest state (from most-recent event with non-None values)
    collateral_usd: Decimal | None = None
    debt_usd: Decimal | None = None
    net_equity_usd: Decimal | None = None
    health_factor: Decimal | None = None
    liquidation_threshold: Decimal | None = None

    # APRs (bps → %)
    supply_apr_pct: Decimal | None = None
    borrow_apr_pct: Decimal | None = None

    # Cumulative
    total_gas_usd: Decimal = Decimal("0")
    total_interest_delta_usd: Decimal = Decimal("0")
    deleverage_count: int = 0
    is_closed: bool = False


@dataclass
class LendingSection:
    positions: list[LendingPositionSummary] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        return not self.positions


def build_lending_report(data: AccountingData) -> LendingSection:  # noqa: C901
    """Build a per-position lending carry summary from accounting events."""
    if not data.lending_events:
        return LendingSection()

    # Group events by position_key, chronological order (events arrive newest-first
    # from the DB query, so we reverse for correct state-accumulation order).
    by_key: dict[str, list[LendingAccountingEvent]] = {}
    for ev in reversed(data.lending_events):
        by_key.setdefault(ev.position_key, []).append(ev)

    summaries: list[LendingPositionSummary] = []
    for position_key, events in by_key.items():
        first = events[0]
        summary = LendingPositionSummary(
            position_key=position_key,
            protocol=first.identity.protocol,
            chain=first.identity.chain,
            asset=first.asset,
            market_id=first.market_id,
        )

        for ev in events:
            # Apply latest non-None values from each event in order
            if ev.collateral_value_after_usd is not None:
                summary.collateral_usd = ev.collateral_value_after_usd
            if ev.debt_value_after_usd is not None:
                summary.debt_usd = ev.debt_value_after_usd
            if ev.net_equity_after_usd is not None:
                summary.net_equity_usd = ev.net_equity_after_usd
            if ev.health_factor_after is not None:
                summary.health_factor = ev.health_factor_after
            if ev.liquidation_threshold is not None:
                summary.liquidation_threshold = ev.liquidation_threshold
            if ev.supply_apr_bps is not None:
                summary.supply_apr_pct = _bps_to_pct(ev.supply_apr_bps)
            if ev.borrow_apr_bps is not None:
                summary.borrow_apr_pct = _bps_to_pct(ev.borrow_apr_bps)
            if ev.gas_usd is not None:
                summary.total_gas_usd += ev.gas_usd
            if ev.interest_delta_usd is not None:
                summary.total_interest_delta_usd += ev.interest_delta_usd
            if ev.event_type == LendingEventType.DELEVERAGE:
                summary.deleverage_count += 1
            if ev.event_type == LendingEventType.CLOSE:
                summary.is_closed = True

        summaries.append(summary)

    return LendingSection(positions=summaries)
