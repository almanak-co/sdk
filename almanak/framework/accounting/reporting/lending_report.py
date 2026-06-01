"""Lending carry report section.

Groups LendingAccountingEvents by position_key and surfaces per-position
collateral/debt state, health factor, supply/borrow APR, and net carry.

W1-2 (VIB-4777): snapshot positions are matched by (protocol, chain, asset)
to enrich each summary with ``unrealized_pnl_usd``, ``supply_balance_usd``,
and ``borrow_balance_usd`` from the latest portfolio snapshot.
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

    # W1-2 (VIB-4777): snapshot-sourced unrealized carry fields.
    # Populated by build_lending_report when a matching PortfolioSnapshot
    # position is found.  None = no snapshot data available.
    unrealized_pnl_usd: Decimal | None = None
    supply_balance_usd: Decimal | None = None
    borrow_balance_usd: Decimal | None = None

    # W1-2 phase-2 (gemini review): which side(s) the events represent.
    # "" (default), "SUPPLY", "BORROW", or "MIXED".  Used by
    # ``_enrich_from_snapshot`` to filter snapshot positions by type so a
    # SUPPLY-side summary cannot inherit BORROW snapshot data (or vice
    # versa) when both sides exist for the same asset.
    position_type: str = ""


@dataclass
class LendingSection:
    positions: list[LendingPositionSummary] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        return not self.positions


def _asset_matches(pos_asset: str, snap_tokens: list[str], snap_details: dict) -> bool:
    """Return True when the snapshot position's asset matches pos_asset (case-insensitive).

    Checks ``details["asset"]`` first (set by most lending connectors), then
    falls back to the first element of ``tokens`` (used by some strategies).
    """
    asset_lower = pos_asset.lower()
    details_asset = snap_details.get("asset", "")
    if details_asset and details_asset.lower() == asset_lower:
        return True
    if snap_tokens and snap_tokens[0].lower() == asset_lower:
        return True
    return False


def _snapshot_has_matching_lending_position(snapshot: object, summary: LendingPositionSummary) -> bool:
    """Return True when ``snapshot`` carries a live SUPPLY/BORROW position
    matching ``summary``'s (protocol, chain, asset, side).

    Used by W1-6 (T3a) to infer closed state when the lending events stop
    short of a ``CLOSE`` row — teardown writes REPAY/WITHDRAW but not
    CLOSE, so the renderer needs a second signal to mark the summary as
    settled.

    Side-filtered (CodeRabbit review, W1-6 phase 2): a SUPPLY-side
    summary must NOT be "kept open" by an unrelated BORROW snapshot row
    for the same asset (and vice versa).  Mirrors the side-gating logic
    used in ``_enrich_from_snapshot``.  Any shape failure resolves to
    False, leaving the summary in its prior state (open by default).
    """
    if snapshot is None:
        return False
    try:
        from almanak.framework.teardown.models import PositionType

        positions = getattr(snapshot, "positions", []) or []
        proto_lower = summary.protocol.lower()
        chain_lower = summary.chain.lower()
        allow_supply = summary.position_type in ("", "SUPPLY", "MIXED")
        allow_borrow = summary.position_type in ("", "BORROW", "MIXED")
        for snap_pos in positions:
            try:
                ptype = snap_pos.position_type
                if ptype == PositionType.SUPPLY and not allow_supply:
                    continue
                if ptype == PositionType.BORROW and not allow_borrow:
                    continue
                if ptype not in (PositionType.SUPPLY, PositionType.BORROW):
                    continue
                if (
                    snap_pos.protocol.lower() == proto_lower
                    and snap_pos.chain.lower() == chain_lower
                    and _asset_matches(summary.asset, snap_pos.tokens, snap_pos.details)
                ):
                    return True
            except Exception:  # pragma: no cover
                continue
    except Exception:  # pragma: no cover
        return False
    return False


def _enrich_from_snapshot(summary: LendingPositionSummary, snapshot: object) -> None:
    """Look up matching SUPPLY/BORROW PositionValues in the snapshot and enrich.

    Mutates ``summary`` in-place: sets ``unrealized_pnl_usd``,
    ``supply_balance_usd``, and ``borrow_balance_usd`` when matching
    snapshot positions are found.  Tolerates any shape failure silently.

    Type-filtered (gemini review, W1-2 phase 2): a SUPPLY-side summary
    will not pick up BORROW snapshot data and vice versa.  ``MIXED``
    summaries (a single position with both sides) match both.  Empty
    ``position_type`` (legacy summaries) falls back to the pre-filter
    behaviour and accepts either side.
    """
    if snapshot is None:
        return

    try:
        from almanak.framework.teardown.models import PositionType

        positions = getattr(snapshot, "positions", []) or []
        proto_lower = summary.protocol.lower()
        chain_lower = summary.chain.lower()

        allow_supply = summary.position_type in ("", "SUPPLY", "MIXED")
        allow_borrow = summary.position_type in ("", "BORROW", "MIXED")

        supply_unrealized: Decimal | None = None
        borrow_unrealized: Decimal | None = None

        for snap_pos in positions:
            try:
                if not (
                    snap_pos.protocol.lower() == proto_lower
                    and snap_pos.chain.lower() == chain_lower
                    and _asset_matches(summary.asset, snap_pos.tokens, snap_pos.details)
                ):
                    continue

                ptype = snap_pos.position_type
                unrealized = snap_pos.unrealized_pnl_usd
                value = snap_pos.value_usd

                if ptype == PositionType.SUPPLY and allow_supply:
                    if value is not None:
                        summary.supply_balance_usd = value
                    if unrealized is not None:
                        supply_unrealized = unrealized
                elif ptype == PositionType.BORROW and allow_borrow:
                    if value is not None:
                        # BORROW value_usd is signed negative; store abs.
                        summary.borrow_balance_usd = abs(value)
                    if unrealized is not None:
                        borrow_unrealized = unrealized
            except Exception:  # pragma: no cover
                continue

        net_parts = [u for u in (supply_unrealized, borrow_unrealized) if u is not None]
        if net_parts:
            summary.unrealized_pnl_usd = sum(net_parts, start=Decimal("0"))
    except Exception:  # pragma: no cover
        return


def _apply_sided_post_state(summary: LendingPositionSummary, ev: LendingAccountingEvent) -> None:
    """Apply an event's collateral/debt post-state, scoped to the side it mutated.

    VIB-4792: ``collateral_value_after_usd`` and ``debt_value_after_usd`` carry
    the WHOLE position's state on every event, so a borrow-side event (e.g. a
    BORROW on the USDT leg of a Looping position) would otherwise leak the
    position's collateral (USDC) into a USDT-keyed summary — and a supply-side
    event would symmetrically leak the debt.  Gate by the event's own type so a
    one-sided summary never inherits the opposite side's dollar value.
    SUPPLY / WITHDRAW update only ``collateral_usd``; BORROW / REPAY update only
    ``debt_usd``; DELEVERAGE / CLOSE / LIQUIDATION_RISK_UPDATE are whole-position
    events and update both.  Read-side only — no writer change, no
    matching_policy_version bump.  ``net_equity_after_usd`` is handled by the
    caller and stays whole-position by design (display-only; out of scope).
    """
    supply_side = ev.event_type in (LendingEventType.SUPPLY, LendingEventType.WITHDRAW)
    borrow_side = ev.event_type in (LendingEventType.BORROW, LendingEventType.REPAY)
    if ev.collateral_value_after_usd is not None and not borrow_side:
        summary.collateral_usd = ev.collateral_value_after_usd
    if ev.debt_value_after_usd is not None and not supply_side:
        summary.debt_usd = ev.debt_value_after_usd


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

        has_supply_event = False
        has_borrow_event = False
        for ev in events:
            # Apply latest non-None values from each event in order.
            # VIB-4792: collateral/debt post-state is scoped to the side the
            # event mutated — see _apply_sided_post_state.
            _apply_sided_post_state(summary, ev)
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
                has_borrow_event = True
                has_supply_event = True
            elif ev.event_type in (LendingEventType.SUPPLY, LendingEventType.WITHDRAW):
                has_supply_event = True
            elif ev.event_type in (LendingEventType.BORROW, LendingEventType.REPAY):
                has_borrow_event = True
            elif ev.event_type == LendingEventType.CLOSE:
                summary.is_closed = True

        if has_supply_event and has_borrow_event:
            summary.position_type = "MIXED"
        elif has_supply_event:
            summary.position_type = "SUPPLY"
        elif has_borrow_event:
            summary.position_type = "BORROW"

        # W1-6 (T3a, VIB-4781): infer closed state when no explicit CLOSE
        # event was written.  Teardown's REPAY/WITHDRAW path leaves the
        # event log without a CLOSE row, so without a second signal the
        # renderer marks settled positions as OPEN.  Rule: when a live
        # snapshot is available AND the latest event is REPAY or WITHDRAW
        # AND the snapshot carries no matching SUPPLY/BORROW position
        # for this (protocol, chain, asset), mark the summary closed.
        # All three signals are required — a missing snapshot alone
        # (None) cannot drive inference, and a partial REPAY/WITHDRAW
        # that leaves a live snapshot position keeps the summary open.
        if not summary.is_closed and events and data.snapshot is not None:
            last_event = events[-1]
            if last_event.event_type in (
                LendingEventType.REPAY,
                LendingEventType.WITHDRAW,
            ) and not _snapshot_has_matching_lending_position(data.snapshot, summary):
                summary.is_closed = True

        # W1-2 (VIB-4777): enrich from snapshot when available.  Closed
        # positions are skipped (coderabbit review): otherwise a closed
        # historical summary could inherit an open position's snapshot
        # carry when ``(protocol, chain, asset)`` matches.
        if not summary.is_closed:
            _enrich_from_snapshot(summary, data.snapshot)

        summaries.append(summary)

    return LendingSection(positions=summaries)
