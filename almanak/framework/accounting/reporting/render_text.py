"""Text rendering for strategy-class-aware accounting sections."""

from __future__ import annotations

from decimal import Decimal

from almanak.framework.accounting.lending_nav import compute_lending_nav

from .data_quality import DataQualitySection
from .lending_report import LendingSection
from .lp_report import LPSection
from .pendle_report import PendleSection

_MISSING = "—"
_LINE = "-" * 44


def _m(value: Decimal | None, width: int = 8) -> str:
    """Format a Decimal as ``$ 1,234.56`` or ``_MISSING``."""
    if value is None:
        return _MISSING
    sign = "-" if value < 0 else " "
    return f"{sign}${abs(value):>{width},.2f}"


def _m_signed(value: Decimal | None, width: int = 8) -> str:
    """Format a Decimal with an explicit '+' for positive values.

    Used for unrealized carry fields where the sign is meaningful.
    Returns ``_MISSING`` when value is None.
    """
    if value is None:
        return _MISSING
    sign = "-" if value < 0 else "+"
    return f"{sign}${abs(value):>{width},.6f}"


def _pct(value: Decimal | None, decimals: int = 2) -> str:
    if value is None:
        return _MISSING
    return f"{value:.{decimals}f}%"


def _hf(value: Decimal | None) -> str:
    if value is None:
        return _MISSING
    return f"{value:.3f}"


def render_lp_section(section: LPSection) -> str:
    if section.is_empty:
        return ""
    lines: list[str] = ["", "LP Positions", _LINE]
    for pos in section.positions:
        pair = f"{pos.token0}/{pos.token1}" if pos.token0 else pos.position_id
        status = "CLOSED" if pos.is_closed else "OPEN"
        lines.append(f"  {pair} [{pos.protocol} / {pos.chain}] [{status}]")
        lines.append(f"    Position:   {pos.position_id[:12]}…")
        lines.append(f"    Entry:      {_m(pos.entry_value_usd)}")
        if pos.is_closed:
            lines.append(f"    Exit:       {_m(pos.exit_value_usd)}")
        if pos.fees_token0 > 0 or pos.fees_token1 > 0:
            lines.append(f"    Fees:       {pos.fees_token0:.4f} {pos.token0} / {pos.fees_token1:.4f} {pos.token1}")
        if pos.protocol_fees_usd > 0:
            lines.append(f"    Proto fees: {_m(pos.protocol_fees_usd)}")
        lines.append(f"    Gas:        {_m(-pos.total_gas_usd)}")
        if pos.il_usd is not None:
            lines.append(f"    IL:         {_m(pos.il_usd)}")
        lines.append(f"    Net PnL:    {_m(pos.net_pnl_usd)}")
        if pos.in_range is not None:
            lines.append(f"    In range:   {'yes' if pos.in_range else 'no'}")
        lines.append("")

    total = section.total_net_pnl_usd
    gas = section.total_gas_usd
    lines.append(f"  Total gas:       {_m(-gas)}")
    lines.append(f"  Total net PnL:   {_m(total)}")
    return "\n".join(lines)


def render_lending_section(section: LendingSection, snapshot: object = None) -> str:
    """Render the lending positions section as a text block.

    Args:
        section: Per-position lending summaries built by ``build_lending_report``.
        snapshot: Optional ``PortfolioSnapshot`` used to compute the net carry
            footer via ``compute_lending_nav``.  When None (or when the snapshot
            contains no lending positions with unrealized carry), the footer is
            suppressed.
    """
    if section.is_empty:
        return ""
    lines: list[str] = ["", "Lending Positions", _LINE]
    any_unrealized = False
    for pos in section.positions:
        status = "CLOSED" if pos.is_closed else "OPEN"
        lines.append(f"  {pos.asset} [{pos.protocol} / {pos.chain}] [{status}]")
        lines.append(f"    Position:   {pos.position_key[:16]}…")
        lines.append(f"    Collateral: {_m(pos.collateral_usd)}")
        lines.append(f"    Debt:       {_m(None if pos.debt_usd is None else -pos.debt_usd)}")
        lines.append(f"    Net equity: {_m(pos.net_equity_usd)}")
        lines.append(f"    Health:     {_hf(pos.health_factor)}")
        if pos.liquidation_threshold is not None:
            lines.append(f"    Liq. thr.:  {_pct(pos.liquidation_threshold * 100, 1)}")
        if pos.supply_apr_pct is not None:
            lines.append(f"    Supply APR: {_pct(pos.supply_apr_pct)}")
        if pos.borrow_apr_pct is not None:
            lines.append(f"    Borrow APR: {_pct(pos.borrow_apr_pct)}")
        lines.append(f"    Gas:        {_m(-pos.total_gas_usd)}")
        # W1-6 (T3b, VIB-4781): realized interest from REPAY/WITHDRAW/
        # DELEVERAGE events.  Use _m_signed to surface sub-cent values — the
        # prior _m formatter rounded to 2 decimals so a real $0.000634 USDC
        # supply yield rendered as "$0.00" and looked like nothing.
        #
        # VIB-4974: render per-side, never netted.  Debt-side interest
        # (REPAY / DELEVERAGE) is a borrow *cost* → "Interest paid: -$…";
        # supply-side interest (WITHDRAW) is a *yield* → "Interest earned:
        # +$…".  A MIXED (same-asset supply+borrow) position sharing one
        # asset-scoped key shows BOTH gross components plus a net carry line —
        # the paid borrow cost is never collapsed into a single figure.  In
        # the normal case the supply and debt legs are separate keys, so each
        # gets exactly one labelled line.
        if pos.total_interest_paid_usd > 0:
            lines.append(f"    Interest paid:    {_m_signed(-pos.total_interest_paid_usd)}")
        if pos.total_interest_earned_usd > 0:
            lines.append(f"    Interest earned:  {_m_signed(pos.total_interest_earned_usd)}")
        if pos.total_interest_paid_usd > 0 and pos.total_interest_earned_usd > 0:
            lines.append(f"    Net interest:     {_m_signed(pos.total_interest_delta_usd)}")
        # W1-2 (VIB-4777): unrealized carry from snapshot.
        if pos.unrealized_pnl_usd is not None:
            lines.append(f"    Unrealized carry: {_m_signed(pos.unrealized_pnl_usd)}")
            any_unrealized = True
        if pos.deleverage_count:
            lines.append(f"    Deleverages: {pos.deleverage_count}")
        lines.append("")

    # W1-2 (VIB-4777): net lending carry footer — only when at least one
    # position has snapshot-sourced unrealized carry data.
    if any_unrealized:
        nav = compute_lending_nav(snapshot)  # type: ignore[arg-type]
        lines.append(f"  Net lending value:   {_m(nav.net_lending_value_usd)}")
        lines.append(f"  Net unrealized:      {_m_signed(nav.net_unrealized_carry_usd)}")

    return "\n".join(lines)


def render_pendle_section(section: PendleSection) -> str:
    if section.is_empty:
        return ""
    lines: list[str] = ["", "Pendle Positions", _LINE]
    for pos in section.positions:
        status = "REDEEMED" if pos.is_redeemed else "ACTIVE"
        lines.append(f"  {pos.pt_token} [{pos.protocol} / {pos.chain}] [{status}]")
        lines.append(f"    Market:        {pos.market_id[:16]}…")
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
            lines.append(f"    Realized yld:  {_m(pos.realized_yield_usd)}")
        lines.append(f"    Events:        {pos.event_count}")
        lines.append("")
    return "\n".join(lines)


def render_data_quality_section(section: DataQualitySection) -> str:
    if section.is_empty:
        return ""
    lines: list[str] = ["", "Data Quality", _LINE]
    if section.issues:
        lines.append(f"  {len(section.issues)} record(s) with UNAVAILABLE confidence:")
        for issue in section.issues:
            ts = issue.timestamp[:19] if issue.timestamp else "?"
            lines.append(f"  [{ts}] {issue.event_type} / {issue.position_key[:16]} [{issue.protocol}]")
            if issue.reason:
                lines.append(f"    Reason: {issue.reason}")
    if section.parse_errors:
        lines.append(f"  {section.parse_errors} event(s) failed to parse (schema mismatch likely)")
    return "\n".join(lines)
