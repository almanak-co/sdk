"""Header and key-metrics renderers for the strategy detail page.

The Senior-Quant header (``render_quant_header``) is the new primary
glance card. It answers the four questions a Senior DeFi Quant scans
top-down at a Greek-letter strategy: *Money* (deployed/NAV/PnL/APR),
*Position & Risk* (open exposure, primary risk gauge, cost stack, cash
buffer), *Audit* (G6 reconciliation status, audit-trail completeness,
Accountant Test posture).

Pure Streamlit UI renderers — no I/O. All data is fetched once in
``detail.py`` and threaded in. Empty / unavailable data collapses
gracefully to ``unavailable_reason`` strings rather than NaN/0 silent
fallbacks.
"""

from __future__ import annotations

import html
from decimal import Decimal
from typing import Any

import streamlit as st

from almanak.framework.dashboard.gateway_client import (
    AuditPosture,
    CostStackInfo,
    PnLSummary,
)
from almanak.framework.dashboard.models import Strategy
from almanak.framework.dashboard.theme import get_chain_color, get_status_color
from almanak.framework.dashboard.utils import (
    format_chain_badge,
    format_usd,
    get_status_icon,
)


def _e(value: Any) -> str:
    """HTML-escape gateway-sourced strings before ``unsafe_allow_html``.

    Used for any field that may carry user/operator-controlled or
    receipt-parsed content (token symbols, oracle source labels,
    Accountant-Test cell IDs from the posture rollup, primary-risk
    labels, etc.). Numeric formatters that emit deterministic output
    do not need this — but escaping a known-safe number is harmless.
    """
    if value is None:
        return ""
    return html.escape(str(value), quote=True)


# ─── Strategy header (title + status badge) ──────────────────────────────


def render_strategy_header(strategy: Strategy) -> None:
    """Render the strategy title and status badge."""
    status_icon = get_status_icon(strategy.status)
    status_color = get_status_color(strategy.status)

    st.markdown(
        f"""
        <div style="display: flex; align-items: center; gap: 1rem; margin-bottom: 1rem;">
            <h2 style="margin: 0;">{_e(strategy.name)}</h2>
            <span style="
                background-color: {status_color}22;
                color: {status_color};
                padding: 0.25rem 0.75rem;
                border-radius: 16px;
                font-weight: bold;
            ">{_e(status_icon)} {_e(strategy.status.value)}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_chain_info_row(strategy: Strategy) -> None:
    """Render the chain / protocol / last-action info row."""
    from almanak.framework.dashboard.pages.detail import render_chain_health_indicators

    if strategy.is_multi_chain and strategy.chains:
        chain_badges_html = ""
        for chain in strategy.chains:
            chain_color = get_chain_color(chain)
            chain_badges_html += format_chain_badge(chain, chain_color)

        st.markdown(
            f"""
            <div style="display: flex; align-items: center; gap: 1rem; margin-bottom: 0.5rem;">
                <strong>Chains:</strong> {chain_badges_html}
            </div>
            """,
            unsafe_allow_html=True,
        )

        render_chain_health_indicators(strategy)

        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**Protocols:** {strategy.protocol}")
        with col2:
            if strategy.last_action_at:
                st.markdown(f"**Last Action:** {strategy.last_action_at.strftime('%Y-%m-%d %H:%M')}")
        return

    col1, col2, col3 = st.columns(3)
    with col1:
        chain_color = get_chain_color(strategy.chain)
        chain_badge = format_chain_badge(strategy.chain, chain_color)
        st.markdown(f"**Chain:** {chain_badge}", unsafe_allow_html=True)
    with col2:
        st.markdown(f"**Protocol:** {strategy.protocol}")
    with col3:
        if strategy.last_action_at:
            st.markdown(f"**Last Action:** {strategy.last_action_at.strftime('%Y-%m-%d %H:%M')}")


# ─── Senior-Quant header ─────────────────────────────────────────────────


_CONFIDENCE_ICONS = {
    "HIGH": "",
    "ESTIMATED": " ⚠️",
    "STALE": " ⏰",
    "UNAVAILABLE": " ❓",
}

_RISK_COLORS = {
    "green": "#00c853",
    "yellow": "#ff9800",
    "red": "#f44336",
    "neutral": "#888888",
}


def _primary_risk_glossary(p: PnLSummary) -> str:
    """VIB-3926 — primitive-aware tooltip text for the primary-risk tile.

    Returns a one-line glossary explaining the metric and the colour
    ladder. Written so a Senior Quant scanning cold understands what
    the tile is asserting without opening a separate doc.
    """
    kind = (p.primary_risk_kind or "").lower()
    if kind == "lp":
        return (
            "LP in-range status. Green = current_tick ∈ [tick_lower, tick_upper); "
            "Red = out-of-range (no fees accruing). 'pending' = tick_metadata "
            "not yet derived (slot0 fallback in flight)."
        )
    if kind == "lending":
        return (
            "Aave/Morpho/Compound health factor. Ladder: ≥1.5 green, "
            "1.2–1.5 yellow, <1.2 red. Liquidation thresholds vary by "
            "protocol and per-asset LTV — treat as a heuristic, not a "
            "guarantee."
        )
    if kind == "perp":
        return (
            "Perpetual position leverage = position size / margin. "
            "Liquidation threshold depends on market params (GMX V2, "
            "Hyperliquid, dYdX); see Trade Tape for protocol context."
        )
    return "No active position. The strategy is idle or fully unwound."


def _signed(value: Decimal, *, decimals: int = 2) -> str:
    if value == 0:
        return f"$0.{'0' * decimals}"
    sign = "+" if value > 0 else "-"
    return f"{sign}${abs(value):,.{decimals}f}"


def _pct(value: Decimal, *, decimals: int = 2) -> str:
    sign = "+" if value > 0 else ("-" if value < 0 else "")
    return f"{sign}{abs(value):.{decimals}f}%"


def render_quant_header(
    strategy: Strategy,
    pnl: PnLSummary | None,
    cost: CostStackInfo | None = None,
    audit: AuditPosture | None = None,
) -> None:
    """Render the Senior-Quant header card.

    Three rows:
      Row 1 — Money (deployed / NAV / lifetime PnL / net APR)
      Row 2 — Position & Risk (open exposure, primary risk, cost stack, cash buffer)
      Row 3 — Audit (G6 reconciliation, audit trail completeness, Accountant posture)

    VIB-3969: takes the three focused slices independently — operator
    console fetches them concurrently from the trio of dedicated RPCs.
    Any slice may be None if its RPC failed; rows that depend on a
    missing slice are skipped gracefully (rather than collapsing the
    whole header).

    When ``pnl`` is None (e.g., a fresh strategy with no snapshots /
    gateway-down on the eyeball card), falls back to a degraded view
    sourced from the existing ``Strategy`` model so the page still
    renders something useful.
    """
    if pnl is None:
        st.warning("Quant header unavailable — gateway returned no aggregations. Falling back to summary metrics.")
        _render_fallback_metrics(strategy)
        return

    _maybe_render_beta_banner(pnl)
    render_money_trail(pnl, cost)
    # Position & Risk row depends mostly on ``pnl`` (Open Exposure,
    # Primary Risk, Cash Buffer) — only the Cost Stack tile needs
    # ``cost``. Render all 4 tiles whenever ``pnl`` is present and let
    # the cost tile degrade to an info banner when its RPC failed, so
    # the operator never panic-thinks their open positions vanished.
    _render_risk_row(pnl, cost, strategy)
    if audit is not None:
        _render_audit_row(audit)


def _maybe_render_beta_banner(p: PnLSummary) -> None:
    """VIB-3929 — beta-accounting banner on lending + perp pages.

    LP ships under the full ship gate (7/7). Lending and perp ship
    behind a "beta accounting" badge per plan §1.1: position-level
    reconciliation (G6 component split, L4 principal-vs-interest) is
    pending VIB-3474 + Track C and not in scope this iteration.

    The banner is rendered AT THE TOP of the header so an operator
    cannot miss it on cold-page-load. LP pages render no banner — the
    LP ship gate is unconditional.

    Primitive detection is `primary_risk_kind` (set by
    `compute_pnl_summary` from the live PositionSummary), which is the
    same source the risk tile uses; the two surfaces stay in sync.
    """
    kind = (getattr(p, "primary_risk_kind", "") or "").lower()
    if kind not in ("lending", "perp"):
        return
    primitive_label = {"lending": "Lending", "perp": "Perpetuals"}[kind]
    blocker = {
        "lending": "lending pre/post-state pipeline",
        "perp": "Track C (position_state_snapshots materialiser)",
    }[kind]
    st.markdown(
        f"""
        <div style="
            background:#3a2e00;border:1px solid #ff9800;
            border-left:4px solid #ff9800;
            border-radius:4px;padding:0.6rem 0.85rem;
            margin-bottom:0.75rem;
            color:#ffd54f;font-size:0.9rem;line-height:1.4;">
          <strong>BETA ACCOUNTING — {_e(primitive_label)}</strong><br>
          Position-level reconciliation pending {_e(blocker)}.
          Execution + gas + outbox state are accurate; G6 component PnL
          and L4 principal/interest split may show partial data.
        </div>
        """,
        unsafe_allow_html=True,
    )


# --- Row 1: Money trail --------------------------------------------------


_STRATEGY_PNL_HELP = (
    "Strategy-scoped PnL = realized (closed positions + swaps, net of LP fees, "
    "funding, interest and gas — from accounting) + unrealized (open position "
    "NAV − open cost basis) + swap-inventory unrealized (mark-to-market of held "
    "directional swap inventory, e.g. a net-long token a swap strategy holds in "
    "the wallet — its FIFO held cost vs current mark). Excludes idle wallet "
    "balances, so unlike the old wallet PnL it is NOT moved by gas spend or by "
    "another strategy sharing the wallet (deployment_id is wallet+chain-scoped)."
)
_STRATEGY_APR_HELP = (
    "Annualised Strategy PnL ÷ open cost basis × (365 / age_days). "
    "Strategy-scoped, not wallet-level. Expect jumpiness for tiny or "
    "short-lived positions."
)
# A snapshot can carry open positions while its cost-basis column is an
# *unmeasured* 0 (the intra-run NAV double-count / stale-cost-basis failure
# mode — VIB-3932 cluster). Below this dust threshold we treat the open
# position NAV as "no position", so a genuinely-flat (all-closed) strategy is
# unaffected; above it, a 0 cost basis means "unmeasured", not "free money".
_COST_BASIS_DUST_USD = Decimal("0.01")


def _net_realized_pnl_usd(cost: CostStackInfo) -> Decimal:
    """Realized component PnL from accounting — mirrors the G6 component
    decomposition in ``quant_aggregations.compute_reconciliation``
    (``component_pnl``): realized close/swap PnL + LP fees earned + funding net
    + interest net − gas. Wallet-independent (sourced from accounting_events,
    not wallet cash deltas), so idle balances never leak in.
    """
    return (
        cost.realized_pnl_usd
        + cost.fees_earned_usd
        + (cost.funding_earned_usd - cost.funding_paid_usd)
        + (cost.interest_earned_usd - cost.interest_paid_usd)
        - cost.cost_gas_usd
    )


def _strategy_pnl_usd(p: PnLSummary, cost: CostStackInfo | None, open_position_nav: Decimal) -> Decimal | None:
    """Strategy PnL = net realized (accounting) + unrealized (open MTM − cost
    basis). Returns ``None`` (tile renders "—") when the result would be
    untrustworthy:

    * ``cost`` is None — the realized RPC failed; don't silently drop the
      realized leg and understate PnL.
    * open positions exist (``open_position_nav`` above dust) but the cost
      basis is an unmeasured 0 — or a sub-dust value that would behave like
      one — **Empty ≠ Zero**. Treating that as a real basis makes unrealized
      = NAV − ~0 = the whole position value, i.e. a strategy reading "+100%".
      This is the intermittent intra-run NAV double-count / stale-cost-basis
      snapshot (VIB-3932 cluster); "—" is correct until a clean snapshot
      lands. A genuinely-flat (all-closed) strategy has ``open_position_nav``
      ~ 0 and computes normally (realized only).
    """
    if cost is None:
        return None
    if p.deployed_capital_usd <= _COST_BASIS_DUST_USD and open_position_nav > _COST_BASIS_DUST_USD:
        return None
    unrealized = open_position_nav - p.deployed_capital_usd
    # VIB-4984: on LEGACY snapshots, held directional swap inventory (e.g. RSI
    # net-long WETH) was valued by the snapshot writer as
    # ``available_cash_usd``, so its mark cancelled out of BOTH
    # ``open_position_nav`` (= nav − available_cash) and
    # ``deployed_capital_usd`` (excluded it) → zero unrealized contribution.
    # ``cost.inventory_unrealized_usd`` is an additive (mark − cost) DELTA that
    # recovers it for those snapshots. VIB-5057: classifier-written snapshots
    # fold the inventory into open_position_nav + deployed_capital_usd, and
    # the gateway suppresses the additive term (returns unmeasured) for them —
    # so the mark enters Strategy PnL exactly once in every writer/reader
    # combination. ``None`` (unmeasured / suppressed) ⇒ 0.
    inventory_unrealized = cost.inventory_unrealized_usd or Decimal("0")
    return _net_realized_pnl_usd(cost) + unrealized + inventory_unrealized


def _strategy_apr_pct(strategy_pnl: Decimal | None, deployed_capital_usd: Decimal, age_days: int) -> Decimal | None:
    """Annualised Strategy PnL ÷ open cost basis. ``None`` when undefined — no
    realized data, sub-dust cost basis (nothing meaningfully deployed —
    avoids astronomical APR from a dust denominator), or zero age."""
    if strategy_pnl is None or deployed_capital_usd <= _COST_BASIS_DUST_USD or age_days <= 0:
        return None
    return (strategy_pnl / deployed_capital_usd) * Decimal("365") / Decimal(str(age_days)) * Decimal("100")


def render_money_trail(p: PnLSummary, cost: CostStackInfo | None = None) -> None:
    """Render the Money Trail rows.

    WALLET TOTALS (idle-inclusive context): Deployed / NAV / Available cash.
    STRATEGY-CONTROLLED FUNDS: Open position NAV / Open cost basis /
    Strategy PnL / Strategy APR.

    The PnL + APR tiles are **strategy-scoped** (realized from accounting +
    unrealized mark-to-market), replacing the old wallet-level ``nav −
    deployed`` PnL / APR which double-counted idle wallet balances and moved
    whenever gas was spent or another strategy traded the same wallet
    (``deployment_id`` is wallet+chain-scoped). VIB-3969.

    VIB-4984: Strategy PnL also folds in ``cost.inventory_unrealized_usd`` —
    the mark-to-market of held *directional swap inventory* (e.g. RSI net-long
    WETH). On LEGACY snapshots that inventory was booked as
    ``available_cash_usd`` by the snapshot writer, so its mark cancelled out
    of both ``open_position_nav`` and ``deployed_capital_usd`` and would
    otherwise be silently omitted from Strategy PnL (NAV stays correct; only
    the attribution missed it). The folded term is an additive (mark − FIFO
    cost) delta. VIB-5057: classifier-written snapshots book the inventory as
    deployed (it flows through ``open_position_nav − deployed_capital_usd``)
    and the gateway suppresses the additive term for them, so it enters PnL
    exactly once either way. ``None`` means unmeasured-or-suppressed and is
    rendered as "—" on its own sub-line, never "$0.00" (Empty ≠ Zero).

    Public renderer — used by the operator-console quant header AND by
    ``render_pnl_section`` (custom-dashboard helper). ``cost`` carries the
    realized-PnL components (``GetCostStack``); when ``None`` (its RPC failed)
    the Strategy PnL / APR tiles render "—" rather than understate PnL.
    """
    st.markdown(
        '<div style="font-size:0.85rem;color:#888;letter-spacing:0.08em;'
        'margin-top:0.5rem;margin-bottom:0.25rem;">WALLET TOTALS — INCLUDES IDLE ASSETS</div>',
        unsafe_allow_html=True,
    )
    # VIB-3926 — every tile carries a glossary tooltip. A redesigned
    # dashboard is unreadable without legends. Each `help=` below is the
    # canonical one-liner for the tile.
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric(
            "Wallet deployed",
            format_usd(p.deployed_usd),
            help=(
                "Money put in. Wallet pre-state at first action × oracle prices "
                "+ deposits − withdrawals (wallet-anchored)."
            ),
        )
    with c2:
        confidence_icon = _CONFIDENCE_ICONS.get(p.value_confidence, "")
        st.metric(
            "Wallet NAV now",
            format_usd(p.nav_usd) + confidence_icon,
            help=(
                f"Net Asset Value = open positions + cash. "
                f"Confidence: {p.value_confidence}. HIGH = oracle "
                "quotes available; ESTIMATED = derived from latest "
                "snapshot."
            ),
        )
    with c3:
        cash_pct = (p.available_cash_usd / p.nav_usd * Decimal("100")) if p.nav_usd > 0 else Decimal("0")
        st.metric(
            "Available wallet cash",
            format_usd(p.available_cash_usd),
            delta=f"{cash_pct:.0f}% of wallet NAV",
            delta_color="off",
            help="Wallet funds not currently in positions. Includes gas ETH and other idle balances.",
        )

    st.markdown(
        '<div style="font-size:0.85rem;color:#888;letter-spacing:0.08em;'
        'margin-top:0.75rem;margin-bottom:0.25rem;">STRATEGY-CONTROLLED FUNDS</div>',
        unsafe_allow_html=True,
    )
    open_position_nav = p.nav_usd - p.available_cash_usd
    if open_position_nav < Decimal("0"):
        open_position_nav = Decimal("0")
    strategy_pnl = _strategy_pnl_usd(p, cost, open_position_nav)
    age_sub = f"max DD {_pct(p.max_drawdown_pct, decimals=1)}" if p.max_drawdown_pct > 0 else f"{p.age_days}d age"
    c4, c5, c6, c7 = st.columns(4)
    with c4:
        open_position_delta = (
            f"{p.open_position_count} open position(s)" if p.open_position_count > 0 else "active exposure"
        )
        st.metric(
            "Open position NAV",
            format_usd(open_position_nav),
            delta=open_position_delta,
            delta_color="off",
            help="Current mark-to-market value of positions controlled by the strategy. Excludes idle wallet cash.",
        )
    with c5:
        st.metric(
            "Open cost basis",
            format_usd(p.deployed_capital_usd),
            help="Capital currently deployed into open positions, measured from accounting cost basis.",
        )
    with c6:
        if strategy_pnl is None:
            st.metric(
                "Strategy PnL",
                "—",
                help=_STRATEGY_PNL_HELP + " Shown as — when realized cost data is unavailable or the open "
                "cost basis is unmeasured for a snapshot with live positions "
                "(Empty ≠ Zero).",
            )
        else:
            pct = (
                (strategy_pnl / p.deployed_capital_usd * Decimal("100"))
                if p.deployed_capital_usd > _COST_BASIS_DUST_USD
                else None
            )
            delta = _signed(strategy_pnl) + (f"  ({_pct(pct)})" if pct is not None else "")
            st.metric(
                "Strategy PnL",
                # Headline carries the sign (``-$0.02`` for a loss). Stripping it
                # with ``abs()`` made a negative PnL read as a gain in the big
                # number while only the delta chip showed red — a direct
                # contradiction. ``format_usd`` renders the leading ``-``.
                format_usd(strategy_pnl),
                delta=delta,
                help=_STRATEGY_PNL_HELP,
            )
    with c7:
        apr = _strategy_apr_pct(strategy_pnl, p.deployed_capital_usd, p.age_days)
        st.metric(
            "Strategy APR",
            f"{_pct(apr)} APR" if apr is not None else "—",
            delta=age_sub,
            delta_color="off",
            help=_STRATEGY_APR_HELP,
        )


def render_cost_stack(cost: CostStackInfo) -> None:
    """Render the life-to-date Cost Stack tile (Gas / Fees / Slip / Earn).

    Public renderer — used inside the operator-console Position & Risk
    row AND by ``render_cost_stack_section`` (custom-dashboard helper).
    VIB-3969.
    """
    # VIB-4984: held directional swap inventory mark-to-market. None =
    # unmeasured ⇒ render "—" (Empty ≠ Zero, NOT "$0.00"); a measured value
    # uses the precise-small formatter (a sub-cent net-long mark is common).
    inv = cost.inventory_unrealized_usd
    if inv is None:
        inv_html = "<span style='color:#888;'>Inventory MTM —</span>"
    else:
        inv_color = "#00c853" if inv >= 0 else "#f44336"
        inv_sign = "+" if inv >= 0 else "−"
        inv_html = (
            f"<span style='color:{inv_color};'>Inventory MTM "
            f"{inv_sign}{format_usd(abs(inv), precise_small=True)}</span>"
        )
    cost_html = (
        f"<div style='color:#888;font-size:0.85rem;'>Cost stack (LTD)</div>"
        f"<div style='font-size:0.95rem;line-height:1.5;'>"
        f"<span style='color:#f44336;'>Gas −{format_usd(cost.cost_gas_usd)}</span><br>"
        # VIB-4980: Fees / Slip / Earn are captured to full Decimal precision
        # and can legitimately be sub-cent; render them with adaptive
        # precision so a real $0.0023 fee no longer reads as "+$0.00".
        f"<span style='color:#f44336;'>Fees −{format_usd(cost.cost_protocol_fees_usd, precise_small=True)}</span><br>"
        f"<span style='color:#f44336;'>Slip −{format_usd(cost.cost_slippage_usd, precise_small=True)}</span><br>"
        f"<span style='color:#00c853;'>Earn +{format_usd(cost.fees_earned_usd + cost.interest_earned_usd, precise_small=True)}</span><br>"
        # VIB-4984: swap-inventory unrealized as its own line.
        f"{inv_html}"
        f"</div>"
    )
    # VIB-3926 — life-to-date cost decomposition. Gas is on every tx;
    # protocol fees apply to swaps (Uniswap 0.05%, etc.) and lending
    # protocols; slippage is the realized vs quoted execution gap;
    # earn is LP fees + lending interest accrued.
    cost_tooltip = _e(
        "Life-to-date cost & earn breakdown. "
        "Gas: native ETH × USD at TX time, every tx. "
        "Fees: Uniswap pool fee + bridge / aggregator fees on swaps. "
        "Slip: realised slippage vs quote. "
        "Earn: LP fees + lending interest accrued."
    )
    st.markdown(
        f"<div title='{cost_tooltip}' style='background:#1e1e1e;border:1px solid #333;"
        f"border-radius:4px;padding:0.5rem 0.75rem;'>{cost_html}</div>",
        unsafe_allow_html=True,
    )


# --- Row 2: Position + risk ----------------------------------------------


def _render_risk_row(p: PnLSummary, cost: CostStackInfo | None, strategy: Strategy) -> None:
    st.markdown(
        '<div style="font-size:0.85rem;color:#888;letter-spacing:0.08em;'
        'margin-top:0.75rem;margin-bottom:0.25rem;">POSITION &amp; RISK</div>',
        unsafe_allow_html=True,
    )
    c1, c2, c3, c4 = st.columns(4)

    with c1:
        deployed = p.deployed_capital_usd
        nav = p.nav_usd
        pct = (deployed / nav * Decimal("100")) if nav > 0 else Decimal("0")
        st.metric(
            "Open exposure",
            format_usd(deployed),
            delta=f"{pct:.0f}% of NAV",
            delta_color="off",
            help=(
                f"Capital currently in positions (sum of cost bases). "
                f"{p.open_position_count} open position(s). "
                "Distinct from Deployed: this is now, that is lifetime."
            ),
        )

    with c2:
        risk_color = _RISK_COLORS.get(p.primary_risk_color, "#888888")
        label_html = _e(p.primary_risk_label or "Positions")
        value_html = _e(p.primary_risk_value or "N/A")
        # VIB-3926 — primary-risk tile uses custom HTML; tooltip lives on
        # the outer div via the standard ``title`` attribute. Glossary
        # text is primitive-aware so the tooltip explains the threshold
        # ladder for whichever metric is currently rendered.
        risk_tooltip = _e(_primary_risk_glossary(p))
        st.markdown(
            f"""
            <div title="{risk_tooltip}" style="
                background:#1e1e1e;border:1px solid #333;
                border-left:3px solid {risk_color};
                border-radius:4px;padding:0.5rem 0.75rem;">
              <div style="color:#888;font-size:0.85rem;">{label_html}</div>
              <div style="color:{risk_color};font-weight:600;font-size:1.4rem;">
                {value_html}
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with c3:
        if cost is not None:
            render_cost_stack(cost)
        else:
            st.info("Cost stack unavailable — gateway returned no aggregations.")

    with c4:
        cash_pct = (p.available_cash_usd / p.nav_usd * Decimal("100")) if p.nav_usd > 0 else Decimal("0")
        st.metric(
            "Cash buffer",
            format_usd(p.available_cash_usd),
            delta=f"{cash_pct:.0f}% of NAV",
            delta_color="off",
            help=(
                "Undeployed wallet capital (cash). Catches stuck-capital "
                "and over-allocation patterns. % of NAV measures how much "
                "of total wallet value is sitting idle."
            ),
        )


# --- Row 3: Audit posture -------------------------------------------------


def _render_audit_row(audit: AuditPosture) -> None:
    st.markdown(
        '<div style="font-size:0.85rem;color:#888;letter-spacing:0.08em;'
        'margin-top:0.75rem;margin-bottom:0.25rem;">AUDIT</div>',
        unsafe_allow_html=True,
    )
    c1, c2, c3 = st.columns(3)

    with c1:
        if audit.g6_status == "PASS":
            color = "#00c853"
            badge = "PASS"
            sub = f"gap {format_usd(audit.g6_gap_usd)} ≤ ε {format_usd(audit.g6_epsilon_usd)}"
        elif audit.g6_status == "FAIL":
            color = "#f44336"
            badge = "FAIL"
            sub = f"gap {format_usd(audit.g6_gap_usd)} > ε {format_usd(audit.g6_epsilon_usd)}"
        else:
            color = "#888888"
            badge = "NA"
            sub = "no events yet"

        components_lines = "".join(
            f"<div>Σ_{name}: <code>{format_usd(value)}</code></div>" for name, value in audit.g6_components.items()
        )
        st.markdown(
            f"""
            <div style="background:#1e1e1e;border:1px solid #333;
                        border-left:3px solid {color};border-radius:4px;
                        padding:0.5rem 0.75rem;">
              <div style="color:#888;font-size:0.85rem;">Reconciliation (G6)</div>
              <div style="color:{color};font-weight:600;font-size:1.2rem;">{badge}</div>
              <div style="color:#aaa;font-size:0.8rem;">{sub}</div>
              <div style="color:#aaa;font-size:0.8rem;">
                wallet: <code>{format_usd(audit.g6_wallet_pnl_usd)}</code> ·
                comp: <code>{format_usd(audit.g6_component_pnl_usd)}</code>
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        with st.expander("G6 component decomposition", expanded=False):
            st.markdown(components_lines, unsafe_allow_html=True)

    with c2:
        total = max(audit.ledger_total, 1)
        price_pct = (audit.ledger_with_price_inputs * 100) // total
        prepost_pct = (audit.ledger_with_pre_post_state * 100) // total
        gas_pct = (audit.ledger_with_gas_usd * 100) // total
        ev_total = max(audit.events_total, 1)
        ver_pct = (audit.events_with_versions * 100) // ev_total

        def _icon(num: int, denom: int) -> str:
            if denom == 0:
                return "·"
            if num == denom:
                return "<span style='color:#00c853;'>✓</span>"
            if num == 0:
                return "<span style='color:#f44336;'>✗</span>"
            return "<span style='color:#ff9800;'>⚠</span>"

        st.markdown(
            f"""
            <div style="background:#1e1e1e;border:1px solid #333;border-radius:4px;
                        padding:0.5rem 0.75rem;">
              <div style="color:#888;font-size:0.85rem;">Audit trail</div>
              <div style="font-size:0.9rem;line-height:1.6;">
                {_icon(audit.ledger_with_price_inputs, audit.ledger_total)}
                  price_inputs &nbsp;<code>{audit.ledger_with_price_inputs}/{audit.ledger_total}</code>
                  ({price_pct}%)<br>
                {_icon(audit.ledger_with_pre_post_state, audit.ledger_total)}
                  pre+post state &nbsp;<code>{audit.ledger_with_pre_post_state}/{audit.ledger_total}</code>
                  ({prepost_pct}%)<br>
                {_icon(audit.ledger_with_gas_usd, audit.ledger_total)}
                  gas_usd &nbsp;<code>{audit.ledger_with_gas_usd}/{audit.ledger_total}</code>
                  ({gas_pct}%)<br>
                {_icon(audit.events_with_versions, audit.events_total)}
                  versions &nbsp;<code>{audit.events_with_versions}/{audit.events_total}</code>
                  ({ver_pct}%)
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with c3:
        passed = audit.cells_passed
        failed = audit.cells_failed
        xfail = audit.cells_xfail
        total = audit.cells_total or 21

        if failed > 0:
            color = "#f44336"
        elif passed >= total - xfail:
            color = "#00c853"
        else:
            color = "#ff9800"

        chips_failing = "".join(
            f"<span style='background:#3a1c1c;color:#f44336;border-radius:4px;"
            f"padding:1px 6px;margin:1px;font-size:0.78rem;display:inline-block;'>{_e(c)}</span>"
            for c in audit.failing_cells[:6]
        )
        chips_xfail = "".join(
            f"<span style='background:#2a2a2a;color:#888;border-radius:4px;"
            f"padding:1px 6px;margin:1px;font-size:0.78rem;display:inline-block;'>{_e(c)}</span>"
            for c in audit.xfail_cells[:6]
        )

        st.markdown(
            f"""
            <div style="background:#1e1e1e;border:1px solid #333;
                        border-left:3px solid {color};border-radius:4px;
                        padding:0.5rem 0.75rem;">
              <div style="color:#888;font-size:0.85rem;">
                Accountant Test posture · primitive: <strong>{_e(audit.primitive)}</strong>
              </div>
              <div style="color:{color};font-weight:600;font-size:1.4rem;">
                {passed} / {total}
              </div>
              <div style="color:#aaa;font-size:0.8rem;">
                pass {passed} · fail {failed} · xfail {xfail}
              </div>
              <div style="margin-top:0.25rem;">{chips_failing}</div>
              <div style="margin-top:0.1rem;">{chips_xfail}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


# --- Fallback (no gateway data) ------------------------------------------


def _render_fallback_metrics(strategy: Strategy) -> None:
    """Pre-redesign 4-tile fallback when the gateway can't return aggregations."""
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Value", format_usd(strategy.total_value_usd))
    with col2:
        net_pnl = strategy.pnl_24h_usd - strategy.bridge_fees_usd
        st.metric("24h PnL (Net)", format_usd(net_pnl), delta=f"{'+' if net_pnl >= 0 else ''}{net_pnl:,.2f}")
    with col3:
        if strategy.position and strategy.position.total_lp_value_usd > 0:
            st.metric("LP Value", format_usd(strategy.position.total_lp_value_usd))
        elif strategy.position and strategy.position.health_factor is not None:
            hf = strategy.position.health_factor
            st.metric("Health Factor", f"{hf:.2f}" if hf > 0 else "0 (no debt)")
        else:
            st.metric("Positions", "N/A")
    with col4:
        if strategy.pnl_history:
            pnl_7d = strategy.pnl_history[-1].pnl_usd
            st.metric("7d PnL", format_usd(pnl_7d), delta=f"{'+' if pnl_7d >= 0 else ''}{pnl_7d:,.2f}")


# Backwards-compatible alias for callers that haven't migrated.
def render_key_metrics(strategy: Strategy) -> None:
    """Deprecated: prefer ``render_quant_header``. Kept for callers that
    don't yet pass the focused PnL/Cost/Audit slices through (e.g.
    AppTest fixtures that only have a ``Strategy`` to render against).
    """
    _render_fallback_metrics(strategy)
