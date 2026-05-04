"""Section helpers callable from inside ``render_custom_dashboard()``.

Strategy authors own every pixel of their custom dashboard, but a few
sections are generic enough across DeFi primitives that every dashboard
should embed them: the PnL eyeball card at the top, the cost-stack
breakdown above the trade tape, and the trade tape at the bottom. This
module is the home for those shared section building blocks.

Distinct from ``almanak/framework/dashboard/pages/`` which contains
*operator-console* page renderers invoked by the multi-strategy
dashboard router (``app.py``). Pages render full pages with their own
chrome; sections render embeddable blocks with just a divider and a
heading, so they slot cleanly into an author-written
``render_custom_dashboard()``.

Recommended layout — three sections framing the author's primitive-
specific content (VIB-3969)::

    from almanak.framework.dashboard import (
        render_pnl_section,         # top — 5-second eyeball
        render_cost_stack_section,  # bottom — life-to-date costs
        render_trade_tape_section,  # bottom — TX-level audit
    )

    def render_custom_dashboard(strategy_id, strategy_config, api_client, session_state):
        # 1. Title / strategy info
        st.title(...)

        # 2. Eyeball — am I making or losing money?
        render_pnl_section(strategy_id)

        # 3. Strategy-specific content (LP plots / HF gauge / RSI chart / ...)
        # ... author's custom UI here ...

        # 4. Audit — paper trade, transactions, full breakdown
        render_cost_stack_section(strategy_id)
        render_trade_tape_section(strategy_id)

Each helper is intentionally thin (divider + heading + delegate) so the
contract is trivially stable across releases. Each calls exactly one
focused gateway RPC — section authors never pay the cost of fetching
data they don't render.
"""

from __future__ import annotations

import streamlit as st

from almanak.framework.dashboard.data_source import (
    GatewayConnectionError,
    get_cost_stack,
    get_pnl_summary,
)
from almanak.framework.dashboard.pages._detail_header import (
    render_cost_stack,
    render_money_trail,
)
from almanak.framework.dashboard.pages.trade_tape import render_trade_tape


def render_pnl_section(strategy_id: str) -> None:
    """Render the 5-second-eyeball PnL section (VIB-3969).

    Money Trail row: Deployed / NAV / Lifetime PnL / Net APR. The
    standard top-of-dashboard card so an operator answers "am I making
    or losing money?" before scrolling. Backed by the gateway's
    ``GetPnLSummary`` RPC; on RPC failure the section degrades to an
    info banner rather than crashing the page.

    Conventionally placed immediately below the strategy title.

    Args:
        strategy_id: The strategy id (passed straight through from
            ``render_custom_dashboard``'s first positional argument).
    """
    st.divider()
    st.markdown("### PnL")
    try:
        pnl = get_pnl_summary(strategy_id)
    except GatewayConnectionError:
        st.info("PnL temporarily unavailable — the gateway is disconnected.")
        return
    if pnl is None:
        st.info("No PnL data yet — run a few iterations to populate the snapshot table.")
        return
    render_money_trail(pnl)


def render_cost_stack_section(strategy_id: str, *, heading: str = "### Cost Stack") -> None:
    """Render the life-to-date Cost Stack section (VIB-3969).

    Gas / Fees / Slippage / Earn — generic across primitives (every
    primitive emits these into ``transaction_ledger`` +
    ``accounting_events``). Backed by the gateway's ``GetCostStack``
    RPC; on RPC failure the section degrades to an info banner.

    Conventionally placed at the start of an "Audit" section, just
    above the trade tape.

    Args:
        strategy_id: The strategy id.
        heading: Override the section heading. Pass an empty string to
            suppress the heading entirely (useful when composing inside
            a larger Audit panel that already has its own heading).
    """
    st.divider()
    if heading:
        st.markdown(heading)
    try:
        cost = get_cost_stack(strategy_id)
    except GatewayConnectionError:
        st.info("Cost data temporarily unavailable — the gateway is disconnected.")
        return
    if cost is None:
        st.info("No cost data yet — gas / fees / slippage accumulate as the strategy executes.")
        return
    render_cost_stack(cost)


def render_trade_tape_section(strategy_id: str, *, limit: int = 50) -> None:
    """Render the standard trade-tape section.

    Conventionally placed at the bottom of every
    ``render_custom_dashboard()`` so accounting can be visually QA'd
    locally and on the hosted platform from the same code path. The
    underlying ``render_trade_tape`` reads through the gateway's
    ``DashboardService.GetTradeTape``, which abstracts SQLite (local)
    and Postgres (hosted) — the section travels everywhere the gateway
    does.

    Args:
        strategy_id: The strategy id (passed straight through from
            ``render_custom_dashboard``'s first positional argument).
        limit: Most recent intents to fetch. Defaults to 50.
    """
    st.divider()
    st.markdown("### Trade Tape")
    render_trade_tape(strategy_id, limit=limit)
