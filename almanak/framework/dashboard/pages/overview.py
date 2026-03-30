"""Command Center page for the Almanak Operator Dashboard.

Displays portfolio summary, attention alerts, and strategy grid
for executed/running strategies from the instance registry.
Only shows strategies that have real execution data.
"""

import html
from decimal import Decimal

import streamlit as st

from almanak.framework.dashboard.data_source import (
    archive_strategy_instance,
    execute_strategy_action,
    purge_strategy_instance,
)
from almanak.framework.dashboard.models import Strategy, StrategyStatus
from almanak.framework.dashboard.theme import get_chain_color, get_status_color
from almanak.framework.dashboard.utils import (
    format_chain_badge,
    format_pnl,
    format_usd,
    get_chain_health_icon,
    get_chain_icon,
    get_status_icon,
)


@st.fragment
def render_portfolio_summary(strategies: list[Strategy]) -> None:
    """Render the portfolio summary section at the top."""
    total_value = sum((s.total_value_usd for s in strategies), Decimal("0"))
    total_pnl_24h = sum((s.pnl_24h_usd for s in strategies), Decimal("0"))
    strategy_count = len(strategies)
    running_count = sum(1 for s in strategies if s.status == StrategyStatus.RUNNING)

    # Calculate multi-chain stats
    multi_chain_count = sum(1 for s in strategies if s.is_multi_chain)
    total_bridge_fees = sum((s.bridge_fees_usd for s in strategies), Decimal("0"))

    # Collect all unique chains across all strategies
    all_chains: set[str] = set()
    value_by_chain: dict[str, Decimal] = {}
    for strategy in strategies:
        if strategy.is_multi_chain and strategy.chains:
            all_chains.update(strategy.chains)
            for chain, position in strategy.positions_by_chain.items():
                value_by_chain[chain] = value_by_chain.get(chain, Decimal("0")) + position.total_value_usd
        else:
            all_chains.add(strategy.chain)
            value_by_chain[strategy.chain] = value_by_chain.get(strategy.chain, Decimal("0")) + strategy.total_value_usd

    st.markdown("## Portfolio Summary")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="Total Value",
            value=format_usd(total_value),
        )

    with col2:
        # Include bridge fees in P&L display
        net_pnl = total_pnl_24h - total_bridge_fees
        pnl_delta = f"{'+' if net_pnl >= 0 else ''}{net_pnl:,.2f}"
        st.metric(
            label="24h PnL (Net)",
            value=format_usd(abs(net_pnl)),
            delta=pnl_delta,
            help=f"Includes ${total_bridge_fees:,.2f} in bridge fees" if total_bridge_fees > 0 else None,
        )

    with col3:
        st.metric(
            label="Active Strategies",
            value=f"{running_count}/{strategy_count}",
            help=f"{multi_chain_count} multi-chain strategies" if multi_chain_count > 0 else None,
        )

    with col4:
        attention_count = sum(1 for s in strategies if s.attention_required)
        st.metric(
            label="Needs Attention",
            value=attention_count,
            delta=f"{attention_count} alerts" if attention_count > 0 else None,
            delta_color="inverse" if attention_count > 0 else "off",
        )

    # Per-chain breakdown if multi-chain strategies exist
    if multi_chain_count > 0 and len(all_chains) > 1:
        render_chain_breakdown(strategies, all_chains, value_by_chain)


@st.fragment
def render_chain_breakdown(
    strategies: list[Strategy],
    all_chains: set[str],
    value_by_chain: dict[str, Decimal],
) -> None:
    """Render per-chain breakdown section for multi-chain portfolios."""
    st.markdown("### Per-Chain Breakdown")

    # Create columns for each chain (max 5 per row)
    chains_list = sorted(all_chains)
    num_chains = len(chains_list)
    cols_per_row = min(num_chains, 5)
    cols = st.columns(cols_per_row)

    for idx, chain in enumerate(chains_list):
        chain_value = value_by_chain.get(chain, Decimal("0"))
        chain_color = get_chain_color(chain)
        chain_icon = get_chain_icon(chain)

        # Get chain health from any strategy that uses this chain
        chain_health = None
        for strategy in strategies:
            if chain in strategy.chain_health:
                chain_health = strategy.chain_health[chain]
                break

        with cols[idx % cols_per_row]:
            # Chain header with health indicator
            health_icon = ""
            if chain_health:
                health_icon = get_chain_health_icon(chain_health.status)

            st.markdown(
                f"""
                <div style="
                    background-color: {chain_color}11;
                    border: 1px solid {chain_color}44;
                    border-radius: 8px;
                    padding: 0.75rem;
                    text-align: center;
                ">
                    <div style="font-size: 1.2rem;">{chain_icon} {health_icon}</div>
                    <div style="color: {chain_color}; font-weight: bold; text-transform: uppercase;">
                        {chain}
                    </div>
                    <div style="font-size: 1.1rem; margin-top: 0.25rem;">
                        {format_usd(chain_value)}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )


@st.fragment
def render_attention_required(strategies: list[Strategy]) -> None:
    """Render the attention required section."""
    attention_strategies = [s for s in strategies if s.attention_required]

    if not attention_strategies:
        return

    st.markdown("## Attention Required")

    for strategy in attention_strategies:
        status_icon = get_status_icon(strategy.status)
        status_color = get_status_color(strategy.status)

        with st.container():
            st.markdown(
                f"""
                <div style="
                    background-color: rgba(255, 193, 7, 0.1);
                    border-left: 4px solid {status_color};
                    padding: 1rem;
                    margin-bottom: 0.5rem;
                    border-radius: 0 8px 8px 0;
                ">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <div>
                            <strong>{status_icon} {strategy.name}</strong>
                            <br/>
                            <span style="color: #666;">{strategy.attention_reason}</span>
                        </div>
                        <div style="text-align: right;">
                            <span style="font-size: 1.2rem; color: {"#f44336" if strategy.pnl_24h_usd < 0 else "#00c853"};">
                                {format_pnl(strategy.pnl_24h_usd)}
                            </span>
                            <br/>
                            <span style="color: #666;">24h PnL</span>
                        </div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

            col1, col2, col3 = st.columns([1, 1, 1])
            with col1:
                if st.button("View Details", key=f"attention_view_{strategy.id}"):
                    st.query_params["page"] = "detail"
                    st.query_params["strategy_id"] = strategy.id
                    st.rerun()
            with col2:
                if st.button("Bump Gas", key=f"attention_bump_{strategy.id}"):
                    st.info("Bump Gas is not yet available via gateway control plane.")
            with col3:
                if st.button("Pause", key=f"attention_pause_{strategy.id}"):
                    success = execute_strategy_action(
                        strategy.id,
                        action="PAUSE",
                        reason="Paused from Command Center attention panel",
                    )
                    if success:
                        st.success(f"Pause requested for {strategy.name}")
                        st.rerun()
                    else:
                        st.error(f"Failed to pause {strategy.name}")


def render_strategy_card(strategy: Strategy, col_idx: int, manage_mode: bool = False) -> None:
    """Render a single strategy card."""
    status_icon = get_status_icon(strategy.status)
    status_color = get_status_color(strategy.status)
    pnl_color = "#00c853" if strategy.pnl_24h_usd >= 0 else "#f44336"

    # Build chain display - for multi-chain strategies, show badges
    if strategy.is_multi_chain and strategy.chains:
        chain_badges_html = ""
        for chain in strategy.chains[:3]:  # Show max 3 chains
            chain_color = get_chain_color(chain)
            chain_badges_html += format_chain_badge(chain, chain_color)
        if len(strategy.chains) > 3:
            chain_badges_html += (
                f'<span style="color: #888; font-size: 0.75rem;">+{len(strategy.chains) - 3} more</span>'
            )
        chain_display = chain_badges_html
    else:
        chain_color = get_chain_color(strategy.chain)
        chain_display = format_chain_badge(strategy.chain, chain_color)

    # Multi-chain indicator
    multi_chain_badge = ""
    if strategy.is_multi_chain:
        multi_chain_badge = """
        <span style="
            background-color: #9c27b022;
            color: #9c27b0;
            padding: 0.1rem 0.4rem;
            border-radius: 8px;
            font-size: 0.65rem;
            font-weight: bold;
            margin-left: 0.5rem;
        ">MULTI-CHAIN</span>
        """

    # Escape user-controllable values to prevent XSS
    safe_name = html.escape(strategy.name)
    safe_protocol = html.escape(strategy.protocol)

    # Pure HTML card - no inline JS (Streamlit strips onclick/onmouseover, corrupting the DOM)
    card_html = f"""<div style="background-color: #1e1e1e; border: 1px solid #333; border-left: 4px solid {status_color}; border-radius: 8px; padding: 1rem; margin-bottom: 0.5rem;">
<div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 0.5rem;">
<span style="font-weight: bold; font-size: 1.1rem;">{safe_name}{multi_chain_badge}</span>
<span>{status_icon}</span>
</div>
<div style="margin-bottom: 0.5rem;">{chain_display}</div>
<div style="color: #888; font-size: 0.8rem; margin-bottom: 0.5rem;">{safe_protocol}</div>
<div style="display: flex; justify-content: space-between; align-items: flex-end;">
<div>
<div style="color: #888; font-size: 0.75rem;">Total Value</div>
<div style="font-size: 1.1rem;">{format_usd(strategy.total_value_usd)}</div>
</div>
<div style="text-align: right;">
<div style="color: #888; font-size: 0.75rem;">24h PnL</div>
<div style="font-size: 1.1rem; color: {pnl_color};">{format_pnl(strategy.pnl_24h_usd)}</div>
</div>
</div>
</div>"""

    st.markdown(card_html, unsafe_allow_html=True)

    if st.button("View Details", key=f"card_view_{strategy.id}_{col_idx}"):
        st.query_params["page"] = "detail"
        st.query_params["strategy_id"] = strategy.id
        st.rerun()

    if manage_mode:
        archive_col, purge_col = st.columns(2)
        with archive_col:
            if st.button("Archive", key=f"card_archive_{strategy.id}_{col_idx}", use_container_width=True):
                success = archive_strategy_instance(
                    strategy.id,
                    reason="Archived from Command Center manage mode",
                )
                if success:
                    st.success(f"Archived {strategy.name}")
                    st.rerun()
                else:
                    st.error(f"Failed to archive {strategy.name}")
        with purge_col:
            purge_confirm_key = f"card_purge_confirm_{strategy.id}_{col_idx}"
            st.checkbox("Confirm purge", key=purge_confirm_key)
            if st.button(
                "Purge",
                key=f"card_purge_{strategy.id}_{col_idx}",
                use_container_width=True,
                disabled=not st.session_state.get(purge_confirm_key, False),
                help="Permanent delete",
            ):
                success = purge_strategy_instance(
                    strategy.id,
                    reason="Purged from Command Center manage mode",
                )
                if success:
                    st.success(f"Purged {strategy.name}")
                    st.rerun()
                else:
                    st.error(f"Failed to purge {strategy.name}")


@st.fragment
def render_strategy_grid(strategies: list[Strategy]) -> None:
    """Render the strategy cards in a grid layout."""
    st.markdown("## Your Strategies")

    # Collect all chains for filter
    all_chains: set[str] = set()
    for strategy in strategies:
        if strategy.is_multi_chain and strategy.chains:
            all_chains.update(strategy.chains)
        else:
            all_chains.add(strategy.chain)

    # Filter controls
    col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
    with col1:
        status_filter = st.selectbox(
            "Filter by Status",
            options=["All"] + [s.value for s in StrategyStatus],
            key="status_filter",
        )

    with col2:
        chain_filter = st.selectbox(
            "Filter by Chain",
            options=["All"] + sorted(all_chains),
            key="chain_filter",
        )
    with col3:
        manage_mode = st.toggle(
            "Manage",
            key="overview_manage_mode",
            help="Enable archive/purge controls",
        )
    with col4:
        hide_archived = st.toggle(
            "Hide Archived",
            value=True,
            key="overview_hide_archived",
        )

    # Apply filters
    filtered_strategies = strategies
    if status_filter != "All":
        filtered_strategies = [s for s in filtered_strategies if s.status.value == status_filter]

    if chain_filter != "All":
        filtered_strategies = [
            s
            for s in filtered_strategies
            if (s.is_multi_chain and chain_filter in s.chains) or (not s.is_multi_chain and s.chain == chain_filter)
        ]
    if hide_archived:
        filtered_strategies = [s for s in filtered_strategies if s.status != StrategyStatus.ARCHIVED]

    if not filtered_strategies:
        st.info("No strategies match the selected filters.")
        return

    if manage_mode:
        id_to_label = {s.id: f"{s.name} ({s.id[:8]}...)" for s in filtered_strategies}
        selected_ids = st.multiselect(
            "Select strategies for bulk action",
            options=list(id_to_label.keys()),
            format_func=lambda sid: id_to_label[sid],
            key="overview_bulk_select",
        )

        bulk_col1, bulk_col2, bulk_col3 = st.columns([1, 1, 2])
        with bulk_col1:
            if st.button("Archive Selected", disabled=not selected_ids, use_container_width=True):
                successes = 0
                for strategy_id in selected_ids:
                    if archive_strategy_instance(strategy_id, reason="Bulk archive from Command Center"):
                        successes += 1
                if successes:
                    st.success(f"Archived {successes}/{len(selected_ids)} strategies")
                    st.rerun()
                else:
                    st.error("No strategies were archived")
        with bulk_col2:
            bulk_purge_confirm = st.checkbox(
                "I understand bulk purge is permanent",
                key="overview_bulk_purge_confirm",
            )
            if st.button(
                "Purge Selected",
                disabled=not selected_ids or not bulk_purge_confirm,
                use_container_width=True,
                help="Permanent delete",
            ):
                successes = 0
                for strategy_id in selected_ids:
                    if purge_strategy_instance(strategy_id, reason="Bulk purge from Command Center"):
                        successes += 1
                if successes:
                    st.success(f"Purged {successes}/{len(selected_ids)} strategies")
                    st.rerun()
                else:
                    st.error("No strategies were purged")
        with bulk_col3:
            st.caption("Bulk purge is permanent. Use archive when you only want to hide instances.")

    # Render grid (3 columns)
    cols = st.columns(3)
    for idx, strategy in enumerate(filtered_strategies):
        with cols[idx % 3]:
            render_strategy_card(strategy, idx, manage_mode=manage_mode)


def page(strategies: list[Strategy]) -> None:
    """Render the overview page.

    Args:
        strategies: List of strategy data objects
    """
    # Show strategy count for debugging
    if st.session_state.get("show_debug", False):
        st.write(f"📊 Found {len(strategies)} strategies")
        if strategies:
            st.write("Strategy IDs:", [s.id for s in strategies[:5]])

    if not strategies:
        st.info("""
        **No strategies running yet.**

        Run a strategy to see it here:
        ```bash
        almanak strat run -d almanak/demo_strategies/uniswap_rsi --once
        ```

        Check the **Strategy Library** page to see available strategy templates.
        """)
        return

    # Portfolio summary
    try:
        render_portfolio_summary(strategies)
    except Exception as e:
        st.error(f"Error rendering portfolio summary: {e}")
        import traceback

        st.code(traceback.format_exc())

    st.divider()

    # Attention required section
    try:
        render_attention_required(strategies)
    except Exception as e:
        st.error(f"Error rendering attention section: {e}")

    st.divider()

    # Strategy grid
    try:
        render_strategy_grid(strategies)
    except Exception as e:
        st.error(f"Error rendering strategy grid: {e}")
        import traceback

        st.code(traceback.format_exc())
