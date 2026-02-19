"""Strategy Library page for the Almanak Operator Dashboard.

Displays available strategy templates discovered from the filesystem.
These are strategies with config.json files that can be run but haven't
been executed yet (not in the instance registry).
"""

import html

import streamlit as st

from almanak.framework.dashboard.data_source import get_available_strategies
from almanak.framework.dashboard.models import Strategy
from almanak.framework.dashboard.theme import get_chain_color
from almanak.framework.dashboard.utils import format_chain_badge


def render_strategy_template_card(strategy: Strategy) -> None:
    """Render a single strategy template card."""
    safe_name = html.escape(strategy.name)
    safe_protocol = html.escape(strategy.protocol)

    # Build chain display
    if strategy.is_multi_chain and strategy.chains:
        chain_badges_html = ""
        for chain in strategy.chains[:3]:
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

    card_html = f"""
    <div style="margin-bottom: 1rem;">
        <div style="
            background-color: #1e1e1e;
            border: 1px solid #333;
            border-left: 4px solid #555;
            border-radius: 8px;
            padding: 1rem;
        ">
            <div style="font-weight: bold; font-size: 1.05rem; margin-bottom: 0.5rem;">
                {safe_name}
            </div>
            <div style="margin-bottom: 0.5rem;">
                {chain_display}
            </div>
            <div style="color: #888; font-size: 0.8rem;">
                {safe_protocol}
            </div>
        </div>
    </div>
    """

    st.markdown(card_html, unsafe_allow_html=True)


def page() -> None:
    """Render the Strategy Library page."""
    st.markdown("## Strategy Library")
    st.caption("Available strategy templates that can be run. These are discovered from config.json files on disk.")

    try:
        strategies = get_available_strategies()
    except Exception as e:  # noqa: BLE001
        st.error(f"Error loading available strategies: {e}")
        strategies = []

    if not strategies:
        st.info("""
        **No additional strategy templates found.**

        Strategy templates are discovered from `strategies/` directories.
        All available strategies may already be running (check the Command Center).
        """)
        return

    # Collect chains for filter
    all_chains: set[str] = set()
    for strategy in strategies:
        if strategy.is_multi_chain and strategy.chains:
            all_chains.update(strategy.chains)
        else:
            all_chains.add(strategy.chain)

    # Filter controls
    col1, _ = st.columns([1, 3])
    with col1:
        chain_filter = st.selectbox(
            "Filter by Chain",
            options=["All", *sorted(all_chains)],
            key="library_chain_filter",
        )

    # Apply filter
    filtered = strategies
    if chain_filter != "All":
        filtered = [
            s
            for s in filtered
            if (s.is_multi_chain and chain_filter in s.chains) or (not s.is_multi_chain and s.chain == chain_filter)
        ]

    if not filtered:
        st.info("No strategies match the selected filter.")
        return

    st.caption(f"{len(filtered)} template{'s' if len(filtered) != 1 else ''} available")

    # Render grid (3 columns)
    cols = st.columns(3)
    for idx, strategy in enumerate(filtered):
        with cols[idx % 3]:
            render_strategy_template_card(strategy)
