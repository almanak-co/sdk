"""Timeline page for the Almanak Operator Dashboard.

Displays full timeline view with pagination and filtering.
"""

import streamlit as st

from almanak.framework.dashboard.models import Strategy, TimelineEventType
from almanak.framework.dashboard.theme import get_chain_color, get_timeline_event_color
from almanak.framework.dashboard.utils import (
    format_bridge_progress,
    format_chain_badge,
    format_timeline_summary,
    get_block_explorer_url,
    get_event_type_category,
    get_timeline_event_icon,
)

# from almanak.framework.dashboard.mock_data import generate_extended_timeline_events


def page(strategies: list[Strategy]) -> None:
    """Render the timeline page.

    Args:
        strategies: List of all strategy data objects
    """
    # Get strategy ID from query params
    strategy_id = st.query_params.get("strategy_id")

    if not strategy_id:
        st.info("👈 Please select a strategy from the sidebar to view its timeline.")
        st.markdown("### Or select a strategy here:")
        if strategies:
            strategy_names = [f"{s.name} ({s.id[:12]}...)" for s in strategies]
            selected_idx = st.selectbox(
                "Choose a strategy",
                range(len(strategy_names)),
                format_func=lambda x: strategy_names[x],
                key="timeline_strategy_selector",
            )
            if st.button("View Timeline", use_container_width=True):
                st.query_params["strategy_id"] = strategies[selected_idx].id
                st.rerun()
        else:
            st.warning("No strategies found. Make sure you have strategies running or check your state database.")
            if st.button("Go to Overview"):
                st.query_params["page"] = "overview"
        return

    strategy = next((s for s in strategies if s.id == strategy_id), None)

    if not strategy:
        st.error(f"Strategy {strategy_id} not found.")
        if st.button("Go to Overview"):
            st.query_params["page"] = "overview"
        return

    # Enrich with full details (timeline events) from gateway
    from almanak.framework.dashboard.data_source import GatewayConnectionError, get_strategy_details

    try:
        detailed = get_strategy_details(strategy_id)
        if detailed is not None:
            strategy = detailed
    except GatewayConnectionError:
        st.warning("Gateway unavailable - showing cached timeline data")

    # Back button
    if st.button("← Back to Strategy Detail"):
        st.query_params["page"] = "detail"

    # Header with chain badges for multi-chain strategies
    st.markdown(f"## Timeline: {strategy.name}")
    if strategy.is_multi_chain and strategy.chains:
        chain_badges_html = ""
        for chain in strategy.chains:
            chain_color = get_chain_color(chain)
            chain_badges_html += format_chain_badge(chain, chain_color)
        st.markdown(f"**Chains:** {chain_badges_html} | **Protocol:** {strategy.protocol}", unsafe_allow_html=True)
    else:
        chain_color = get_chain_color(strategy.chain)
        chain_badge = format_chain_badge(strategy.chain, chain_color)
        st.markdown(f"**Chain:** {chain_badge} | **Protocol:** {strategy.protocol}", unsafe_allow_html=True)

    st.divider()

    # Initialize pagination state
    if "timeline_page" not in st.session_state:
        st.session_state.timeline_page = 0

    # Collect all chains for filter - for multi-chain, include all chains
    available_chains: list[str] = []
    if strategy.is_multi_chain and strategy.chains:
        available_chains = list(strategy.chains)
    else:
        available_chains = [strategy.chain]

    # Filter controls - add chain filter for multi-chain strategies
    if strategy.is_multi_chain and len(available_chains) > 1:
        col1, col2, col3, col4, col5 = st.columns([2, 2, 1, 1, 1])
    else:
        col1, col2, col3, col5 = st.columns([2, 2, 1, 1])
        col4 = None

    with col1:
        event_type_options = ["All"] + [e.value for e in TimelineEventType]
        selected_event_type = st.selectbox(
            "Filter by Event Type",
            options=event_type_options,
            key="timeline_event_filter",
        )

    with col2:
        category_options = ["All", "Success", "Warning", "Error"]
        selected_category = st.selectbox(
            "Filter by Category",
            options=category_options,
            key="timeline_category_filter",
        )

    with col3:
        page_size = st.selectbox(
            "Events per page",
            options=[10, 20, 50],
            index=1,
            key="timeline_page_size",
        )

    with col5:
        verbosity = st.selectbox("Verbosity", options=["Operator", "Debug"], key="timeline_verbosity")

    # Chain filter for multi-chain strategies
    selected_chain = "All"
    if col4 is not None:
        with col4:
            selected_chain = st.selectbox(
                "Filter by Chain",
                options=["All"] + available_chains,
                key="timeline_chain_filter",
            )

    quick_col1, quick_col2 = st.columns(2)
    with quick_col1:
        failures_only = st.checkbox("Failures only", key="timeline_failures_only")
    with quick_col2:
        has_tx_only = st.checkbox("Has tx hash", key="timeline_has_tx_only")

    st.divider()

    # Get events from strategy object
    events = strategy.timeline_events

    # Apply filters
    filtered_events = events
    if selected_event_type != "All":
        filtered_events = [e for e in filtered_events if e.event_type.value == selected_event_type]

    if selected_category != "All":
        category_map = {
            "Success": "success",
            "Warning": "warning",
            "Error": "error",
        }
        target_category = category_map.get(selected_category, "")
        filtered_events = [
            e for e in filtered_events if get_event_type_category(e.event_type, e.description) == target_category
        ]

    # Apply chain filter
    if selected_chain != "All":
        filtered_events = [e for e in filtered_events if (e.chain or strategy.chain) == selected_chain]

    if failures_only:
        filtered_events = [
            e for e in filtered_events if get_event_type_category(e.event_type, e.description) == "error"
        ]

    if has_tx_only:
        filtered_events = [e for e in filtered_events if bool(e.tx_hash)]

    # Paginate filtered events
    total_events = len(filtered_events)
    total_pages = (total_events + page_size - 1) // page_size
    current_page = st.session_state.timeline_page

    start_idx = current_page * page_size
    end_idx = min(start_idx + page_size, total_events)
    page_events = filtered_events[start_idx:end_idx]

    # Event count info
    if total_events == 0:
        st.info("No timeline events match the current filters.")
        return
    st.markdown(f"Showing {start_idx + 1}-{end_idx} of {total_events} events")

    # Render events with visual timeline
    for _idx, event in enumerate(page_events):
        icon = get_timeline_event_icon(event.event_type)
        category = get_event_type_category(event.event_type, event.description)
        # Use error color for failed trades, otherwise use event type color
        if category == "error":
            color = "#f44336"  # Error red
        else:
            color = get_timeline_event_color(event.event_type)
        time_str = event.timestamp.strftime("%Y-%m-%d %H:%M:%S")
        event_label = event.event_type.value.replace("_", " ").title()
        summary = format_timeline_summary(event.event_type, event.description, event.details or {})

        # Category-based background color
        if category == "success":
            bg_color = "rgba(0, 200, 83, 0.05)"
        elif category == "warning":
            bg_color = "rgba(255, 193, 7, 0.05)"
        elif category == "error":
            bg_color = "rgba(244, 67, 54, 0.05)"
        else:
            bg_color = "rgba(158, 158, 158, 0.05)"

        # Get event chain (use event's chain if available, otherwise strategy's)
        event_chain = event.chain or strategy.chain

        # Chain badge for multi-chain strategies
        chain_badge_html = ""
        if strategy.is_multi_chain:
            event_chain_color = get_chain_color(event_chain)
            chain_badge_html = format_chain_badge(event_chain, event_chain_color)

        # TX link
        tx_link_html = ""
        if event.tx_hash:
            explorer_url = get_block_explorer_url(event_chain, event.tx_hash)
            short_hash = f"{event.tx_hash[:10]}...{event.tx_hash[-8:]}"
            tx_link_html = f'''
                <a href="{explorer_url}" target="_blank"
                   style="color: #2196f3; text-decoration: none; font-size: 0.85rem;">
                    View on {event_chain.capitalize()}scan ({short_hash})
                </a>
            '''

        # Bridge progress bar for bridge events
        bridge_progress_html = ""
        if event.event_type in [
            TimelineEventType.BRIDGE_INITIATED,
            TimelineEventType.BRIDGE_COMPLETED,
            TimelineEventType.BRIDGE_FAILED,
        ]:
            from_chain = event.details.get("from_chain", event.chain or "")
            to_chain = event.destination_chain or event.details.get("to_chain", "")
            if from_chain and to_chain:
                status = (
                    "IN_FLIGHT"
                    if event.event_type == TimelineEventType.BRIDGE_INITIATED
                    else event.event_type.value.replace("BRIDGE_", "")
                )
                bridge_progress_html = format_bridge_progress(
                    from_chain, to_chain, status, 50 if status == "IN_FLIGHT" else 100
                )

        # Event card with timeline visual
        st.markdown(
            f"""
            <div style="
                display: flex;
                margin-bottom: 0;
            ">
                <div style="
                    width: 60px;
                    display: flex;
                    flex-direction: column;
                    align-items: center;
                ">
                    <div style="
                        width: 12px;
                        height: 12px;
                        border-radius: 50%;
                        background-color: {color};
                        border: 2px solid {color};
                    "></div>
                    <div style="
                        width: 2px;
                        flex-grow: 1;
                        background-color: #333;
                        min-height: 60px;
                    "></div>
                </div>
                <div style="
                    flex-grow: 1;
                    background-color: {bg_color};
                    border: 1px solid #333;
                    border-left: 3px solid {color};
                    border-radius: 0 8px 8px 0;
                    padding: 1rem;
                    margin-bottom: 0.5rem;
                    margin-left: -6px;
                ">
                    <div style="display: flex; justify-content: space-between; align-items: flex-start;">
                        <div>
                            <span style="font-size: 1.2rem;">{icon}</span>
                            <strong style="margin-left: 0.5rem; font-size: 1rem;">{event_label}</strong>
                            {chain_badge_html}
                        </div>
                        <span style="color: #888; font-size: 0.85rem;">{time_str}</span>
                    </div>
                    <div style="margin-top: 0.5rem; color: #ccc; font-size: 0.95rem;">
                        {summary}
                    </div>
                    {bridge_progress_html}
                    {tx_link_html}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # Expandable details
        if verbosity == "Debug" and event.details:
            with st.expander("View Details", expanded=False):
                for key, value in event.details.items():
                    st.markdown(f"**{key}:** {value}")

    st.divider()

    # Pagination controls
    col1, col2, col3, col4, col5 = st.columns([1, 1, 2, 1, 1])

    with col1:
        if st.button("First", disabled=current_page == 0, use_container_width=True):
            st.session_state.timeline_page = 0
            st.rerun()

    with col2:
        if st.button("Earlier", disabled=current_page == 0, use_container_width=True):
            st.session_state.timeline_page = max(0, current_page - 1)
            st.rerun()

    with col3:
        st.markdown(
            f"<div style='text-align: center; padding: 0.5rem;'>Page {current_page + 1} of {max(1, total_pages)}</div>",
            unsafe_allow_html=True,
        )

    with col4:
        if st.button("Later", disabled=current_page >= total_pages - 1, use_container_width=True):
            st.session_state.timeline_page = min(total_pages - 1, current_page + 1)
            st.rerun()

    with col5:
        if st.button("Last", disabled=current_page >= total_pages - 1, use_container_width=True):
            st.session_state.timeline_page = max(0, total_pages - 1)
            st.rerun()
