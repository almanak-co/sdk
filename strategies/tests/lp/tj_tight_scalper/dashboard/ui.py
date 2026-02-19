"""
TraderJoe Tight-Range Scalper Dashboard.

Custom dashboard showing bin distribution, fee metrics, and rebalance history.
"""

from datetime import datetime
from decimal import Decimal
from typing import Any

import streamlit as st


def render_custom_dashboard(
    strategy_id: str,
    strategy_config: dict[str, Any],
    api_client: Any,
    session_state: dict[str, Any],
) -> None:
    """Render the TJ Tight-Range Scalper custom dashboard.

    Shows:
    - Current price vs position range
    - Active bins count
    - Fees earned estimate
    - Rebalance history using TimelineEvent model
    """
    st.title("TJ Tight-Range Scalper Dashboard")

    # Extract config values with defaults
    pool = strategy_config.get("pool", "WAVAX/USDC/20")
    range_width_pct = Decimal(str(strategy_config.get("range_width_pct", "0.05")))
    num_bins = int(strategy_config.get("num_bins", 11))
    rebalance_threshold_pct = Decimal(str(strategy_config.get("rebalance_threshold_pct", "0.025")))
    amount_x = Decimal(str(strategy_config.get("amount_x", "0.15")))
    amount_y = Decimal(str(strategy_config.get("amount_y", "3")))

    pool_parts = pool.split("/")
    token_x = pool_parts[0] if len(pool_parts) > 0 else "WAVAX"
    token_y = pool_parts[1] if len(pool_parts) > 1 else "USDC"
    bin_step = int(pool_parts[2]) if len(pool_parts) > 2 else 20

    # Strategy info header
    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Pool:** {token_x}/{token_y} (bin step: {bin_step})")

    # Metrics row
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            label="Range Width",
            value=f"{float(range_width_pct) * 100:.1f}%",
            help="Price range width as percentage of current price",
        )

    with col2:
        st.metric(
            label="Active Bins",
            value=str(num_bins),
            help="Number of bins used for liquidity distribution",
        )

    with col3:
        st.metric(
            label="Rebalance Threshold",
            value=f"{float(rebalance_threshold_pct) * 100:.1f}%",
            help="Price deviation that triggers rebalancing",
        )

    st.divider()

    # Position section
    st.subheader("Position Overview")

    # Get position state from session_state or config
    position_center_price = session_state.get("position_center_price")
    position_bin_ids = session_state.get("position_bin_ids", [])

    if position_center_price:
        center_price = Decimal(str(position_center_price))
        half_width = range_width_pct / Decimal("2")
        range_lower = center_price * (Decimal("1") - half_width)
        range_upper = center_price * (Decimal("1") + half_width)

        # Price vs range visualization
        _render_price_range_chart(center_price, range_lower, range_upper)

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Range Lower", f"${float(range_lower):.4f}")
        with col2:
            st.metric("Center Price", f"${float(center_price):.4f}")
        with col3:
            st.metric("Range Upper", f"${float(range_upper):.4f}")

        if position_bin_ids:
            st.info(f"Active bins: {min(position_bin_ids)} to {max(position_bin_ids)}")
    else:
        st.info("No active position. Position will be opened when strategy executes.")

    st.divider()

    # Liquidity allocation section
    st.subheader("Liquidity Allocation")

    col1, col2 = st.columns(2)
    with col1:
        st.metric(f"{token_x} Amount", f"{float(amount_x):.4f}")
    with col2:
        st.metric(f"{token_y} Amount", f"{float(amount_y):.2f}")

    # Bin distribution visualization
    _render_bin_distribution(num_bins, bin_step)

    st.divider()

    # Fees estimate section
    st.subheader("Fee Metrics")
    _render_fee_metrics(api_client, strategy_id)

    st.divider()

    # Rebalance history section
    st.subheader("Rebalance History")
    _render_rebalance_history(api_client, strategy_id)


def _render_price_range_chart(
    center_price: Decimal,
    range_lower: Decimal,
    range_upper: Decimal,
) -> None:
    """Render a visual representation of current price within range."""
    # Simple text-based visualization
    range_size = float(range_upper - range_lower)
    if range_size <= 0:
        return

    st.markdown("**Current Price Position in Range:**")

    # Progress bar from range_lower to range_upper
    # Assuming center price is current price for visualization
    progress = 0.5  # Center position
    st.progress(progress, text="Price at center of range")


def _render_bin_distribution(num_bins: int, bin_step: int) -> None:
    """Render bin distribution visualization."""
    st.markdown("**Bin Distribution:**")

    # Create a simple bar chart showing uniform distribution
    import pandas as pd

    bins_data = []
    center_bin = num_bins // 2
    for i in range(num_bins):
        bin_offset = i - center_bin
        bins_data.append(
            {
                "Bin": f"Bin {bin_offset:+d}",
                "Liquidity %": 100 / num_bins,
            }
        )

    df = pd.DataFrame(bins_data)
    st.bar_chart(df.set_index("Bin")["Liquidity %"])

    st.caption(f"Liquidity distributed across {num_bins} bins with bin step of {bin_step} basis points")


def _render_fee_metrics(api_client: Any, strategy_id: str) -> None:
    """Render fee metrics from timeline events."""
    # Get LP events to estimate fees
    events = []
    if api_client:
        try:
            events = api_client.get_timeline(strategy_id, limit=100)
        except Exception:
            pass

    # Count LP events
    lp_open_count = sum(1 for e in events if e.get("event_type") in ["LP_OPEN", "lp_open"])
    lp_close_count = sum(1 for e in events if e.get("event_type") in ["LP_CLOSE", "lp_close"])

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "LP Opens",
            str(lp_open_count),
            help="Number of LP positions opened",
        )

    with col2:
        st.metric(
            "LP Closes",
            str(lp_close_count),
            help="Number of LP positions closed",
        )

    with col3:
        # Estimate fees (placeholder - would need actual fee data)
        st.metric(
            "Est. Fees Earned",
            "N/A",
            help="Fee earnings require on-chain data",
        )

    st.caption(
        "Note: Actual fee earnings require querying on-chain position data. "
        "This dashboard shows event counts from timeline."
    )


def _render_rebalance_history(api_client: Any, strategy_id: str) -> None:
    """Render rebalance history using TimelineEvent data."""
    events = []
    if api_client:
        try:
            events = api_client.get_timeline(strategy_id, limit=50)
        except Exception:
            pass

    # Filter for rebalance-related events (state changes with rebalance details)
    rebalance_events = []
    for event in events:
        event_type = event.get("event_type", "")
        details = event.get("details", {})

        # Check if it's a rebalance event
        if event_type in ["STATE_CHANGE", "state_change"]:
            trigger = details.get("trigger", "")
            if trigger == "price_deviation":
                rebalance_events.append(event)

    if not rebalance_events:
        st.info("No rebalance events yet. Rebalances occur when price moves >2.5% from position center.")
        return

    # Display rebalance events as timeline
    st.markdown(f"**Recent Rebalances ({len(rebalance_events)}):**")

    for event in rebalance_events[:10]:  # Show last 10
        timestamp = event.get("timestamp", "")
        details = event.get("details", {})
        deviation_pct = details.get("deviation_pct", "?")

        # Format timestamp
        try:
            if isinstance(timestamp, str):
                dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                time_str = dt.strftime("%Y-%m-%d %H:%M:%S UTC")
            else:
                time_str = str(timestamp)
        except Exception:
            time_str = str(timestamp)

        st.markdown(f"- **{time_str}**: Rebalanced due to {deviation_pct}% price deviation")
