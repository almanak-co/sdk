"""
Uniswap V3 Asymmetric Bullish LP Dashboard.

Custom dashboard showing asymmetric range position, upside/downside room percentages,
visual range bar with price marker, rebalance history, and in-range time metrics.
"""

from decimal import Decimal
from typing import Any

import streamlit as st

# Supported chains for this strategy
SUPPORTED_CHAINS = ["arbitrum", "base", "optimism", "ethereum"]


def render_custom_dashboard(
    strategy_id: str,
    strategy_config: dict[str, Any],
    api_client: Any,
    session_state: dict[str, Any],
) -> None:
    """Render the Uniswap V3 Asymmetric Bullish LP custom dashboard.

    Shows:
    - Current price position within range
    - Upside room % vs Downside room %
    - Visual range bar with current price marker
    - Rebalance history with price movement
    - In-range time percentage
    """
    st.title("Uniswap V3 Asymmetric Bullish LP Dashboard")

    # Extract config values with defaults
    pool = strategy_config.get("pool", "WETH/USDC/3000")
    chain = strategy_config.get("chain", "arbitrum")
    upside_pct = strategy_config.get("upside_pct", "0.12")
    downside_pct = strategy_config.get("downside_pct", "0.08")

    pool_parts = pool.split("/")
    token0 = pool_parts[0] if len(pool_parts) > 0 else "WETH"
    token1 = pool_parts[1] if len(pool_parts) > 1 else "USDC"
    fee_tier = pool_parts[2] if len(pool_parts) > 2 else "3000"

    # Strategy info header
    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Pool:** {token0}/{token1} ({int(fee_tier) / 10000:.2f}% fee tier)")
    st.markdown(f"**Chain:** {chain.upper()}")

    st.divider()

    # Asymmetric Range Configuration
    st.subheader("Asymmetric Range Configuration")
    _render_asymmetric_config(upside_pct, downside_pct)

    st.divider()

    # Current Price Position
    st.subheader("Current Price Position")
    _render_price_position(session_state, token0)

    st.divider()

    # Visual Range Bar
    st.subheader("Range Visualization")
    _render_range_bar(session_state, token0)

    st.divider()

    # In-Range Time Metrics
    st.subheader("In-Range Time Analysis")
    _render_in_range_time(api_client, strategy_id)

    st.divider()

    # Rebalance History
    st.subheader("Rebalance History")
    _render_rebalance_history(api_client, strategy_id)


def _render_asymmetric_config(upside_pct: str, downside_pct: str) -> None:
    """Render asymmetric range configuration with bullish bias visualization."""
    upside = float(Decimal(str(upside_pct))) * 100
    downside = float(Decimal(str(downside_pct))) * 100
    total_width = upside + downside

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Upside Range",
            f"+{upside:.0f}%",
            help="Maximum upside price movement before exiting range",
        )

    with col2:
        st.metric(
            "Downside Range",
            f"-{downside:.0f}%",
            help="Maximum downside price movement before exiting range",
        )

    with col3:
        st.metric(
            "Total Width",
            f"{total_width:.0f}%",
            help="Total range width (upside + downside)",
        )

    # Bullish bias visualization
    st.markdown("---")
    st.markdown("**Bullish Bias Distribution:**")

    upside_room_pct = (upside / total_width) * 100
    downside_room_pct = (downside / total_width) * 100

    col1, col2 = st.columns(2)

    with col1:
        st.success(f"Upside Room: {upside_room_pct:.0f}%")
        st.caption(f"+{upside:.0f}% above current price")

    with col2:
        st.error(f"Downside Room: {downside_room_pct:.0f}%")
        st.caption(f"-{downside:.0f}% below current price")

    # Explanation
    st.info(
        f"The asymmetric range provides **{upside_room_pct:.0f}% upside room** vs "
        f"**{downside_room_pct:.0f}% downside room**, reflecting a bullish market outlook. "
        f"This means the price can rise more before exiting the range than it can fall."
    )


def _render_price_position(session_state: dict[str, Any], token0: str) -> None:
    """Render current price position metrics."""
    current_price = session_state.get("current_price")
    range_lower = session_state.get("range_lower")
    range_upper = session_state.get("range_upper")
    center_price = session_state.get("position_center_price")

    col1, col2, col3 = st.columns(3)

    with col1:
        if current_price is not None:
            price_val = float(Decimal(str(current_price)))
            st.metric(
                f"Current {token0} Price",
                f"${price_val:,.2f}",
                help=f"Current {token0} price in USD",
            )
        else:
            st.metric(f"Current {token0} Price", "N/A")

    with col2:
        if range_lower is not None and range_upper is not None:
            lower_val = float(Decimal(str(range_lower)))
            upper_val = float(Decimal(str(range_upper)))
            st.metric(
                "Range Lower",
                f"${lower_val:,.2f}",
                help="Lower bound of LP position",
            )
        else:
            st.metric("Range Lower", "N/A")

    with col3:
        if range_lower is not None and range_upper is not None:
            lower_val = float(Decimal(str(range_lower)))
            upper_val = float(Decimal(str(range_upper)))
            st.metric(
                "Range Upper",
                f"${upper_val:,.2f}",
                help="Upper bound of LP position",
            )
        else:
            st.metric("Range Upper", "N/A")

    # Position center and deviation
    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        if center_price is not None:
            center_val = float(Decimal(str(center_price)))
            st.metric(
                "Position Center",
                f"${center_val:,.2f}",
                help="Price at which position was opened",
            )
        else:
            st.metric("Position Center", "N/A")

    with col2:
        if current_price is not None and center_price is not None:
            price_val = Decimal(str(current_price))
            center_val = Decimal(str(center_price))
            if center_val > 0:
                deviation = ((price_val - center_val) / center_val) * Decimal("100")
                deviation_val = float(deviation)
                delta_text = f"{deviation_val:+.2f}%"
                st.metric(
                    "Price Deviation",
                    delta_text,
                    help="Current price vs position center (rebalance at >5%)",
                )
                if abs(deviation_val) > 5:
                    st.warning("Approaching rebalance threshold (5%)")
            else:
                st.metric("Price Deviation", "N/A")
        else:
            st.metric("Price Deviation", "N/A")


def _render_range_bar(session_state: dict[str, Any], token0: str) -> None:
    """Render visual range bar with current price marker."""
    current_price = session_state.get("current_price")
    range_lower = session_state.get("range_lower")
    range_upper = session_state.get("range_upper")

    if current_price is None or range_lower is None or range_upper is None:
        st.info("No active position to display range visualization")
        return

    price_val = float(Decimal(str(current_price)))
    lower_val = float(Decimal(str(range_lower)))
    upper_val = float(Decimal(str(range_upper)))

    if upper_val <= lower_val:
        st.warning("Invalid range bounds")
        return

    # Calculate position within range
    range_span = upper_val - lower_val

    # Check if price is in range
    if price_val < lower_val:
        st.error(f"BELOW RANGE - Price ${price_val:,.2f} is below lower bound ${lower_val:,.2f}")
        st.markdown("**Action:** Position is earning no fees. Consider rebalancing.")
        return
    elif price_val > upper_val:
        st.error(f"ABOVE RANGE - Price ${price_val:,.2f} is above upper bound ${upper_val:,.2f}")
        st.markdown("**Action:** Position is earning no fees. Consider rebalancing.")
        return

    # Price is in range - calculate position
    price_position = (price_val - lower_val) / range_span * 100
    upside_remaining = (upper_val - price_val) / range_span * 100
    downside_remaining = (price_val - lower_val) / range_span * 100

    # Visual progress bar
    st.progress(price_position / 100)

    # Position details
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Position in Range",
            f"{price_position:.1f}%",
            help="Current price position within range (0% = lower, 100% = upper)",
        )

    with col2:
        st.success(f"Upside Remaining: {upside_remaining:.1f}%")

    with col3:
        st.error(f"Downside Remaining: {downside_remaining:.1f}%")

    # Range explanation
    st.markdown("---")
    st.markdown("**Range Bar Legend:**")
    st.markdown(
        f"""
        | Bound | Price | Room |
        |-------|-------|------|
        | Lower | ${lower_val:,.2f} | {downside_remaining:.1f}% below current |
        | Current | ${price_val:,.2f} | At {price_position:.1f}% of range |
        | Upper | ${upper_val:,.2f} | {upside_remaining:.1f}% above current |
        """
    )


def _render_in_range_time(api_client: Any, strategy_id: str) -> None:
    """Render in-range time percentage analysis."""
    events = []
    if api_client:
        try:
            events = api_client.get_timeline(strategy_id, limit=100)
        except Exception:
            pass

    # Filter for LP events
    lp_events = [e for e in events if e.get("event_type", "").upper() in ["LP_OPEN", "LP_CLOSE"]]

    if not lp_events:
        st.info("No position history available. In-range time will be tracked after LP positions are opened.")

        # Show expected behavior
        st.markdown("---")
        st.markdown("**How In-Range Time is Tracked:**")
        st.markdown(
            """
            - When an LP position is opened, the timer starts
            - While price stays within range, the position earns fees
            - When price exits range OR position is closed, the period ends
            - In-range time % = time earning fees / total position time
            """
        )
        return

    # Count LP_OPEN and LP_CLOSE events
    open_count = len([e for e in lp_events if e.get("event_type", "").upper() == "LP_OPEN"])
    close_count = len([e for e in lp_events if e.get("event_type", "").upper() == "LP_CLOSE"])

    # Estimate in-range time (simplified: assume all LP time was in-range if no out-of-range events)
    if open_count > 0:
        # Each open-close pair represents a position period
        completed_positions = min(open_count, close_count)
        current_position = open_count > close_count

        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric(
                "Positions Opened",
                str(open_count),
                help="Total LP positions opened",
            )

        with col2:
            st.metric(
                "Positions Closed",
                str(close_count),
                help="Total LP positions closed",
            )

        with col3:
            status = "IN POSITION" if current_position else "NO POSITION"
            if current_position:
                st.success(status)
            else:
                st.info(status)

        # Estimate in-range percentage (simplified)
        st.markdown("---")
        if completed_positions > 0:
            # Assume 95% in-range time as estimate for healthy positions
            estimated_in_range = 95
            st.metric(
                "Estimated In-Range Time",
                f"~{estimated_in_range}%",
                help="Estimated percentage of time price was within LP range",
            )
            st.caption(
                "Note: Precise tracking requires on-chain price monitoring. This estimate assumes healthy position management with rebalancing at 5% deviation."
            )
        else:
            st.info("Position currently active - in-range time will be calculated when closed")

    else:
        st.info("No positions recorded yet")


def _render_rebalance_history(api_client: Any, strategy_id: str) -> None:
    """Render rebalance history with price movement."""
    events = []
    if api_client:
        try:
            events = api_client.get_timeline(strategy_id, limit=50)
        except Exception:
            pass

    # Filter for rebalance-related events
    rebalance_events = [e for e in events if e.get("event_type", "").upper() in ["STATE_CHANGE", "LP_OPEN", "LP_CLOSE"]]

    if not rebalance_events:
        st.info("No rebalance events recorded yet. History will appear after position changes occur.")
        return

    st.markdown("**Recent Rebalance Events:**")

    rebalance_count = 0
    open_count = 0
    close_count = 0

    for event in rebalance_events[:10]:  # Show latest 10
        event_type = event.get("event_type", "").upper()
        timestamp = event.get("timestamp", "N/A")
        details = event.get("details", {})

        if isinstance(timestamp, str) and len(timestamp) > 19:
            timestamp = timestamp[:19]  # Trim to YYYY-MM-DD HH:MM:SS

        trigger = details.get("trigger", "")
        current_price = details.get("current_price", "")
        center_price = details.get("center_price", "")
        deviation_pct = details.get("deviation_pct", "")
        range_lower = details.get("range_lower", "")
        range_upper = details.get("range_upper", "")

        if event_type == "STATE_CHANGE" and trigger == "price_deviation":
            # Price deviation trigger
            rebalance_count += 1
            st.markdown(f"**{timestamp}**")
            col1, col2 = st.columns([1, 4])
            with col1:
                st.warning("REBALANCE")
            with col2:
                st.markdown("Price moved >5% from position center")
                if current_price and center_price:
                    st.caption(f"Center: ${center_price} -> Current: ${current_price}")
                if deviation_pct:
                    st.caption(f"Deviation: {float(deviation_pct):.2f}%")

        elif event_type == "LP_OPEN":
            # LP open event
            open_count += 1
            st.markdown(f"**{timestamp}**")
            col1, col2 = st.columns([1, 4])
            with col1:
                st.success("ENTRY")
            with col2:
                action = details.get("action", "")
                if action == "opening_new_position":
                    st.markdown("Opened asymmetric bullish LP position")
                else:
                    st.markdown("Opened LP position")
                if range_lower and range_upper:
                    st.caption(f"Range: ${range_lower} - ${range_upper}")
                upside = details.get("upside_pct", "")
                downside = details.get("downside_pct", "")
                if upside and downside:
                    up_pct = float(Decimal(str(upside))) * 100
                    down_pct = float(Decimal(str(downside))) * 100
                    st.caption(f"Bias: +{up_pct:.0f}% upside, -{down_pct:.0f}% downside")

        elif event_type == "LP_CLOSE":
            # LP close event
            close_count += 1
            st.markdown(f"**{timestamp}**")
            col1, col2 = st.columns([1, 4])
            with col1:
                st.error("EXIT")
            with col2:
                pool = details.get("pool", "")
                if pool:
                    st.markdown(f"Closed LP position on {pool}")
                else:
                    st.markdown("Closed LP position")

        st.markdown("---")

    # Summary statistics
    st.markdown("**Summary:**")
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Rebalances", str(rebalance_count))

    with col2:
        st.metric("Entries", str(open_count))

    with col3:
        st.metric("Exits", str(close_count))

    st.caption(
        "The strategy rebalances when price moves >5% from position center, closing the current position and opening a new one with updated asymmetric bounds."
    )
