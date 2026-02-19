"""
Aerodrome Trend-Following LP Dashboard.

Custom dashboard showing trend status, EMA values, in-position/out-of-position
time percentage, and trend reversal history with entry/exit markers.
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import streamlit as st


def render_custom_dashboard(
    strategy_id: str,
    strategy_config: dict[str, Any],
    api_client: Any,
    session_state: dict[str, Any],
) -> None:
    """Render the Aerodrome Trend-Following LP custom dashboard.

    Shows:
    - Current trend (Bullish/Bearish indicator)
    - EMA(9) and EMA(21) values
    - In-position vs out-of-position percentage
    - Trend reversal history with entry/exit markers
    """
    st.title("Aerodrome Trend-Following LP Dashboard")

    # Extract config values with defaults
    pool = strategy_config.get("pool", "WETH/USDC")
    stable = strategy_config.get("stable", False)

    pool_parts = pool.split("/")
    token0 = pool_parts[0] if len(pool_parts) > 0 else "WETH"
    token1 = pool_parts[1] if len(pool_parts) > 1 else "USDC"

    # Strategy info header
    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Pool:** {token0}/{token1} ({'Stable' if stable else 'Volatile'})")
    st.markdown("**Chain:** Base")

    # Pool type badge
    if stable:
        st.info("Stable Pool")
    else:
        st.warning("Volatile Pool - Trend Following Active")

    st.divider()

    # Current Trend Section
    st.subheader("Current Trend")
    _render_trend_indicator(session_state)

    st.divider()

    # EMA Values Section
    st.subheader("EMA Indicators")
    _render_ema_values(session_state, token0)

    st.divider()

    # Position Time Section
    st.subheader("Position Time Analysis")
    _render_position_time(api_client, strategy_id)

    st.divider()

    # Trend Reversal History Section
    st.subheader("Trend Reversal History")
    _render_trend_history(api_client, strategy_id)


def _render_trend_indicator(session_state: dict[str, Any]) -> None:
    """Render current trend indicator (Bullish/Bearish)."""
    last_trend = session_state.get("last_trend", "")
    is_in_position = session_state.get("is_in_position", False)

    col1, col2 = st.columns(2)

    with col1:
        # Trend indicator
        if last_trend == "bullish":
            st.success("BULLISH")
            st.markdown("EMA(9) > EMA(21)")
        elif last_trend == "bearish":
            st.error("BEARISH")
            st.markdown("EMA(9) < EMA(21)")
        else:
            st.info("NEUTRAL / INITIALIZING")
            st.markdown("Waiting for EMA data")

    with col2:
        # Position status
        if is_in_position:
            st.success("IN LP POSITION")
            st.markdown("Collecting trading fees")
        else:
            st.warning("OUT OF LP")
            st.markdown("Holding tokens, waiting for bullish trend")

    # Strategy explanation
    st.markdown("---")
    st.markdown("**Strategy Logic:**")
    st.markdown(
        """
        - **Bullish (EMA9 > EMA21):** Open LP position to collect fees
        - **Bearish (EMA9 < EMA21):** Close LP, hold tokens to avoid impermanent loss
        - The strategy exits during downtrends to protect capital
        """
    )


def _render_ema_values(session_state: dict[str, Any], token0: str) -> None:
    """Render EMA(9) and EMA(21) values."""
    # Get EMA values from session state
    ema9 = session_state.get("ema9")
    ema21 = session_state.get("ema21")
    current_price = session_state.get("current_price")

    col1, col2, col3 = st.columns(3)

    with col1:
        if ema9 is not None:
            ema9_val = float(Decimal(str(ema9)))
            st.metric(
                "EMA(9)",
                f"${ema9_val:,.2f}",
                help="9-period Exponential Moving Average (fast)",
            )
        else:
            st.metric(
                "EMA(9)",
                "Calculating...",
                help="Need at least 9 price points",
            )

    with col2:
        if ema21 is not None:
            ema21_val = float(Decimal(str(ema21)))
            st.metric(
                "EMA(21)",
                f"${ema21_val:,.2f}",
                help="21-period Exponential Moving Average (slow)",
            )
        else:
            st.metric(
                "EMA(21)",
                "Calculating...",
                help="Need at least 21 price points",
            )

    with col3:
        if current_price is not None:
            price_val = float(Decimal(str(current_price)))
            st.metric(
                f"Current {token0} Price",
                f"${price_val:,.2f}",
                help=f"Current {token0} price in USD",
            )
        else:
            st.metric(
                f"Current {token0} Price",
                "N/A",
                help="Price not available",
            )

    # EMA spread
    if ema9 is not None and ema21 is not None:
        ema9_val = Decimal(str(ema9))
        ema21_val = Decimal(str(ema21))
        spread = ema9_val - ema21_val
        spread_pct = (spread / ema21_val * Decimal("100")) if ema21_val > 0 else Decimal("0")

        st.markdown("---")
        col1, col2 = st.columns(2)

        with col1:
            spread_str = f"${float(spread):+,.2f}"
            st.metric(
                "EMA Spread (EMA9 - EMA21)",
                spread_str,
                help="Positive = Bullish, Negative = Bearish",
            )

        with col2:
            spread_pct_str = f"{float(spread_pct):+.2f}%"
            st.metric(
                "EMA Spread (%)",
                spread_pct_str,
                help="Percentage difference between EMAs",
            )

    # EMA explanation
    st.markdown("---")
    st.markdown("**About EMA Crossovers:**")
    st.markdown(
        """
        - **EMA (Exponential Moving Average)** gives more weight to recent prices
        - **Golden Cross (EMA9 > EMA21):** Short-term momentum exceeds long-term, bullish signal
        - **Death Cross (EMA9 < EMA21):** Short-term momentum falls below long-term, bearish signal
        """
    )


def _render_position_time(api_client: Any, strategy_id: str) -> None:
    """Render in-position vs out-of-position time percentage."""
    events = []
    if api_client:
        try:
            events = api_client.get_timeline(strategy_id, limit=100)
        except Exception:
            pass

    # Collect LP_OPEN and LP_CLOSE events to calculate time in/out of position
    lp_events = [e for e in events if e.get("event_type") in ["LP_OPEN", "LP_CLOSE", "lp_open", "lp_close"]]

    # Sort events by timestamp (oldest first)
    lp_events_sorted = sorted(
        lp_events,
        key=lambda e: e.get("timestamp", ""),
    )

    total_in_position_seconds = 0
    total_out_position_seconds = 0
    last_event_time = None
    in_position = False

    for event in lp_events_sorted:
        try:
            timestamp = event.get("timestamp", "")
            if isinstance(timestamp, str):
                event_time = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            else:
                event_time = timestamp

            if last_event_time is not None:
                duration = (event_time - last_event_time).total_seconds()
                if in_position:
                    total_in_position_seconds += duration
                else:
                    total_out_position_seconds += duration

            event_type = event.get("event_type", "").upper()
            if event_type == "LP_OPEN":
                in_position = True
            elif event_type == "LP_CLOSE":
                in_position = False

            last_event_time = event_time

        except Exception:
            continue

    # Add time from last event to now
    if last_event_time is not None:
        now = datetime.now(UTC)
        duration = (now - last_event_time).total_seconds()
        if in_position:
            total_in_position_seconds += duration
        else:
            total_out_position_seconds += duration

    total_seconds = total_in_position_seconds + total_out_position_seconds

    col1, col2, col3 = st.columns(3)

    with col1:
        in_pct = (total_in_position_seconds / total_seconds * 100) if total_seconds > 0 else 0
        st.metric(
            "Time In Position",
            f"{in_pct:.1f}%",
            help="Percentage of time in LP position (collecting fees)",
        )

    with col2:
        out_pct = (total_out_position_seconds / total_seconds * 100) if total_seconds > 0 else 0
        st.metric(
            "Time Out of Position",
            f"{out_pct:.1f}%",
            help="Percentage of time out of LP (protecting capital)",
        )

    with col3:
        total_hours = total_seconds / 3600
        st.metric(
            "Total Tracked Time",
            f"{total_hours:.1f} hrs",
            help="Total time tracked from first LP event",
        )

    # Visual bar chart
    if total_seconds > 0:
        import pandas as pd

        chart_data = pd.DataFrame(
            {
                "Status": ["In Position", "Out of Position"],
                "Percentage": [in_pct, out_pct],
            }
        )
        st.bar_chart(chart_data.set_index("Status"))

    # Explanation
    st.markdown("---")
    st.markdown("**Position Time Analysis:**")
    if in_pct > out_pct:
        st.success(
            f"The strategy has been in position {in_pct:.1f}% of the time, "
            "indicating a predominantly bullish market with good fee collection."
        )
    elif out_pct > in_pct:
        st.warning(
            f"The strategy has been out of position {out_pct:.1f}% of the time, "
            "indicating a predominantly bearish market. Capital is being protected."
        )
    else:
        st.info("Position time is evenly split between in and out of LP.")


def _render_trend_history(api_client: Any, strategy_id: str) -> None:
    """Render trend reversal history with entry/exit markers."""
    events = []
    if api_client:
        try:
            events = api_client.get_timeline(strategy_id, limit=50)
        except Exception:
            pass

    # Filter for trend-related events (STATE_CHANGE with ema_crossover trigger, or LP events)
    trend_events = [
        e
        for e in events
        if e.get("event_type") in ["STATE_CHANGE", "state_change", "LP_OPEN", "LP_CLOSE", "lp_open", "lp_close"]
    ]

    if not trend_events:
        st.info("No trend reversal events recorded yet. History will appear after trend changes occur.")
        return

    # Display events in a table-like format
    st.markdown("**Recent Trend Reversals:**")

    for event in trend_events[:10]:  # Show latest 10
        event_type = event.get("event_type", "").upper()
        timestamp = event.get("timestamp", "N/A")
        details = event.get("details", {})
        description = event.get("description", "")

        if isinstance(timestamp, str) and len(timestamp) > 19:
            timestamp = timestamp[:19]  # Trim to YYYY-MM-DD HH:MM:SS

        trend = details.get("trend", "")
        ema9 = details.get("ema9", "")
        ema21 = details.get("ema21", "")
        trigger = details.get("trigger", "")

        if event_type in ["LP_OPEN", "lp_open"]:
            # Entry marker
            st.markdown(f"**{timestamp}**")
            col1, col2 = st.columns([1, 4])
            with col1:
                st.success("ENTRY")
            with col2:
                st.markdown("Opened LP position (Bullish trend)")
                if ema9 and ema21:
                    st.caption(f"EMA9: ${ema9} | EMA21: ${ema21}")

        elif event_type in ["LP_CLOSE", "lp_close"]:
            # Exit marker
            st.markdown(f"**{timestamp}**")
            col1, col2 = st.columns([1, 4])
            with col1:
                st.error("EXIT")
            with col2:
                st.markdown("Closed LP position (Bearish trend)")
                if ema9 and ema21:
                    st.caption(f"EMA9: ${ema9} | EMA21: ${ema21}")

        elif event_type in ["STATE_CHANGE", "state_change"] and trigger == "ema_crossover":
            # Trend change event
            st.markdown(f"**{timestamp}**")
            col1, col2 = st.columns([1, 4])
            with col1:
                if trend == "bullish":
                    st.success("BULLISH")
                elif trend == "bearish":
                    st.error("BEARISH")
                else:
                    st.info(trend.upper() if trend else "CHANGE")
            with col2:
                st.markdown(description if description else "EMA crossover detected")
                if ema9 and ema21:
                    st.caption(f"EMA9: ${ema9} | EMA21: ${ema21}")

        st.markdown("---")

    # Summary statistics
    lp_opens = len([e for e in trend_events if e.get("event_type", "").upper() in ["LP_OPEN"]])
    lp_closes = len([e for e in trend_events if e.get("event_type", "").upper() in ["LP_CLOSE"]])

    st.markdown("**Summary:**")
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Total Entries", str(lp_opens))

    with col2:
        st.metric("Total Exits", str(lp_closes))

    with col3:
        total_reversals = lp_opens + lp_closes
        st.metric("Total Reversals", str(total_reversals))

    st.caption(
        "Entry = Bullish crossover (EMA9 > EMA21), Exit = Bearish crossover (EMA9 < EMA21). "
        "The strategy enters LP during uptrends and exits during downtrends."
    )
