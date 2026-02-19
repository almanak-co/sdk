"""
TraderJoe Wide-Range Accumulator Dashboard.

Custom dashboard showing JOE/AVAX balance tracking, time-based rebalance countdown,
position value breakdown, and rebalance trigger history.
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
    """Render the TJ Wide-Range Accumulator custom dashboard.

    Shows:
    - JOE balance over time
    - AVAX balance over time
    - Days until next time-based rebalance
    - Position value breakdown by token
    - Rebalance trigger type history (time vs price)
    """
    st.title("TJ Wide-Range Accumulator Dashboard")

    # Extract config values with defaults
    pool = strategy_config.get("pool", "JOE/WAVAX/20")
    range_width_pct = Decimal(str(strategy_config.get("range_width_pct", "0.15")))
    rebalance_price_threshold_pct = Decimal(str(strategy_config.get("rebalance_price_threshold_pct", "0.07")))
    rebalance_time_days = int(strategy_config.get("rebalance_time_days", 7))
    amount_x = Decimal(str(strategy_config.get("amount_x", "15")))
    amount_y = Decimal(str(strategy_config.get("amount_y", "0.15")))

    pool_parts = pool.split("/")
    token_x = pool_parts[0] if len(pool_parts) > 0 else "JOE"
    token_y = pool_parts[1] if len(pool_parts) > 1 else "WAVAX"
    bin_step = int(pool_parts[2]) if len(pool_parts) > 2 else 20

    # Strategy info header
    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Pool:** {token_x}/{token_y} (bin step: {bin_step})")

    # Metrics row - Rebalance thresholds
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            label="Range Width",
            value=f"{float(range_width_pct) * 100:.1f}%",
            help="Price range width as percentage (wide range for accumulation)",
        )

    with col2:
        st.metric(
            label="Time Rebalance",
            value=f"{rebalance_time_days} days",
            help="Rebalance triggered after this many days since last rebalance",
        )

    with col3:
        st.metric(
            label="Price Threshold",
            value=f"{float(rebalance_price_threshold_pct) * 100:.1f}%",
            help="Price deviation that triggers rebalancing",
        )

    st.divider()

    # Time until next rebalance section
    st.subheader("Rebalance Countdown")
    _render_rebalance_countdown(session_state, rebalance_time_days)

    st.divider()

    # Position value breakdown by token
    st.subheader("Position Value Breakdown")
    _render_position_breakdown(amount_x, amount_y, token_x, token_y)

    st.divider()

    # Token balance tracking over time
    st.subheader("Token Balance Tracking")
    _render_balance_tracking(api_client, strategy_id, token_x, token_y, amount_x, amount_y)

    st.divider()

    # Rebalance trigger history (time vs price)
    st.subheader("Rebalance Trigger History")
    _render_rebalance_trigger_history(api_client, strategy_id)


def _render_rebalance_countdown(
    session_state: dict[str, Any],
    rebalance_time_days: int,
) -> None:
    """Render countdown to next time-based rebalance."""
    last_rebalance_time = session_state.get("last_rebalance_time")

    if last_rebalance_time:
        try:
            if isinstance(last_rebalance_time, str):
                last_rebalance = datetime.fromisoformat(last_rebalance_time.replace("Z", "+00:00"))
            else:
                last_rebalance = last_rebalance_time

            now = datetime.now(UTC)
            time_since = now - last_rebalance
            days_elapsed = time_since.days
            days_remaining = max(0, rebalance_time_days - days_elapsed)

            col1, col2, col3 = st.columns(3)

            with col1:
                st.metric(
                    "Days Since Last Rebalance",
                    str(days_elapsed),
                    help="Days elapsed since the last rebalance",
                )

            with col2:
                st.metric(
                    "Days Until Time Rebalance",
                    str(days_remaining),
                    delta=f"-{days_elapsed}" if days_elapsed > 0 else None,
                    help="Days remaining until time-based rebalance triggers",
                )

            with col3:
                progress = min(1.0, days_elapsed / rebalance_time_days)
                if days_remaining == 0:
                    st.warning("Time-based rebalance due!")
                else:
                    st.progress(progress, text=f"{int(progress * 100)}% to time rebalance")

            # Last rebalance timestamp
            last_rebalance_str = last_rebalance.strftime("%Y-%m-%d %H:%M UTC")
            st.caption(f"Last rebalance: {last_rebalance_str}")

        except Exception:
            st.info("Unable to parse last rebalance time from state.")
    else:
        st.info(
            f"No rebalance recorded yet. Time-based rebalance will trigger {rebalance_time_days} days after position opens."
        )


def _render_position_breakdown(
    amount_x: Decimal,
    amount_y: Decimal,
    token_x: str,
    token_y: str,
) -> None:
    """Render position value breakdown by token."""
    # Default prices for JOE and WAVAX
    joe_price_usd = Decimal("0.4")  # ~$0.40 per JOE
    wavax_price_usd = Decimal("30")  # ~$30 per WAVAX

    value_x_usd = amount_x * joe_price_usd
    value_y_usd = amount_y * wavax_price_usd
    total_value_usd = value_x_usd + value_y_usd

    col1, col2 = st.columns(2)

    with col1:
        st.metric(
            f"{token_x} Position",
            f"{float(amount_x):.2f} {token_x}",
            help=f"~${float(value_x_usd):.2f} USD at ${float(joe_price_usd):.2f}/{token_x}",
        )
        st.caption(f"Value: ${float(value_x_usd):.2f} USD")

    with col2:
        st.metric(
            f"{token_y} Position",
            f"{float(amount_y):.4f} {token_y}",
            help=f"~${float(value_y_usd):.2f} USD at ${float(wavax_price_usd):.2f}/{token_y}",
        )
        st.caption(f"Value: ${float(value_y_usd):.2f} USD")

    # Total value and allocation pie
    st.markdown(f"**Total Position Value:** ${float(total_value_usd):.2f} USD")

    # Allocation breakdown
    if total_value_usd > 0:
        x_pct = float(value_x_usd / total_value_usd * 100)
        y_pct = float(value_y_usd / total_value_usd * 100)
        st.markdown(f"**Allocation:** {token_x}: {x_pct:.1f}% | {token_y}: {y_pct:.1f}%")


def _render_balance_tracking(
    api_client: Any,
    strategy_id: str,
    token_x: str,
    token_y: str,
    amount_x: Decimal,
    amount_y: Decimal,
) -> None:
    """Render token balance tracking over time from timeline events."""
    import pandas as pd

    events = []
    if api_client:
        try:
            events = api_client.get_timeline(strategy_id, limit=100)
        except Exception:
            pass

    # Extract balance snapshots from LP events
    balance_data = []

    for event in events:
        event_type = event.get("event_type", "")
        timestamp = event.get("timestamp", "")

        # Track LP opens and closes as balance changes
        if event_type in ["LP_OPEN", "lp_open", "LP_CLOSE", "lp_close"]:
            try:
                if isinstance(timestamp, str):
                    dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                else:
                    dt = timestamp

                # For LP_OPEN, tokens go into pool
                # For LP_CLOSE, tokens come back
                if event_type in ["LP_OPEN", "lp_open"]:
                    balance_data.append(
                        {
                            "Time": dt,
                            f"{token_x} (in LP)": float(amount_x),
                            f"{token_y} (in LP)": float(amount_y),
                        }
                    )
                else:
                    balance_data.append(
                        {
                            "Time": dt,
                            f"{token_x} (in LP)": 0.0,
                            f"{token_y} (in LP)": 0.0,
                        }
                    )
            except Exception:
                pass

    if balance_data:
        df = pd.DataFrame(balance_data)
        df = df.sort_values("Time")
        df = df.set_index("Time")

        st.markdown(f"**{token_x} in LP Position Over Time:**")
        st.line_chart(df[[f"{token_x} (in LP)"]])

        st.markdown(f"**{token_y} in LP Position Over Time:**")
        st.line_chart(df[[f"{token_y} (in LP)"]])
    else:
        st.info("No LP position history yet. Balance tracking will appear after position opens and rebalances occur.")

    st.caption(
        "Note: Balance tracking shows tokens deposited in LP positions. Actual balances may vary due to trading activity and fees."
    )


def _render_rebalance_trigger_history(api_client: Any, strategy_id: str) -> None:
    """Render rebalance trigger history showing time vs price triggers."""
    import pandas as pd

    events = []
    if api_client:
        try:
            events = api_client.get_timeline(strategy_id, limit=100)
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
            if trigger in ["time", "price"]:
                rebalance_events.append(
                    {
                        "timestamp": event.get("timestamp", ""),
                        "trigger": trigger,
                        "details": details,
                    }
                )

    if not rebalance_events:
        st.info("No rebalance events yet. Rebalances occur when either 7 days elapse OR price moves >7% from center.")
        return

    # Count triggers by type
    time_triggers = sum(1 for e in rebalance_events if e["trigger"] == "time")
    price_triggers = sum(1 for e in rebalance_events if e["trigger"] == "price")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "Total Rebalances",
            str(len(rebalance_events)),
            help="Total number of rebalance events",
        )

    with col2:
        st.metric(
            "Time-Based",
            str(time_triggers),
            help="Rebalances triggered by 7-day time threshold",
        )

    with col3:
        st.metric(
            "Price-Based",
            str(price_triggers),
            help="Rebalances triggered by >7% price deviation",
        )

    # Trigger type distribution chart
    if time_triggers + price_triggers > 0:
        st.markdown("**Rebalance Trigger Distribution:**")
        trigger_df = pd.DataFrame(
            [
                {"Trigger Type": "Time (7 days)", "Count": time_triggers},
                {"Trigger Type": "Price (>7%)", "Count": price_triggers},
            ]
        )
        st.bar_chart(trigger_df.set_index("Trigger Type")["Count"])

    # Recent rebalance history
    st.markdown(f"**Recent Rebalances ({len(rebalance_events)}):**")

    for event in rebalance_events[:10]:  # Show last 10
        timestamp = event["timestamp"]
        trigger = event["trigger"]
        details = event["details"]

        # Format timestamp
        try:
            if isinstance(timestamp, str):
                dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                time_str = dt.strftime("%Y-%m-%d %H:%M UTC")
            else:
                time_str = str(timestamp)
        except Exception:
            time_str = str(timestamp)

        # Format trigger reason
        if trigger == "time":
            days = details.get("days_since_rebalance", "?")
            reason = f"Time trigger ({days} days elapsed)"
        else:
            price_change = details.get("price_change_pct", "?")
            reason = f"Price trigger ({price_change}% deviation)"

        st.markdown(f"- **{time_str}**: {reason}")
