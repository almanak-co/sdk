"""
Uniswap V3 Volatility-Adaptive LP Dashboard.

Custom dashboard showing ATR-based volatility metrics, range width adjustments,
volatility regimes, and multi-chain deployment status.
"""

from decimal import Decimal
from typing import Any

import streamlit as st

# Volatility regime thresholds (matching strategy.py)
LOW_VOL_THRESHOLD = Decimal("0.02")  # ATR < 2% = low volatility
HIGH_VOL_THRESHOLD = Decimal("0.05")  # ATR > 5% = high volatility

# Range widths for each regime
LOW_VOL_RANGE_WIDTH = Decimal("0.05")  # 5% range for low vol
MEDIUM_VOL_RANGE_WIDTH = Decimal("0.10")  # 10% range for medium vol
HIGH_VOL_RANGE_WIDTH = Decimal("0.15")  # 15% range for high vol

# Supported chains for this strategy
SUPPORTED_CHAINS = ["arbitrum", "base", "optimism", "ethereum"]


def render_custom_dashboard(
    strategy_id: str,
    strategy_config: dict[str, Any],
    api_client: Any,
    session_state: dict[str, Any],
) -> None:
    """Render the Uniswap V3 Volatility-Adaptive LP custom dashboard.

    Shows:
    - Current ATR value and percentage
    - Volatility regime (Low/Medium/High)
    - Current range width and how it was computed
    - Historical range width adjustments
    - Chain selector if deployed on multiple chains
    """
    st.title("Uniswap V3 Volatility-Adaptive LP Dashboard")

    # Extract config values with defaults
    pool = strategy_config.get("pool", "WETH/USDC/3000")
    chain = strategy_config.get("chain", "arbitrum")
    base_range_width_pct = strategy_config.get("base_range_width_pct", "0.10")

    pool_parts = pool.split("/")
    token0 = pool_parts[0] if len(pool_parts) > 0 else "WETH"
    token1 = pool_parts[1] if len(pool_parts) > 1 else "USDC"
    fee_tier = pool_parts[2] if len(pool_parts) > 2 else "3000"

    # Strategy info header
    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Pool:** {token0}/{token1} ({int(fee_tier) / 10000:.2f}% fee tier)")
    st.markdown(f"**Base Range Width:** {float(Decimal(str(base_range_width_pct))) * 100:.1f}%")

    st.divider()

    # Multi-chain selector section
    st.subheader("Chain Deployment")
    _render_chain_selector(chain, session_state)

    st.divider()

    # ATR and Volatility Section
    st.subheader("Volatility Analysis")
    _render_atr_metrics(session_state, token0)

    st.divider()

    # Volatility Regime Section
    st.subheader("Current Volatility Regime")
    _render_volatility_regime(session_state)

    st.divider()

    # Range Width Section
    st.subheader("Adaptive Range Width")
    _render_range_width(session_state)

    st.divider()

    # Historical Adjustments Section
    st.subheader("Range Width Adjustment History")
    _render_adjustment_history(api_client, strategy_id)


def _render_chain_selector(current_chain: str, session_state: dict[str, Any]) -> None:
    """Render chain selector for multi-chain deployment."""
    st.markdown("**Multi-Chain Support:** This strategy can be deployed on multiple chains.")

    # Show supported chains
    col1, col2, col3, col4 = st.columns(4)
    chains_cols = [col1, col2, col3, col4]

    for i, chain in enumerate(SUPPORTED_CHAINS):
        with chains_cols[i]:
            is_current = chain.lower() == current_chain.lower()
            if is_current:
                st.success(f"{chain.upper()}")
                st.caption("(Current)")
            else:
                st.info(f"{chain.upper()}")
                st.caption("Available")

    # Chain-specific info
    st.markdown("---")
    chain_info = {
        "arbitrum": "Layer 2 with low gas fees, high Uniswap V3 liquidity",
        "base": "Coinbase L2, growing DeFi ecosystem",
        "optimism": "OP Stack L2, strong ecosystem incentives",
        "ethereum": "Mainnet, highest liquidity but higher gas costs",
    }

    st.markdown(f"**Current Chain:** {current_chain.upper()}")
    st.caption(chain_info.get(current_chain.lower(), "EVM-compatible chain"))


def _render_atr_metrics(session_state: dict[str, Any], token0: str) -> None:
    """Render ATR value and percentage metrics."""
    # Get values from session state
    current_atr = session_state.get("current_atr")
    current_price = session_state.get("current_price")

    col1, col2, col3 = st.columns(3)

    with col1:
        if current_atr is not None:
            atr_val = float(Decimal(str(current_atr)))
            st.metric(
                "ATR(14)",
                f"${atr_val:,.2f}",
                help="14-period Average True Range - measures price volatility",
            )
        else:
            st.metric(
                "ATR(14)",
                "Calculating...",
                help="Need at least 15 price points to calculate ATR",
            )

    with col2:
        if current_atr is not None and current_price is not None:
            atr_val = Decimal(str(current_atr))
            price_val = Decimal(str(current_price))
            atr_pct = (atr_val / price_val * Decimal("100")) if price_val > 0 else Decimal("0")
            st.metric(
                "ATR %",
                f"{float(atr_pct):.2f}%",
                help="ATR as percentage of price - key volatility metric",
            )
        else:
            st.metric(
                "ATR %",
                "N/A",
                help="ATR percentage not available",
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

    # ATR explanation
    st.markdown("---")
    st.markdown("**About ATR (Average True Range):**")
    st.markdown(
        """
        - **ATR** measures market volatility by calculating the average range of price movement
        - **ATR %** = ATR / Current Price, normalizes volatility across different assets
        - Higher ATR % means wider price swings, requiring wider LP ranges to stay in range
        - Lower ATR % allows tighter ranges for higher fee concentration
        """
    )


def _render_volatility_regime(session_state: dict[str, Any]) -> None:
    """Render volatility regime indicator (Low/Medium/High)."""
    volatility_regime = session_state.get("volatility_regime", "")

    col1, col2 = st.columns(2)

    with col1:
        # Regime indicator
        if volatility_regime == "low":
            st.success("LOW VOLATILITY")
            st.markdown("ATR < 2% of price")
            st.markdown(f"Range Width: {float(LOW_VOL_RANGE_WIDTH) * 100:.0f}%")
        elif volatility_regime == "high":
            st.error("HIGH VOLATILITY")
            st.markdown("ATR > 5% of price")
            st.markdown(f"Range Width: {float(HIGH_VOL_RANGE_WIDTH) * 100:.0f}%")
        elif volatility_regime == "medium":
            st.warning("MEDIUM VOLATILITY")
            st.markdown("ATR between 2-5% of price")
            st.markdown(f"Range Width: {float(MEDIUM_VOL_RANGE_WIDTH) * 100:.0f}%")
        else:
            st.info("CALCULATING...")
            st.markdown("Waiting for ATR data")

    with col2:
        # Regime thresholds visualization
        st.markdown("**Volatility Thresholds:**")
        st.markdown(f"- LOW: ATR < {float(LOW_VOL_THRESHOLD) * 100:.0f}%")
        st.markdown(f"- MEDIUM: {float(LOW_VOL_THRESHOLD) * 100:.0f}% - {float(HIGH_VOL_THRESHOLD) * 100:.0f}%")
        st.markdown(f"- HIGH: ATR > {float(HIGH_VOL_THRESHOLD) * 100:.0f}%")

    # Strategy explanation
    st.markdown("---")
    st.markdown("**Volatility Regime Strategy:**")
    st.markdown(
        """
        | Regime | ATR % | Range Width | Strategy |
        |--------|-------|-------------|----------|
        | LOW | < 2% | 5% | Tight range for maximum fee capture |
        | MEDIUM | 2-5% | 10% | Balanced range for moderate conditions |
        | HIGH | > 5% | 15% | Wide range to stay in range longer |
        """
    )


def _render_range_width(session_state: dict[str, Any]) -> None:
    """Render current range width and computation details."""
    volatility_regime = session_state.get("volatility_regime", "")
    current_range_width = session_state.get("current_range_width")
    range_lower = session_state.get("range_lower")
    range_upper = session_state.get("range_upper")
    current_price = session_state.get("current_price")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Current Position Range:**")
        if current_range_width is not None:
            width_val = float(Decimal(str(current_range_width))) * 100
            st.metric(
                "Range Width",
                f"{width_val:.1f}%",
                help="Width of the LP position range",
            )
        else:
            st.metric(
                "Range Width",
                "N/A",
                help="No active position",
            )

        if range_lower is not None and range_upper is not None:
            lower_val = float(Decimal(str(range_lower)))
            upper_val = float(Decimal(str(range_upper)))
            st.markdown(f"**Range:** ${lower_val:,.2f} - ${upper_val:,.2f}")
        else:
            st.markdown("**Range:** No active position")

    with col2:
        st.markdown("**How Range Width is Computed:**")
        if volatility_regime:
            regime_display = volatility_regime.upper()
            if volatility_regime == "low":
                computed_width = LOW_VOL_RANGE_WIDTH
            elif volatility_regime == "high":
                computed_width = HIGH_VOL_RANGE_WIDTH
            else:
                computed_width = MEDIUM_VOL_RANGE_WIDTH

            st.markdown(
                f"""
                1. Calculate ATR(14) from price history
                2. Compute ATR % = ATR / Price
                3. Determine regime: **{regime_display}**
                4. Apply range width: **{float(computed_width) * 100:.0f}%**
                """
            )
        else:
            st.markdown("Waiting for volatility data...")

    # Visual range indicator
    st.markdown("---")
    if current_price is not None and range_lower is not None and range_upper is not None:
        price_val = float(Decimal(str(current_price)))
        lower_val = float(Decimal(str(range_lower)))
        upper_val = float(Decimal(str(range_upper)))

        # Calculate position within range
        if upper_val > lower_val:
            range_span = upper_val - lower_val
            price_position = (price_val - lower_val) / range_span * 100
            price_position = max(0, min(100, price_position))  # Clamp to 0-100

            st.markdown("**Price Position in Range:**")

            # Simple text-based indicator
            if price_val < lower_val:
                st.error(f"BELOW RANGE - Price: ${price_val:,.2f} < Lower: ${lower_val:,.2f}")
            elif price_val > upper_val:
                st.error(f"ABOVE RANGE - Price: ${price_val:,.2f} > Upper: ${upper_val:,.2f}")
            else:
                st.progress(price_position / 100)
                st.caption(f"Price at {price_position:.1f}% of range (${lower_val:,.2f} - ${upper_val:,.2f})")
    else:
        st.info("No active position to display range indicator")


def _render_adjustment_history(api_client: Any, strategy_id: str) -> None:
    """Render historical range width adjustments."""
    events = []
    if api_client:
        try:
            events = api_client.get_timeline(strategy_id, limit=50)
        except Exception:
            pass

    # Filter for regime change and LP events
    adjustment_events = [
        e
        for e in events
        if e.get("event_type") in ["STATE_CHANGE", "state_change", "LP_OPEN", "LP_CLOSE", "lp_open", "lp_close"]
    ]

    if not adjustment_events:
        st.info("No range adjustment events recorded yet. History will appear after volatility regime changes occur.")
        return

    # Display events
    st.markdown("**Recent Range Adjustments:**")

    regime_changes = 0
    lp_events_count = 0

    for event in adjustment_events[:10]:  # Show latest 10
        event_type = event.get("event_type", "").upper()
        timestamp = event.get("timestamp", "N/A")
        details = event.get("details", {})

        if isinstance(timestamp, str) and len(timestamp) > 19:
            timestamp = timestamp[:19]  # Trim to YYYY-MM-DD HH:MM:SS

        trigger = details.get("trigger", "")
        old_regime = details.get("old_regime", "")
        new_regime = details.get("new_regime", details.get("volatility_regime", ""))
        range_width = details.get("range_width", "")
        atr = details.get("atr", "")

        if event_type in ["STATE_CHANGE"] and trigger == "regime_change":
            # Regime change event
            regime_changes += 1
            st.markdown(f"**{timestamp}**")
            col1, col2 = st.columns([1, 4])
            with col1:
                if new_regime == "high":
                    st.error("HIGH VOL")
                elif new_regime == "low":
                    st.success("LOW VOL")
                else:
                    st.warning("MEDIUM")
            with col2:
                st.markdown(f"Regime changed: {old_regime.upper()} -> {new_regime.upper()}")
                if range_width:
                    width_pct = float(Decimal(str(range_width))) * 100
                    st.caption(f"New range width: {width_pct:.0f}%")
                if atr:
                    st.caption(f"ATR: ${atr}")

        elif event_type in ["LP_OPEN"]:
            # LP open event
            lp_events_count += 1
            st.markdown(f"**{timestamp}**")
            col1, col2 = st.columns([1, 4])
            with col1:
                st.success("OPEN LP")
            with col2:
                if new_regime:
                    st.markdown(f"Opened LP in {new_regime.upper()} volatility regime")
                else:
                    st.markdown("Opened LP position")
                if range_width:
                    width_pct = float(Decimal(str(range_width))) * 100
                    st.caption(f"Range width: {width_pct:.0f}%")
                range_lower = details.get("range_lower", "")
                range_upper = details.get("range_upper", "")
                if range_lower and range_upper:
                    st.caption(f"Range: ${range_lower} - ${range_upper}")

        elif event_type in ["LP_CLOSE"]:
            # LP close event
            lp_events_count += 1
            st.markdown(f"**{timestamp}**")
            col1, col2 = st.columns([1, 4])
            with col1:
                st.error("CLOSE LP")
            with col2:
                if trigger == "regime_change":
                    st.markdown("Closed LP due to volatility regime change")
                elif trigger == "out_of_range":
                    st.markdown("Closed LP - price exited range")
                else:
                    st.markdown("Closed LP position")

        st.markdown("---")

    # Summary statistics
    st.markdown("**Summary:**")
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Regime Changes", str(regime_changes))

    with col2:
        st.metric("LP Events", str(lp_events_count))

    with col3:
        total_adjustments = regime_changes + lp_events_count
        st.metric("Total Adjustments", str(total_adjustments))

    st.caption(
        "The strategy automatically adjusts range width based on ATR volatility. "
        "When volatility increases significantly (low -> high), it widens the range. "
        "When volatility decreases significantly (high -> low), it tightens the range for better fee capture."
    )
