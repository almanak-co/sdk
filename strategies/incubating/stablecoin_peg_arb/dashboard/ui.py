"""
Stablecoin Peg Arbitrage Strategy Dashboard.

Custom dashboard showing stablecoin prices, depeg detection,
Curve pool routing, and arbitrage opportunities.
"""

from decimal import Decimal
from typing import Any

import streamlit as st


def render_custom_dashboard(
    strategy_id: str,
    strategy_config: dict[str, Any],
    api_client: Any,
    session_state: dict[str, Any],
) -> None:
    """Render the Stablecoin Peg Arbitrage custom dashboard."""
    st.title("Stablecoin Peg Arbitrage Dashboard")

    # Extract config
    chain = strategy_config.get("chain", "ethereum")
    stablecoins = strategy_config.get("stablecoins", ["USDC", "USDT", "DAI", "FRAX"])
    min_depeg_bps = strategy_config.get("min_depeg_bps", 50)

    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Chain:** {chain.capitalize()}")
    st.markdown("**Protocol:** Curve Finance")

    st.divider()

    # Strategy state
    st.subheader("Strategy State")
    state = session_state.get("state", "monitoring")
    cooldown = session_state.get("cooldown_remaining", 0)

    col1, col2 = st.columns(2)
    with col1:
        if state == "monitoring":
            st.info("State: MONITORING - Watching for depegs")
        elif state == "opportunity_found":
            st.success("State: DEPEG DETECTED - Opportunity found")
        elif state == "cooldown":
            st.warning(f"State: COOLDOWN - {cooldown}s remaining")
        else:
            st.info(f"State: {state}")
    with col2:
        st.metric("Cooldown", f"{cooldown}s")

    st.divider()

    # Stablecoin prices
    st.subheader("Stablecoin Prices")

    cols = st.columns(len(stablecoins))
    for i, token in enumerate(stablecoins):
        price = Decimal(str(session_state.get(f"{token.lower()}_price", "1.0")))
        depeg_bps = int((price - Decimal("1.0")) * 10000)

        with cols[i]:
            st.markdown(f"**{token}**")
            st.metric("Price", f"${float(price):.4f}")

            if abs(depeg_bps) >= min_depeg_bps:
                if depeg_bps > 0:
                    st.error(f"+{depeg_bps} bps PREMIUM")
                else:
                    st.error(f"{depeg_bps} bps DISCOUNT")
            elif abs(depeg_bps) >= 10:
                if depeg_bps > 0:
                    st.warning(f"+{depeg_bps} bps")
                else:
                    st.warning(f"{depeg_bps} bps")
            else:
                st.success("ON PEG")

    st.divider()

    # Depeg visualization
    st.subheader("Peg Status")

    for token in stablecoins:
        price = Decimal(str(session_state.get(f"{token.lower()}_price", "1.0")))
        # Normalize price for progress bar (0.99 to 1.01 range)
        normalized = (float(price) - 0.99) / 0.02  # Maps 0.99-1.01 to 0-1
        normalized = max(0, min(1, normalized))

        depeg_bps = int((price - Decimal("1.0")) * 10000)
        label = f"{token}: ${float(price):.4f} ({depeg_bps:+d} bps)"
        st.progress(normalized, text=label)

    st.divider()

    # Current opportunity
    st.subheader("Current Opportunity")
    opportunity = session_state.get("current_opportunity")

    if opportunity:
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Depegged Token", opportunity.get("depegged_token", "N/A"))
            st.metric("Current Price", f"${float(opportunity.get('current_price', 1)):.4f}")
            st.metric("Depeg Amount", f"{opportunity.get('depeg_bps', 0)} bps")
        with col2:
            st.metric("Stable Token", opportunity.get("stable_token", "N/A"))
            st.metric("Curve Pool", opportunity.get("curve_pool", "N/A"))
            st.metric("Expected Profit", f"{opportunity.get('expected_profit_bps', 0)} bps")

        direction = opportunity.get("direction", "")
        depegged = opportunity.get("depegged_token", "")
        if direction == "below_peg":
            st.success(
                f"Buy {depegged} at discount via Curve - expected {opportunity.get('expected_profit_bps')} bps profit"
            )
        else:
            st.success(
                f"Sell {depegged} at premium via Curve - expected {opportunity.get('expected_profit_bps')} bps profit"
            )
    else:
        st.info("No depeg opportunity found. Stablecoins near peg.")

    st.divider()

    # Curve pool info
    st.subheader("Curve Pool Routing")
    pool_info = {
        "3pool": ["DAI", "USDC", "USDT"],
        "frax_usdc": ["FRAX", "USDC"],
        "lusd_3crv": ["LUSD", "DAI", "USDC", "USDT"],
    }

    cols = st.columns(3)
    for i, (pool, tokens) in enumerate(pool_info.items()):
        with cols[i % 3]:
            st.markdown(f"**{pool}**")
            st.write(f"Tokens: {', '.join(tokens)}")
            st.write("Fee: ~4 bps")

    st.divider()

    # Configuration
    st.subheader("Configuration")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Min Depeg", f"{min_depeg_bps} bps")
    with col2:
        trade_size = strategy_config.get("default_trade_size_usd", 10000)
        st.metric("Trade Size", f"${trade_size:,.0f}")
    with col3:
        max_slippage = strategy_config.get("max_slippage_bps", 10)
        st.metric("Max Slippage", f"{max_slippage} bps")

    st.divider()

    # Performance
    st.subheader("Performance")
    _render_performance(session_state)


def _render_performance(session_state: dict[str, Any]) -> None:
    total_trades = session_state.get("total_trades", 0)
    total_profit = Decimal(str(session_state.get("total_profit_usd", "0")))

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Trades", str(total_trades))
    with col2:
        st.metric("Total Profit", f"${float(total_profit):+,.2f}")
    with col3:
        avg_profit = total_profit / total_trades if total_trades > 0 else Decimal("0")
        st.metric("Avg Profit/Trade", f"${float(avg_profit):+,.2f}")
