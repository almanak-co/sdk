"""
Flash Triangular Arbitrage Strategy Dashboard.

Custom dashboard showing triangular paths, swap legs,
flash loan details, and profitability analysis.
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
    """Render the Flash Triangular Arbitrage custom dashboard."""
    st.title("Triangular Arbitrage Dashboard")

    # Extract config
    chain = strategy_config.get("chain", "arbitrum")
    tokens = strategy_config.get("tokens", ["WETH", "USDC", "USDT", "WBTC"])
    strategy_config.get("dexs", ["uniswap_v3", "curve"])
    min_profit_bps = strategy_config.get("min_profit_bps", 10)

    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Chain:** {chain.capitalize()}")
    st.markdown(f"**Tokens:** {', '.join(tokens)}")

    st.divider()

    # Strategy state
    st.subheader("Strategy State")
    state = session_state.get("state", "scanning")
    cooldown = session_state.get("cooldown_remaining", 0)
    paths_count = session_state.get("paths_count", 0)

    col1, col2, col3 = st.columns(3)
    with col1:
        if state == "scanning":
            st.info("State: SCANNING")
        elif state == "opportunity_found":
            st.success("State: OPPORTUNITY FOUND")
        elif state == "cooldown":
            st.warning("State: COOLDOWN")
        else:
            st.info(f"State: {state}")
    with col2:
        st.metric("Cooldown", f"{cooldown}s")
    with col3:
        st.metric("Paths Evaluated", str(paths_count))

    st.divider()

    # Current opportunity
    st.subheader("Current Opportunity")
    opportunity = session_state.get("current_opportunity")

    if opportunity:
        # Path visualization
        path = opportunity.get("path", [])
        if path:
            path_str = " -> ".join(path)
            st.success(f"Path: {path_str}")

        # Profitability
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Gross Profit", f"{opportunity.get('gross_profit_bps', 0)} bps")
        with col2:
            st.metric("Net Profit", f"${float(opportunity.get('net_profit_usd', 0)):.2f}")
        with col3:
            st.metric("Price Impact", f"{opportunity.get('total_price_impact_bps', 0)} bps")

        # Flash loan
        col4, col5, col6 = st.columns(3)
        with col4:
            st.metric("Flash Token", opportunity.get("flash_loan_token", "N/A"))
        with col5:
            st.metric("Flash Amount", f"${float(opportunity.get('flash_loan_amount', 0)):,.2f}")
        with col6:
            st.metric("Flash Provider", opportunity.get("flash_loan_provider", "auto"))

        # Swap legs
        st.divider()
        st.markdown("**Swap Legs:**")
        legs = opportunity.get("legs", [])
        for i, leg in enumerate(legs):
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.write(f"**Leg {i + 1}:**")
            with col2:
                st.write(f"{leg.get('from_token')} -> {leg.get('to_token')}")
            with col3:
                st.write(f"DEX: {leg.get('dex')}")
            with col4:
                st.write(f"Impact: {leg.get('price_impact_bps', 0)} bps")
    else:
        st.info("No triangular opportunity currently found. Evaluating paths...")

    st.divider()

    # Configuration
    st.subheader("Configuration")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Min Profit", f"{min_profit_bps} bps")
    with col2:
        max_hops = strategy_config.get("max_hops", 4)
        st.metric("Max Hops", str(max_hops))
    with col3:
        max_slippage = strategy_config.get("max_total_slippage_bps", 100)
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
