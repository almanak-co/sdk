"""
Lending Rate Arbitrage Strategy Dashboard.

Custom dashboard showing lending rates across protocols, positions,
rebalance opportunities, and yield metrics.
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
    """Render the Lending Rate Arbitrage custom dashboard."""
    st.title("Lending Rate Arbitrage Dashboard")

    # Extract config
    chain = strategy_config.get("chain", "ethereum")
    protocols = strategy_config.get("protocols", ["aave_v3", "morpho_blue", "compound_v3"])
    tokens = strategy_config.get("tokens", ["USDC", "USDT", "DAI"])
    min_spread_bps = strategy_config.get("min_spread_bps", 50)

    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Chain:** {chain.capitalize()}")
    st.markdown(f"**Protocols:** {', '.join(protocols)}")

    st.divider()

    # Current rates
    st.subheader("Current Supply APYs")
    rates = session_state.get("rates", {})

    if rates:
        # Create rate comparison table
        for token in tokens:
            st.markdown(f"**{token}:**")
            token_rates = rates.get(token, {})
            cols = st.columns(len(protocols))
            best_rate = max(token_rates.values()) if token_rates else 0
            for i, protocol in enumerate(protocols):
                with cols[i]:
                    rate = token_rates.get(protocol, 0)
                    is_best = rate == best_rate and rate > 0
                    label = f"{protocol}"
                    if is_best:
                        st.metric(label, f"{float(rate):.2f}%", delta="BEST")
                    else:
                        st.metric(label, f"{float(rate):.2f}%")
    else:
        st.info("Loading rates from protocols...")

    st.divider()

    # Current positions
    st.subheader("Current Positions")
    positions = session_state.get("positions", {})

    if positions:
        for token, protocol_amounts in positions.items():
            st.markdown(f"**{token}:**")
            for protocol, amount in protocol_amounts.items():
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.write(f"Protocol: {protocol}")
                with col2:
                    st.write(f"Amount: ${float(amount):,.2f}")
                with col3:
                    rate = rates.get(token, {}).get(protocol, 0)
                    st.write(f"APY: {float(rate):.2f}%")
    else:
        st.info("No active positions")

    st.divider()

    # Best rebalance opportunity
    st.subheader("Best Rebalance Opportunity")
    opportunity = session_state.get("best_opportunity")

    if opportunity:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Token", opportunity.get("token", "N/A"))
            st.metric("From Protocol", opportunity.get("from_protocol", "N/A"))
        with col2:
            st.metric("From APY", f"{float(opportunity.get('from_apy', 0)):.2f}%")
            st.metric("To Protocol", opportunity.get("to_protocol", "N/A"))
        with col3:
            st.metric("To APY", f"{float(opportunity.get('to_apy', 0)):.2f}%")
            spread = opportunity.get("spread_bps", 0)
            st.metric("Spread", f"{spread} bps")

        if spread >= min_spread_bps:
            st.success(
                f"Profitable rebalance: Move {opportunity.get('token')} from {opportunity.get('from_protocol')} to {opportunity.get('to_protocol')} (+{spread} bps)"
            )
        else:
            st.warning(f"Spread {spread} bps below threshold {min_spread_bps} bps")
    else:
        st.info("No rebalance opportunity found - positions optimally allocated")

    st.divider()

    # Configuration
    st.subheader("Configuration")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Min Spread", f"{min_spread_bps} bps")
    with col2:
        threshold = strategy_config.get("rebalance_threshold_usd", 100)
        st.metric("Min Rebalance", f"${threshold:,.0f}")
    with col3:
        st.metric("Tokens", str(len(tokens)))

    st.divider()

    # Performance
    st.subheader("Performance")
    _render_performance(session_state)


def _render_performance(session_state: dict[str, Any]) -> None:
    total_rebalances = session_state.get("total_rebalances", 0)
    total_yield = Decimal(str(session_state.get("total_yield_earned", "0")))
    avg_apy = Decimal(str(session_state.get("avg_apy", "0")))

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Rebalances", str(total_rebalances))
    with col2:
        st.metric("Total Yield", f"${float(total_yield):+,.2f}")
    with col3:
        st.metric("Avg APY", f"{float(avg_apy):.2f}%")
