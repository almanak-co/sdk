"""
Cross-Chain Arbitrage Strategy Dashboard.

Custom dashboard showing cross-chain price opportunities, spread analysis,
bridge info, execution stats, and profitability metrics.
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
    """Render the Cross-Chain Arbitrage custom dashboard."""
    st.title("Cross-Chain Arbitrage Dashboard")

    # Extract config
    chains = strategy_config.get("chains", ["arbitrum", "optimism", "base"])
    min_spread_bps = strategy_config.get("min_spread_bps", 50)
    min_spread_after_fees = strategy_config.get("min_spread_after_fees_bps", 10)
    quote_token = strategy_config.get("quote_token", "WETH")
    base_token = strategy_config.get("base_token", "USDC")

    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Token Pair:** {quote_token}/{base_token}")
    st.markdown(f"**Chains:** {', '.join(chains)}")

    st.divider()

    # Strategy state
    st.subheader("Strategy State")
    state = session_state.get("state", "monitoring")
    cooldown = session_state.get("cooldown_remaining", 0)

    col1, col2 = st.columns(2)
    with col1:
        if state == "monitoring":
            st.info("State: MONITORING - Scanning for opportunities")
        elif state == "opportunity_found":
            st.success("State: OPPORTUNITY FOUND - Ready to execute")
        elif state == "cooldown":
            st.warning(f"State: COOLDOWN - {cooldown}s remaining")
        else:
            st.info(f"State: {state}")
    with col2:
        st.metric("Cooldown", f"{cooldown}s")

    st.divider()

    # Current opportunity
    st.subheader("Current Opportunity")
    opportunity = session_state.get("current_opportunity")

    if opportunity:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Buy Chain", opportunity.get("buy_chain", "N/A"))
            st.metric("Buy Price", f"${float(opportunity.get('buy_price', 0)):.4f}")
        with col2:
            st.metric("Sell Chain", opportunity.get("sell_chain", "N/A"))
            st.metric("Sell Price", f"${float(opportunity.get('sell_price', 0)):.4f}")
        with col3:
            st.metric("Raw Spread", f"{opportunity.get('raw_spread_bps', 0)} bps")
            st.metric("Net Profit", f"{opportunity.get('net_profit_bps', 0)} bps")

        # Bridge info
        st.markdown("**Bridge Info:**")
        col4, col5, col6 = st.columns(3)
        with col4:
            st.metric("Provider", opportunity.get("bridge_provider", "auto"))
        with col5:
            st.metric("Bridge Fee", f"{opportunity.get('bridge_fee_bps', 0)} bps")
        with col6:
            st.metric("Est. Profit", f"${float(opportunity.get('estimated_profit_usd', 0)):.2f}")

        st.success(f"Profitable opportunity: {opportunity.get('buy_chain')} -> {opportunity.get('sell_chain')}")
    else:
        st.info("No opportunity currently found. Monitoring cross-chain prices...")

    st.divider()

    # Chain prices
    st.subheader("Cross-Chain Prices")
    prices = session_state.get("chain_prices", {})

    if prices:
        cols = st.columns(len(chains))
        for i, chain in enumerate(chains):
            with cols[i]:
                price = prices.get(chain, 0)
                st.metric(chain.capitalize(), f"${float(price):.4f}")
    else:
        st.info("Waiting for price data...")

    st.divider()

    # Configuration
    st.subheader("Configuration")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Min Spread", f"{min_spread_bps} bps")
    with col2:
        st.metric("Min Net Profit", f"{min_spread_after_fees} bps")
    with col3:
        trade_amount = strategy_config.get("trade_amount_usd", 1000)
        st.metric("Trade Amount", f"${trade_amount:,.0f}")

    st.divider()

    # Performance
    st.subheader("Performance")
    _render_performance(session_state)


def _render_performance(session_state: dict[str, Any]) -> None:
    total_trades = session_state.get("total_trades", 0)
    total_profit = Decimal(str(session_state.get("total_profit_usd", "0")))
    failed_trades = session_state.get("failed_trades", 0)

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Trades", str(total_trades))
    with col2:
        st.metric("Total Profit", f"${float(total_profit):+,.2f}")
    with col3:
        success_rate = ((total_trades - failed_trades) / total_trades * 100) if total_trades > 0 else 0
        st.metric("Success Rate", f"{success_rate:.0f}%")
