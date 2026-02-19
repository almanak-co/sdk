"""
Cross-DEX Arbitrage Strategy Dashboard.

Custom dashboard showing DEX price differences, flash loan details,
arbitrage opportunities, and execution stats.
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
    """Render the Cross-DEX Arbitrage custom dashboard."""
    st.title("Cross-DEX Arbitrage Dashboard")

    # Extract config
    chain = strategy_config.get("chain", "arbitrum")
    dexs = strategy_config.get("dexs", ["uniswap_v3", "curve", "enso"])
    tokens = strategy_config.get("tokens", ["WETH", "USDC", "USDT"])
    min_profit_bps = strategy_config.get("min_profit_bps", 10)

    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Chain:** {chain.capitalize()}")
    st.markdown(f"**DEXs:** {', '.join(dexs)}")
    st.markdown(f"**Tokens:** {', '.join(tokens)}")

    st.divider()

    # Strategy state
    st.subheader("Strategy State")
    state = session_state.get("state", "scanning")
    cooldown = session_state.get("cooldown_remaining", 0)

    col1, col2 = st.columns(2)
    with col1:
        if state == "scanning":
            st.info("State: SCANNING - Looking for opportunities")
        elif state == "opportunity_found":
            st.success("State: OPPORTUNITY FOUND - Flash loan ready")
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
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Token In", opportunity.get("token_in", "N/A"))
            st.metric("Buy DEX", opportunity.get("buy_dex", "N/A"))
            st.metric("Amount In", f"${float(opportunity.get('amount_in', 0)):,.2f}")
        with col2:
            st.metric("Token Out", opportunity.get("token_out", "N/A"))
            st.metric("Sell DEX", opportunity.get("sell_dex", "N/A"))
            st.metric("Gross Profit", f"{opportunity.get('gross_profit_bps', 0)} bps")

        st.divider()

        # Flash loan details
        st.markdown("**Flash Loan Details:**")
        col3, col4, col5 = st.columns(3)
        with col3:
            st.metric("Provider", opportunity.get("flash_loan_provider", "auto"))
        with col4:
            st.metric("Flash Fee", f"${float(opportunity.get('flash_loan_fee', 0)):.4f}")
        with col5:
            st.metric("Net Profit", f"${float(opportunity.get('net_profit_usd', 0)):.2f}")

        st.success(
            f"Arbitrage: Buy {opportunity.get('token_out')} on {opportunity.get('buy_dex')}, Sell on {opportunity.get('sell_dex')}"
        )
    else:
        st.info("No opportunity currently found. Scanning DEX prices...")

    st.divider()

    # DEX prices comparison
    st.subheader("DEX Price Comparison")
    dex_prices = session_state.get("dex_prices", {})

    if dex_prices:
        for pair, prices in dex_prices.items():
            st.markdown(f"**{pair}:**")
            cols = st.columns(len(prices))
            for i, (dex, price) in enumerate(prices.items()):
                with cols[i]:
                    st.metric(dex, f"${float(price):.6f}")
    else:
        st.info("Waiting for DEX price data...")

    st.divider()

    # Configuration
    st.subheader("Configuration")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Min Profit", f"{min_profit_bps} bps")
    with col2:
        fl_provider = strategy_config.get("flash_loan_provider", "auto")
        st.metric("Flash Loan", fl_provider)
    with col3:
        trade_size = strategy_config.get("default_trade_size_usd", 10000)
        st.metric("Trade Size", f"${trade_size:,.0f}")

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
