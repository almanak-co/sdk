"""
LST Basis Trading Strategy Dashboard.

Custom dashboard showing LST prices, fair values, basis spreads,
trading opportunities, and performance metrics.
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
    """Render the LST Basis Trading custom dashboard."""
    st.title("LST Basis Trading Dashboard")

    # Extract config
    chain = strategy_config.get("chain", "ethereum")
    lst_tokens = strategy_config.get("lst_tokens", ["stETH", "rETH", "cbETH"])
    min_spread_bps = strategy_config.get("min_spread_bps", 20)

    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Chain:** {chain.capitalize()}")
    st.markdown(f"**LST Tokens:** {', '.join(lst_tokens)}")

    st.divider()

    # Strategy state
    st.subheader("Strategy State")
    state = session_state.get("state", "monitoring")
    cooldown = session_state.get("cooldown_remaining", 0)

    col1, col2 = st.columns(2)
    with col1:
        if state == "monitoring":
            st.info("State: MONITORING - Watching LST basis")
        elif state == "opportunity_found":
            st.success("State: OPPORTUNITY FOUND")
        elif state == "cooldown":
            st.warning(f"State: COOLDOWN - {cooldown}s remaining")
        else:
            st.info(f"State: {state}")
    with col2:
        st.metric("Cooldown", f"{cooldown}s")

    st.divider()

    # LST Basis comparison
    st.subheader("LST Basis Analysis")

    for token in lst_tokens:
        token_data = session_state.get(f"{token.lower()}_data", {})
        market_price = Decimal(str(token_data.get("market_price", "1.0")))
        fair_value = Decimal(str(token_data.get("fair_value", "1.0")))
        spread_bps = token_data.get("spread_bps", 0)

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.markdown(f"**{token}**")
        with col2:
            st.metric("Market", f"{float(market_price):.4f} ETH")
        with col3:
            st.metric("Fair Value", f"{float(fair_value):.4f} ETH")
        with col4:
            if spread_bps > 0:
                st.metric("Basis", f"+{spread_bps} bps", delta="PREMIUM")
            elif spread_bps < 0:
                st.metric("Basis", f"{spread_bps} bps", delta="DISCOUNT")
            else:
                st.metric("Basis", "0 bps", delta="FAIR")

        # Opportunity indicator
        if abs(spread_bps) >= min_spread_bps:
            direction = "PREMIUM (Sell)" if spread_bps > 0 else "DISCOUNT (Buy)"
            st.success(f"{token}: {direction} opportunity at {abs(spread_bps)} bps")

    st.divider()

    # Current opportunity
    st.subheader("Current Opportunity")
    opportunity = session_state.get("current_opportunity")

    if opportunity:
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Token", opportunity.get("lst_token", "N/A"))
            st.metric("Direction", opportunity.get("direction", "N/A"))
            st.metric("Spread", f"{opportunity.get('spread_bps', 0)} bps")
        with col2:
            st.metric("Trade Size", f"{float(opportunity.get('trade_amount_eth', 0)):.4f} ETH")
            st.metric("Expected Profit", f"{opportunity.get('expected_profit_bps', 0)} bps")
            st.metric("Protocol", opportunity.get("swap_protocol", "curve"))

        direction = opportunity.get("direction", "")
        if direction == "discount":
            st.success(
                f"Buy {opportunity.get('lst_token')} at discount - expected {opportunity.get('expected_profit_bps')} bps profit"
            )
        else:
            st.success(
                f"Sell {opportunity.get('lst_token')} at premium - expected {opportunity.get('expected_profit_bps')} bps profit"
            )
    else:
        st.info("No basis opportunity found. LST prices near fair value.")

    st.divider()

    # LST protocol info
    st.subheader("LST Protocol Info")
    protocol_info = {
        "stETH": ("Lido", "3.5%", "Rebasing"),
        "wstETH": ("Lido", "3.5%", "Accumulating"),
        "rETH": ("Rocket Pool", "3.2%", "Accumulating"),
        "cbETH": ("Coinbase", "3.0%", "Accumulating"),
        "frxETH": ("Frax", "4.0%", "Rebasing"),
    }

    cols = st.columns(len(lst_tokens))
    for i, token in enumerate(lst_tokens):
        info = protocol_info.get(token, ("Unknown", "0%", "Unknown"))
        with cols[i]:
            st.markdown(f"**{token}**")
            st.write(f"Protocol: {info[0]}")
            st.write(f"APY: {info[1]}")
            st.write(f"Type: {info[2]}")

    st.divider()

    # Configuration
    st.subheader("Configuration")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Min Spread", f"{min_spread_bps} bps")
    with col2:
        trade_size = strategy_config.get("default_trade_size_eth", 1)
        st.metric("Trade Size", f"{trade_size} ETH")
    with col3:
        max_slippage = strategy_config.get("max_slippage_bps", 50)
        st.metric("Max Slippage", f"{max_slippage} bps")

    st.divider()

    # Performance
    st.subheader("Performance")
    _render_performance(session_state)


def _render_performance(session_state: dict[str, Any]) -> None:
    total_trades = session_state.get("total_trades", 0)
    total_profit_eth = Decimal(str(session_state.get("total_profit_eth", "0")))
    total_profit_usd = Decimal(str(session_state.get("total_profit_usd", "0")))

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Trades", str(total_trades))
    with col2:
        st.metric("Profit (ETH)", f"{float(total_profit_eth):+.4f}")
    with col3:
        st.metric("Profit (USD)", f"${float(total_profit_usd):+,.2f}")
